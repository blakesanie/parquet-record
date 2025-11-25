use arrow::array::{as_primitive_array, Array, RecordBatch};
use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use std::fs::File;
use std::io::BufWriter;
use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
use std::{io};
use std::sync::mpsc;
use std::thread;
use tokio::sync::mpsc as async_mpsc;
use tokio::task;


/// Configuration for parquet record operations.
#[derive(Debug, Clone)]
pub struct ParquetRecordConfig {
    /// Whether to show verbose logging output.
    pub verbose: bool,
}

impl ParquetRecordConfig {
    /// Creates a new configuration with verbose logging enabled.
    pub fn with_verbose(verbose: bool) -> Self {
        Self { verbose }
    }

    /// Creates a new configuration with verbose logging disabled.
    pub fn silent() -> Self {
        Self { verbose: false }
    }
}

impl Default for ParquetRecordConfig {
    fn default() -> Self {
        Self { verbose: true }
    }
}

/// A trait for types that can be serialized to and from Parquet format.
///
/// Implementors must provide schema definition and conversion methods to and from RecordBatch.
pub trait ParquetRecord: Send + Sync {
    /// Returns the schema for this record type.
    fn schema() -> Arc<Schema>;

    /// Converts a vector of items to a RecordBatch.
    fn dump_record_batch(schema: Arc<Schema>, items: &[Self]) -> RecordBatch
    where
        Self: Sized;

    /// Converts a RecordBatch back to a vector of items.
    fn from_record_batch(record_batch: &RecordBatch) -> io::Result<Vec<Self>> where Self: Sized;

    /// Returns the name of the ID column in the schema.
    /// This is used by read_id_batches to efficiently read only the ID column.
    fn id_column_name() -> &'static str;
}

/// A structure to hold a record batch along with its metadata and file path
#[derive(Debug)]
pub struct RecordBatchWithMetadata {
    pub record_batch: RecordBatch,
    pub batch_size_bytes: usize,
    pub file_path: String,  // File path to write to
}

/// Message to send to the batch write manager
pub enum ManagerMessage {
    RecordBatch(RecordBatchWithMetadata),
    Shutdown(tokio::sync::oneshot::Sender<()>),
}

/// Manager for parquet writing operations that facilitates writes to multiple parquet files
pub struct BatchWriteManager {
    sender: async_mpsc::UnboundedSender<ManagerMessage>,
    pending_bytes: Arc<AtomicUsize>,
    max_pending_bytes: usize,
}

/// A handle to manage the batch write manager
pub struct BatchWriteManagerHandle {
    handle: Option<task::JoinHandle<io::Result<()>>>,
}

impl BatchWriteManagerHandle {
    /// Waits for the batch write manager to complete
    pub async fn join(mut self) -> io::Result<()> {
        if let Some(handle) = self.handle.take() {
            handle.await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Join error: {:?}", e)))?
        } else {
            Ok(())
        }
    }
}

impl BatchWriteManager {
    /// Creates a new batch write manager
    pub fn new(config: ParquetRecordConfig, max_pending_bytes: usize) -> io::Result<(Self, BatchWriteManagerHandle)> {
        let (sender, mut receiver) = async_mpsc::unbounded_channel::<ManagerMessage>();
        let pending_bytes = Arc::new(AtomicUsize::new(0));
        let pending_bytes_clone = pending_bytes.clone();

        let handle = tokio::spawn(async move {
            use std::collections::HashMap;

            // Track active writers for different files
            let mut file_writers: HashMap<String, (ArrowWriter<BufWriter<std::fs::File>>, Arc<Schema>)> = HashMap::new();

            while let Some(message) = receiver.recv().await {
                match message {
                    ManagerMessage::RecordBatch(batch_with_metadata) => {
                        let file_path = batch_with_metadata.file_path.clone();

                        // Get or create writer for this file
                        // The first write to a file determines the schema
                        if !file_writers.contains_key(&file_path) {
                            // Create new file writer for this path
                            let file = File::create(&file_path)
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("File creation error: {}", e)))?;
                            let buf_writer = BufWriter::new(file);

                            // Create writer using the schema from the batch
                            let writer = ArrowWriter::try_new(buf_writer, batch_with_metadata.record_batch.schema(), None)
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("ArrowWriter creation error: {}", e)))?;

                            file_writers.insert(file_path.clone(), (writer, batch_with_metadata.record_batch.schema()));
                        }

                        if let Some((writer, _)) = file_writers.get_mut(&file_path) {
                            writer
                                .write(&batch_with_metadata.record_batch)
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Parquet write error: {}", e)))?;

                            // Update pending bytes after writing
                            let new_pending = pending_bytes_clone.load(Ordering::Relaxed).saturating_sub(batch_with_metadata.batch_size_bytes);
                            pending_bytes_clone.store(new_pending, Ordering::Relaxed);

                            if config.verbose {
                                let batch_size_mb = batch_with_metadata.batch_size_bytes as f64 / (1024.0 * 1024.0);
                                println!(
                                    "{} FLUSHED BATCH (~{:.2} MB in memory) to Parquet",
                                    file_path,
                                    batch_size_mb,
                                );
                            }
                        }
                    },
                    ManagerMessage::Shutdown(response_sender) => {
                        // Close all writers
                        for (file_path, (writer, _)) in file_writers.drain() {
                            if let Err(e) = writer.close() {
                                eprintln!("Error closing writer for {}: {}", file_path, e);
                            }
                        }
                        let _ = response_sender.send(());
                        break;
                    }
                }
            }
            
            if config.verbose {
                println!("[BatchWriteManager] Finished writing to all files");
            }
            
            Ok(())
        });
        
        let manager_handle = BatchWriteManagerHandle {
            handle: Some(handle),
        };
        
        let manager = Self { 
            sender, 
            pending_bytes,
            max_pending_bytes,
        };
        
        Ok((manager, manager_handle))
    }
    
    /// Sends a record batch to be written to the specified file
    /// This method will block if there are too many pending writes
    /// Sends a record batch to be written to its designated file
    /// This method will block if there are too many pending writes
    pub fn send_record_batch(&self, batch_with_metadata: RecordBatchWithMetadata) -> Result<(), io::Error> {
        let buffer_size = batch_with_metadata.batch_size_bytes;

        // Wait until there's enough space for the buffer
        loop {
            let current_pending = self.pending_bytes.load(Ordering::Relaxed);
            if current_pending + buffer_size <= self.max_pending_bytes {
                // There's enough space, add to pending and send
                self.pending_bytes.fetch_add(buffer_size, Ordering::Relaxed);
                break;
            }

            // Brief pause to wait for more space to free up
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        self.sender.send(ManagerMessage::RecordBatch(batch_with_metadata))
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to send record batch to batch write manager"))
    }

    /// Sends a record batch to be written to the specified file
    /// This method will block if there are too many pending writes
    pub fn send_record_batch_to_file(&self, batch_with_metadata: RecordBatchWithMetadata) -> Result<(), io::Error> {
        let buffer_size = batch_with_metadata.batch_size_bytes;

        // Wait until there's enough space for the buffer
        loop {
            let current_pending = self.pending_bytes.load(Ordering::Relaxed);
            if current_pending + buffer_size <= self.max_pending_bytes {
                // There's enough space, add to pending and send
                self.pending_bytes.fetch_add(buffer_size, Ordering::Relaxed);
                break;
            }

            // Brief pause to wait for more space to free up
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        self.sender.send(ManagerMessage::RecordBatch(batch_with_metadata))
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to send record batch to batch write manager"))
    }

    /// Shuts down the manager
    pub fn shutdown(&self) -> Result<(), io::Error> {
        let (response_tx, _response_rx) = tokio::sync::oneshot::channel();

        self.sender.send(ManagerMessage::Shutdown(response_tx))
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to send shutdown message"))?;

        // We don't wait here since it would block - the shutdown happens asynchronously
        Ok(())
    }
}

/// A buffer of items that can be sent to the batch write manager
pub struct BatchBuffer<T: ParquetRecord> {
    pub items: Vec<T>,      // Unconverted items
}

impl<T: ParquetRecord> BatchBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            items: Vec::with_capacity(capacity),
        }
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }
}

/// A batch writer that maintains a buffer under a mutex and sends it to the batch write manager when full
pub struct ParquetBatchWriter<T: ParquetRecord> {
    buffer: Arc<Mutex<BatchBuffer<T>>>,
    manager: Arc<BatchWriteManager>,
    output_file: String,  // The file this batch writer writes to
    buffer_size: usize,
}

impl<T: ParquetRecord + 'static> ParquetBatchWriter<T> {
    /// Creates a new batch writer for a specific output file
    pub fn new(manager: Arc<BatchWriteManager>, output_file: String, buffer_size: usize) -> Self {
        let buffer = Arc::new(Mutex::new(BatchBuffer::new(buffer_size)));

        Self {
            buffer,
            manager,
            output_file,
            buffer_size,
        }
    }

    /// Adds a batch of items to the buffer. If the buffer exceeds the specified size,
    /// it will be transferred to the batch write manager.
    pub fn add_item_batch(&self, items: Vec<T>) -> Result<(), io::Error> {
        let mut buffer_guard = self.buffer.lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;

        buffer_guard.items.extend(items);

        // Check if buffer needs to be flushed
        if buffer_guard.items.len() >= self.buffer_size {
            let mut full_buffer = BatchBuffer::new(buffer_guard.items.len());
            std::mem::swap(&mut *buffer_guard, &mut full_buffer);

            // Unlock before converting and sending to manager to avoid blocking
            drop(buffer_guard);

            // Convert to RecordBatch before sending to manager
            self.send_buffer_to_manager(full_buffer)?;
        }

        Ok(())
    }

    /// Flushes the current buffer to the batch write manager
    pub fn flush(&self) -> Result<(), io::Error> {
        let mut buffer_guard = self.buffer.lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;

        if buffer_guard.items.is_empty() {
            return Ok(());
        }

        let mut full_buffer = BatchBuffer::new(buffer_guard.items.len());
        std::mem::swap(&mut *buffer_guard, &mut full_buffer);

        // Unlock before converting and sending to manager to avoid blocking
        drop(buffer_guard);

        self.send_buffer_to_manager(full_buffer)
    }

    /// Converts buffer to RecordBatch and sends to manager
    fn send_buffer_to_manager(&self, buffer: BatchBuffer<T>) -> Result<(), io::Error> {
        if buffer.items.is_empty() {
            return Ok(());
        }

        // Convert items to RecordBatch (this is the expensive operation that happens right before sending to manager)
        let schema = T::schema();
        let record_batch = T::dump_record_batch(schema, &buffer.items);

        // Calculate batch size for tracking
        let batch_size_bytes = record_batch.get_array_memory_size();

        let batch_with_metadata = RecordBatchWithMetadata {
            record_batch,
            batch_size_bytes,
            file_path: self.output_file.clone(),
        };

        self.manager.send_record_batch_to_file(batch_with_metadata)?;

        Ok(())
    }

    /// Gets the number of items currently in the buffer
    pub fn buffer_len(&self) -> usize {
        match self.buffer.lock() {
            Ok(guard) => guard.items.len(),
            Err(_) => 0,
        }
    }
}



/// Read batches with the provided configuration.
pub fn read_batches_with_config<T>(
    _schema: Arc<Schema>,
    file_path: &str,
    batch_size: usize,
    _config: &ParquetRecordConfig,
) -> Option<impl Iterator<Item = Vec<T>>>
where
    T: ParquetRecord + Send + 'static,
{
    // 1. Open the file
    let file = match File::open(file_path) {
        Ok(f) => f,
        // If file open fails, we return an iterator that yields one error and stops.
        Err(e) => {
            eprintln!("[ParquetRecord] Cannot open file {}: {}", file_path, e);
            return None;
        }
    };

    // 2. Create the ParquetRecordBatchReader
    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("[ParquetRecordBatchReaderBuilder] Cannot create ParquetRecordBatchReaderBuilder: {}", e);
            return None;
        }
    };

    // Set the batch size to something reasonable, or infer it from the reader settings.
    let reader = match builder.with_batch_size(batch_size).build() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[ParquetRecordBatchReaderBuilder] Cannot build ParquetRecordBatchReaderBuilder: {}", e);
            return None;
        }
    };

    let scanned_iterator = reader.scan(false, |errored, record_batch_result| {
        // 1. Check for previous error
        if *errored {
            return None;
        }

        match record_batch_result {
            // Case A: Successful Arrow RecordBatch read
            Ok(record_batch) => {
                match T::from_record_batch(&record_batch) {
                    // ⭐ SUCCESS: Wrap the result in Some()
                    Ok(vec_t) => Some(vec_t),
                    Err(e) => {
                        // Conversion error: Log, set state to stop, and return None.
                        eprintln!("[ParquetReader] Error converting batch: {}", e);
                        *errored = true;
                        // ⭐ STOP: Return None
                        None
                    }
                }
            }
            // Case B: Parquet I/O read error
            Err(e) => {
                // I/O error: Log, set state to stop, and return None.
                eprintln!(
                    "[ParquetReader] Parquet read error, stopping iteration: {}",
                    e
                );
                *errored = true;
                // ⭐ STOP: Return None
                None
            }
        }
    });

    Some(scanned_iterator)
}

/// Read batches with default configuration (verbose enabled).
pub fn read_batches<T>(
    schema: Arc<Schema>,
    file_path: &str,
    batch_size: usize,
) -> Option<impl Iterator<Item = Vec<T>>>
where
    T: ParquetRecord + Send + 'static,
{
    read_batches_with_config(schema, file_path, batch_size, &ParquetRecordConfig::default())
}

/// Read only ID batches with the provided configuration.
/// This function efficiently reads only the ID column from parquet files, which is faster
/// because only the ID column needs to be scanned.
pub fn read_id_batches_with_config<T>(
    _schema: Arc<Schema>,
    file_path: &str,
    batch_size: usize,
    _config: &ParquetRecordConfig,
) -> Option<impl Iterator<Item = Vec<i32>>>
where
    T: ParquetRecord + Send + 'static,
{
    // 1. Open the file
    let file = match File::open(file_path) {
        Ok(f) => f,
        // If file open fails, we return an iterator that yields one error and stops.
        Err(e) => {
            eprintln!("[ParquetRecord] Cannot open file {}: {}", file_path, e);
            return None;
        }
    };

    // 2. Create the ParquetRecordBatchReader with column projection to read only the ID column
    let id_column_name = T::id_column_name();
    
    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("[ParquetRecordIDReader] Cannot create ParquetRecordBatchReaderBuilder: {}", e);
            return None;
        }
    };

    // Project to read only the ID column
    let column_indices: Vec<usize> = builder
        .parquet_schema()
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, col)| {
            if col.name() == id_column_name {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    if column_indices.is_empty() {
        eprintln!("[ParquetRecordIDReader] ID column '{}' not found in parquet file schema", id_column_name);
        return None;
    }

    let mask = ProjectionMask::roots(builder.parquet_schema(), column_indices);
    let builder = builder.with_projection(mask);

    // Set the batch size and build the reader
    let reader = match builder.with_batch_size(batch_size).build() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[ParquetRecordIDReader] Cannot build ParquetRecordBatchReader: {}", e);
            return None;
        }
    };

    let id_column_name = T::id_column_name().to_string();
    let scanned_iterator = reader.scan(false, move |errored, record_batch_result| {
        // 1. Check for previous error
        if *errored {
            return None;
        }

        match record_batch_result {
            // Case A: Successful Arrow RecordBatch read
            Ok(record_batch) => {
                // Extract the ID column from the record batch
                let id_array = record_batch.column_by_name(&id_column_name)?;
                let id_values = as_primitive_array::<arrow::datatypes::Int32Type>(id_array);
                
                let mut id_vec = Vec::with_capacity(id_values.len());
                for i in 0..id_values.len() {
                    if id_values.is_null(i) {
                        // Handle null values - in this case we'll skip them
                        continue;
                    } else {
                        id_vec.push(id_values.value(i));
                    }
                }

                // Return the vector of ID values
                Some(id_vec)
            }
            // Case B: Parquet I/O read error
            Err(e) => {
                // I/O error: Log, set state to stop, and return None.
                eprintln!(
                    "[ParquetIDReader] Parquet read error, stopping iteration: {}",
                    e
                );
                *errored = true;
                // ⭐ STOP: Return None
                None
            }
        }
    });

    Some(scanned_iterator)
}

/// Read only ID batches with default configuration (verbose enabled).
pub fn read_id_batches<T>(
    schema: Arc<Schema>,
    file_path: &str,
    batch_size: usize,
) -> Option<impl Iterator<Item = Vec<i32>>>
where
    T: ParquetRecord + Send + 'static,
{
    read_id_batches_with_config::<T>(schema, file_path, batch_size, &ParquetRecordConfig::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::datatypes::{DataType, Field, Schema};

    // Simple test record for testing
    #[derive(Debug)]
    struct TestRecord {
        id: i32,
        value: String,
    }

    impl ParquetRecord for TestRecord {
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
            ]))
        }

        fn dump_record_batch(schema: Arc<Schema>, items: &[Self]) -> RecordBatch {
            use arrow::array::{Int32Array, StringArray};
            use std::sync::Arc;

            let ids: Vec<i32> = items.iter().map(|item| item.id).collect();
            let values: Vec<&str> = items.iter().map(|item| item.value.as_str()).collect();

            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(StringArray::from(values)),
                ],
            )
                .unwrap()
        }

        fn from_record_batch(record_batch: &RecordBatch) -> io::Result<Vec<Self>> {
            use arrow::array::{as_primitive_array, StringArray};

            let ids = as_primitive_array::<arrow::datatypes::Int32Type>(record_batch.column(0));
            let values = record_batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let mut result = Vec::new();
            for i in 0..record_batch.num_rows() {
                result.push(TestRecord {
                    id: ids.value(i),
                    value: values.value(i).to_string(),
                });
            }

            Ok(result)
        }

        fn id_column_name() -> &'static str {
            "id"
        }
    }
}