use arrow::array::{as_primitive_array, Array, RecordBatch};
use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use std::fs::File;
use std::io::BufWriter;
use std::sync::{Arc, LockResult, Mutex, MutexGuard};
use std::{io};
use arrow::datatypes::{ArrowPrimitiveType, DataType};

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

/// A buffer of items that can be processed
#[derive(Debug)]
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

/// A batch writer that maintains a primary buffer protected by a mutex.
/// When the buffer is full, it's swapped with a secondary buffer and written to disk 
/// on the same calling thread, while the primary buffer remains available for other threads.

#[derive(Debug)]
pub struct ParquetBatchWriter<T: ParquetRecord> {
    buffer: Mutex<BatchBuffer<T>>,
    buffer_size: usize,
    file_writer: Mutex<ArrowWriter<BufWriter<File>>>, // Writer is created when first batch is processed
    schema: Arc<Schema>, // Store the schema to avoid calling T::schema() repeatedly
    output_file: String,
    _config: ParquetRecordConfig,
}

impl<T: ParquetRecord + 'static> ParquetBatchWriter<T> {
    /// Creates a new batch writer for a specific output file
    pub fn new(output_file: String, buffer_size: usize) -> Self {
        Self::with_config(output_file, buffer_size, ParquetRecordConfig::default())
    }

    pub fn with_config(output_file: String, buffer_size: usize, config: ParquetRecordConfig) -> Self {
        let buffer = Mutex::new(BatchBuffer::new(buffer_size));
        let schema = T::schema();
        let file = File::create(&output_file).unwrap_or_else(|e| panic!("could not create file {}: {}", output_file, e));
        let buf_writer = BufWriter::new(file);

        let file_writer = Mutex::new(ArrowWriter::try_new(buf_writer, schema.clone(), None).unwrap());
        if config.verbose {
            println!("Created parquet batch writer for {}", output_file);
        }
        Self {
            buffer,
            buffer_size,
            file_writer,
            schema,
            output_file,
            _config: config,
        }
    }

    /// Adds a batch of items to the buffer. If the buffer exceeds the specified size,
    /// it will be swapped with a secondary buffer and written to disk on the same thread.
    pub fn add_item_batch(&self, items: Vec<T>) -> Result<(), io::Error> {
        let mut buffer_guard = self.buffer.lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;

        buffer_guard.items.extend(items);

        // Check if buffer needs to be flushed to disk
        if buffer_guard.items.len() >= self.buffer_size {
            let writer_guard = self.file_writer.lock();
            let mut secondary_buffer = BatchBuffer::new(buffer_guard.items.len());
            std::mem::swap(&mut *buffer_guard, &mut secondary_buffer);

            // Unlock the primary buffer immediately so other threads can continue
            drop(buffer_guard);

            // Write the secondary buffer to disk on the same thread
            self.write_buffer_to_disk(writer_guard, secondary_buffer)?;
        }

        Ok(())
    }

    /// Write the buffer to disk (this happens on the same thread as the caller)
    fn write_buffer_to_disk(&self, writer_guard:  LockResult<MutexGuard<ArrowWriter<BufWriter<File>>>>, buffer: BatchBuffer<T>) -> Result<(), io::Error> {
        if buffer.items.is_empty() {
            return Ok(());
        }
            // For subsequent batches, convert and write them
            let record_batch = T::dump_record_batch(self.schema.clone(), &buffer.items);
        if self._config.verbose {
            println!("{} Flushed {} records ~{} MB", self.output_file, record_batch.num_rows(), record_batch.get_array_memory_size() / 1024 / 1024);
        }
        writer_guard.unwrap().write(&record_batch)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Parquet write error: {}", e)))?;


        Ok(())
    }

    /// Flushes the current buffer to disk
    pub fn flush(&self) -> Result<(), io::Error> {
        let mut buffer_guard = self.buffer.lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;

        if buffer_guard.items.is_empty() {
            return Ok(());
        }
        let writer_guard = self.file_writer.lock();
        let mut secondary_buffer = BatchBuffer::new(buffer_guard.items.len());
        std::mem::swap(&mut *buffer_guard, &mut secondary_buffer);

        // Unlock the primary buffer immediately so other threads can continue
        drop(buffer_guard);

        // Write the secondary buffer to disk on the same thread
        self.write_buffer_to_disk(writer_guard, secondary_buffer)
    }

    /// Gets the number of items currently in the buffer
    pub fn buffer_len(&self) -> usize {
        match self.buffer.lock() {
            Ok(guard) => guard.items.len(),
            Err(_) => 0,
        }
    }

    /// Closes the writer and finalizes the file
    pub fn close(self) -> Result<(), io::Error> {
        if self._config.verbose {
            println!("{} flushing remaining items", self.output_file);
        }
        self.flush()?;

        let x = self.file_writer.into_inner().unwrap();
        x.close().expect("Could not close parquet writer");
        if self._config.verbose {
            println!("{} closed writer", self.output_file);
        }
        Ok(())
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
pub fn read_id_batches_with_config<T, I>(
    _schema: Arc<Schema>,
    file_path: &str,
    batch_size: usize,
    _config: &ParquetRecordConfig,
) -> Option<impl Iterator<Item = Vec<<I as ArrowPrimitiveType>::Native>>>
where
    T: ParquetRecord + Send + 'static,
    I: ArrowPrimitiveType
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
                
                // Attempt to cast to the expected primitive array type
                if let Some(id_values) = id_array.as_any().downcast_ref::<arrow::array::PrimitiveArray<I>>() {
                    let mut id_vec = Vec::with_capacity(id_values.len());
                    for i in 0..id_values.len() {
                        if !id_values.is_null(i) {
                            id_vec.push(id_values.value(i));
                        }
                    }

                    // Return the vector of ID values
                    Some(id_vec)
                } else {
                    // Type mismatch - could not cast to expected type
                    *errored = true;
                    None
                }
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
pub fn read_id_batches<T, I>(
    schema: Arc<Schema>,
    file_path: &str,
    batch_size: usize,
) -> Option<impl Iterator<Item = Vec<<I as ArrowPrimitiveType>::Native>>>
where
    T: ParquetRecord + Send + 'static,
    I: ArrowPrimitiveType,
{
    read_id_batches_with_config::<T, I>(schema, file_path, batch_size, &ParquetRecordConfig::default())
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

    #[test]
    fn test_parquet_batch_writer() {
        let mut writer = ParquetBatchWriter::<TestRecord>::new("test_output.parquet".to_string(), 2);
        
        // Create test items
        let test_items = vec![
            TestRecord {
                id: 1,
                value: "test1".to_string(),
            },
            TestRecord {
                id: 2,
                value: "test2".to_string(),
            },
        ];

        // Add items to writer
        writer.add_item_batch(test_items).unwrap();
        
        // Close the writer to finalize the file
        writer.close().unwrap();

        // Clean up test file
        use std::fs;
        let _ = fs::remove_file("test_output.parquet");
    }
}