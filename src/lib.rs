use arrow::array::{as_primitive_array, Array, RecordBatch};
use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use std::fs::File;
use std::io::BufWriter;
use std::sync::{Arc, Mutex, MutexGuard};
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
/// The file and writers are created lazily on first use, only when there's actual data to write.

#[derive(Debug)]
pub struct ParquetBatchWriter<T: ParquetRecord> {
    buffer: Mutex<BatchBuffer<T>>,
    buffer_size: usize,
    file_writer: Mutex<Option<ArrowWriter<BufWriter<File>>>>, // Writer is created lazily on first write
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
        Self {
            buffer: Mutex::new(BatchBuffer::new(buffer_size)),
            buffer_size,
            file_writer: Mutex::new(None), // Initially None - no file operations yet
            schema: T::schema(),
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

        self.check_for_buffer_overflow(buffer_guard)
    }

    fn check_for_buffer_overflow(&self, mut buffer_guard: MutexGuard<BatchBuffer<T>>) -> Result<(), io::Error> {
        if buffer_guard.items.len() >= self.buffer_size {
            // Swap with secondary buffer
            let mut secondary_buffer = BatchBuffer::new(buffer_guard.items.len());
            std::mem::swap(&mut *buffer_guard, &mut secondary_buffer);

            // Unlock primary buffer immediately
            drop(buffer_guard);

            // The actual file operations happen here - only when we know there's data to write
            self.write_buffer_to_disk(secondary_buffer)?;
        }
        Ok(())
    }

    pub fn add_item(&self, item: T) -> Result<(), io::Error> {
        let mut buffer_guard = self.buffer.lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;

        buffer_guard.items.push(item);

        self.check_for_buffer_overflow(buffer_guard)
    }

    /// Write the buffer to disk (this happens on the same thread as the caller)
    /// This function also handles lazy initialization of file and writer on first use
    fn write_buffer_to_disk(&self, buffer: BatchBuffer<T>) -> Result<(), io::Error> {
        if buffer.items.is_empty() {
            return Ok(());
        }

        // This is the first function that knows there's actually data to write,
        // so it's where we do all the lazy initialization
        let mut writer_guard = self.file_writer.lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Writer lock poisoned"))?;

        // Lazy initialization: create file, buf_writer, and arrow_writer only when data arrives
        if writer_guard.is_none() {
            // Now we know we need to create the file - do all the expensive operations here
            let file = File::create(&self.output_file)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("File creation error: {}", e)))?;
            let buf_writer = BufWriter::new(file);  // Create BufWriter
            
            // Convert the buffer to a record batch to get the initial schema/data
            let record_batch = T::dump_record_batch(self.schema.clone(), &buffer.items);
            
            // Create the ArrowWriter with the file and schema
            let mut writer = ArrowWriter::try_new(buf_writer, record_batch.schema(), None)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("ArrowWriter creation error: {}", e)))?;
            
            // Write the first batch immediately
            writer.write(&record_batch)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Initial write error: {}", e)))?;
            
            // Store the writer for future use
            *writer_guard = Some(writer);
        } else if let Some(ref mut writer) = *writer_guard {
            // For subsequent writes, just write the data
            let record_batch = T::dump_record_batch(self.schema.clone(), &buffer.items);
            writer.write(&record_batch)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Parquet write error: {}", e)))?;
        }

        Ok(())
    }

    /// Flushes the current buffer to disk
    pub fn flush(&self) -> Result<(), io::Error> {
        let mut buffer_guard = self.buffer.lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;

        if buffer_guard.items.is_empty() {
            return Ok(());
        }

        let mut secondary_buffer = BatchBuffer::new(buffer_guard.items.len());
        std::mem::swap(&mut *buffer_guard, &mut secondary_buffer);

        // Unlock the primary buffer immediately so other threads can continue
        drop(buffer_guard);

        // Write the secondary buffer to disk
        self.write_buffer_to_disk(secondary_buffer)
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
        // Flush any remaining items
        self.flush()?;

        // Close the writer if it was created
        let maybe_writer = {
            let mut writer_guard = self.file_writer.lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Writer lock poisoned"))?;
            writer_guard.take() // This removes the writer from the option
        };

        if let Some(mut writer) = maybe_writer {
            writer.close()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Writer close error: {}", e)))?;
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

/// Read only specified column batches with the provided configuration.
/// This function efficiently reads only the specified column from parquet files, which is faster
/// because only the specified column needs to be scanned.
pub fn read_col_batches_with_config<I>(
    file_path: &str,
    column_name: &str,
    batch_size: usize,
    _config: &ParquetRecordConfig,
) -> Option<impl Iterator<Item = Vec<<I as ArrowPrimitiveType>::Native>>>
where
    I: ArrowPrimitiveType,
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

    // 2. Create the ParquetRecordBatchReader with column projection to read only the specified column
    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("[ParquetRecordColReader] Cannot create ParquetRecordBatchReaderBuilder: {}", e);
            return None;
        }
    };

    // Project to read only the specified column
    let column_indices: Vec<usize> = builder
        .parquet_schema()
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, col)| {
            if col.name() == column_name {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    if column_indices.is_empty() {
        eprintln!("[ParquetRecordColReader] Column '{}' not found in parquet file schema", column_name);
        return None;
    }

    let mask = ProjectionMask::roots(builder.parquet_schema(), column_indices);
    let builder = builder.with_projection(mask);

    // Set the batch size and build the reader
    let reader = match builder.with_batch_size(batch_size).build() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[ParquetRecordColReader] Cannot build ParquetRecordBatchReader: {}", e);
            return None;
        }
    };

    let col_name = column_name.to_string();
    let scanned_iterator = reader.scan(false, move |errored, record_batch_result| {
        // 1. Check for previous error
        if *errored {
            return None;
        }

        match record_batch_result {
            // Case A: Successful Arrow RecordBatch read
            Ok(record_batch) => {
                // Extract the specified column from the record batch
                let col_array = record_batch.column_by_name(&col_name)?;
                
                // Attempt to cast to the expected primitive array type
                if let Some(values) = col_array.as_any().downcast_ref::<arrow::array::PrimitiveArray<I>>() {
                    let mut col_vec = Vec::with_capacity(values.len());
                    for i in 0..values.len() {
                        if !values.is_null(i) {
                            col_vec.push(values.value(i));
                        }
                    }

                    // Return the vector of column values
                    Some(col_vec)
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
                    "[ParquetColReader] Parquet read error, stopping iteration: {}",
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

/// Read only specified column batches with default configuration (verbose enabled).
pub fn read_col_batches<I>(
    file_path: &str,
    column_name: &str,
    batch_size: usize,
) -> Option<impl Iterator<Item = Vec<<I as ArrowPrimitiveType>::Native>>>
where
    I: ArrowPrimitiveType,
{
    read_col_batches_with_config::<I>(file_path, column_name, batch_size, &ParquetRecordConfig::default())
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
        let writer = ParquetBatchWriter::<TestRecord>::new("test_output.parquet".to_string(), 2);
        
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

        // Add items to writer (this triggers file creation)
        writer.add_item_batch(test_items).unwrap();
        
        // Close the writer to finalize the file
        writer.close().unwrap();

        // Clean up test file
        use std::fs;
        let _ = fs::remove_file("test_output.parquet");
    }
}