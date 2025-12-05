use arrow::array::{Array, RecordBatch};
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use rayon::prelude::*;
use rayon::iter::ParallelBridge; // ⬅️ The fix for E0412
use rayon::iter::FilterMap;
use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

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

    /// Converts a slice of items to a RecordBatch.
    fn items_to_records(schema: Arc<Schema>, items: &[Self]) -> RecordBatch
    where
        Self: Sized;

    /// Converts a RecordBatch back to a vector of items.
    fn records_to_items(record_batch: &RecordBatch) -> io::Result<Vec<Self>>
    where
        Self: Sized;

    /// Returns the name of the ID column in the schema.
    /// This is used by read_id_batches to efficiently read only the ID column.
    fn id_column_name() -> &'static str;
}

/// A buffer of items that can be processed
#[derive(Debug)]
pub struct BatchBuffer<T: ParquetRecord> {
    pub items: Vec<T>, // Unconverted items
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

/// Statistics about the writing process.
#[derive(Debug, Default, Clone)]
pub struct WriteStats {
    pub total_items_written: usize,
    pub total_batches_written: usize,
    pub total_bytes_written: usize,
}

#[derive(Debug)]
pub struct ParquetBatchWriter<T: ParquetRecord> {
    buffer: Mutex<BatchBuffer<T>>,
    buffer_size: usize,
    file_writer: Mutex<Option<ArrowWriter<BufWriter<File>>>>, // Writer is created lazily on first write
    schema: Arc<Schema>, // Store the schema to avoid calling T::schema() repeatedly
    output_file: String,
    config: ParquetRecordConfig,
    stats: Mutex<WriteStats>,
}

impl<T: ParquetRecord + Clone + 'static> ParquetBatchWriter<T> {
    /// Creates a new batch writer for a specific output file
    pub fn new(output_file: String, buffer_size: Option<usize>) -> Self {
        Self::with_config(output_file, buffer_size, ParquetRecordConfig::default())
    }

    pub fn with_config(
        output_file: String,
        buffer_size: Option<usize>,
        config: ParquetRecordConfig,
    ) -> Self {
        Self {
            buffer: Mutex::new(BatchBuffer::new(buffer_size.unwrap_or(1024))), // Default to 1024 if None
            buffer_size: buffer_size.unwrap_or(usize::MAX), // Use usize::MAX when no buffer size is provided
            file_writer: Mutex::new(None), // Initially None - no file operations yet
            schema: T::schema(),
            output_file,
            config,
            stats: Mutex::new(WriteStats::default()),
        }
    }

    /// Adds items to the buffer. If the buffer exceeds the specified size,
    /// it will be swapped with a secondary buffer and written to disk on the same thread.
    pub fn add_items(&self, items: Vec<T>) -> Result<(), io::Error> {
        if self.buffer_size == usize::MAX {
            // No buffer size limit - just add to buffer
            let mut buffer_guard = self
                .buffer
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;
            buffer_guard.items.extend(items);
            Ok(())
        } else {
            // Process items in chunks of buffer_size to ensure exact batch sizes
            let mut remaining_items = items;
            while !remaining_items.is_empty() {
                let mut buffer_guard = self
                    .buffer
                    .lock()
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;

                let available_space = self.buffer_size - buffer_guard.items.len();

                if available_space == 0 {
                    // Buffer is full, write it out
                    let mut secondary_buffer = BatchBuffer::new(self.buffer_size);
                    std::mem::swap(&mut *buffer_guard, &mut secondary_buffer);
                    drop(buffer_guard);

                    self.write_buffer_to_disk(secondary_buffer)?;
                    continue; // Check again with empty buffer
                }

                let take_count = std::cmp::min(available_space, remaining_items.len());
                let (items_to_add, remaining) = remaining_items.split_at(take_count);
                buffer_guard.items.extend_from_slice(items_to_add);
                remaining_items = remaining.to_vec();

                // If buffer is now full, write it out
                if buffer_guard.items.len() >= self.buffer_size {
                    let mut secondary_buffer = BatchBuffer::new(self.buffer_size);
                    std::mem::swap(&mut *buffer_guard, &mut secondary_buffer);
                    drop(buffer_guard);

                    self.write_buffer_to_disk(secondary_buffer)?;
                }
            }
            Ok(())
        }
    }

    pub fn add_item(&self, item: T) -> Result<(), io::Error> {
        if self.buffer_size == usize::MAX {
            // No buffer size limit - just add to buffer
            let mut buffer_guard = self
                .buffer
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;
            buffer_guard.items.push(item);
            Ok(())
        } else {
            // Check if buffer has space, if not, write it out first
            let mut buffer_guard = self
                .buffer
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;

            if buffer_guard.items.len() >= self.buffer_size {
                // Buffer is full, write it out
                let mut secondary_buffer = BatchBuffer::new(self.buffer_size);
                std::mem::swap(&mut *buffer_guard, &mut secondary_buffer);
                drop(buffer_guard);

                self.write_buffer_to_disk(secondary_buffer)?;

                // Now add the item to the newly available buffer
                let mut buffer_guard = self
                    .buffer
                    .lock()
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;
                buffer_guard.items.push(item);
            } else {
                buffer_guard.items.push(item);

                // Check if buffer is now full after adding the item
                if buffer_guard.items.len() >= self.buffer_size {
                    let mut secondary_buffer = BatchBuffer::new(self.buffer_size);
                    std::mem::swap(&mut *buffer_guard, &mut secondary_buffer);
                    drop(buffer_guard);

                    self.write_buffer_to_disk(secondary_buffer)?;
                }
            }
            Ok(())
        }
    }

    /// Write the buffer to disk (this happens on the same thread as the caller)
    /// This function also handles lazy initialization of file and writer on first use
    fn write_buffer_to_disk(&self, buffer: BatchBuffer<T>) -> Result<(), io::Error> {
        if buffer.items.is_empty() {
            return Ok(());
        }

        // This is the first function that knows there's actually data to write,
        // so it's where we do all the lazy initialization
        let mut writer_guard = self
            .file_writer
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Writer lock poisoned"))?;

        // Convert the buffer to a record batch to get the initial schema/data
        let record_batch = T::items_to_records(self.schema.clone(), &buffer.items);

        // Get the size of the record batch before writing for logging purposes
        let batch_size_bytes = self.calculate_batch_size(&record_batch);

        // Lazy initialization: create file, buf_writer, and arrow_writer only when data arrives
        if writer_guard.is_none() {
            // Now we know we need to create the file - do all the expensive operations here
            let file = File::create(&self.output_file).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("File creation error: {}", e))
            })?;
            let buf_writer = BufWriter::new(file); // Create BufWriter

            // Create the ArrowWriter with the file and schema
            let mut writer = ArrowWriter::try_new(buf_writer, record_batch.schema(), None)
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("ArrowWriter creation error: {}", e),
                    )
                })?;

            // Write the first batch immediately
            writer.write(&record_batch).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Initial write error: {}", e))
            })?;

            // Store the writer for future use
            *writer_guard = Some(writer);
        } else if let Some(ref mut writer) = *writer_guard {
            // For subsequent writes, just write the data
            writer.write(&record_batch).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Parquet write error: {}", e))
            })?;
        }

        // Update and print statistics if verbose
        let mut stats_guard = self
            .stats
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Stats lock poisoned"))?;

        stats_guard.total_items_written += buffer.items.len();
        stats_guard.total_batches_written += 1;
        stats_guard.total_bytes_written += batch_size_bytes;

        if self.config.verbose {
            let mb_written = (batch_size_bytes as f64 / (1024.0 * 1024.0)).ceil() as usize;
            let total_mb_written =
                (stats_guard.total_bytes_written as f64 / (1024.0 * 1024.0)).ceil() as usize;
            println!(
                "[ParquetWriter {}] Wrote batch of {} items ({} MB) (batch #{}/{}) Total: {} MB",
                self.output_file,
                buffer.items.len(),
                mb_written,
                stats_guard.total_batches_written,
                stats_guard.total_items_written,
                total_mb_written
            );
        }

        Ok(())
    }

    /// Flushes the current buffer to disk
    pub fn flush(&self) -> Result<(), io::Error> {
        let mut buffer_guard = self
            .buffer
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer lock poisoned"))?;

        if buffer_guard.items.is_empty() {
            return Ok(());
        }

        // Only write if we have items
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

        // Print final stats if verbose
        if self.config.verbose {
            let stats_guard = self
                .stats
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Stats lock poisoned"))?;
            let total_mb_written =
                (stats_guard.total_bytes_written as f64 / (1024.0 * 1024.0)).ceil() as usize;
            println!(
                "[ParquetWriter {}] Final stats - Total items: {}, Total batches: {}, Total MB: {}",
                self.output_file,
                stats_guard.total_items_written,
                stats_guard.total_batches_written,
                total_mb_written
            );
        }

        // Close the writer if it was created
        let maybe_writer = {
            let mut writer_guard = self
                .file_writer
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Writer lock poisoned"))?;
            writer_guard.take() // This removes the writer from the option
        };

        if let Some(writer) = maybe_writer {
            writer.close().map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Writer close error: {}", e))
            })?;
        }

        Ok(())
    }

    /// Returns the current write statistics
    pub fn get_stats(&self) -> Result<WriteStats, io::Error> {
        let stats_guard = self
            .stats
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Stats lock poisoned"))?;
        Ok(stats_guard.clone())
    }

    /// Calculate the approximate size of a record batch in bytes
    fn calculate_batch_size(&self, record_batch: &RecordBatch) -> usize {
        let mut total_size = 0;

        for column in record_batch.columns() {
            // Use Arrow's built-in method to get the physical size of the array
            total_size += column.get_array_memory_size();
        }

        total_size
    }
}

/// Read parquet file with the provided configuration.
pub fn read_parquet_with_config<T>(
    _schema: Arc<Schema>,
    file_path: &str,
    batch_size: Option<usize>,
    _config: &ParquetRecordConfig,
) -> Option<impl Iterator<Item = Vec<T>>>
where
    T: ParquetRecord + Send + 'static,
{
    // 1. Open the file
    let file = match File::open(file_path) {
        Ok(f) => f,
        // If file open fails, we return an iterator that yields one error and stops.
        Err(_e) => {
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

    // Set the batch size to the provided value, or use the default from the reader if not specified
    let actual_batch_size = batch_size.unwrap_or_else(|| {
        let metadata = builder.metadata();
        std::cmp::max(1, metadata.file_metadata().num_rows() as usize) // Use at least 1 as batch size
    });
    let reader = match builder.with_batch_size(actual_batch_size).build() {
        Ok(r) => r,
        Err(_e) => {
            eprintln!("[ParquetRecordBatchReaderBuilder] Cannot build ParquetRecordBatchReaderBuilder: {_e}");
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
                match T::records_to_items(&record_batch) {
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

/// Read parquet file with default configuration (verbose enabled).
pub fn read_parquet<T>(
    schema: Arc<Schema>,
    file_path: &str,
    batch_size: Option<usize>,
) -> Option<impl Iterator<Item = Vec<T>>>
where
    T: ParquetRecord + Send + 'static,
{
    read_parquet_with_config(
        schema,
        file_path,
        batch_size,
        &ParquetRecordConfig::default(),
    )
}

/// Read only specified column from parquet file with the provided configuration.
/// This function efficiently reads only the specified column from parquet files, which is faster
/// because only the specified column needs to be scanned.
pub fn read_parquet_columns_with_config<I>(
    file_path: &str,
    column_name: &str,
    batch_size: Option<usize>,
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
            eprintln!(
                "[ParquetRecordColReader] Cannot create ParquetRecordBatchReaderBuilder: {}",
                e
            );
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
        eprintln!(
            "[ParquetRecordColReader] Column '{}' not found in parquet file schema",
            column_name
        );
        return None;
    }

    let mask = ProjectionMask::roots(builder.parquet_schema(), column_indices);
    let builder = builder.with_projection(mask);

    // Set the batch size and build the reader
    let actual_batch_size = batch_size.unwrap_or_else(|| {
        let metadata = builder.metadata();
        std::cmp::max(1, metadata.file_metadata().num_rows() as usize) // Use at least 1 as batch size
    });
    let reader = match builder.with_batch_size(actual_batch_size).build() {
        Ok(r) => r,
        Err(_e) => {
            eprintln!("[ParquetRecordColReader] Cannot build ParquetRecordBatchReader: {_e}");
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
                if let Some(values) = col_array
                    .as_any()
                    .downcast_ref::<arrow::array::PrimitiveArray<I>>()
                {
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

/// Read only specified column from parquet file with default configuration (verbose enabled).
pub fn read_parquet_columns<I>(
    file_path: &str,
    column_name: &str,
    batch_size: Option<usize>,
) -> Option<impl Iterator<Item = Vec<<I as ArrowPrimitiveType>::Native>>>
where
    I: ArrowPrimitiveType,
{
    read_parquet_columns_with_config::<I>(
        file_path,
        column_name,
        batch_size,
        &ParquetRecordConfig::default(),
    )
}

/// Read only specified column from parquet in parallel with the provided configuration using Rayon.
/// Each row group is read in a separate thread with its own file handle, providing true concurrent I/O.
/// Returns a parallel iterator that the caller can continue to operate on.
pub fn read_parquet_columns_with_config_par<I>(
    file_path: &str,
    column_name: &str,
    batch_size: Option<usize>,
    _config: &ParquetRecordConfig,
) -> Option<impl ParallelIterator<Item = Vec<<I as ArrowPrimitiveType>::Native>> + Send>
where
    I: ArrowPrimitiveType + Send + Sync + 'static,
{
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rayon::prelude::*;
    use std::fs::File;

    // Open file to get metadata
    let file = File::open(file_path).ok()?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
    let metadata = builder.metadata();
    let num_row_groups = metadata.num_row_groups();

    let file_path = file_path.to_string();
    let column_name = column_name.to_string();

    Some(
        (0..num_row_groups)
            .into_par_iter()
            .filter_map(move |row_group_idx| {
                // Open file for this row group
                let file = File::open(&file_path).ok()?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
                let actual_batch_size = batch_size.unwrap_or_else(|| {
                    std::cmp::max(1, builder.metadata().file_metadata().num_rows() as usize)
                });

                let reader = builder
                    .with_batch_size(actual_batch_size)
                    .with_row_groups(vec![row_group_idx])
                    .build()
                    .ok()?;

                let mut all_column_values = Vec::new();

                for batch_result in reader {
                    let batch = batch_result.ok()?;
                    let col_array = batch.column_by_name(&column_name)?;
                    let values = col_array
                        .as_any()
                        .downcast_ref::<arrow::array::PrimitiveArray<I>>()?;

                    for i in 0..values.len() {
                        if !values.is_null(i) {
                            all_column_values.push(values.value(i));
                        }
                    }
                }

                if all_column_values.is_empty() {
                    None
                } else {
                    Some(all_column_values)
                }
            }),
    )
}


/// Read only specified column from parquet in parallel with default configuration (verbose enabled) using Rayon.
/// Returns a parallel iterator that the caller can continue to operate on.
pub fn read_parquet_columns_par<I>(
    file_path: &str,
    column_name: &str,
    batch_size: Option<usize>,
) -> Option<impl ParallelIterator<Item = Vec<<I as ArrowPrimitiveType>::Native>> + Send>
where
    I: ParquetRecord + Send + 'static + arrow::array::ArrowPrimitiveType,
{
    read_parquet_columns_with_config_par::<I>(
        file_path,
        column_name,
        batch_size,
        &ParquetRecordConfig::default(),
    )
}

/// Read parquet in parallel with the provided configuration using Rayon.
/// Each row group is read in a separate thread with its own file handle, providing true concurrent I/O.
/// Returns a vector of results that can be converted to a parallel iterator.
pub fn read_parquet_with_config_par<T>(
    _schema: Arc<Schema>,
    file_path: &str,
    batch_size: Option<usize>,
    _config: &ParquetRecordConfig,
) -> Option<impl ParallelIterator<Item = Vec<T>>>
where
    T: ParquetRecord + Send + 'static,
{
    let file = File::open(file_path).ok()?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
    let metadata = builder.metadata();
    let num_row_groups = metadata.num_row_groups();

    let row_group_indices = 0..num_row_groups;

    let file_path = file_path.to_string();

    Some(
        row_group_indices
            .into_par_iter()
            // Step 1: try to create a reader; skip failed ones
            .filter_map(move |row_group_idx| RowGroupReader::new(row_group_idx, &file_path, batch_size))
            // Step 2: flatten each reader into its batches
            .flat_map(|reader| reader)  // reader: RowGroupReader<T> implements ParallelIterator<Item=Vec<T>>
    )
}

pub struct RowGroupReader<T: ParquetRecord + Send + 'static> {
    reader: ParquetRecordBatchReader,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: ParquetRecord + Send + 'static> RowGroupReader<T> {
    pub fn new(row_group_idx: usize, file_path: &str, batch_size: Option<usize>) -> Option<Self> {
        let file = File::open(file_path).ok()?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;

        let actual_batch_size = batch_size.unwrap_or_else(|| {
            std::cmp::max(1, builder.metadata().file_metadata().num_rows() as usize)
        });

        let reader = builder
            .with_batch_size(actual_batch_size)
            .with_row_groups(vec![row_group_idx])
            .build()
            .ok()?;

        Some(Self {
            reader,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<T: ParquetRecord + Send + 'static> ParallelIterator for RowGroupReader<T> {
    type Item = Vec<T>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>
    {
        // Create the parallel iterator we want to delegate to
        let par_iter = self.reader
            .into_iter()
            .par_bridge()
            .filter_map(process_batch_result::<T>);

        par_iter.drive_unindexed(consumer)
    }
}

fn process_batch_result<T: ParquetRecord>(
    batch: Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>
) -> Option<Vec<T>>
{
    match batch {
        Ok(batch) => T::records_to_items(&batch).ok(),
        Err(_) => None,
    }
}


/// Read parquet in parallel with default configuration (verbose enabled) using Rayon.
/// Returns a vector of results that can be converted to a parallel iterator.
pub fn read_parquet_par<T>(
    schema: Arc<Schema>,
    file_path: &str,
    batch_size: Option<usize>,
) -> Option<impl ParallelIterator<Item = Vec<T>>>
where
    T: ParquetRecord + Send + 'static,
{
    read_parquet_with_config_par(
        schema,
        file_path,
        batch_size,
        &ParquetRecordConfig::default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    // Simple test record for testing
    #[derive(Debug, Clone)]
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

        fn items_to_records(schema: Arc<Schema>, items: &[Self]) -> RecordBatch {
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

        fn records_to_items(record_batch: &RecordBatch) -> io::Result<Vec<Self>> {
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
        let writer =
            ParquetBatchWriter::<TestRecord>::new("test_output.parquet".to_string(), Some(2));

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
        writer.add_items(test_items).unwrap();

        // Close the writer to finalize the file
        writer.close().unwrap();

        // Clean up test file
        use std::fs;
        let _ = fs::remove_file("test_output.parquet");
    }

    #[test]
    fn test_mb_logging() {
        // Test that MB logging works properly by creating a writer with verbose config
        let config = ParquetRecordConfig::with_verbose(true);
        let writer = ParquetBatchWriter::<TestRecord>::with_config(
            "test_mb_logging.parquet".to_string(),
            Some(2),
            config,
        );

        // Add some test items to trigger verbose logging
        let test_items = vec![
            TestRecord {
                id: 1,
                value: "test1_with_some_additional_data_to_increase_size".to_string(),
            },
            TestRecord {
                id: 2,
                value: "test2_with_some_additional_data_to_increase_size".to_string(),
            },
            TestRecord {
                id: 3,
                value: "test3_with_some_additional_data_to_increase_size".to_string(),
            },
        ];

        writer.add_items(test_items).unwrap();
        writer.close().unwrap();

        // Clean up test file
        use std::fs;
        let _ = fs::remove_file("test_mb_logging.parquet");
    }

    #[test]
    fn test_read_parquet_columns_par() {
        // Create a dummy parquet file
        let path = "test_read_columns_par.parquet";
        let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(10));
        let items = vec![
            TestRecord {
                id: 1,
                value: "a".to_string(),
            },
            TestRecord {
                id: 2,
                value: "b".to_string(),
            },
            TestRecord {
                id: 3,
                value: "c".to_string(),
            },
        ];
        writer.add_items(items).unwrap();
        writer.close().unwrap();

        // Read only the "id" column in parallel
        let results = read_parquet_columns_par::<arrow::datatypes::Int32Type>(path, "id", Some(10));
        assert!(results.is_some());

        let all_ids: Vec<i32> = results.unwrap().into_iter().flatten().collect();
        assert_eq!(all_ids.len(), 3);
        assert!(all_ids.contains(&1));
        assert!(all_ids.contains(&2));
        assert!(all_ids.contains(&3));

        // Clean up
        use std::fs;
        fs::remove_file(path).unwrap();
    }
}
