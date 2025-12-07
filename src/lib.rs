use arrow::array::{Array, RecordBatch};
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use rayon::prelude::*;
use rayon::iter::ParallelBridge;
use std::fs::File;
use std::{fs, io};
use std::io::BufWriter;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug, Clone)]
pub struct ParquetRecordConfig {
    pub verbose: bool,
}

impl ParquetRecordConfig {
    pub fn with_verbose(verbose: bool) -> Self {
        Self { verbose }
    }

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
}

#[derive(Debug)]
pub struct BatchBuffer<T: ParquetRecord> {
    pub items: Vec<T>,
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

/// Statistics about the writing process.
#[derive(Debug, Default, Clone)]
pub struct WriteStats {
    pub total_items_written: usize,
    pub total_batches_written: usize,
    pub total_bytes_written: usize,
}

#[derive(Debug)]
pub struct ParquetBatchWriter<T: ParquetRecord + Clone> {
    buffer: Mutex<BatchBuffer<T>>,
    buffer_size: usize,
    file_writer: Mutex<Option<Box<ArrowWriter<BufWriter<File>>>>>, // Created lazily on first write
    schema: Arc<Schema>,
    output_file: String,
    config: ParquetRecordConfig,
    stats: Mutex<WriteStats>,
    closed: RwLock<bool>,
}

impl<T: ParquetRecord + Clone> ParquetBatchWriter<T> {
    pub fn new(output_file: String, buffer_size: Option<usize>) -> Self {
        Self::with_config(output_file, buffer_size, ParquetRecordConfig::default())
    }

    pub fn with_config(
        output_file: String,
        buffer_size: Option<usize>,
        config: ParquetRecordConfig,
    ) -> Self {
        Self {
            buffer: Mutex::new(BatchBuffer::new(buffer_size.unwrap_or(1024))),
            buffer_size: buffer_size.unwrap_or(usize::MAX),
            file_writer: Mutex::new(None),
            schema: T::schema(),
            output_file,
            config,
            stats: Mutex::new(WriteStats::default()),
            closed: RwLock::new(false),
        }
    }

    /// Adds items to the buffer. If the buffer exceeds the specified size,
    /// it will be swapped with a secondary buffer and written to disk on the same thread.
    pub fn add_items(&self, items: Vec<T>) -> Result<(), io::Error> {
        let closed_guard = self.closed.read().unwrap();
        if *closed_guard {
            return Err(io::Error::other("already closed"));
        }
        if self.buffer_size == usize::MAX {
            let mut buffer_guard = self
                .buffer
                .lock()
                .map_err(|_| io::Error::other("Buffer lock poisoned"))?;
            buffer_guard.items.extend(items);
            Ok(())
        } else {
            let mut remaining_items = items;
            while !remaining_items.is_empty() {
                let mut buffer_guard = self
                    .buffer
                    .lock()
                    .map_err(|_| io::Error::other("Buffer lock poisoned"))?;

                let available_space = self.buffer_size - buffer_guard.items.len();

                if available_space == 0 {
                    let mut secondary_buffer = BatchBuffer::new(self.buffer_size);
                    std::mem::swap(&mut *buffer_guard, &mut secondary_buffer);
                    drop(buffer_guard);

                    self.write_buffer_to_disk(secondary_buffer)?;
                    continue;
                }

                let take_count = std::cmp::min(available_space, remaining_items.len());
                let (items_to_add, remaining) = remaining_items.split_at(take_count);
                buffer_guard.items.extend_from_slice(items_to_add);
                remaining_items = remaining.to_vec();

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
        let closed_guard = self.closed.read().unwrap();
        if *closed_guard {
            return Err(io::Error::other("already closed"));
        }
        if self.buffer_size == usize::MAX {
            let mut buffer_guard = self
                .buffer
                .lock()
                .map_err(|_| io::Error::other("Buffer lock poisoned"))?;
            buffer_guard.items.push(item);
            Ok(())
        } else {
            let mut buffer_guard = self
                .buffer
                .lock()
                .map_err(|_| io::Error::other("Buffer lock poisoned"))?;

            if buffer_guard.items.len() >= self.buffer_size {
                let mut secondary_buffer = BatchBuffer::new(self.buffer_size);
                std::mem::swap(&mut *buffer_guard, &mut secondary_buffer);
                drop(buffer_guard);

                self.write_buffer_to_disk(secondary_buffer)?;

                let mut buffer_guard = self
                    .buffer
                    .lock()
                    .map_err(|_| io::Error::other("Buffer lock poisoned"))?;
                buffer_guard.items.push(item);
            } else {
                buffer_guard.items.push(item);

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

        let mut writer_guard = self
            .file_writer
            .lock()
            .map_err(|_| io::Error::other("Writer lock poisoned"))?;

        let record_batch = T::items_to_records(self.schema.clone(), &buffer.items);

        let batch_size_bytes = self.calculate_batch_size(&record_batch);

        if writer_guard.is_none() {
            let path = Path::new(&self.output_file);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = File::create(path).map_err(|e| {
                io::Error::other(format!("File creation error: {}", e))
            })?;
            let buf_writer = BufWriter::new(file);

            let writer = ArrowWriter::try_new(buf_writer, record_batch.schema(), None)
                .map_err(|e| {
                    io::Error::other(
                        format!("ArrowWriter creation error: {}", e),
                    )
                })?;
            let mut boxed_writer = Box::new(writer);

            boxed_writer.write(&record_batch).map_err(|e| {
                io::Error::other(format!("Initial write error: {}", e))
            })?;

            *writer_guard = Some(boxed_writer);
        } else if let Some(ref mut writer) = *writer_guard {
            writer.write(&record_batch).map_err(|e| {
                io::Error::other(format!("Parquet write error: {}", e))
            })?;
        }

        let mut stats_guard = self
            .stats
            .lock()
            .map_err(|_| io::Error::other("Stats lock poisoned"))?;

        stats_guard.total_items_written += buffer.items.len();
        stats_guard.total_batches_written += 1;
        stats_guard.total_bytes_written += batch_size_bytes;

        if self.config.verbose {
            let mb_written = (batch_size_bytes as f64 / (1024.0 * 1024.0)).ceil() as usize;
            let total_mb_written =
                (stats_guard.total_bytes_written as f64 / (1024.0 * 1024.0)).ceil() as usize;
            println!(
                "[ParquetWriter {}] Wrote batch of {} items ({} MB) (batch #{}) Total: {} records / {} MB",
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
            .map_err(|_| io::Error::other("Buffer lock poisoned"))?;

        if buffer_guard.items.is_empty() {
            return Ok(());
        }

        let mut secondary_buffer = BatchBuffer::new(buffer_guard.items.len());
        std::mem::swap(&mut *buffer_guard, &mut secondary_buffer);

        drop(buffer_guard);

        self.write_buffer_to_disk(secondary_buffer)
    }

    /// Gets the number of items currently in the buffer
    pub fn buffer_len(&self) -> usize {
        match self.buffer.lock() {
            Ok(guard) => guard.items.len(),
            Err(_) => 0,
        }
    }

    pub fn close(self) -> Result<(), io::Error> {
        self.close_no_consume()
    }

    /// Closes the writer and finalizes the file
    pub fn close_no_consume(&self) -> Result<(), io::Error> {
        let mut closed_guard = self.closed.write().unwrap();
        if *closed_guard {
            return Ok(())
        }
        *closed_guard = true;
        // Flush any remaining items
        self.flush()?;

        // Print final stats if verbose
        if self.config.verbose {
            let stats_guard = self
                .stats
                .lock()
                .map_err(|_| io::Error::other("Stats lock poisoned"))?;
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

        let maybe_writer = {
            let mut writer_guard = self
                .file_writer
                .lock()
                .map_err(|_| io::Error::other("Writer lock poisoned"))?;
            writer_guard.take()
        };

        if let Some(writer) = maybe_writer {
            writer.close().map_err(|e| {
                io::Error::other(format!("Writer close error: {}", e))
            })?;
        }

        Ok(())
    }

    /// Returns the current write statistics
    pub fn get_stats(&self) -> Result<WriteStats, io::Error> {
        let stats_guard = self
            .stats
            .lock()
            .map_err(|_| io::Error::other("Stats lock poisoned"))?;
        Ok(stats_guard.clone())
    }

    /// Calculate the approximate size of a record batch in bytes
    fn calculate_batch_size(&self, record_batch: &RecordBatch) -> usize {
        let mut total_size = 0;

        for column in record_batch.columns() {
            total_size += column.get_array_memory_size();
        }

        total_size
    }
}

impl<T:ParquetRecord + Clone>Drop for ParquetBatchWriter<T> {
    fn drop(&mut self) {
        self.close_no_consume().expect("Could not close ParquetBatchWriter during drop");
    }
}

/// Read parquet file with the provided configuration.
pub fn read_parquet_with_config<T>(
    _schema: Arc<Schema>,
    file_path: &str,
    batch_size: Option<usize>,
    _config: &ParquetRecordConfig,
) -> Option<(usize, impl Iterator<Item = Vec<T>>)>
where
    T: ParquetRecord,
{
    let file = match File::open(file_path) {
        Ok(f) => f,
        Err(_e) => {
            return None;
        }
    };

    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("[ParquetReader] Cannot create reader: {}", e);
            return None;
        }
    };

    let metadata = builder.metadata();
    let total_rows = metadata.file_metadata().num_rows() as usize;

    let actual_batch_size = batch_size.unwrap_or_else(|| std::cmp::max(1, total_rows));
    let reader = match builder.with_batch_size(actual_batch_size).build() {
        Ok(r) => r,
        Err(_e) => {
            eprintln!("[ParquetReader] Cannot build reader: {_e}");
            return None;
        }
    };

    let scanned_iterator = reader.scan(false, |errored, record_batch_result| {
        if *errored {
            return None;
        }

        match record_batch_result {
            Ok(record_batch) => {
                match T::records_to_items(&record_batch) {
                    Ok(vec_t) => Some(vec_t),
                    Err(e) => {
                        eprintln!("[ParquetReader] Error converting batch: {}", e);
                        *errored = true;
                        None
                    }
                }
            }
            Err(e) => {
                eprintln!("[ParquetReader] Parquet read error: {}", e);
                *errored = true;
                None
            }
        }
    });

    Some((total_rows, scanned_iterator))
}

/// Read parquet file with default configuration (verbose enabled).
pub fn read_parquet<T>(
    schema: Arc<Schema>,
    file_path: &str,
    batch_size: Option<usize>,
) -> Option<(usize, impl Iterator<Item = Vec<T>>)>
where
    T: ParquetRecord,
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
) -> Option<(usize, impl Iterator<Item = Vec<<I as ArrowPrimitiveType>::Native>>)>
where
    I: ArrowPrimitiveType,
{
    let file = match File::open(file_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[ParquetReader] Cannot open file {}: {}", file_path, e);
            return None;
        }
    };

    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("[ParquetReader] Cannot create reader: {}", e);
            return None;
        }
    };

    let metadata = builder.metadata();
    let total_rows = metadata.file_metadata().num_rows() as usize;

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
        eprintln!("[ParquetReader] Column '{}' not found in parquet file schema", column_name);
        return None;
    }

    let mask = ProjectionMask::roots(builder.parquet_schema(), column_indices);
    let builder = builder.with_projection(mask);

    let actual_batch_size = batch_size.unwrap_or_else(|| std::cmp::max(1, total_rows));
    let reader = match builder.with_batch_size(actual_batch_size).build() {
        Ok(r) => r,
        Err(_e) => {
            eprintln!("[ParquetReader] Cannot build reader: {_e}");
            return None;
        }
    };

    let col_name = column_name.to_string();
    let scanned_iterator = reader.scan(false, move |errored, record_batch_result| {
        if *errored {
            return None;
        }

        match record_batch_result {
            Ok(record_batch) => {
                let col_array = record_batch.column_by_name(&col_name)?;

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

                    Some(col_vec)
                } else {
                    *errored = true;
                    None
                }
            }
            Err(e) => {
                eprintln!("[ParquetReader] Parquet read error: {}", e);
                *errored = true;
                None
            }
        }
    });

    Some((total_rows, scanned_iterator))
}

/// Read only specified column from parquet file with default configuration (verbose enabled).
pub fn read_parquet_columns<I>(
    file_path: &str,
    column_name: &str,
    batch_size: Option<usize>,
) -> Option<(usize, impl Iterator<Item = Vec<<I as ArrowPrimitiveType>::Native>>)>
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
pub fn read_parquet_columns_with_config_par<I>(
    file_path: &str,
    column_name: &str,
    batch_size: Option<usize>,
    _config: &ParquetRecordConfig,
) -> Option<(usize, impl ParallelIterator<Item = Vec<<I as ArrowPrimitiveType>::Native>>)>
where
    I: ArrowPrimitiveType + Send + Sync,
{
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rayon::prelude::*;
    use std::fs::File;

    let file = File::open(file_path).ok()?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
    let metadata = builder.metadata();
    let total_rows = metadata.file_metadata().num_rows() as usize;
    let num_row_groups = metadata.num_row_groups();

    let file_path = file_path.to_string();
    let column_name = column_name.to_string();

    let iterator = (0..num_row_groups)
        .into_par_iter()
        .filter_map(move |row_group_idx| {
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
        });

    Some((total_rows, iterator))
}


/// Read only specified column from parquet in parallel with default configuration (verbose enabled) using Rayon.
pub fn read_parquet_columns_par<I>(
    file_path: &str,
    column_name: &str,
    batch_size: Option<usize>,
) -> Option<(usize, impl ParallelIterator<Item = Vec<<I as ArrowPrimitiveType>::Native>>)>
where
    I: ArrowPrimitiveType + Sync + Send,
{
    read_parquet_columns_with_config_par::<I>(
        file_path,
        column_name,
        batch_size,
        &ParquetRecordConfig::default(),
    )
}

/// Read parquet in parallel with the provided configuration using Rayon.
pub fn read_parquet_with_config_par<T>(
    _schema: Arc<Schema>,
    file_path: &str,
    batch_size: Option<usize>,
    _config: &ParquetRecordConfig,
) -> Option<(usize, impl ParallelIterator<Item = Vec<T>>)>
where
    T: ParquetRecord + 'static,
{
    let file = File::open(file_path).ok()?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
    let metadata = builder.metadata();
    let total_rows = metadata.file_metadata().num_rows() as usize;
    let num_row_groups = metadata.num_row_groups();

    let row_group_indices = 0..num_row_groups;

    let file_path = file_path.to_string();

    let iterator = row_group_indices
        .into_par_iter()
        .filter_map(move |row_group_idx| RowGroupReader::new(row_group_idx, &file_path, batch_size))
        .flat_map(|reader| reader);

    Some((total_rows, iterator))
}

pub struct RowGroupReader<T: ParquetRecord> {
    reader: ParquetRecordBatchReader,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: ParquetRecord> RowGroupReader<T> {
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

impl<T: ParquetRecord> ParallelIterator for RowGroupReader<T> {
    type Item = Vec<T>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>
    {
        let par_iter = self.reader
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
pub fn read_parquet_par<T>(
    schema: Arc<Schema>,
    file_path: &str,
    batch_size: Option<usize>,
) -> Option<(usize, impl ParallelIterator<Item = Vec<T>>)>
where
    T: ParquetRecord + 'static,
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
        let config = ParquetRecordConfig::with_verbose(true);
        let writer = ParquetBatchWriter::<TestRecord>::with_config(
            "test_mb_logging.parquet".to_string(),
            Some(2),
            config,
        );

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

        use std::fs;
        let _ = fs::remove_file("test_mb_logging.parquet");
    }

    #[test]
    fn test_read_parquet_columns_par() {
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

        let results = read_parquet_columns_par::<arrow::datatypes::Int32Type>(path, "id", Some(10));
        assert!(results.is_some());

        let (num_rows, iter) = results.unwrap();
        let all_ids: Vec<i32> = iter.into_par_iter().flatten().collect();
        assert_eq!(all_ids.len(), 3);
        assert_eq!(num_rows, 3);
        assert!(all_ids.contains(&1));
        assert!(all_ids.contains(&2));
        assert!(all_ids.contains(&3));

        use std::fs;
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_all_functions_comprehensive() {
        let path = "test_comprehensive.parquet";
        let test_items = vec![
            TestRecord { id: 1, value: "value1".to_string() },
            TestRecord { id: 2, value: "value2".to_string() },
            TestRecord { id: 3, value: "value3".to_string() },
            TestRecord { id: 4, value: "value4".to_string() },
            TestRecord { id: 5, value: "value5".to_string() },
        ];

        let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(2));
        writer.add_items(test_items.clone()).unwrap();
        writer.flush().unwrap();

        let writer_stats = writer.get_stats().unwrap();
        writer.close().unwrap();

        let (num_rows, read_iter) = read_parquet(TestRecord::schema(), path, Some(2))
            .unwrap();
        let read_items: Vec<TestRecord> = read_iter
            .flatten()
            .collect();
        assert_eq!(read_items.len(), 5);
        assert_eq!(num_rows, 5);
        for (i, item) in test_items.iter().enumerate() {
            assert_eq!(read_items[i].id, item.id);
            assert_eq!(read_items[i].value, item.value);
        }

        let config = ParquetRecordConfig::with_verbose(false);
        let (num_rows_config, read_iter_config) = read_parquet_with_config(TestRecord::schema(), path, Some(2), &config)
            .unwrap();
        let read_items_config: Vec<TestRecord> = read_iter_config
            .flatten()
            .collect();
        assert_eq!(read_items_config.len(), 5);
        assert_eq!(num_rows_config, 5);

        let (num_rows_col, col_iter) = read_parquet_columns::<arrow::datatypes::Int32Type>(path, "id", Some(2))
            .unwrap();
        let id_values: Vec<i32> = col_iter
            .flatten()
            .collect();
        assert_eq!(id_values.len(), 5);
        assert_eq!(num_rows_col, 5);
        assert!(id_values.contains(&1));
        assert!(id_values.contains(&2));
        assert!(id_values.contains(&3));
        assert!(id_values.contains(&4));
        assert!(id_values.contains(&5));

        let (num_rows_col_config, col_iter_config) = read_parquet_columns_with_config::<arrow::datatypes::Int32Type>(path, "id", Some(2), &config)
            .unwrap();
        let id_values_config: Vec<i32> = col_iter_config
            .flatten()
            .collect();
        assert_eq!(id_values_config.len(), 5);
        assert_eq!(num_rows_col_config, 5);

        let (num_rows_par, par_iter) = read_parquet_par(TestRecord::schema(), path, Some(2))
            .unwrap();
        let flat_par_items: Vec<TestRecord> = par_iter.flatten().collect();
        assert_eq!(flat_par_items.len(), 5);
        assert_eq!(num_rows_par, 5);

        let (num_rows_par_config, par_iter_config) = read_parquet_with_config_par(TestRecord::schema(), path, Some(2), &config)
            .unwrap();
        let flat_par_items_config: Vec<TestRecord> = par_iter_config.flatten().collect();
        assert_eq!(flat_par_items_config.len(), 5);
        assert_eq!(num_rows_par_config, 5);

        let (num_rows_col_par, col_par_iter) = read_parquet_columns_par::<arrow::datatypes::Int32Type>(path, "id", Some(2))
            .unwrap();
        let flat_par_col_values: Vec<i32> = col_par_iter.flatten().collect();
        assert_eq!(flat_par_col_values.len(), 5);
        assert_eq!(num_rows_col_par, 5);

        let (num_rows_col_par_config, col_par_iter_config) = read_parquet_columns_with_config_par::<arrow::datatypes::Int32Type>(path, "id", Some(2), &config)
            .unwrap();
        let flat_par_col_values_config: Vec<i32> = col_par_iter_config.flatten().collect();
        assert_eq!(flat_par_col_values_config.len(), 5);
        assert_eq!(num_rows_col_par_config, 5);

        assert_eq!(writer_stats.total_items_written, 5);
        assert!(writer_stats.total_bytes_written > 0);

        use std::fs;
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_buffer_operations() {
        let path = "test_buffer.parquet";

        let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(2));

        for i in 1..=5 {
            writer.add_item(TestRecord {
                id: i,
                value: format!("item{}", i),
            }).unwrap();
        }

        let more_items = vec![
            TestRecord { id: 6, value: "item6".to_string() },
            TestRecord { id: 7, value: "item7".to_string() },
        ];
        writer.add_items(more_items).unwrap();

        let buffer_len_before_close = writer.buffer_len();
        writer.close().unwrap();

        let (num_rows, iter) = read_parquet(TestRecord::schema(), path, Some(3))
            .unwrap();
        let read_items: Vec<TestRecord> = iter
            .flatten()
            .collect();
        assert_eq!(read_items.len(), 7);
        assert_eq!(num_rows, 7);

        assert_eq!(buffer_len_before_close, 1);

        use std::fs;
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_edge_cases() {
        let path = "test_edge.parquet";

        let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(5));
        writer.close().unwrap();

        let empty_reader = read_parquet::<TestRecord>(TestRecord::schema(), path, Some(2));
        assert!(empty_reader.is_none());

        let single_path = "test_single.parquet";
        let single_writer = ParquetBatchWriter::<TestRecord>::new(single_path.to_string(), Some(5));
        single_writer.add_item(TestRecord { id: 42, value: "single".to_string() }).unwrap();
        single_writer.close().unwrap();

        let (single_num_rows, single_iter): (usize, _) = read_parquet::<TestRecord>(TestRecord::schema(), single_path, Some(2))
            .unwrap();
        let single_items: Vec<TestRecord> = single_iter
            .flatten()
            .collect();
        assert_eq!(single_items.len(), 1);
        assert_eq!(single_num_rows, 1);
        assert_eq!(single_items[0].id, 42);
        assert_eq!(single_items[0].value, "single");

        use std::fs;
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(single_path);
    }

    #[test]
    fn test_write_stats() {
        let path = "test_stats.parquet";

        let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(3));

        let items = vec![
            TestRecord { id: 1, value: "one".to_string() },
            TestRecord { id: 2, value: "two".to_string() },
            TestRecord { id: 3, value: "three".to_string() },
            TestRecord { id: 4, value: "four".to_string() },
        ];

        writer.add_items(items).unwrap();
        writer.flush().unwrap();
        let stats = writer.get_stats().unwrap();
        writer.close().unwrap();

        assert_eq!(stats.total_items_written, 4);
        assert!(stats.total_bytes_written > 0);
        assert!(stats.total_batches_written >= 1);

        use std::fs;
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_flush_operation() {
        let path = "test_flush.parquet";

        let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(10));

        for i in 1..=3 {
            writer.add_item(TestRecord { id: i, value: format!("item{}", i) }).unwrap();
        }

        assert_eq!(writer.buffer_len(), 3);

        writer.flush().unwrap();
        assert_eq!(writer.buffer_len(), 0);

        writer.close().unwrap();

        let (num_rows, iter) = read_parquet(TestRecord::schema(), path, Some(2))
            .unwrap();
        let items: Vec<TestRecord> = iter
            .flatten()
            .collect();
        assert_eq!(items.len(), 3);
        assert_eq!(num_rows, 3);

        use std::fs;
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_verbose_logging_functionality() {
        let path = "test_verbose.parquet";

        let config = ParquetRecordConfig::with_verbose(true);
        let writer = ParquetBatchWriter::<TestRecord>::with_config(
            path.to_string(),
            Some(2),
            config
        );

        let items = vec![
            TestRecord { id: 1, value: "verbose1".to_string() },
            TestRecord { id: 2, value: "verbose2".to_string() },
        ];

        writer.add_items(items).unwrap();
        writer.close().unwrap();

        let (num_rows, iter) = read_parquet(TestRecord::schema(), path, None)
            .unwrap();
        let read_items: Vec<TestRecord> = iter
            .flatten()
            .collect();
        assert_eq!(read_items.len(), 2);
        assert_eq!(num_rows, 2);

        use std::fs;
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_read_parquet_sequential() {
        let path = "test_read_seq.parquet";

        let items = vec![
            TestRecord { id: 10, value: "first".to_string() },
            TestRecord { id: 20, value: "second".to_string() },
            TestRecord { id: 30, value: "third".to_string() },
            TestRecord { id: 40, value: "fourth".to_string() },
        ];

        let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(2));
        writer.add_items(items.clone()).unwrap();
        writer.close().unwrap();

        for batch_size in [1, 2, 3, 4, 10] {
            let (num_rows, iter) = read_parquet(TestRecord::schema(), path, Some(batch_size))
                .unwrap();
            let read_items: Vec<TestRecord> = iter
                .flatten()
                .collect();

            assert_eq!(read_items.len(), 4);
            assert_eq!(num_rows, 4);

            for (i, original_item) in items.iter().enumerate() {
                assert_eq!(read_items[i].id, original_item.id);
                assert_eq!(read_items[i].value, original_item.value);
            }
        }

        let (num_rows_default, iter_default) = read_parquet(TestRecord::schema(), path, None)
            .unwrap();
        let read_items_default: Vec<TestRecord> = iter_default
            .flatten()
            .collect();
        assert_eq!(read_items_default.len(), 4);
        assert_eq!(num_rows_default, 4);

        let config = ParquetRecordConfig::with_verbose(false);
        let (num_rows_config, iter_config) = read_parquet_with_config(TestRecord::schema(), path, Some(2), &config)
            .unwrap();
        let read_items_config: Vec<TestRecord> = iter_config
            .flatten()
            .collect();
        assert_eq!(read_items_config.len(), 4);
        assert_eq!(num_rows_config, 4);

        use std::fs;
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_read_parquet_parallel() {
        let path = "test_read_par.parquet";

        let items: Vec<TestRecord> = (1..=20).map(|i| TestRecord {
            id: i,
            value: format!("item{}", i),
        }).collect();

        let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(5));
        writer.add_items(items.clone()).unwrap();
        writer.close().unwrap();

        let (par_num_rows, par_iter) = read_parquet_par(TestRecord::schema(), path, Some(3))
            .unwrap();

        let all_items: Vec<TestRecord> = par_iter.flatten().collect();
        assert_eq!(all_items.len(), 20);
        assert_eq!(par_num_rows, 20);

        let mut all_items_sorted = all_items;
        all_items_sorted.sort_by(|a, b| a.id.cmp(&b.id));
        let mut items_sorted = items.clone();
        items_sorted.sort_by(|a, b| a.id.cmp(&b.id));

        for i in 0..20 {
            assert_eq!(all_items_sorted[i].id, items_sorted[i].id);
            assert_eq!(all_items_sorted[i].value, items_sorted[i].value);
        }

        let config = ParquetRecordConfig::with_verbose(false);
        let (par_num_rows_config, par_iter_config) = read_parquet_with_config_par::<TestRecord>(TestRecord::schema(), path, Some(3), &config)
            .unwrap();
        let total_items_config: usize = par_iter_config.flatten().count();
        assert_eq!(total_items_config, 20);
        assert_eq!(par_num_rows_config, 20);

        use std::fs;
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_read_parquet_edge_cases() {
        let empty_path = "test_read_empty.parquet";

        let empty_writer = ParquetBatchWriter::<TestRecord>::new(empty_path.to_string(), Some(5));
        empty_writer.close().unwrap();

        let empty_result = read_parquet::<TestRecord>(TestRecord::schema(), empty_path, Some(2));
        assert!(empty_result.is_none());

        let empty_par_result = read_parquet_par::<TestRecord>(TestRecord::schema(), empty_path, Some(2));
        assert!(empty_par_result.is_none());

        let single_path = "test_read_single.parquet";
        let single_writer = ParquetBatchWriter::<TestRecord>::new(single_path.to_string(), Some(5));
        single_writer.add_item(TestRecord { id: 100, value: "single_item".to_string() }).unwrap();
        single_writer.close().unwrap();

        let (single_num_rows, single_iter) = read_parquet::<TestRecord>(TestRecord::schema(), single_path, Some(2))
            .unwrap();
        let single_items: Vec<TestRecord> = single_iter
            .flatten()
            .collect();
        assert_eq!(single_items.len(), 1);
        assert_eq!(single_num_rows, 1);
        assert_eq!(single_items[0].id, 100);
        assert_eq!(single_items[0].value, "single_item");

        let (single_par_num_rows, single_par_iter) = read_parquet_par::<TestRecord>(TestRecord::schema(), single_path, Some(2))
            .unwrap();
        let total_single_items: usize = single_par_iter.flatten().count();
        assert_eq!(total_single_items, 1);
        assert_eq!(single_par_num_rows, 1);

        use std::fs;
        let _ = fs::remove_file(empty_path);
        let _ = fs::remove_file(single_path);
    }
}
