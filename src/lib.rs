use arrow::array::{as_primitive_array, Array, RecordBatch};
use arrow::datatypes::Schema;
use crossbeam_channel::{bounded, Receiver, Sender};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{io, thread};

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
pub trait ParquetRecord {
    /// Returns the schema for this record type.
    fn schema() -> Arc<Schema>;
    
    /// Converts a vector of records to a RecordBatch.
    fn dump_record_batch(schema: Arc<Schema>, objects: &[Self]) -> RecordBatch
    where
        Self: Sized;
        
    /// Converts a RecordBatch back to a vector of records.
    fn from_record_batch(record_batch: &RecordBatch) -> io::Result<Vec<Self>> where Self: Sized;

    /// Returns the name of the ID column in the schema.
    /// This is used by read_id_batches to efficiently read only the ID column.
    fn id_column_name() -> &'static str;
}

fn write_batch<T>(
    out_path: &str,
    schema: Arc<Schema>,
    batch: &mut Vec<T>,
    total_written_count: &mut usize,
    writer: &mut ArrowWriter<BufWriter<File>>,
    config: &ParquetRecordConfig,
) -> io::Result<()>
where
    T: ParquetRecord,
{
    if batch.is_empty() {
        return Ok(());
    }
    let record_batch = T::dump_record_batch(schema, batch);

    // Get the in-memory size of the batch for logging purposes.
    let batch_size_bytes = record_batch.get_array_memory_size();
    let batch_size_mb = batch_size_bytes as f64 / (1024.0 * 1024.0);

    writer
        .write(&record_batch)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Parquet write error: {}", e)))?;
    *total_written_count += batch.len();
    
    if config.verbose {
        println!(
            "{} FLUSHED BATCH of {} elements (~{:.2} MB in memory) to Parquet. Total written: {}",
            out_path,
            batch.len(),
            batch_size_mb,
            *total_written_count
        );
    }
    
    batch.clear();
    Ok(())
}

/// Start a writer with the provided configuration.
pub fn start_writer_with_config<T>(
    out_path: String,
    records_buffer: usize,
    channel_buffer: usize,
    config: ParquetRecordConfig,
) -> (Sender<Vec<T>>, JoinHandle<io::Result<()>>)
where
    T: ParquetRecord + Send + 'static,
{
    let (s, r): (Sender<Vec<T>>, Receiver<Vec<T>>) = bounded(channel_buffer); // Channel sends Vecs, capacity 10

    let handle = thread::spawn(move || {
        let file = File::create(out_path.as_str())?;
        let buf_writer = BufWriter::new(file);
        let schema = T::schema();
        let mut writer = ArrowWriter::try_new(buf_writer, schema.clone(), None).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to create ArrowWriter: {}", e),
            )
        })?;
        let mut buffer: Vec<T> = Vec::with_capacity(records_buffer);
        let mut total_written_count: usize = 0;
        loop {
            match r.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(mut received_vec) => {
                    // received_vec is a Vec<T>
                    buffer.append(&mut received_vec); // Efficiently add elements

                    if buffer.len() >= records_buffer {
                        if let Err(e) = write_batch(
                            out_path.as_str(),
                            schema.clone(),
                            &mut buffer,
                            &mut total_written_count,
                            &mut writer,
                            &config,
                        ) {
                            eprintln!("[Writer] File I/O Error: {}", e);
                            return Err(e);
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    std::thread::yield_now();
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    if config.verbose {
                        println!("[Writer] Channel disconnected. Writing remaining buffer...");
                    }

                    // Write any remaining elements in the buffer
                    if !buffer.is_empty() {
                        if let Err(e) = write_batch(
                            out_path.as_str(),
                            schema.clone(),
                            &mut buffer,
                            &mut total_written_count,
                            &mut writer,
                            &config,
                        ) {
                            eprintln!("[Writer] Final File I/O Error: {}", e);
                            return Err(e);
                        }
                    }

                    // CRITICAL: Close the Parquet writer to write file metadata
                    writer.close().map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("Failed to close Parquet writer: {}", e),
                        )
                    })?;

                    if config.verbose {
                        println!(
                            "[Writer] Finished writing. Total elements written: {}",
                            total_written_count
                        );
                    }
                    return Ok(()); // Exit the consumer thread
                }
            }
        }
    });

    (s, handle)
}

/// Start a writer with default configuration (verbose enabled).
pub fn start_writer<T>(
    out_path: String,
    records_buffer: usize,
    channel_buffer: usize,
) -> (Sender<Vec<T>>, JoinHandle<io::Result<()>>)
where
    T: ParquetRecord + Send + 'static,
{
    start_writer_with_config(out_path, records_buffer, channel_buffer, ParquetRecordConfig::default())
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
