use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use rayon::iter::ParallelIterator;
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{io, thread};

pub trait ParquetRecord {
    fn schema() -> Arc<Schema>;
    fn dump_record_batch(schema: Arc<Schema>, objects: &Vec<Self>) -> RecordBatch
    where
        Self: Sized;
    fn from_record_batch(record_batch: &RecordBatch) -> io::Result<Vec<Self>> where Self: Sized;
}

fn write_batch<T>(
    out_path: &str,
    schema: Arc<Schema>,
    batch: &mut Vec<T>,
    total_written_count: &mut usize,
    writer: &mut ArrowWriter<BufWriter<File>>,
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
    println!(
        "{} FLUSHED BATCH of {} elements (~{:.2} MB in memory) to Parquet. Total written: {}",
        out_path,
        batch.len(),
        batch_size_mb,
        *total_written_count
    );
    batch.clear();
    Ok(())
}

pub fn start_writer<T>(
    out_path: String,
    records_buffer: usize,
    channel_buffer: usize,
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
                        ) {
                            eprintln!("[Writer] File I/O Error: {}", e);
                            return Err(e);
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if !buffer.is_empty() {
                        if let Err(e) = write_batch(
                            out_path.as_str(),
                            schema.clone(),
                            &mut buffer,
                            &mut total_written_count,
                            &mut writer,
                        ) {
                            eprintln!("[Writer] File I/O Error on timeout write: {}", e);
                            return Err(e);
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    println!("[Writer] Channel disconnected. Writing remaining buffer...");

                    // Write any remaining elements in the buffer
                    if !buffer.is_empty() {
                        if let Err(e) = write_batch(
                            out_path.as_str(),
                            schema.clone(),
                            &mut buffer,
                            &mut total_written_count,
                            &mut writer,
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

                    println!(
                        "[Writer] Finished writing. Total elements written: {}",
                        total_written_count
                    );
                    return Ok(()); // Exit the consumer thread
                }
                _ => {}
            }
        }
    });

    (s, handle)
}

fn read_batches<T>(
    schema: Arc<Schema>,
    file_path: &str,
    batch_size: usize,
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
