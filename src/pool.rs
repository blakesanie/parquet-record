use arrow::array::{Array, RecordBatch};
use arrow::datatypes::Schema;
use crossbeam_channel::{Receiver, Select, Sender};
use dashmap::DashMap;
use parquet::arrow::ArrowWriter;
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufWriter;
use std::sync::{Arc, RwLock};
use std::thread;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;
use arrow::error::ArrowError;

struct Message {
    record_batch: RecordBatch,
    bytes: usize,
}

impl Message {
    fn new(record_batch: RecordBatch) -> Self {
        let mut total_size = 0;
        for column in record_batch.columns() {
            total_size += column.get_array_memory_size();
        }
        Self {
            record_batch,
            bytes: total_size,
        }
    }
}

struct WriteThread<'a> {
    receivers: Arc<RwLock<VecDeque<Receiver<Option<Message>>>>>,
    // writers: Arc<DashMap<String, ArrowWriter<BufWriter<File>>>>,
    pool: &'a WritePool,
    handle: JoinHandle<()>,
}

struct Writer {
    receiver: Receiver<Option<Message>>,

}

impl<'a> WriteThread<'a> {
    fn spawn(file_path: &str, schema: Arc<Schema>, pool: &'a WritePool) -> Result<(Self, Sender<Option<Message>>), ArrowError> {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let mut deque = VecDeque::new();
        deque.push_back(receiver);
        let receivers = Arc::new(RwLock::new(deque));
        let recs_clone = Arc::clone(&receivers);

        // let writers = Arc::new(DashMap::new());
        // let writers_clone = Arc::clone(&writers);

        let file_path_string = file_path.to_string();

        let handle = thread::spawn(move || {
            Self::run(recs_clone, file_path_string, schema);
        });

        Ok((Self {
            receivers,
            pool,
            handle,
            // writers,
        }, sender))
    }

    fn run(receivers: Arc<RwLock<VecDeque<Receiver<Option<Message>>>>>, file_path: String, schema: Arc<Schema>) {
        loop {
            let recs_guard = receivers.read().unwrap();
            if recs_guard.is_empty() {
                drop(recs_guard);
                sleep(Duration::from_millis(1));
                continue;
            }
            let mut sel = Select::new();
            for r in recs_guard.iter() {
                sel.recv(r);
            }
            let oper = sel.select();
            let index = oper.index();

            // Get the receiver again
            let msg = recs_guard.get(index).unwrap().try_recv();
            match msg {
                Ok(message) => match message {
                    Some(message) => {}
                    None => {}
                },
                Err(_) => {
                    // no message available or channel closed
                }
            }
        }
    }

    /// Add a new receiver to the WriteThread
    fn add_writer(&self, file_path: &str, schema: Arc<Schema>) -> Sender<Option<Message>> {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let mut recs_guard = self.receivers.write().unwrap();
        recs_guard.push_back(receiver);
    }

    fn new_writer(file_path: &str, schema: Arc<Schema>) -> Result<ArrowWriter<BufWriter<File>>, ArrowError> {
        let file = File::create(&file_path)?;
        let buf_writer = BufWriter::new(file);
        let writer = ArrowWriter::try_new(buf_writer, schema, None)?;
        Ok(writer)
    }
}

struct WritePool {}
