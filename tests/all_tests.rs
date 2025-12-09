use parquet_record::{ParquetRecord, ParquetBatchWriter, read_parquet, read_parquet_with_config, read_parquet_par, read_parquet_with_config_par, read_parquet_columns, read_parquet_columns_with_config, read_parquet_columns_par, read_parquet_columns_with_config_par};
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use rayon::iter::ParallelIterator;
use std::sync::Arc;
use std::io;

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
        let ids: Vec<i32> = items.iter().map(|item| item.id).collect();
        let values: Vec<&str> = items.iter().map(|item| item.value.as_str()).collect();

        RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int32Array::from(ids)),
                std::sync::Arc::new(StringArray::from(values)),
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
    writer.add_items(&test_items).unwrap();

    // Close the writer to finalize the file
    writer.close().unwrap();

    // Clean up test file
    use std::fs;
    let _ = fs::remove_file("test_output.parquet");
}

#[test]
fn test_mb_logging() {
    // Test that MB logging works properly by creating a writer with verbose config
    let config = parquet_record::ParquetRecordConfig::with_verbose(true);
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

    writer.add_items(&test_items).unwrap();
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
        TestRecord { id: 1, value: "a".to_string() },
        TestRecord { id: 2, value: "b".to_string() },
        TestRecord { id: 3, value: "c".to_string() },
    ];
    writer.add_items(&items).unwrap();
    writer.close().unwrap();

    // Read only the "id" column in parallel
    let results = read_parquet_columns_par::<arrow::datatypes::Int32Type>(path, "id", Some(10));
    assert!(results.is_some());

    let (num_rows, iter) = results.unwrap();
    let all_ids: Vec<i32> = iter.flatten().collect();
    assert_eq!(all_ids.len(), 3);
    assert_eq!(num_rows, 3); // Check that the reported row count is correct
    assert!(all_ids.contains(&1));
    assert!(all_ids.contains(&2));
    assert!(all_ids.contains(&3));

    // Clean up
    std::fs::remove_file(path).unwrap();
}

#[test]
fn test_all_functions_comprehensive() {
    // Create test data
    let path = "test_comprehensive.parquet";
    let test_items = vec![
        TestRecord { id: 1, value: "value1".to_string() },
        TestRecord { id: 2, value: "value2".to_string() },
        TestRecord { id: 3, value: "value3".to_string() },
        TestRecord { id: 4, value: "value4".to_string() },
        TestRecord { id: 5, value: "value5".to_string() },
    ];

    // Test ParquetBatchWriter functionality
    let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(2));
    writer.add_items(&test_items.clone()).unwrap();
    writer.flush().unwrap();  // Ensure all items are written to update stats properly

    // Get writer stats before closing
    let writer_stats = writer.get_stats().unwrap();
    writer.close().unwrap();

    // Test read_parquet (Sequential reading with default config)
    let (num_rows, read_iter) = read_parquet(TestRecord::schema(), path, Some(2))
        .unwrap();
    let read_items: Vec<TestRecord> = read_iter
        .flat_map(|batch| batch)
        .collect();
    assert_eq!(read_items.len(), 5);
    assert_eq!(num_rows, 5); // Check that the reported row count is correct
    for (i, item) in test_items.iter().enumerate() {
        assert_eq!(read_items[i].id, item.id);
        assert_eq!(read_items[i].value, item.value);
    }

    // Test read_parquet_with_config
    let config = parquet_record::ParquetRecordConfig::with_verbose(false);
    let (num_rows_config, read_iter_config) = read_parquet_with_config(TestRecord::schema(), path, Some(2), &config)
        .unwrap();
    let read_items_config: Vec<TestRecord> = read_iter_config
        .flat_map(|batch| batch)
        .collect();
    assert_eq!(read_items_config.len(), 5);
    assert_eq!(num_rows_config, 5); // Check that the reported row count is correct

    // Test read_parquet_columns (Sequential column reading)
    let (num_rows_col, col_iter) = read_parquet_columns::<arrow::datatypes::Int32Type>(path, "id", Some(2))
        .unwrap();
    let id_values: Vec<i32> = col_iter
        .flat_map(|batch| batch)
        .collect();
    assert_eq!(id_values.len(), 5);
    assert_eq!(num_rows_col, 5); // Check that the reported row count is correct
    assert!(id_values.contains(&1));
    assert!(id_values.contains(&2));
    assert!(id_values.contains(&3));
    assert!(id_values.contains(&4));
    assert!(id_values.contains(&5));

    // Test read_parquet_columns_with_config
    let (num_rows_col_config, col_iter_config) = read_parquet_columns_with_config::<arrow::datatypes::Int32Type>(path, "id", Some(2), &config)
        .unwrap();
    let id_values_config: Vec<i32> = col_iter_config
        .flat_map(|batch| batch)
        .collect();
    assert_eq!(id_values_config.len(), 5);
    assert_eq!(num_rows_col_config, 5); // Check that the reported row count is correct

    // Test read_parquet_par (Parallel reading)
    let (num_rows_par, par_iter) = read_parquet_par(TestRecord::schema(), path, Some(2))
        .unwrap();
    let flat_par_items: Vec<TestRecord> = par_iter.flat_map(|batch| batch).collect();
    assert_eq!(flat_par_items.len(), 5);
    assert_eq!(num_rows_par, 5); // Check that the reported row count is correct

    // Test read_parquet_with_config_par (Parallel reading with config)
    let (num_rows_par_config, par_iter_config) = read_parquet_with_config_par(TestRecord::schema(), path, Some(2), &config)
        .unwrap();
    let flat_par_items_config: Vec<TestRecord> = par_iter_config.flat_map(|batch| batch).collect();
    assert_eq!(flat_par_items_config.len(), 5);
    assert_eq!(num_rows_par_config, 5); // Check that the reported row count is correct

    // Test read_parquet_columns_par (Parallel column reading)
    let (num_rows_col_par, col_par_iter) = read_parquet_columns_par::<arrow::datatypes::Int32Type>(path, "id", Some(2))
        .unwrap();
    let flat_par_col_values: Vec<i32> = col_par_iter.flat_map(|batch| batch).collect();
    assert_eq!(flat_par_col_values.len(), 5);
    assert_eq!(num_rows_col_par, 5); // Check that the reported row count is correct

    // Test read_parquet_columns_with_config_par (Parallel column reading with config)
    let (num_rows_col_par_config, col_par_iter_config) = read_parquet_columns_with_config_par::<arrow::datatypes::Int32Type>(path, "id", Some(2), &config)
        .unwrap();
    let flat_par_col_values_config: Vec<i32> = col_par_iter_config.flat_map(|batch| batch).collect();
    assert_eq!(flat_par_col_values_config.len(), 5);
    assert_eq!(num_rows_col_par_config, 5); // Check that the reported row count is correct

    assert_eq!(writer_stats.total_items_written, 5);
    assert!(writer_stats.total_bytes_written > 0);

    // Clean up
    use std::fs;
    fs::remove_file(path).unwrap();
}

#[test]
fn test_buffer_operations() {
    let path = "test_buffer.parquet";

    // Test with small buffer to trigger swapping
    let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(2));

    // Add items one by one to test add_item
    for i in 1..=5 {
        writer.add_item(TestRecord {
            id: i,
            value: format!("item{}", i),
        }).unwrap();
    }

    // Add some more with add_items
    let more_items = vec![
        TestRecord { id: 6, value: "item6".to_string() },
        TestRecord { id: 7, value: "item7".to_string() },
    ];
    writer.add_items(&more_items).unwrap();

    // Get the buffer length before closing
    let buffer_len_before_close = writer.buffer_len();
    writer.close().unwrap();

    // Verify we can read back all items
    let (num_rows, iter) = read_parquet(TestRecord::schema(), path, Some(3))
        .unwrap();
    let read_items: Vec<TestRecord> = iter
        .flat_map(|batch| batch)
        .collect();
    assert_eq!(read_items.len(), 7);
    assert_eq!(num_rows, 7); // Check that the reported row count is correct

    // Test buffer operations - with 7 items and buffer size 2, buffer should have 1 item remaining
    assert_eq!(buffer_len_before_close, 1); // Buffer has remaining items that haven't been flushed yet

    // Clean up
    use std::fs;
    fs::remove_file(path).unwrap();
}

#[test]
fn test_edge_cases() {
    let path = "test_edge.parquet";

    // Test with empty data
    let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(5));
    writer.close().unwrap();

    // Reading empty file should return None (no data to read)
    let empty_reader = read_parquet::<TestRecord>(TestRecord::schema(), path, Some(2));
    assert!(empty_reader.is_none());  // No data to read means no iterator

    // Test with single item
    let single_path = "test_single.parquet";
    let single_writer = ParquetBatchWriter::<TestRecord>::new(single_path.to_string(), Some(5));
    single_writer.add_item(TestRecord { id: 42, value: "single".to_string() }).unwrap();
    single_writer.close().unwrap();

    let (single_num_rows, single_iter): (usize, _) = read_parquet::<TestRecord>(TestRecord::schema(), single_path, Some(2))
        .unwrap();
    let single_items: Vec<TestRecord> = single_iter
        .flat_map(|batch| batch)
        .collect();
    assert_eq!(single_items.len(), 1);
    assert_eq!(single_num_rows, 1); // Check that the reported row count is correct
    assert_eq!(single_items[0].id, 42);
    assert_eq!(single_items[0].value, "single");

    // Clean up
    use std::fs;
    let _ = fs::remove_file(path);      // Ignore error if file doesn't exist
    let _ = fs::remove_file(single_path); // Ignore error if file doesn't exist
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

    writer.add_items(&items).unwrap();
    writer.flush().unwrap();  // Ensure all items in buffer are written
    let stats = writer.get_stats().unwrap();
    writer.close().unwrap();

    assert_eq!(stats.total_items_written, 4);
    assert!(stats.total_bytes_written > 0);
    assert!(stats.total_batches_written >= 1);  // Should be at least 1 batch, maybe 2 depending on buffering

    // Clean up
    use std::fs;
    fs::remove_file(path).unwrap();
}

#[test]
fn test_flush_operation() {
    let path = "test_flush.parquet";

    let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(10));  // Large buffer

    // Add some items
    for i in 1..=3 {
        writer.add_item(TestRecord { id: i, value: format!("item{}", i) }).unwrap();
    }

    // Buffer should have items
    assert_eq!(writer.buffer_len(), 3);

    // Flush the buffer
    writer.flush().unwrap();
    assert_eq!(writer.buffer_len(), 0);

    writer.close().unwrap();

    // Verify items were written
    let (num_rows, iter) = read_parquet(TestRecord::schema(), path, Some(2))
        .unwrap();
    let items: Vec<TestRecord> = iter
        .flat_map(|batch| batch)
        .collect();
    assert_eq!(items.len(), 3);
    assert_eq!(num_rows, 3); // Check that the reported row count is correct

    // Clean up
    use std::fs;
    fs::remove_file(path).unwrap();
}

#[test]
fn test_verbose_logging_functionality() {
    let path = "test_verbose.parquet";

    // Test with verbose logging enabled
    let config = parquet_record::ParquetRecordConfig::with_verbose(true);
    let writer = ParquetBatchWriter::<TestRecord>::with_config(
        path.to_string(),
        Some(2),
        config
    );

    let items = vec![
        TestRecord { id: 1, value: "verbose1".to_string() },
        TestRecord { id: 2, value: "verbose2".to_string() },
    ];

    writer.add_items(&items).unwrap();
    writer.close().unwrap();

    // Verify items were written
    let (num_rows, iter) = read_parquet(TestRecord::schema(), path, None)
        .unwrap();
    let read_items: Vec<TestRecord> = iter
        .flat_map(|batch| batch)
        .collect();
    assert_eq!(read_items.len(), 2);
    assert_eq!(num_rows, 2); // Check that the reported row count is correct

    // Clean up
    use std::fs;
    fs::remove_file(path).unwrap();
}

#[test]
fn test_read_parquet_sequential() {
    let path = "test_read_seq.parquet";

    // Create test data
    let items = vec![
        TestRecord { id: 10, value: "first".to_string() },
        TestRecord { id: 20, value: "second".to_string() },
        TestRecord { id: 30, value: "third".to_string() },
        TestRecord { id: 40, value: "fourth".to_string() },
    ];

    let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(2));
    writer.add_items(&items.clone()).unwrap();
    writer.close().unwrap();

    // Test read_parquet with different batch sizes
    for batch_size in [1, 2, 3, 4, 10] {
        let (num_rows, iter) = read_parquet(TestRecord::schema(), path, Some(batch_size))
            .unwrap();
        let read_items: Vec<TestRecord> = iter
            .flat_map(|batch| batch)
            .collect();

        assert_eq!(read_items.len(), 4);
        assert_eq!(num_rows, 4); // Check that the reported row count is correct

        // Check that all original items are present
        for (i, original_item) in items.iter().enumerate() {
            assert_eq!(read_items[i].id, original_item.id);
            assert_eq!(read_items[i].value, original_item.value);
        }
    }

    // Test with default batch size (None)
    let (num_rows_default, iter_default) = read_parquet(TestRecord::schema(), path, None)
        .unwrap();
    let read_items_default: Vec<TestRecord> = iter_default
        .flat_map(|batch| batch)
        .collect();
    assert_eq!(read_items_default.len(), 4);
    assert_eq!(num_rows_default, 4); // Check that the reported row count is correct

    // Test with configuration
    let config = parquet_record::ParquetRecordConfig::with_verbose(false);
    let (num_rows_config, iter_config) = read_parquet_with_config(TestRecord::schema(), path, Some(2), &config)
        .unwrap();
    let read_items_config: Vec<TestRecord> = iter_config
        .flat_map(|batch| batch)
        .collect();
    assert_eq!(read_items_config.len(), 4);
    assert_eq!(num_rows_config, 4); // Check that the reported row count is correct

    // Clean up
    use std::fs;
    fs::remove_file(path).unwrap();
}

#[test]
fn test_read_parquet_parallel() {
    let path = "test_read_par.parquet";

    // Create test data with multiple items to test different scenarios
    let items: Vec<TestRecord> = (1..=20).map(|i| TestRecord {
        id: i,
        value: format!("item{}", i),
    }).collect();

    let writer = ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(5)); // Buffer size 5
    writer.add_items(&items.clone()).unwrap();
    writer.close().unwrap();

    // Test read_parquet_par - getting individual batches
    let (par_num_rows, par_iter) = read_parquet_par(TestRecord::schema(), path, Some(3))
        .unwrap();

    // Verify total count by collecting all items
    let all_items: Vec<TestRecord> = par_iter.flat_map(|batch| batch).collect();
    assert_eq!(all_items.len(), 20);
    assert_eq!(par_num_rows, 20); // Check that the reported row count is correct

    // Sort both vectors by ID to compare (parallel processing order may vary)
    let mut all_items_sorted = all_items;
    all_items_sorted.sort_by(|a, b| a.id.cmp(&b.id));
    let mut items_sorted = items.clone();
    items_sorted.sort_by(|a, b| a.id.cmp(&b.id));

    for i in 0..20 {
        assert_eq!(all_items_sorted[i].id, items_sorted[i].id);
        assert_eq!(all_items_sorted[i].value, items_sorted[i].value);
    }

    // Test with configuration
    let config = parquet_record::ParquetRecordConfig::with_verbose(false);
    let (par_num_rows_config, par_iter_config) = read_parquet_with_config_par::<TestRecord>(TestRecord::schema(), path, Some(3), &config)
        .unwrap();
    let total_items_config: usize = par_iter_config.flat_map(|batch| batch).count();
    assert_eq!(total_items_config, 20);
    assert_eq!(par_num_rows_config, 20); // Check that the reported row count is correct

    // Clean up
    use std::fs;
    fs::remove_file(path).unwrap();
}

#[test]
fn test_read_parquet_edge_cases() {
    let empty_path = "test_read_empty.parquet";

    // Create empty file
    let empty_writer = ParquetBatchWriter::<TestRecord>::new(empty_path.to_string(), Some(5));
    empty_writer.close().unwrap();

    // Test empty file with sequential reader
    let empty_result = read_parquet::<TestRecord>(TestRecord::schema(), empty_path, Some(2));
    assert!(empty_result.is_none()); // No data means no iterator

    // Test empty file with parallel reader
    let empty_par_result = read_parquet_par::<TestRecord>(TestRecord::schema(), empty_path, Some(2));
    assert!(empty_par_result.is_none()); // No data means no iterator

    // Test single item
    let single_path = "test_read_single.parquet";
    let single_writer = ParquetBatchWriter::<TestRecord>::new(single_path.to_string(), Some(5));
    single_writer.add_item(TestRecord { id: 100, value: "single_item".to_string() }).unwrap();
    single_writer.close().unwrap();

    // Test single item with sequential reader
    let (single_num_rows, single_iter) = read_parquet::<TestRecord>(TestRecord::schema(), single_path, Some(2))
        .unwrap();
    let single_items: Vec<TestRecord> = single_iter
        .flat_map(|batch| batch)
        .collect();
    assert_eq!(single_items.len(), 1);
    assert_eq!(single_num_rows, 1); // Check that the reported row count is correct
    assert_eq!(single_items[0].id, 100);
    assert_eq!(single_items[0].value, "single_item");

    // Test single item with parallel reader
    let (single_par_num_rows, single_par_iter) = read_parquet_par::<TestRecord>(TestRecord::schema(), single_path, Some(2))
        .unwrap();
    let total_single_items: usize = single_par_iter.flat_map(|batch| batch).count();
    assert_eq!(total_single_items, 1);
    assert_eq!(single_par_num_rows, 1); // Check that the reported row count is correct

    // Clean up
    use std::fs;
    let _ = fs::remove_file(empty_path);
    let _ = fs::remove_file(single_path);
}