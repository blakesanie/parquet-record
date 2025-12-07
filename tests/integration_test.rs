use parquet_record::{ParquetRecord, ParquetBatchWriter, read_parquet, read_parquet_columns};
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::io;

// Define a test record for integration testing
#[derive(Debug, Clone)]
struct IntegrationTestRecord {
    id: i32,
    name: String,
}

impl ParquetRecord for IntegrationTestRecord {
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn items_to_records(schema: Arc<Schema>, items: &[Self]) -> RecordBatch {
        let ids: Vec<i32> = items.iter().map(|item| item.id).collect();
        let names: Vec<&str> = items.iter().map(|item| item.name.as_str()).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn records_to_items(record_batch: &RecordBatch) -> io::Result<Vec<Self>> {
        use arrow::array::{as_primitive_array, StringArray};

        let id_array = as_primitive_array::<arrow::datatypes::Int32Type>(record_batch.column(0));
        let name_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let mut result = Vec::new();
        for i in 0..record_batch.num_rows() {
            result.push(IntegrationTestRecord {
                id: id_array.value(i),
                name: name_array.value(i).to_string(),
            });
        }

        Ok(result)
    }
}

#[test]
fn test_full_workflow_integration() {
    // Test the full workflow: write -> read -> verify
    let path = "integration_test.parquet";
    
    // Create and write records
    let writer = ParquetBatchWriter::<IntegrationTestRecord>::new(path.to_string(), Some(5));
    let test_data = vec![
        IntegrationTestRecord { id: 1, name: "Alice".to_string() },
        IntegrationTestRecord { id: 2, name: "Bob".to_string() },
        IntegrationTestRecord { id: 3, name: "Charlie".to_string() },
    ];
    
    writer.add_items(test_data.clone()).unwrap();
    writer.close().unwrap();

    // Read all records back
    let (total_rows, reader) = read_parquet(IntegrationTestRecord::schema(), path, Some(2))
        .expect("Should be able to read parquet file");
    
    let read_records: Vec<IntegrationTestRecord> = reader
        .flat_map(|batch| batch)
        .collect();
    
    assert_eq!(total_rows, 3);
    assert_eq!(read_records.len(), 3);
    assert_eq!(read_records[0].id, 1);
    assert_eq!(read_records[0].name, "Alice");
    assert_eq!(read_records[1].id, 2);
    assert_eq!(read_records[1].name, "Bob");
    assert_eq!(read_records[2].id, 3);
    assert_eq!(read_records[2].name, "Charlie");

    // Read only specific column
    let (col_rows, col_reader) = read_parquet_columns::<arrow::datatypes::Int32Type>(path, "id", Some(2))
        .expect("Should be able to read specific column");
    
    let all_ids: Vec<i32> = col_reader
        .flat_map(|batch| batch)
        .collect();
    
    assert_eq!(col_rows, 3);
    assert_eq!(all_ids, vec![1, 2, 3]);

    // Clean up
    std::fs::remove_file(path).unwrap();
}