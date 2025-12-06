use parquet_record::{ParquetRecord, read_parquet_columns};
use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use std::io;

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
fn test_read_columns() {
    // Create a dummy parquet file
    let path = "test_read_columns.parquet";
    let writer = parquet_record::ParquetBatchWriter::<TestRecord>::new(path.to_string(), Some(10));
    let items = vec![
        TestRecord { id: 1, value: "a".to_string() },
        TestRecord { id: 2, value: "b".to_string() },
    ];
    writer.add_items(items).unwrap();
    writer.close().unwrap();

    // Read only the "id" column
    let reader = read_parquet_columns::<arrow::datatypes::Int32Type>(path, "id", Some(10));
    assert!(reader.is_some());

    let mut count = 0;
    for array_result in reader.unwrap() {
        count += array_result.len();
    }
    assert_eq!(count, 2);

    // Clean up
    std::fs::remove_file(path).unwrap();
}
