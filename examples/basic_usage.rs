use parquet_record::{ParquetRecord, ParquetBatchWriter};
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::io;

#[derive(Debug, Clone)]
struct Person {
    id: i32,
    name: String,
    age: u8,
}

impl ParquetRecord for Person {
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::UInt8, false),
        ]))
    }

    fn items_to_records(schema: Arc<Schema>, items: &[Self]) -> RecordBatch {
        let ids: Vec<i32> = items.iter().map(|item| item.id).collect();
        let names: Vec<&str> = items.iter().map(|item| item.name.as_str()).collect();
        let ages: Vec<u8> = items.iter().map(|item| item.age).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(arrow::array::UInt8Array::from(ages)),
            ],
        ).unwrap()
    }

    fn records_to_items(record_batch: &RecordBatch) -> io::Result<Vec<Self>> {
        use arrow::array::{as_primitive_array, StringArray};

        let id_array = as_primitive_array::<arrow::datatypes::Int32Type>(record_batch.column(0));
        let name_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let age_array = as_primitive_array::<arrow::datatypes::UInt8Type>(record_batch.column(2));

        let mut result = Vec::new();
        for i in 0..record_batch.num_rows() {
            result.push(Person {
                id: id_array.value(i),
                name: name_array.value(i).to_string(),
                age: age_array.value(i),
            });
        }

        Ok(result)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a batch writer that buffers up to 100 items before writing to disk
    let writer = ParquetBatchWriter::<Person>::new("example.parquet".to_string(), Some(100));

    // Create some sample data
    let people = vec![
        Person { id: 1, name: "Alice".to_string(), age: 30 },
        Person { id: 2, name: "Bob".to_string(), age: 25 },
        Person { id: 3, name: "Charlie".to_string(), age: 35 },
        Person { id: 4, name: "Diana".to_string(), age: 28 },
    ];

    // Add items to the writer
    writer.add_items(&people)?;

    // Close the writer to finalize the file
    writer.close()?;

    println!("Successfully wrote example.parquet");

    // Now read the data back
    if let Some((total_rows, reader)) = parquet_record::read_parquet(
        Person::schema(),
        "example.parquet",
        Some(50)
    ) {
        let all_people: Vec<Person> = reader.flatten().collect();
        println!("Read {} people from {} rows", all_people.len(), total_rows);
        for person in all_people {
            println!("  - ID: {}, Name: {}, Age: {}", person.id, person.name, person.age);
        }
    }

    // Clean up the example file
    std::fs::remove_file("example.parquet")?;

    Ok(())
}