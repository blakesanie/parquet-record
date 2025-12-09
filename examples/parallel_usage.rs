use parquet_record::{ParquetRecord, ParquetBatchWriter};
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use rayon::iter::ParallelIterator;
use std::sync::Arc;
use std::io;

#[derive(Debug, Clone)]
struct Product {
    id: i32,
    name: String,
    price: f32,
}

impl ParquetRecord for Product {
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float32, false),
        ]))
    }

    fn items_to_records(schema: Arc<Schema>, items: &[Self]) -> RecordBatch {
        let ids: Vec<i32> = items.iter().map(|item| item.id).collect();
        let names: Vec<&str> = items.iter().map(|item| item.name.as_str()).collect();
        let prices: Vec<f32> = items.iter().map(|item| item.price).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(arrow::array::Float32Array::from(prices)),
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
        let price_array = as_primitive_array::<arrow::datatypes::Float32Type>(record_batch.column(2));

        let mut result = Vec::new();
        for i in 0..record_batch.num_rows() {
            result.push(Product {
                id: id_array.value(i),
                name: name_array.value(i).to_string(),
                price: price_array.value(i),
            });
        }

        Ok(result)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a batch writer with smaller buffer size for demonstration
    let writer = ParquetBatchWriter::<Product>::new("parallel_example.parquet".to_string(), Some(50));

    // Create a larger dataset
    let mut products = Vec::new();
    for i in 1..=200 {
        products.push(Product {
            id: i,
            name: format!("Product {}", i),
            price: (i as f32) * 10.99,
        });
    }

    // Add all items
    writer.add_items(&products)?;
    writer.close()?;

    println!("Successfully wrote parallel_example.parquet with 200 products");

    // Read in parallel
    if let Some((total_rows, par_reader)) = parquet_record::read_parquet_par::<Product>(
        Product::schema(),
        "parallel_example.parquet",
        Some(100)
    ) {
        let all_products: Vec<Product> = par_reader.flat_map(|batch| batch).collect();
        println!("Read {} products in parallel from {} rows", all_products.len(), total_rows);
    }

    // Read only specific columns
    if let Some((_total_rows, id_reader)) =
        parquet_record::read_parquet_columns::<arrow::datatypes::Int32Type>(
            "parallel_example.parquet",
            "id",
            Some(100)
        ) {
        let all_ids: Vec<i32> = id_reader.flatten().collect();
        println!("Read {} IDs (first 10: {:?})", all_ids.len(), &all_ids[..std::cmp::min(10, all_ids.len())]);
    }

    // Clean up the example file
    std::fs::remove_file("parallel_example.parquet")?;

    Ok(())
}