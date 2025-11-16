use parquet_record::ParquetSerialize;

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct TestStruct {
    id: u32,
    values: Vec<i32>,
}

fn main() {
    println!("Test struct with Vec<i32> compiles successfully!");
}