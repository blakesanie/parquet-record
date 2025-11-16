use parquet_record::ParquetSerialize;

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct SimpleVecStruct {
    id: u32,
    values: Vec<i32>,
}

fn main() {
    println!("If this compiles, the macro is working");
}