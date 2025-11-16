use parquet_record::ParquetSerialize;

#[derive(ParquetSerialize)]
struct Test {
    id: u32,
}