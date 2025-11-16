use arrow::datatypes::Schema;
use std::sync::Arc;
use parquet::errors::ParquetError;

pub use parquet_record_derive::ParquetSerialize;
pub use parquet_record_derive::parquet_flat;

pub trait ParquetSerialize {
    fn dump(items: Vec<Self>) -> Result<Vec<u8>, ParquetError> 
    where 
        Self: Sized;
    
    fn load(buf: &[u8]) -> Result<Vec<Self>, ParquetError> 
    where 
        Self: Sized;
    
    fn arrow_schema() -> Arc<Schema>;
}