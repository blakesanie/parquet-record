# parquet-record

A Rust library for efficient serialization and deserialization of structs to/from Parquet format with support for batch writing and parallel reading.

## Features

- **Batch Writing**: Efficiently write records in batches to Parquet files with configurable buffer sizes
- **Parallel Reading**: Read Parquet files in parallel across row groups
- **Column Projection**: Read specific columns only for faster access
- **Thread-Safe**: All writers and operations are thread-safe
- **Configurable**: Verbose logging and batch size configuration

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
parquet-record = "0.1.0"  # Replace with the actual version
```

## Usage

### Basic Usage

```rust
use parquet_record::{ParquetRecord, ParquetBatchWriter};
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::io;

// Define a struct that implements ParquetRecord
#[derive(Debug, Clone)]
struct MyRecord {
    id: i32,
    name: String,
}

impl ParquetRecord for MyRecord {
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

        let mut result = Vec::new();
        for i in 0..record_batch.num_rows() {
            result.push(MyRecord {
                id: id_array.value(i),
                name: name_array.value(i).to_string(),
            });
        }

        Ok(result)
    }
}

// Writing to a Parquet file
let writer = ParquetBatchWriter::<MyRecord>::new("output.parquet".to_string(), Some(100));

let records = vec![
    MyRecord { id: 1, name: "Alice".to_string() },
    MyRecord { id: 2, name: "Bob".to_string() },
    MyRecord { id: 3, name: "Charlie".to_string() },
];

writer.add_items(records).unwrap();
writer.close().unwrap();

// Reading from a Parquet file
if let Some((total_rows, reader)) = parquet_record::read_parquet(
    MyRecord::schema(),
    "output.parquet",
    Some(50)
) {
    let all_records: Vec<MyRecord> = reader.flat_map(|batch| batch).collect();
    println!("Read {} records from {} rows", all_records.len(), total_rows);
}
```

### Parallel Reading

```rust
// Read in parallel across row groups
if let Some((total_rows, par_reader)) = parquet_record::read_parquet_par::<MyRecord>(
    MyRecord::schema(),
    "output.parquet",
    Some(100)
) {
    let all_records: Vec<MyRecord> = par_reader.flat_map(|batch| batch).collect();
    println!("Read {} records in parallel", all_records.len());
}
```

### Column Projection (Reading Specific Columns)

```rust
use arrow::datatypes::Int32Type;

// Read only the "id" column
if let Some((total_rows, id_reader)) = 
    parquet_record::read_parquet_columns::<Int32Type>(
        "output.parquet",
        "id",
        Some(100)
    ) {
    let all_ids: Vec<i32> = id_reader.flat_map(|batch| batch).collect();
    println!("Read {} IDs", all_ids.len());
}
```

### Using Custom Configuration

```rust
use parquet_record::ParquetRecordConfig;

let config = ParquetRecordConfig::with_verbose(false);
let writer = ParquetBatchWriter::<MyRecord>::with_config(
    "output.parquet".to_string(),
    Some(100),
    config
);
```

## API Documentation

### Core Types

#### `ParquetRecord` Trait
This trait must be implemented for any struct that you want to serialize to or deserialize from Parquet files.

**Required Methods:**
- `schema() -> Arc<Schema>`: Returns the Arrow schema for this record type.
  - **Input**: None
  - **Output**: An Arc-wrapped Arrow Schema defining the structure of the record
  - **Design**: Defines the column structure and data types for serialization

- `items_to_records(schema: Arc<Schema>, items: &[Self]) -> RecordBatch`: Converts a slice of items to an Arrow RecordBatch.
  - **Input**: Schema definition and slice of items to convert
  - **Output**: An Arrow RecordBatch containing the data
  - **Design**: Transforms your Rust struct data into Arrow format for Parquet storage

- `records_to_items(record_batch: &RecordBatch) -> io::Result<Vec<Self>>`: Converts an Arrow RecordBatch back to a vector of items.
  - **Input**: Arrow RecordBatch to convert
  - **Output**: Vector of your struct instances
  - **Design**: Transforms Arrow data back to your Rust struct format

#### `ParquetRecordConfig` Struct
Configuration options for parquet operations.

**Methods:**
- `with_verbose(verbose: bool) -> Self`: Creates a configuration with specified verbose setting.
  - **Input**: Boolean indicating whether to show verbose output
  - **Output**: ParquetRecordConfig instance
  - **Design**: Allows control over logging verbosity

- `silent() -> Self`: Creates a configuration with verbose output disabled.
  - **Input**: None
  - **Output**: ParquetRecordConfig instance with verbose = false
  - **Design**: Provides a convenient way to create a quiet configuration

#### `ParquetBatchWriter<T>` Struct
A thread-safe batch writer for writing records to Parquet files with buffering and automatic file management.

**Methods:**
- `new(output_file: String, buffer_size: Option<usize>) -> Self`: Creates a new batch writer with default configuration.
  - **Input**: Path to output file and optional buffer size (None = no buffering)
  - **Output**: ParquetBatchWriter instance
  - **Design**: Creates a writer that buffers items before writing to improve I/O performance

- `with_config(output_file: String, buffer_size: Option<usize>, config: ParquetRecordConfig) -> Self`: Creates a new batch writer with custom configuration.
  - **Input**: File path, buffer size, and configuration
  - **Output**: ParquetBatchWriter instance
  - **Design**: Provides full control over writer behavior

- `add_items(&self, items: Vec<T>) -> Result<(), io::Error>`: Adds multiple items to the buffer and writes when buffer is full.
  - **Input**: Vector of items to add
  - **Output**: Result indicating success or error
  - **Design**: Efficiently adds multiple items, automatically managing buffer flushing

- `add_item(&self, item: T) -> Result<(), io::Error>`: Adds a single item to the buffer and writes when buffer is full.
  - **Input**: Single item to add
  - **Output**: Result indicating success or error
  - **Design**: For adding items individually, with automatic buffer management

- `flush(&self) -> Result<(), io::Error>`: Forces writing of all buffered items to the file.
  - **Input**: None
  - **Output**: Result indicating success or error
  - **Design**: Ensures all buffered data is written to disk

- `close(self) -> Result<(), io::Error>`: Closes the writer and finalizes the Parquet file.
  - **Input**: None (consumes self)
  - **Output**: Result indicating success or error
  - **Design**: Completes the file and handles cleanup

- `close_no_consume(&self) -> Result<(), io::Error>`: Closes the writer without consuming it (non-consuming close).
  - **Input**: None
  - **Output**: Result indicating success or error
  - **Design**: For use when you can't consume the writer

- `get_stats(&self) -> Result<WriteStats, io::Error>`: Returns current write statistics.
  - **Input**: None
  - **Output**: WriteStats struct with metrics
  - **Design**: Provides performance and usage metrics

- `buffer_len(&self) -> usize`: Returns the current number of items in the buffer.
  - **Input**: None
  - **Output**: Number of items currently buffered
  - **Design**: For monitoring buffer state

#### `WriteStats` Struct
Contains statistics about the writing process.

**Fields:**
- `total_items_written`: Total number of items written to the file
- `total_batches_written`: Total number of batches written
- `total_bytes_written`: Total bytes written to the file

### Reading Functions

#### `read_parquet_with_config<T>`
Sequential reading with custom configuration.

- **Input**: Schema, file path, optional batch size, and configuration
- **Output**: Option containing (total row count, iterator of record batches)
- **Design**: Provides sequential reading with full configuration control

#### `read_parquet<T>`
Sequential reading with default configuration.

- **Input**: Schema, file path, and optional batch size
- **Output**: Option containing (total row count, iterator of record batches)
- **Design**: Convenience function with default configuration

#### `read_parquet_columns_with_config<I>`
Read specific columns sequentially with custom configuration.

- **Input**: File path, column name, optional batch size, and configuration
- **Output**: Option containing (total row count, iterator of column values)
- **Design**: Optimized for reading only specific columns (faster for large files with many columns)

#### `read_parquet_columns<I>`
Read specific columns sequentially with default configuration.

- **Input**: File path, column name, and optional batch size
- **Output**: Option containing (total row count, iterator of column values)
- **Design**: Convenience function for column reading

#### `read_parquet_with_config_par<T>`
Parallel reading with custom configuration.

- **Input**: Schema, file path, optional batch size, and configuration
- **Output**: Option containing (total row count, parallel iterator of record batches)
- **Design**: Reads different row groups in parallel using Rayon

#### `read_parquet_par<T>`
Parallel reading with default configuration.

- **Input**: Schema, file path, and optional batch size
- **Output**: Option containing (total row count, parallel iterator of record batches)
- **Design**: Convenience function for parallel reading

#### `read_parquet_columns_with_config_par<I>`
Parallel column reading with custom configuration.

- **Input**: File path, column name, optional batch size, and configuration
- **Output**: Option containing (total row count, parallel iterator of column values)
- **Design**: Parallel reading optimized for specific columns

#### `read_parquet_columns_par<I>`
Parallel column reading with default configuration.

- **Input**: File path, column name, and optional batch size
- **Output**: Option containing (total row count, parallel iterator of column values)
- **Design**: Convenience function for parallel column reading

## Thread Safety

All writer operations are thread-safe and can be called from multiple threads simultaneously. The internal buffer is protected by a mutex, and when full, secondary buffers are swapped and written concurrently to maintain performance.

## Architecture

The library is designed around Arrow's memory layout for optimal performance:
- **Batching**: Items are accumulated in memory before conversion to Arrow RecordBatches
- **Buffering**: Configurable buffer sizes allow for performance tuning
- **Lazy Writing**: Files and writers are created only when data arrives
- **Parallel Operations**: Reading operations can leverage multiple cores via Rayon

## Release Process

This library uses a simple GitHub Action to publish to crates.io:

### Publishing (publish.yml)
- Automatically publishes to crates.io when changes are pushed to the main branch
- Checks the version in `Cargo.toml` and attempts to publish that version
- Creates a Git tag for the published version
- If the version already exists on crates.io, the publish will fail safely

### Requirements
- Set up the `CARGO_REGISTRY_TOKEN` secret in GitHub repository settings
- Manually update the version in `Cargo.toml` before merging to main to trigger a new release

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Maintainer

- Blake Sanie (blake@sanie.com)