# Parquet Record

A Rust crate that provides a custom derive macro for serializing and deserializing structs to/from parquet format.

## Features

- Derive macro for automatic implementation of parquet serialization
- Supports common primitive types: `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`, `f32`, `f64`, `bool`, `String`
- Automatic schema generation based on struct definition
- Efficient serialization using Apache Arrow and Parquet

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
parquet-record = { path = "path/to/parquet-record" }
```

Then use the `ParquetSerialize` derive macro on your structs:

```rust
use parquet_record::ParquetSerialize;

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct MyStruct {
    id: u32,
    name: String,
    value: f64,
    active: bool,
}

fn main() {
    let items = vec![
        MyStruct {
            id: 1,
            name: "Item 1".to_string(),
            value: 3.14,
            active: true,
        },
        MyStruct {
            id: 2,
            name: "Item 2".to_string(),
            value: 2.71,
            active: false,
        },
    ];

    // Serialize to parquet bytes
    let bytes = MyStruct::dump(items.clone()).expect("Serialization failed");
    
    // Deserialize from parquet bytes
    let loaded = MyStruct::load(&bytes).expect("Deserialization failed");
    
    assert_eq!(items, loaded);
}
```

## Supported Types

The derive macro currently supports:
- Signed integers: `i8`, `i16`, `i32`, `i64`
- Unsigned integers: `u8`, `u16`, `u32`, `u64`
- Floating point: `f32`, `f64`
- Boolean: `bool`
- String: `String`
- Collections: 
  - `Vec<T>` where `T` is a supported type (stored as native LIST arrays in parquet)
  - `HashSet<T>` where `T` is a supported type (stored as native LIST arrays in parquet)
  - `HashMap<K, V>` where `K` and `V` are supported types (currently serialized as JSON strings in parquet)

## Collections in Parquet

Collection types (Vec and HashSet) are now stored as native Arrow LIST arrays in Parquet, providing better performance and type safety:

```rust
use parquet_record::ParquetSerialize;
use std::collections::{HashSet, HashMap};

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct StructWithCollections {
    id: u32,
    tags: Vec<String>,          // Stored as LIST<STRING> in parquet
    scores: Vec<i32>,           // Stored as LIST<INT32> in parquet
    unique_ids: HashSet<i64>,   // Stored as LIST<INT64> in parquet
    metadata: HashMap<String, String>, // Currently stored as JSON string
}

fn main() {
    let mut unique_set = HashSet::new();
    unique_set.insert(100i64);
    unique_set.insert(200i64);
    
    let mut metadata = HashMap::new();
    metadata.insert("key1".to_string(), "value1".to_string());
    
    let items = vec![
        StructWithCollections {
            id: 1,
            tags: vec!["tag1".to_string(), "tag2".to_string()],
            scores: vec![10, 20, 30],
            unique_ids: unique_set,
            metadata,
        }
    ];

    // Serialize to parquet bytes
    let bytes = StructWithCollections::dump(items.clone()).expect("Serialization failed");
    
    // Deserialize from parquet bytes
    let loaded = StructWithCollections::load(&bytes).expect("Deserialization failed");
    
    assert_eq!(items, loaded);
}
```

## Limitations

- Currently only supports structs with named fields
- HashMap is serialized to JSON strings in parquet (native MAP type support may be added in the future)
- Does not support nested structs or complex types (though this could be extended)
- All fields are treated as non-nullable

## License

MIT