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
- Collections: `Vec<T>`, `HashSet<T>`, `HashMap<K, V>` where `T`, `K`, `V` are supported types (serialized as JSON strings in parquet)

## Limitations

- Currently only supports structs with named fields
- Collection types are serialized to JSON strings in parquet (native LIST/MAP types may be added in the future)
- Does not support nested structs or complex types (though this could be extended)
- All fields are treated as non-nullable

## License

MIT