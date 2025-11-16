use parquet_record::ParquetSerialize;

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct SimpleStruct {
    id: u32,
    name: String,
}

fn main() {
    println!("Testing simple struct");
    
    let items = vec![
        SimpleStruct {
            id: 1,
            name: "test1".to_string(),
        },
        SimpleStruct {
            id: 2,
            name: "test2".to_string(),
        },
    ];

    let bytes = SimpleStruct::dump(items.clone()).expect("Serialization failed");
    let loaded = SimpleStruct::load(&bytes).expect("Deserialization failed");
    
    println!("Original: {:?}", items);
    println!("Loaded: {:?}", loaded);
    assert_eq!(items, loaded);
    
    let schema = SimpleStruct::arrow_schema();
    println!("Schema: {:?}", schema);
    
    println!("All tests passed!");
}