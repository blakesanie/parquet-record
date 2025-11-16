use parquet_record::ParquetSerialize;

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct TestWithVec {
    id: u32,
    values: Vec<i32>,
}

fn main() {
    println!("Testing struct with Vec");
    
    let items = vec![
        TestWithVec {
            id: 1,
            values: vec![1, 2, 3],
        },
        TestWithVec {
            id: 2,
            values: vec![4, 5],
        },
    ];

    let bytes = TestWithVec::dump(items.clone()).expect("Serialization failed");
    let loaded = TestWithVec::load(&bytes).expect("Deserialization failed");
    
    println!("Original: {:?}", items);
    println!("Loaded: {:?}", loaded);
    assert_eq!(items, loaded);
    
    let schema = TestWithVec::arrow_schema();
    println!("Schema: {:?}", schema);
    
    println!("All tests passed!");
}