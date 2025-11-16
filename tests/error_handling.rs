use parquet_record::ParquetSerialize;

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct SimpleStruct {
    id: u32,
    value: String,
}

// Test error handling - try to load from invalid parquet data
#[test]
fn test_invalid_data_error() {
    let invalid_data = b"this is not valid parquet data";
    
    let result = SimpleStruct::load(invalid_data);
    assert!(result.is_err());
}

#[test]
fn test_roundtrip_consistency() {
    let original_items: Vec<SimpleStruct> = (0..50).map(|i| {
        SimpleStruct {
            id: i,
            value: format!("Value {}", i),
        }
    }).collect();
    
    // Serialize and deserialize multiple times to ensure consistency
    let mut current_items = original_items.clone();
    
    for _ in 0..3 {
        let bytes = SimpleStruct::dump(current_items.clone()).expect("Serialization failed");
        current_items = SimpleStruct::load(&bytes).expect("Deserialization failed");
        
        assert_eq!(current_items, original_items);
    }
}