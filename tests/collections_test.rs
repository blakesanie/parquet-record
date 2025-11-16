use parquet_record::ParquetSerialize;
use std::collections::{HashMap, HashSet};

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct StructWithCollections {
    id: u32,
    tags: Vec<String>,
    scores: Vec<i32>,
    metadata: HashMap<String, String>,
    unique_items: HashSet<String>,
}

#[test]
fn test_struct_with_collections() {
    use std::collections::HashSet;
    
    let mut metadata = HashMap::new();
    metadata.insert("key1".to_string(), "value1".to_string());
    metadata.insert("key2".to_string(), "value2".to_string());

    let mut unique_items = HashSet::new();
    unique_items.insert("item1".to_string());
    unique_items.insert("item2".to_string());
    
    let items = vec![
        StructWithCollections {
            id: 1,
            tags: vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()],
            scores: vec![10, 20, 30],
            metadata: {
                let mut m = HashMap::new();
                m.insert("key1".to_string(), "value1".to_string());
                m.insert("key2".to_string(), "value2".to_string());
                m
            },
            unique_items: {
                let mut s = HashSet::new();
                s.insert("item1".to_string());
                s.insert("item2".to_string());
                s
            },
        }
    ];

    // Serialize to parquet bytes
    let bytes = StructWithCollections::dump(items.clone()).expect("Serialization failed");
    
    // Deserialize from parquet bytes
    let loaded = StructWithCollections::load(&bytes).expect("Deserialization failed");
    
    assert_eq!(items, loaded);
}

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct SimpleVecStruct {
    id: u32,
    values: Vec<i32>,
}

#[test]
fn test_simple_vec_serialization() {
    let items = vec![
        SimpleVecStruct {
            id: 1,
            values: vec![1, 2, 3, 4, 5],
        },
        SimpleVecStruct {
            id: 2,
            values: vec![10, 20],
        },
    ];

    // Serialize to parquet bytes
    let bytes = SimpleVecStruct::dump(items.clone()).expect("Serialization failed");
    
    // Deserialize from parquet bytes
    let loaded = SimpleVecStruct::load(&bytes).expect("Deserialization failed");
    
    assert_eq!(items, loaded);
}

#[test]
fn test_empty_collections() {
    let items = vec![
        StructWithCollections {
            id: 1,
            tags: vec![],  // Empty vector
            scores: vec![], // Empty vector
            metadata: HashMap::new(),  // Empty map
            unique_items: HashSet::new(), // Empty set
        }
    ];

    // Serialize to parquet bytes
    let bytes = StructWithCollections::dump(items.clone()).expect("Serialization failed");
    
    // Deserialize from parquet bytes
    let loaded = StructWithCollections::load(&bytes).expect("Deserialization failed");
    
    assert_eq!(items, loaded);
}