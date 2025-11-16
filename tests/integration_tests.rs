use parquet_record::ParquetSerialize;

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct TestStruct {
    id: u32,
    name: String,
    value: f64,
    active: bool,
}

#[test]
fn test_parquet_serialization() {
    let items = vec![
        TestStruct {
            id: 1,
            name: "Test 1".to_string(),
            value: 3.14,
            active: true,
        },
        TestStruct {
            id: 2,
            name: "Test 2".to_string(),
            value: 2.71,
            active: false,
        },
    ];

    // Serialize to parquet bytes
    let bytes = TestStruct::dump(items.clone()).expect("Serialization failed");
    
    // Deserialize from parquet bytes
    let loaded = TestStruct::load(&bytes).expect("Deserialization failed");
    
    assert_eq!(items, loaded);
}

#[test]
fn test_empty_vector() {
    let items: Vec<TestStruct> = vec![];
    
    // Serialize empty vector
    let bytes = TestStruct::dump(items.clone()).expect("Serialization failed");
    
    // Deserialize from parquet bytes
    let loaded = TestStruct::load(&bytes).expect("Deserialization failed");
    
    assert_eq!(items, loaded);
}

#[test]
fn test_single_item() {
    let items = vec![TestStruct {
        id: 42,
        name: "Single".to_string(),
        value: 1.23,
        active: true,
    }];

    // Serialize single item
    let bytes = TestStruct::dump(items.clone()).expect("Serialization failed");
    
    // Deserialize from parquet bytes
    let loaded = TestStruct::load(&bytes).expect("Deserialization failed");
    
    assert_eq!(items, loaded);
}

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct AllTypesStruct {
    int8_field: i8,
    int16_field: i16,
    int32_field: i32,
    int64_field: i64,
    uint8_field: u8,
    uint16_field: u16,
    uint32_field: u32,
    uint64_field: u64,
    float32_field: f32,
    float64_field: f64,
    bool_field: bool,
    string_field: String,
}

#[test]
fn test_all_supported_types() {
    let items = vec![
        AllTypesStruct {
            int8_field: -128,
            int16_field: -32768,
            int32_field: -2147483648,
            int64_field: -9223372036854775808,
            uint8_field: 255,
            uint16_field: 65535,
            uint32_field: 4294967295,
            uint64_field: 18446744073709551615,
            float32_field: 3.14159,
            float64_field: 2.718281828,
            bool_field: true,
            string_field: "Hello, World!".to_string(),
        },
        AllTypesStruct {
            int8_field: 127,
            int16_field: 32767,
            int32_field: 2147483647,
            int64_field: 9223372036854775807,
            uint8_field: 0,
            uint16_field: 0,
            uint32_field: 0,
            uint64_field: 0,
            float32_field: -1.414,
            float64_field: -1.732,
            bool_field: false,
            string_field: "Rust is awesome!".to_string(),
        },
    ];

    // Serialize to parquet bytes
    let bytes = AllTypesStruct::dump(items.clone()).expect("Serialization failed");
    
    // Deserialize from parquet bytes
    let loaded = AllTypesStruct::load(&bytes).expect("Deserialization failed");
    
    assert_eq!(items, loaded);
}

#[test]
fn test_large_dataset() {
    let items: Vec<TestStruct> = (0..100).map(|i| {  // Using smaller dataset for faster tests
        TestStruct {
            id: i,
            name: format!("Item {}", i),
            value: (i as f64) * 1.5,
            active: i % 2 == 0,
        }
    }).collect();

    // Serialize to parquet bytes
    let bytes = TestStruct::dump(items.clone()).expect("Serialization failed");
    
    // Deserialize from parquet bytes
    let loaded = TestStruct::load(&bytes).expect("Deserialization failed");
    
    assert_eq!(items, loaded);
}

#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct Edge {
    way_id: i64,
    index: u16,
    start_node_index: u16,
    end_node_index: u16,
    start_bearing: u8,
    end_bearing: u8,
}

#[test]
fn test_edge_serialization() {
    // Test the exact example from the original problem
    let edges = vec![
        Edge {
            way_id: 12345,
            index: 1,
            start_node_index: 10,
            end_node_index: 20,
            start_bearing: 45,
            end_bearing: 90,
        },
        Edge {
            way_id: 67890,
            index: 2,
            start_node_index: 30,
            end_node_index: 40,
            start_bearing: 180,
            end_bearing: 200,
        },
    ];

    // Serialize to parquet bytes
    let bytes = Edge::dump(edges.clone()).expect("Serialization failed");
    
    // Deserialize from parquet bytes
    let loaded = Edge::load(&bytes).expect("Deserialization failed");
    
    assert_eq!(edges, loaded);
}