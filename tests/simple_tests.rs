use parquet_record::ParquetSerialize;

#[cfg(test)]
mod simple_tests {
    use super::*;

    #[derive(ParquetSerialize, Debug, Clone, PartialEq)]
    struct SimpleStruct {
        id: u32,
        name: String,
        value: i64,
    }

    #[test]
    fn test_simple_struct() {
        let items = vec![
            SimpleStruct {
                id: 1,
                name: "Test".to_string(),
                value: 123,
            },
        ];

        let bytes = SimpleStruct::dump(items.clone()).expect("Serialization failed");
        let loaded = SimpleStruct::load(&bytes).expect("Deserialization failed");
        
        assert_eq!(items, loaded);
    }
}