use parquet_record::ParquetSerialize;

#[cfg(test)]
mod parquet_flat_tests {
    use super::*;

    // Test basic skip functionality
    #[derive(ParquetSerialize, Debug, Clone, PartialEq)]
    struct FlatTestStruct {
        id: u32,
        name: String,
        #[parquet_flat(skip)]
        metadata: String,  // This field should be skipped during serialization
        count: u64,
    }

    #[test]
    fn test_parquet_flat_skip() {
        let items = vec![
            FlatTestStruct {
                id: 1,
                name: "test1".to_string(),
                metadata: "meta1".to_string(),
                count: 100,
            },
            FlatTestStruct {
                id: 2,
                name: "test2".to_string(),
                metadata: "meta2".to_string(),
                count: 200,
            },
        ];

        // This should compile and run successfully, skipping the 'metadata' field
        let bytes = FlatTestStruct::dump(items.clone()).expect("Serialization failed");
        let loaded = FlatTestStruct::load(&bytes).expect("Deserialization failed");
        
        // Note: The loaded items will have default values for skipped fields
        // This test primarily ensures the macro compiles and runs correctly
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, 1);
        assert_eq!(loaded[0].name, "test1");
        assert_eq!(loaded[0].count, 100);
        assert_eq!(loaded[1].id, 2);
        assert_eq!(loaded[1].name, "test2");
        assert_eq!(loaded[1].count, 200);
    }

    // Test that without parquet_flat attributes, everything works normally
    #[derive(ParquetSerialize, Debug, Clone, PartialEq)]
    struct NormalStruct {
        id: u32,
        values: Vec<i32>,
    }

    #[test]
    fn test_normal_struct_still_works() {
        let items = vec![
            NormalStruct {
                id: 1,
                values: vec![1, 2, 3],
            },
            NormalStruct {
                id: 2,
                values: vec![4, 5, 6],
            },
        ];

        let bytes = NormalStruct::dump(items.clone()).expect("Serialization failed");
        let loaded = NormalStruct::load(&bytes).expect("Deserialization failed");
        
        assert_eq!(items, loaded);
    }

    // Test more complex skip scenario
    #[derive(ParquetSerialize, Debug, Clone, PartialEq)]
    struct ComplexSkipStruct {
        id: u32,
        #[parquet_flat(skip)]
        internal_data: String,
        values: Vec<i32>,
        #[parquet_flat(skip)]
        temp_field: u64,
        name: String,
    }

    #[test]
    fn test_complex_skip_struct() {
        let items = vec![
            ComplexSkipStruct {
                id: 1,
                internal_data: "secret".to_string(),
                values: vec![10, 20],
                temp_field: 999,
                name: "item1".to_string(),
            },
            ComplexSkipStruct {
                id: 2,
                internal_data: "hidden".to_string(),
                values: vec![30, 40, 50],
                temp_field: 888,
                name: "item2".to_string(),
            },
        ];

        let bytes = ComplexSkipStruct::dump(items.clone()).expect("Serialization failed");
        let loaded = ComplexSkipStruct::load(&bytes).expect("Deserialization failed");
        
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, 1);
        assert_eq!(loaded[0].values, vec![10, 20]);
        assert_eq!(loaded[0].name, "item1");
        assert_eq!(loaded[1].id, 2);
        assert_eq!(loaded[1].values, vec![30, 40, 50]);
        assert_eq!(loaded[1].name, "item2");
    }
}