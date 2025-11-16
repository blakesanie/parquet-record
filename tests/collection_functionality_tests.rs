use parquet_record::ParquetSerialize;

#[cfg(test)]
mod collection_functionality_tests {
    use super::*;

    #[derive(ParquetSerialize, Debug, Clone, PartialEq)]
    struct VecTestStruct {
        id: u32,
        values: Vec<i32>,
    }

    #[test]
    fn test_vec_i32_functionality() {
        let items = vec![
            VecTestStruct {
                id: 1,
                values: vec![1, 2, 3, 4, 5],
            },
            VecTestStruct {
                id: 2,
                values: vec![10, 20],
            },
        ];

        let bytes = VecTestStruct::dump(items.clone()).expect("Serialization failed");
        let loaded = VecTestStruct::load(&bytes).expect("Deserialization failed");
        
        assert_eq!(items, loaded);
    }

    #[derive(ParquetSerialize, Debug, Clone, PartialEq)]
    struct VecStringTestStruct {
        id: u32,
        tags: Vec<String>,
    }

    #[test]
    fn test_vec_string_functionality() {
        let items = vec![
            VecStringTestStruct {
                id: 1,
                tags: vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()],
            },
            VecStringTestStruct {
                id: 2,
                tags: vec!["hello".to_string(), "world".to_string()],
            },
        ];

        let bytes = VecStringTestStruct::dump(items.clone()).expect("Serialization failed");
        let loaded = VecStringTestStruct::load(&bytes).expect("Deserialization failed");
        
        assert_eq!(items, loaded);
    }

    #[derive(ParquetSerialize, Debug, Clone, PartialEq)]
    struct HashSetTestStruct {
        id: u32,
        values: std::collections::HashSet<i32>,
    }

    #[test]
    fn test_hashset_i32_functionality() {
        use std::collections::HashSet;
        
        let set1: HashSet<i32> = [1, 2, 3].iter().cloned().collect();
        let set2: HashSet<i32> = [10, 20].iter().cloned().collect();

        let items = vec![
            HashSetTestStruct {
                id: 1,
                values: set1,
            },
            HashSetTestStruct {
                id: 2,
                values: set2,
            },
        ];

        let bytes = HashSetTestStruct::dump(items.clone()).expect("Serialization failed");
        let loaded = HashSetTestStruct::load(&bytes).expect("Deserialization failed");
        
        assert_eq!(items.len(), loaded.len());
        for i in 0..items.len() {
            assert_eq!(items[i].id, loaded[i].id);
            assert_eq!(items[i].values, loaded[i].values);
        }
    }

    #[derive(ParquetSerialize, Debug, Clone, PartialEq)]
    struct EmptyCollectionTestStruct {
        id: u32,
        empty_vec: Vec<i32>,
        empty_set: std::collections::HashSet<String>,
    }

    #[test]
    fn test_empty_collections_functionality() {
        use std::collections::HashSet;
        
        let items = vec![
            EmptyCollectionTestStruct {
                id: 1,
                empty_vec: vec![],  // Empty vector
                empty_set: HashSet::new(),  // Empty hashset
            },
        ];

        let bytes = EmptyCollectionTestStruct::dump(items.clone()).expect("Serialization failed");
        let loaded = EmptyCollectionTestStruct::load(&bytes).expect("Deserialization failed");
        
        assert_eq!(items.len(), loaded.len());
        assert_eq!(items[0].id, loaded[0].id);
        assert_eq!(items[0].empty_vec, loaded[0].empty_vec);
        assert_eq!(items[0].empty_set, loaded[0].empty_set);
    }
}