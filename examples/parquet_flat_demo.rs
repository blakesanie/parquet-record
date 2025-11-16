use parquet_record::{ParquetSerialize, parquet_flat};

#[derive(Debug, Clone, PartialEq)]
struct Address {
    street: String,
    city: String,
    country: String,
}

// Example of using parquet_flat as a documentation marker
// The actual flattening is done by naming fields with prefixes
#[parquet_flat]  // indicates this struct uses flattened fields
#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct PersonWithAddress {
    id: u32,
    name: String,
    addr_street: String,  // flattened from Address.street
    addr_city: String,    // flattened from Address.city  
    addr_country: String, // flattened from Address.country
}

fn main() {
    println!("Testing parquet_flat functionality...");
    
    let people = vec![
        PersonWithAddress {
            id: 1,
            name: "Alice".to_string(),
            addr_street: "123 Main St".to_string(),
            addr_city: "New York".to_string(),
            addr_country: "USA".to_string(),
        },
        PersonWithAddress {
            id: 2,
            name: "Bob".to_string(),
            addr_street: "456 Oak Ave".to_string(),
            addr_city: "Los Angeles".to_string(),
            addr_country: "USA".to_string(),
        },
    ];

    // Serialize to parquet
    let bytes = PersonWithAddress::dump(people.clone()).expect("Serialization failed");
    println!("Successfully serialized {} records", people.len());
    
    // Deserialize from parquet
    let loaded = PersonWithAddress::load(&bytes).expect("Deserialization failed");
    println!("Successfully loaded {} records", loaded.len());
    
    // Verify data integrity
    assert_eq!(people, loaded);
    println!("Data integrity verified: original == loaded");
    
    // Show schema
    let schema = PersonWithAddress::arrow_schema();
    println!("\nParquet Schema:");
    for field in schema.fields() {
        println!("  {}: {:?}", field.name(), field.data_type());
    }
    
    // Test with different field types
    #[parquet_flat]
    #[derive(ParquetSerialize, Debug, Clone, PartialEq)]
    struct ComplexFlattened {
        id: u64,
        enabled: bool,
        score: f64,
        tags: Vec<String>,
        counts: Vec<u32>,
    }
    
    let complex_data = vec![
        ComplexFlattened {
            id: 100,
            enabled: true,
            score: 95.5,
            tags: vec!["important".to_string(), "new".to_string()],
            counts: vec![10, 20, 30],
        },
        ComplexFlattened {
            id: 200,
            enabled: false,
            score: 87.2,
            tags: vec!["old".to_string()],
            counts: vec![5],
        },
    ];
    
    let complex_bytes = ComplexFlattened::dump(complex_data.clone()).expect("Complex serialization failed");
    let complex_loaded = ComplexFlattened::load(&complex_bytes).expect("Complex deserialization failed");
    
    assert_eq!(complex_data, complex_loaded);
    println!("\nComplex flattened struct test: PASSED");
    
    println!("\nAll parquet_flat tests passed!");
    println!("The macro allows manual field flattening with prefixed field names.");
}