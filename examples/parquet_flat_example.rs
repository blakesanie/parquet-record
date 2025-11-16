use parquet_record::ParquetSerialize;

#[derive(Debug, Clone, PartialEq)]
struct Address {
    street: String,
    city: String,
}

// Test a struct with manually flattened fields
#[derive(ParquetSerialize, Debug, Clone, PartialEq)]
struct Person {
    id: u32,
    name: String,
    addr_street: String,  // flattened address street
    addr_city: String,    // flattened address city
}

fn main() {
    println!("Testing manually flattened struct");
    
    let people = vec![
        Person {
            id: 1,
            name: "Alice".to_string(),
            addr_street: "123 Main St".to_string(),
            addr_city: "New York".to_string(),
        },
        Person {
            id: 2,
            name: "Bob".to_string(),
            addr_street: "456 Oak Ave".to_string(),
            addr_city: "Los Angeles".to_string(),
        },
    ];

    let bytes = Person::dump(people.clone()).expect("Serialization failed");
    let loaded = Person::load(&bytes).expect("Deserialization failed");
    
    println!("Original: {:?}", people);
    println!("Loaded: {:?}", loaded);
    assert_eq!(people, loaded);
    
    let schema = Person::arrow_schema();
    println!("Schema: {:?}", schema);
    
    println!("All tests passed!");
}