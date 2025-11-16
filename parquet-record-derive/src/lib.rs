use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, Ident, Type};

// Define the trait that will be implemented
#[proc_macro_derive(ParquetSerialize)]
pub fn parquet_serialize_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields_named) => &fields_named.named,
            Fields::Unnamed(_) => {
                return syn::Error::new_spanned(
                    &input,
                    "ParquetSerialize can only be derived for structs with named fields",
                )
                .to_compile_error()
                .into();
            }
            Fields::Unit => {
                return syn::Error::new_spanned(
                    &input,
                    "ParquetSerialize can only be derived for structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        Data::Enum(_) | Data::Union(_) => {
            return syn::Error::new_spanned(
                &input,
                "ParquetSerialize can only be derived for structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let field_definitions = fields.iter().map(|field| {
        let field_name = field.ident.as_ref();
        let field_type = &field.ty;
        (field_name, field_type)
    }).collect::<Vec<_>>();

    let arrow_schema_impl = generate_arrow_schema_impl(name, &field_definitions);
    let dump_impl = generate_dump_impl(name, &field_definitions);
    let load_impl = generate_load_impl(name, &field_definitions);

    let expanded = quote! {
        impl ParquetSerialize for #name {
            #arrow_schema_impl
            #dump_impl
            #load_impl
        }
    };

    TokenStream::from(expanded)
}



fn generate_arrow_schema_impl(_name: &Ident, fields: &[(Option<&Ident>, &Type)]) -> proc_macro2::TokenStream {
    let schema_fields = fields.iter().map(|(field_name, field_type)| {
        let field_name_str = field_name.unwrap().to_string();
        let arrow_data_type = rust_type_to_arrow_type(field_type);
        quote! {
            arrow::datatypes::Field::new(#field_name_str, #arrow_data_type, false)
        }
    });

    quote! {
        fn arrow_schema() -> std::sync::Arc<arrow::datatypes::Schema> {
            std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                #(#schema_fields),*
            ]))
        }
    }
}

fn generate_dump_impl(name: &Ident, fields: &[(Option<&Ident>, &Type)]) -> proc_macro2::TokenStream {
    let field_extraction = fields.iter().map(|(field_name, field_type)| {
        let field_name_ident = field_name.unwrap();
        
        if is_vec_type(field_type) {
            if let Some(element_type) = get_collection_element_type(field_type) {
                if let Type::Path(element_path) = element_type {
                    let type_name = element_path.path.segments.last().unwrap().ident.to_string();
                    
                    // Generate inline list array construction based on the element type
                    match type_name.as_str() {
                        "i8" => {
                            quote! {
                                let mut offsets = vec![0i32];
                                let mut values = Vec::new();
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    values.extend_from_slice(&item.#field_name_ident);
                                }
                                
                                let value_array = ::arrow::array::Int8Array::from(values);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Int8, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                            }
                        },
                        "i16" => {
                            quote! {
                                let mut offsets = vec![0i32];
                                let mut values = Vec::new();
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    values.extend_from_slice(&item.#field_name_ident);
                                }
                                
                                let value_array = ::arrow::array::Int16Array::from(values);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Int16, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                            }
                        },
                        "i32" => {
                            quote! {
                                let mut offsets = vec![0i32];
                                let mut values = Vec::new();
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    values.extend_from_slice(&item.#field_name_ident);
                                }
                                
                                let value_array = ::arrow::array::Int32Array::from(values);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Int32, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                            }
                        },
                        "i64" => {
                            quote! {
                                let mut offsets = vec![0i32];
                                let mut values = Vec::new();
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    values.extend_from_slice(&item.#field_name_ident);
                                }
                                
                                let value_array = ::arrow::array::Int64Array::from(values);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Int64, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                            }
                        },
                        "u8" => {
                            quote! {
                                let mut offsets = vec![0i32];
                                let mut values = Vec::new();
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    values.extend_from_slice(&item.#field_name_ident);
                                }
                                
                                let value_array = ::arrow::array::UInt8Array::from(values);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::UInt8, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                            }
                        },
                        "u16" => {
                            quote! {
                                let mut offsets = vec![0i32];
                                let mut values = Vec::new();
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    values.extend_from_slice(&item.#field_name_ident);
                                }
                                
                                let value_array = ::arrow::array::UInt16Array::from(values);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::UInt16, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                            }
                        },
                        "u32" => {
                            quote! {
                                let mut offsets = vec![0i32];
                                let mut values = Vec::new();
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    values.extend_from_slice(&item.#field_name_ident);
                                }
                                
                                let value_array = ::arrow::array::UInt32Array::from(values);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::UInt32, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                            }
                        },
                        "u64" => {
                            quote! {
                                let mut offsets = vec![0i32];
                                let mut values = Vec::new();
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    values.extend_from_slice(&item.#field_name_ident);
                                }
                                
                                let value_array = ::arrow::array::UInt64Array::from(values);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::UInt64, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                            }
                        },
                        "f32" => {
                            quote! {
                                let mut offsets = vec![0i32];
                                let mut values = Vec::new();
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    values.extend_from_slice(&item.#field_name_ident);
                                }
                                
                                let value_array = ::arrow::array::Float32Array::from(values);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Float32, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                            }
                        },
                        "f64" => {
                            quote! {
                                let mut offsets = vec![0i32];
                                let mut values = Vec::new();
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    values.extend_from_slice(&item.#field_name_ident);
                                }
                                
                                let value_array = ::arrow::array::Float64Array::from(values);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Float64, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                            }
                        },
                        "bool" => {
                            quote! {
                                let mut offsets = vec![0i32];
                                let mut values = Vec::new();
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    values.extend_from_slice(&item.#field_name_ident);
                                }
                                
                                let value_array = ::arrow::array::BooleanArray::from(values);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Boolean, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                            }
                        },
                        "String" => {
                            quote! {
                                let mut all_strings: Vec<String> = Vec::new();
                                let mut offsets = vec![0i32];
                                
                                let mut current_offset = 0i32;
                                for item in &items {
                                    current_offset += item.#field_name_ident.len() as i32;
                                    offsets.push(current_offset);
                                    for s in &item.#field_name_ident {
                                        all_strings.push(s.clone());
                                    }
                                }
                                
                                let string_array = ::arrow::array::StringArray::from(all_strings);
                                let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Utf8, false));
                                let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                                
                                let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(string_array), None);
                            }
                        },
                        _ => {
                            return quote! {
                                compile_error!("Unsupported element type for Vec");
                            };
                        }
                    }
                } else {
                    quote! {
                        compile_error!("Could not determine element type for Vec");
                    }
                }
            } else {
                quote! {
                    compile_error!("Could not determine element type for Vec");
                }
            }
        } else if is_hashset_type(field_type) {
            // For HashSet<T>, convert to Vec and then create ListArray
            if let Some(element_type) = get_collection_element_type(field_type) {
                if let Type::Path(element_path) = element_type {
                    let type_name = element_path.path.segments.last().unwrap().ident.to_string();
                    
                    // Generate inline list array construction based on the element type
                    match type_name.as_str() {
                        "i8" => quote! {
                            let mut offsets = vec![0i32];
                            let mut values = Vec::new();
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<i8> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                values.extend_from_slice(&vec_from_set);
                            }
                            
                            let value_array = ::arrow::array::Int8Array::from(values);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Int8, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new((offsets).into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                        },
                        "i16" => quote! {
                            let mut offsets = vec![0i32];
                            let mut values = Vec::new();
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<i16> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                values.extend_from_slice(&vec_from_set);
                            }
                            
                            let value_array = ::arrow::array::Int16Array::from(values);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Int16, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new((offsets).into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                        },
                        "i32" => quote! {
                            let mut offsets = vec![0i32];
                            let mut values = Vec::new();
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<i32> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                values.extend_from_slice(&vec_from_set);
                            }
                            
                            let value_array = ::arrow::array::Int32Array::from(values);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Int32, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new((offsets).into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                        },
                        "i64" => quote! {
                            let mut offsets = vec![0i32];
                            let mut values = Vec::new();
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<i64> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                values.extend_from_slice(&vec_from_set);
                            }
                            
                            let value_array = ::arrow::array::Int64Array::from(values);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Int64, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new((offsets).into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                        },
                        "u8" => quote! {
                            let mut offsets = vec![0i32];
                            let mut values = Vec::new();
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<u8> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                values.extend_from_slice(&vec_from_set);
                            }
                            
                            let value_array = ::arrow::array::UInt8Array::from(values);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::UInt8, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new((offsets).into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                        },
                        "u16" => quote! {
                            let mut offsets = vec![0i32];
                            let mut values = Vec::new();
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<u16> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                values.extend_from_slice(&vec_from_set);
                            }
                            
                            let value_array = ::arrow::array::UInt16Array::from(values);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::UInt16, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new((offsets).into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                        },
                        "u32" => quote! {
                            let mut offsets = vec![0i32];
                            let mut values = Vec::new();
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<u32> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                values.extend_from_slice(&vec_from_set);
                            }
                            
                            let value_array = ::arrow::array::UInt32Array::from(values);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::UInt32, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new((offsets).into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                        },
                        "u64" => quote! {
                            let mut offsets = vec![0i32];
                            let mut values = Vec::new();
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<u64> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                values.extend_from_slice(&vec_from_set);
                            }
                            
                            let value_array = ::arrow::array::UInt64Array::from(values);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::UInt64, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new((offsets).into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                        },
                        "f32" => quote! {
                            let mut offsets = vec![0i32];
                            let mut values = Vec::new();
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<f32> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                values.extend_from_slice(&vec_from_set);
                            }
                            
                            let value_array = ::arrow::array::Float32Array::from(values);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Float32, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new((offsets).into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                        },
                        "f64" => quote! {
                            let mut offsets = vec![0i32];
                            let mut values = Vec::new();
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<f64> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                values.extend_from_slice(&vec_from_set);
                            }
                            
                            let value_array = ::arrow::array::Float64Array::from(values);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Float64, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new((offsets).into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                        },
                        "bool" => quote! {
                            let mut offsets = vec![0i32];
                            let mut values = Vec::new();
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<bool> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                values.extend_from_slice(&vec_from_set);
                            }
                            
                            let value_array = ::arrow::array::BooleanArray::from(values);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Boolean, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new((offsets).into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(value_array), None);
                        },
                        "String" => quote! {
                            let mut all_strings: Vec<String> = Vec::new();
                            let mut offsets = vec![0i32];
                            
                            let mut current_offset = 0i32;
                            for item in &items {
                                let vec_from_set: Vec<String> = item.#field_name_ident.iter().cloned().collect();
                                current_offset += vec_from_set.len() as i32;
                                offsets.push(current_offset);
                                for s in &vec_from_set {
                                    all_strings.push(s.clone());
                                }
                            }
                            
                            let string_array = ::arrow::array::StringArray::from(all_strings);
                            let field = std::sync::Arc::new(::arrow::datatypes::Field::new("item", ::arrow::datatypes::DataType::Utf8, false));
                            let offsets = ::arrow::buffer::OffsetBuffer::new(offsets.into());
                            
                            let #field_name_ident = ::arrow::array::ListArray::new(field, offsets, std::sync::Arc::new(string_array), None);
                        },
                        _ => {
                            return quote! {
                                compile_error!("Unsupported element type for HashSet");
                            };
                        }
                    }
                } else {
                    quote! {
                        compile_error!("Could not determine element type for HashSet");
                    }
                }
            } else {
                quote! {
                    compile_error!("Could not determine element type for HashSet");
                }
            }
        } else if is_map_type(field_type) {
            // For map types, fallback to JSON string for now
            // The MapArray implementation is complex and has compatibility issues
            quote! {
                let #field_name_ident: Vec<String> = items.iter().map(|item| ::serde_json::to_string(&item.#field_name_ident).unwrap()).collect();
            }
        } else if is_string_type(field_type) {
            // For string types
            quote! {
                let #field_name_ident: Vec<String> = items.iter().map(|item| item.#field_name_ident.clone()).collect();
            }
        } else {
            // For primitive types
            quote! {
                let #field_name_ident: Vec<_> = items.iter().map(|item| item.#field_name_ident).collect();
            }
        }
    });

    let columns_creation = fields.iter().enumerate().map(|(_i, (field_name, field_type))| {
        let field_name_ident = field_name.unwrap();
        
        if is_vec_type(field_type) || is_hashset_type(field_type) {
            // For collection types, the value was already built as an array
            quote! {
                std::sync::Arc::new(#field_name_ident) as arrow::array::ArrayRef
            }
        } else {
            // For other types, create the array normally
            let arrow_array_type = rust_type_to_arrow_array_type(field_type);
            quote! {
                std::sync::Arc::new(#arrow_array_type::from(#field_name_ident)) as arrow::array::ArrayRef
            }
        }
    });

    quote! {
        fn dump(items: Vec<#name>) -> Result<Vec<u8>, parquet::errors::ParquetError> {
            let schema = Self::arrow_schema();
            let mut buf = Vec::new();
            let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(&mut buf, schema.clone(), None)?;

            #(#field_extraction)*

            let columns: Vec<arrow::array::ArrayRef> = vec![
                #(#columns_creation),*
            ];

            let batch = arrow::record_batch::RecordBatch::try_new(schema, columns)?;
            writer.write(&batch)?;
            writer.close()?;
            Ok(buf)
        }
    }
}

fn generate_load_impl(name: &Ident, fields: &[(Option<&Ident>, &Type)]) -> proc_macro2::TokenStream {
    // Generate the column access code
    let column_access = fields.iter().enumerate().map(|(i, (field_name, field_type))| {
        let field_name_ident = field_name.unwrap();
        let field_name_str = field_name_ident.to_string();
        let arrow_array_type = rust_type_to_arrow_array_type(field_type);
        quote! {
            let #field_name_ident = batch
                .column(#i)
                .as_any()
                .downcast_ref::<#arrow_array_type>()
                .ok_or_else(|| parquet::errors::ParquetError::General(format!("column '{}' is not an {}", #field_name_str, stringify!(#arrow_array_type))))?;
        }
    });

    // Generate the struct construction code for each row
    let struct_fields = fields.iter().enumerate().map(|(_i, (field_name, field_type))| {
        let field_name_ident = field_name.unwrap();
        
        if is_vec_type(field_type) {
            if let Some(element_type) = get_collection_element_type(field_type) {
                if let Type::Path(element_path) = element_type {
                    let type_name = element_path.path.segments.last().unwrap().ident.to_string();
                    
                    // Generate extraction code based on the element type
                    match type_name.as_str() {
                        "i8" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Int8Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx)).collect()
                            }
                        },
                        "i16" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Int16Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx)).collect()
                            }
                        },
                        "i32" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Int32Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx)).collect()
                            }
                        },
                        "i64" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Int64Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx)).collect()
                            }
                        },
                        "u8" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::UInt8Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx)).collect()
                            }
                        },
                        "u16" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::UInt16Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx)).collect()
                            }
                        },
                        "u32" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::UInt32Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx)).collect()
                            }
                        },
                        "u64" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::UInt64Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx)).collect()
                            }
                        },
                        "f32" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Float32Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx)).collect()
                            }
                        },
                        "f64" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Float64Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx)).collect()
                            }
                        },
                        "bool" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::BooleanArray>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx)).collect()
                            }
                        },
                        "String" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::StringArray>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                (start..end).map(|idx| values.value(idx).to_string()).collect()
                            }
                        },
                        _ => {
                            quote! {
                                compile_error!("Unsupported element type for Vec")
                            }
                        }
                    }
                } else {
                    quote! {
                        compile_error!("Could not determine element type for Vec")
                    }
                }
            } else {
                quote! {
                    compile_error!("Could not determine element type for Vec")
                }
            }
        } else if is_hashset_type(field_type) {
            if let Some(element_type) = get_collection_element_type(field_type) {
                if let Type::Path(element_path) = element_type {
                    let type_name = element_path.path.segments.last().unwrap().ident.to_string();
                    
                    // Generate extraction code based on the element type for HashSet
                    match type_name.as_str() {
                        "i8" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Int8Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx));
                                }
                                set
                            }
                        },
                        "i16" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Int16Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx));
                                }
                                set
                            }
                        },
                        "i32" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Int32Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx));
                                }
                                set
                            }
                        },
                        "i64" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Int64Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx));
                                }
                                set
                            }
                        },
                        "u8" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::UInt8Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx));
                                }
                                set
                            }
                        },
                        "u16" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::UInt16Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx));
                                }
                                set
                            }
                        },
                        "u32" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::UInt32Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx));
                                }
                                set
                            }
                        },
                        "u64" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::UInt64Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx));
                                }
                                set
                            }
                        },
                        "f32" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Float32Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx));
                                }
                                set
                            }
                        },
                        "f64" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::Float64Array>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx));
                                }
                                set
                            }
                        },
                        "bool" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::BooleanArray>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx));
                                }
                                set
                            }
                        },
                        "String" => quote! {
                            {
                                let list_array = #field_name_ident.value(i);
                                let values = list_array.as_any().downcast_ref::<::arrow::array::StringArray>().ok_or_else(|| parquet::errors::ParquetError::General("Failed to downcast list values".to_string()))?;
                                let (start, end) = (#field_name_ident.value_offsets()[i] as usize, #field_name_ident.value_offsets()[i+1] as usize);
                                let mut set = std::collections::HashSet::new();
                                for idx in start..end {
                                    set.insert(values.value(idx).to_string());
                                }
                                set
                            }
                        },
                        _ => {
                            quote! {
                                compile_error!("Unsupported element type for HashSet")
                            }
                        }
                    }
                } else {
                    quote! {
                        compile_error!("Could not determine element type for HashSet")
                    }
                }
            } else {
                quote! {
                    compile_error!("Could not determine element type for HashSet")
                }
            }
        } else if is_map_type(field_type) {
            // For map types, deserialize from JSON string
            quote! {
                #field_name_ident: ::serde_json::from_str(#field_name_ident.value(i)).map_err(|e| parquet::errors::ParquetError::General(format!("Failed to deserialize map: {}", e)))?
            }
        } else if is_string_type(field_type) {
            // For string types
            quote! {
                #field_name_ident: #field_name_ident.value(i).to_string()
            }
        } else {
            // For primitive types
            quote! {
                #field_name_ident: #field_name_ident.value(i)
            }
        }
    });

    quote! {
        fn load(buf: &[u8]) -> Result<Vec<#name>, parquet::errors::ParquetError> {
            use std::sync::Arc;
            use arrow::array::*;
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            
            let bytes = ::bytes::Bytes::copy_from_slice(buf);
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
            let mut reader = builder.build()?;

            let mut items = vec![];
            
            for batch_result in reader {
                let batch = batch_result?;
                #(#column_access)*

                for i in 0..batch.num_rows() {
                    items.push(#name {
                        #(#struct_fields),*
                    });
                }
            }

            Ok(items)
        }
    }
}

fn is_string_type(field_type: &Type) -> bool {
    match field_type {
        Type::Path(type_path) => {
            let path = &type_path.path;
            let type_name = path.segments.last().unwrap().ident.to_string();
            type_name == "String"
        }
        _ => false,
    }
}

fn is_vec_type(field_type: &Type) -> bool {
    match field_type {
        Type::Path(type_path) => {
            let path = &type_path.path;
            if path.segments.is_empty() {
                return false;
            }
            
            let type_name = path.segments.last().unwrap().ident.to_string();
            type_name == "Vec"
        }
        _ => false,
    }
}

fn is_hashset_type(field_type: &Type) -> bool {
    match field_type {
        Type::Path(type_path) => {
            let path = &type_path.path;
            if path.segments.is_empty() {
                return false;
            }
            
            let type_name = path.segments.last().unwrap().ident.to_string();
            matches!(type_name.as_str(), "HashSet" | "BTreeSet")
        }
        _ => false,
    }
}

fn is_map_type(field_type: &Type) -> bool {
    match field_type {
        Type::Path(type_path) => {
            let path = &type_path.path;
            if path.segments.is_empty() {
                return false;
            }
            
            let type_name = path.segments.last().unwrap().ident.to_string();
            matches!(type_name.as_str(), "HashMap" | "BTreeMap")
        }
        _ => false,
    }
}



fn get_collection_element_type(field_type: &Type) -> Option<&Type> {
    match field_type {
        Type::Path(type_path) => {
            let path = &type_path.path;
            if path.segments.is_empty() {
                return None;
            }
            
            let segment = &path.segments.last().unwrap();
            if let syn::PathArguments::AngleBracketed(ref args) = segment.arguments {
                if !args.args.is_empty() {
                    // For HashMap, we take the value type (second argument) as main type
                    if segment.ident == "HashMap" || segment.ident == "BTreeMap" {
                        if args.args.len() >= 2 {
                            if let syn::GenericArgument::Type(ref ty) = args.args[1] {
                                return Some(ty);
                            }
                        }
                    } else {
                        // For other collections, take the first type parameter
                        if let syn::GenericArgument::Type(ref ty) = args.args[0] {
                            return Some(ty);
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn rust_type_to_arrow_type(field_type: &Type) -> proc_macro2::TokenStream {
    if is_vec_type(field_type) {
        // Vec<T> becomes List<T>
        if let Some(element_type) = get_collection_element_type(field_type) {
            let element_arrow_type = rust_type_to_arrow_type_single(element_type);
            return quote! { 
                arrow::datatypes::DataType::List(std::sync::Arc::new(
                    arrow::datatypes::Field::new("item", #element_arrow_type, false)
                ))
            };
        }
    } else if is_hashset_type(field_type) {
        // HashSet<T> becomes List<T> (since both have same underlying concept)
        if let Some(element_type) = get_collection_element_type(field_type) {
            let element_arrow_type = rust_type_to_arrow_type_single(element_type);
            return quote! { 
                arrow::datatypes::DataType::List(std::sync::Arc::new(
                    arrow::datatypes::Field::new("item", #element_arrow_type, false)
                ))
            };
        }
    } else if is_map_type(field_type) {
        // For now, keep map types as JSON string until we implement proper MapArray support
        return quote! { arrow::datatypes::DataType::Utf8 };
    }
    
    rust_type_to_arrow_type_single(field_type)
}

fn rust_type_to_arrow_type_single(field_type: &Type) -> proc_macro2::TokenStream {
    match field_type {
        Type::Path(type_path) => {
            let path = &type_path.path;
            let type_name = path.segments.last().unwrap().ident.to_string();
            
            match type_name.as_str() {
                "i8" => quote! { ::arrow::datatypes::DataType::Int8 },
                "i16" => quote! { ::arrow::datatypes::DataType::Int16 },
                "i32" => quote! { ::arrow::datatypes::DataType::Int32 },
                "i64" => quote! { ::arrow::datatypes::DataType::Int64 },
                "u8" => quote! { ::arrow::datatypes::DataType::UInt8 },
                "u16" => quote! { ::arrow::datatypes::DataType::UInt16 },
                "u32" => quote! { ::arrow::datatypes::DataType::UInt32 },
                "u64" => quote! { ::arrow::datatypes::DataType::UInt64 },
                "f32" => quote! { ::arrow::datatypes::DataType::Float32 },
                "f64" => quote! { ::arrow::datatypes::DataType::Float64 },
                "bool" => quote! { ::arrow::datatypes::DataType::Boolean },
                "String" => quote! { ::arrow::datatypes::DataType::Utf8 },
                _ => {
                    // For now, return an error for unsupported types
                    quote! { 
                        compile_error!("Unsupported type for parquet serialization: {}. Only primitive types, String, Vec, and HashSet are supported.", #type_name) 
                    }
                }
            }
        }
        _ => {
            quote! { compile_error!("Unsupported type for parquet serialization. Only primitive types, String, Vec, and HashSet are supported.") }
        }
    }
}

fn rust_type_to_arrow_array_type(field_type: &Type) -> proc_macro2::TokenStream {
    if is_vec_type(field_type) || is_hashset_type(field_type) {
        // For Vec<T> and HashSet<T>, return ListArray
        return quote! { arrow::array::ListArray };
    } else if is_map_type(field_type) {
        // For now, map types remain as StringArray until MapArray is properly implemented
        return quote! { arrow::array::StringArray };
    }
    
    rust_type_to_arrow_array_type_single(field_type)
}

fn rust_type_to_arrow_array_type_single(field_type: &Type) -> proc_macro2::TokenStream {
    match field_type {
        Type::Path(type_path) => {
            let path = &type_path.path;
            let type_name = path.segments.last().unwrap().ident.to_string();
            
            match type_name.as_str() {
                "i8" => quote! { ::arrow::array::Int8Array },
                "i16" => quote! { ::arrow::array::Int16Array },
                "i32" => quote! { ::arrow::array::Int32Array },
                "i64" => quote! { ::arrow::array::Int64Array },
                "u8" => quote! { ::arrow::array::UInt8Array },
                "u16" => quote! { ::arrow::array::UInt16Array },
                "u32" => quote! { ::arrow::array::UInt32Array },
                "u64" => quote! { ::arrow::array::UInt64Array },
                "f32" => quote! { ::arrow::array::Float32Array },
                "f64" => quote! { ::arrow::array::Float64Array },
                "bool" => quote! { ::arrow::array::BooleanArray },
                "String" => quote! { ::arrow::array::StringArray },
                _ => {
                    quote! { 
                        compile_error!("Unsupported type for parquet serialization: {}. Only primitive types, String, Vec, and HashSet are supported.", #type_name) 
                    }
                }
            }
        }
        _ => {
            quote! { compile_error!("Unsupported type for parquet serialization. Only primitive types, String, Vec, and HashSet are supported.") }
        }
    }
}

// Add a practical attribute macro for parquet_flat functionality
#[proc_macro_attribute] 
pub fn parquet_flat(_args: TokenStream, input: TokenStream) -> TokenStream {
    // For a practical implementation, the parquet_flat macro can be used to mark
    // that the struct contains flattened fields. The actual flattening would be
    // done by the user defining fields with appropriate prefixed names.
    // This macro will just return the input unchanged but can be used to indicate
    // the intent to flatten nested fields.
    TokenStream::from(input)
}