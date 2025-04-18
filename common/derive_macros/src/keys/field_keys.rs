use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DataStruct, DeriveInput, Fields, FieldsNamed};

pub fn derive_field_keys_inner(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(input).unwrap();
    let name = &ast.ident;
    let fields = match ast.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => named,
        _ => unimplemented!(),
    };
    let mut push_into_buf = proc_macro2::TokenStream::new();
    fields.iter().for_each(|field| {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;
        push_into_buf.extend(quote! {
            {
                let key = stringify!(#field_name).to_uppercase();
                let sub_keys = <#field_type as derive_traits::FieldKeys>::field_keys();
                if sub_keys.is_empty() {
                    buf.push(key);
                } else {
                    sub_keys.into_iter().for_each(|sk| buf.push(format!("{key}.{sk}")));
                }
            }
        });
    });
    let expanded = quote! {
        impl derive_traits::FieldKeys for #name {
            fn field_keys() -> Vec<String> {
                let mut buf: Vec<String> = Vec::new();
                #push_into_buf
                buf
            }
        }
    };
    expanded.into()
}
