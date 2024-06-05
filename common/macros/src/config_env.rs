use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DataStruct, DeriveInput, Fields, FieldsNamed};

pub fn derive_env_keys_inner(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(input).unwrap();
    let name = &ast.ident;
    let fields = match ast.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => named,
        _ => unimplemented!(),
    };
    let mut vec_quote = quote!(vec![].into_iter());
    fields.iter().for_each(|field| {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;
        let env_keys = quote! { <#field_type as crate::EnvKeys>::env_keys() };
        let env_keys = quote! {
            if #env_keys.is_empty() {
                 vec![stringify!(#field_name).to_uppercase()]
            } else {
                #env_keys.iter().map(|key| format!("{}.{}", stringify!(#field_name).to_uppercase(), key)).collect::<Vec<String>>()
            }
        };
        vec_quote = quote! { #vec_quote.chain(#env_keys.into_iter()) };
    });
    let expanded = quote! {
        impl crate::EnvKeys for #name {
            fn env_keys() -> Vec<String> {
                #vec_quote.collect()
            }
        }
    };
    expanded.into()
}
