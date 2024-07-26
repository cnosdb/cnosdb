mod parse;

use parse::{parse_error_code_enum, EnumInfo, FieldContainer};
use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, ToTokens};
use syn::{Data, Fields, LitStr};

type MultiSynResult<T> = std::result::Result<T, Vec<syn::Error>>;

const fn default_code() -> usize {
    0
}
const fn default_mode_code() -> &'static str {
    "00"
}

struct ErrorCodeImpl<'a>(&'a EnumInfo);

impl<'a> ToTokens for ErrorCodeImpl<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let enum_name = &self.0.name;
        let mod_code = &self.0.mod_code;

        let mut match_arms = Vec::with_capacity(self.0.variants.len());
        let mut get_error_code_methods = Vec::with_capacity(self.0.variants.len());
        for variant in self.0.variants.iter() {
            let variant_name = &variant.name;
            let arm = ErrorCodeMatchArm {
                field_container: variant,
                mod_code,
                pattern_ident: &quote! {#enum_name::#variant_name},
            };
            match_arms.push(quote! { #arm });
            if variant.code.is_some() {
                // If variant has #[error_code(code = 0)]
                let method =
                    GetErrorCodeMethod::new(&variant_name.to_string(), arm.normalized_code());
                get_error_code_methods.push(quote! { #method });
            }
        }

        let gen = quote! {
            impl ErrorCode for #enum_name {
                fn code(&self) -> &'static str {
                    match self {
                        #(#match_arms),*
                    }
                }
                fn message(&self) -> String {
                    self.to_string()
                }
            }
        };
        tokens.extend(gen);

        if !match_arms.is_empty() {
            let gen = quote! {
                impl #enum_name {
                    #(#get_error_code_methods)*
                }
            };
            tokens.extend(gen);
        }
    }
}

struct ErrorCodeMatchArm<'a> {
    mod_code: &'a LitStr,
    field_container: &'a FieldContainer,
    pattern_ident: &'a dyn ToTokens,
}

impl ErrorCodeMatchArm<'_> {
    fn normalized_code(&self) -> String {
        use std::fmt::Write;

        match self.field_container.code.as_ref() {
            Some(c) => {
                let code: u64 = c.code_num.base10_parse().unwrap();
                assert!(code > 0 && code < 10_000);

                let mut mod_code = self.mod_code.value();
                write!(&mut mod_code, "{:04}", code).unwrap();
                mod_code
            }
            None => {
                format!("{}{:04}", self.mod_code.value(), default_code())
            }
        }
    }
}

impl ToTokens for ErrorCodeMatchArm<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let code = LitStr::new(&self.normalized_code(), Span::call_site());
        let code = quote! {#code};
        let pattern_ident = self.pattern_ident;

        let match_arm = match self.field_container.fields {
            Fields::Unit => {
                quote! {
                    #pattern_ident => #code
                }
            }
            Fields::Named(_) => {
                quote! {
                    #pattern_ident {..} => #code
                }
            }
            Fields::Unnamed(_) => {
                quote! {
                    #pattern_ident(_) => #code
                }
            }
        };
        tokens.extend(match_arm)
    }
}

struct GetErrorCodeMethod {
    method_name: Ident,
    method_return: LitStr,
}

impl GetErrorCodeMethod {
    fn new(variant_name: &str, code: String) -> GetErrorCodeMethod {
        let normalized_name = format!("code_{}", camel_to_snake(variant_name));
        GetErrorCodeMethod {
            method_name: Ident::new(&normalized_name, Span::mixed_site()),
            method_return: LitStr::new(&code, Span::mixed_site()),
        }
    }
}

impl ToTokens for GetErrorCodeMethod {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Self {
            method_name: normalized_name,
            method_return: normalized_code,
        } = self;
        let method_name = quote! { #normalized_name };
        let method_return = quote! { #normalized_code };
        let get_error_code_token = quote! {
            pub const fn #method_name() -> &'static str {
                #method_return
            }
        };
        tokens.extend(get_error_code_token);
    }
}

fn impl_error_code_macro(ast: syn::DeriveInput) -> proc_macro::TokenStream {
    let syn::DeriveInput {
        ident, data, attrs, ..
    } = ast;

    let enum_info = match data {
        Data::Enum(enum_) => parse_error_code_enum(enum_, ident, attrs).expect("parse error code"),
        _ => panic!("only support enum"),
    };
    let error_code_impl = ErrorCodeImpl(&enum_info);

    let gen = quote! {#error_code_impl};
    gen.into()
}

pub fn error_code_derive_inner(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_error_code_macro(ast)
}

fn camel_to_snake(camel_name: &str) -> String {
    let mut buf = String::with_capacity(camel_name.len());
    let mut prev_is_number = false;
    for (i, c) in camel_name.chars().enumerate() {
        if c.is_alphabetic() {
            if prev_is_number || (i != 0 && c.is_uppercase()) {
                buf.push('_');
            }
            prev_is_number = false;
        }
        if c.is_numeric() {
            if !prev_is_number {
                buf.push('_');
            }
            prev_is_number = true;
        }
        buf.push(c);
    }
    buf.to_lowercase()
}

#[test]
fn test_camel_to_snake() {
    assert_eq!(camel_to_snake("HelloWorld"), "hello_world");
    assert_eq!(camel_to_snake("HelloWorld"), "hello_world");
    assert_eq!(camel_to_snake("Hello11World1"), "hello_11_world_1");
    assert_eq!(camel_to_snake("H11W1"), "h_11_w_1");
    assert_eq!(camel_to_snake("HW"), "h_w");
    assert_eq!(camel_to_snake("hw"), "hw");
    assert_eq!(camel_to_snake("HW1"), "h_w_1");
    assert_eq!(camel_to_snake("hw1"), "hw_1");
    assert_eq!(camel_to_snake("H"), "h");
    assert_eq!(camel_to_snake("h"), "h");
    assert_eq!(camel_to_snake("1H"), "_1_h");
    assert_eq!(camel_to_snake("1h"), "_1_h");
    assert_eq!(camel_to_snake("1"), "_1");
}
