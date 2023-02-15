mod parse;

extern crate core;

use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::{Data, Fields, LitStr};

use crate::parse::{parse_error_code_enum, EnumInfo, FieldContainer};

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

        let arms: Vec<_> = self
            .0
            .variants
            .iter()
            .map(|variant| {
                let variant_name = &variant.name;
                let arm = ErrorCodeMatchArm {
                    field_container: variant,
                    mod_code,
                    pattern_ident: &quote! {#enum_name::#variant_name},
                };
                quote! { #arm }
            })
            .collect();

        let gen = quote! {
            impl ErrorCode for #enum_name {
                fn code(&self) -> &'static str {
                    match self {
                        #(#arms),*
                    }
                }
                fn message(&self) -> String {
                    self.to_string()
                }
            }
        };
        tokens.extend(gen)
    }
}

struct ErrorCodeMatchArm<'a> {
    mod_code: &'a LitStr,
    field_container: &'a FieldContainer,
    pattern_ident: &'a dyn ToTokens,
}

impl ToTokens for ErrorCodeMatchArm<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Self {
            mod_code,
            field_container,
            pattern_ident,
        } = *self;
        let mut mod_code = mod_code.value();
        // let source_type = field_container.source_type.as_ref();
        let default_code = mod_code.clone() + &format!("{:04}", default_code());

        let code = match field_container.code.as_ref() {
            Some(c) => {
                let code: u64 = c.code_num.base10_parse().unwrap();
                assert!(code > 0 && code < 10_000);

                mod_code.push_str(&format!("{:04}", code));
                let code = LitStr::new(&mod_code, Span::call_site());
                quote! {#code}
            }

            None => {
                quote! { #default_code }
            }
        };

        let match_arm = match field_container.fields {
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

#[proc_macro_derive(ErrorCoder, attributes(error_code))]
pub fn error_code_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_error_code_macro(ast)
}
