mod parse;

use parse::{ErrorCodeEnum, FieldContainer};
use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, ToTokens};
use syn::{Data, Fields, LitStr};

type MultiSynResult<T> = Result<T, Vec<syn::Error>>;

const DEFAULT_CODE: u64 = 0;
const DEFAULT_MODE_CODE: &str = "00";

struct ErrorCodeImpl<'a> {
    error_code_enum: &'a ErrorCodeEnum,
    match_arms: Vec<ErrorCodeMatchArm<'a>>,
    code_methods: Vec<GetErrorCodeMethod>,
}

impl<'a> ErrorCodeImpl<'a> {
    pub fn new(error_code_enum: &'a ErrorCodeEnum) -> Self {
        let mut match_arms = Vec::with_capacity(error_code_enum.variants.len());
        let mut code_methods = Vec::with_capacity(error_code_enum.variants.len());
        for variant in error_code_enum.variants.iter() {
            let match_arm = ErrorCodeMatchArm {
                enum_name: &error_code_enum.name,
                field_container: variant,
                mod_code: &error_code_enum.mod_code,
            };
            if variant.code.is_some() {
                // If variant has #[error_code(code = 0)]
                code_methods.push(GetErrorCodeMethod::new(
                    &variant.name,
                    match_arm.normalized_code(),
                ));
            }
            match_arms.push(match_arm);
        }

        Self {
            error_code_enum,
            match_arms,
            code_methods,
        }
    }
}

impl ToTokens for ErrorCodeImpl<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let enum_name = &self.error_code_enum.name;
        let match_arms = &self.match_arms;
        tokens.extend(quote! {
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
        });
        if !self.code_methods.is_empty() {
            let code_methods = &self.code_methods;
            tokens.extend(quote! {
                impl #enum_name {
                    #(#code_methods)*
                }
            });
        }
    }
}

struct ErrorCodeMatchArm<'a> {
    enum_name: &'a Ident,
    mod_code: &'a LitStr,
    field_container: &'a FieldContainer,
}

impl ErrorCodeMatchArm<'_> {
    fn normalized_code(&self) -> String {
        use std::fmt::Write;

        match self.field_container.code.as_ref() {
            Some(c) => {
                let mut mod_code = self.mod_code.value();
                write!(&mut mod_code, "{:04}", c.code_num).unwrap();
                mod_code
            }
            None => {
                format!("{}{DEFAULT_CODE:04}", self.mod_code.value())
            }
        }
    }
}

impl ToTokens for ErrorCodeMatchArm<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let code = LitStr::new(&self.normalized_code(), Span::call_site());
        let code = quote! {#code};

        let enum_name = self.enum_name;
        let variant_name = &self.field_container.name;
        let pattern_ident = &quote! {#enum_name::#variant_name};

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

/// Generate method `fn code_{variant_name}() -> &'static str` like:
///
/// ```text
/// impl ModError {
///     pub const fn code_sub_mode_error_1() -> &'static str { "010001" }
///     pub const fn code_sub_mode_error_2() -> &'static str { "010002" }
/// }
/// ```
struct GetErrorCodeMethod {
    method_name: Ident,
    method_return: LitStr,
}

impl GetErrorCodeMethod {
    fn new(variant_name: &Ident, code: String) -> GetErrorCodeMethod {
        let variant_name = variant_name.to_string();
        let normalized_name = format!("code_{}", camel_to_snake(&variant_name));
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
        Data::Enum(enum_) => match ErrorCodeEnum::new(enum_, ident, attrs) {
            Ok(err_code_enum) => err_code_enum,
            Err(e) => {
                panic!("error parsing tokens for derive macro ErrorCode: {e:?}");
            }
        },
        _ => panic!("only enum types supported for derive macro ErrorCode"),
    };
    let error_code_impl = ErrorCodeImpl::new(&enum_info);

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
