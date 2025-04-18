use proc_macro::TokenStream;

mod error_code;
mod keys;

/// Implements traits to generate keys for the struct.
///
/// ### `derive_traits::FieldKeys`.
/// ```ignore
/// impl derive_traits::FieldKeys for S {
///     fn field_keys() -> Vec<String> {
///         let mut buf = Vec::new();
///         ...
///         buf
///     }
/// }
/// ```
#[proc_macro_derive(Keys)]
pub fn derive_keys(input: TokenStream) -> TokenStream {
    keys::field_keys::derive_field_keys_inner(input)
}

/// Implements the `derive_traits::ErrorCode` trait for the enum.
///
/// ```ignore
/// impl derive_traits::ErrorCode for E {
///     fn code(&self) -> &'static str {
///         match self {
///             ErrorCode::... => "..."
///         }
///     }
///     fn message(&self) -> String {
///         self.to_string()
///     }
/// }
/// ```
#[proc_macro_derive(ErrorCoder, attributes(error_code))]
pub fn error_code_derive(input: TokenStream) -> TokenStream {
    error_code::error_code_derive_inner(input)
}
