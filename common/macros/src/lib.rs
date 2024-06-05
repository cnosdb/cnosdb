use proc_macro::TokenStream;

mod config_env;
mod error_code;

#[proc_macro_derive(EnvKeys)]
pub fn derive_env_keys(input: TokenStream) -> TokenStream {
    config_env::derive_env_keys_inner(input)
}

#[proc_macro_derive(ErrorCoder, attributes(error_code))]
pub fn error_code_derive(input: TokenStream) -> TokenStream {
    error_code::error_code_derive_inner(input)
}
