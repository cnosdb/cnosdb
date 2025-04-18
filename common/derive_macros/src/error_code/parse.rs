use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use proc_macro2::{Ident, Span};
use quote::ToTokens;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{token, Attribute, Field, Fields, LitInt, LitStr, Variant};

use super::{MultiSynResult, DEFAULT_MODE_CODE};

/// Custom keywords.
mod kw {
    syn::custom_keyword!(code);
    syn::custom_keyword!(mod_code);
}

/// Represents macro attribute: `code = "xx"`.
/// - `code` should be less than 10000.
#[derive(Clone, Copy)]
pub struct Code {
    pub code_num: u64,
}

impl PartialEq for Code {
    fn eq(&self, other: &Self) -> bool {
        self.code_num == other.code_num
    }
}

impl Eq for Code {}

impl Hash for Code {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.code_num.hash(state)
    }
}

impl Parse for Code {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let _ = input.parse::<kw::code>()?;
        let _ = input.parse::<token::Eq>()?;
        let code_num = input.parse::<LitInt>()?.base10_parse()?;
        if code_num >= 10_000 {
            return Err(syn::Error::new(
                Span::call_site(),
                format!("Error parsing Code({input}): attribute 'code' must be less than 10_000"),
            ));
        }
        Ok(Self { code_num })
    }
}

impl Code {
    fn new(value: Vec<ErrorCodeAttribute>) -> Option<Code> {
        let mut res = None;
        for attr in value {
            match attr {
                ErrorCodeAttribute::ErrorCode(code) => {
                    if res.is_some() {
                        panic!("detected multiple attribute of 'code' used here")
                    } else {
                        res = Some(code)
                    }
                }
            }
        }
        res
    }
}

/// Represents macro attribute: `mod_code = "xx"`.
/// - `code_str` should be 2 characters, e.g. "02".
pub struct ModCode {
    pub code_str: LitStr,
}

impl Default for ModCode {
    fn default() -> Self {
        Self {
            code_str: LitStr::new(DEFAULT_MODE_CODE, Span::call_site()),
        }
    }
}

impl Parse for ModCode {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let _ = input.parse::<kw::mod_code>()?;
        let _ = input.parse::<token::Eq>()?;
        let code_str = input.parse::<LitStr>()?;
        let code_str_value = code_str.value();
        if code_str_value.len() != 2 {
            return Err(syn::Error::new(
                Span::call_site(),
                format!(
                    "Error parsing ModCode({input}): attribute 'mod_code' must be 2 characters"
                ),
            ));
        }
        Ok(Self { code_str })
    }
}

pub struct FieldContainer {
    pub name: Ident,
    pub fields: Fields,
    pub code: Option<Code>,
    _source_type: Option<syn::Type>,
}

impl FieldContainer {
    pub fn new(v: &Variant) -> MultiSynResult<Self> {
        let error_code_attrs: Vec<ErrorCodeAttribute> =
            parse_syn_attributes(v.attrs.clone(), "error_code")?;
        let code = Code::new(error_code_attrs);
        let _source_type = get_syn_type_of_source_field(&v.fields);

        Ok(FieldContainer {
            name: v.ident.clone(),
            fields: v.fields.clone(),
            code,
            _source_type,
        })
    }
}

pub struct ErrorCodeEnum {
    pub mod_code: LitStr,
    pub name: Ident,
    pub variants: Vec<FieldContainer>,
}

impl ErrorCodeEnum {
    pub fn new(
        enum_: syn::DataEnum,
        name: Ident,
        attrs: Vec<Attribute>,
    ) -> MultiSynResult<ErrorCodeEnum> {
        let mut fields = Vec::new();

        let mod_code_attrs: Vec<ErrorCodeEnumAttribute> =
            parse_syn_attributes(attrs, "error_code")?;
        let mod_code = get_mod_code_from_attribute(mod_code_attrs)?.code_str;

        {
            let mut used_codes = HashSet::new();
            let mut max_code = 1;
            for v in enum_.variants.iter() {
                let field = FieldContainer::new(v)?;
                if let Some(code) = &field.code {
                    max_code = max_code.max(code.code_num);
                    if !used_codes.insert(code.code_num) {
                        return Err(vec![syn::Error::new(
                            Span::call_site(),
                            format!(
                                "detected the same error codes: the next code must be greater than {max_code}",
                            ),
                        )]);
                    }
                }
                fields.push(field)
            }
        }

        Ok(ErrorCodeEnum {
            mod_code,
            name,
            variants: fields,
        })
    }
}

/// Error code attributes of enum variants.
///
/// - `#[error_code(code = 1)]`
pub enum ErrorCodeAttribute {
    ErrorCode(Code),
}

impl Parse for ErrorCodeAttribute {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(kw::code) {
            input.parse::<Code>().map(ErrorCodeAttribute::ErrorCode)
        } else {
            Err(syn::Error::new(
                Span::call_site(),
                format!(
                    "Error parsing ErrorCodeAttribute({input}): {}",
                    lookahead.error()
                ),
            ))
        }
    }
}

/// Error code attributes of enum types.
///
/// - `#[error_code(mod_code = "01")]`
pub enum ErrorCodeEnumAttribute {
    ModCode(ModCode),
}

impl Parse for ErrorCodeEnumAttribute {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(kw::mod_code) {
            let c = input.parse::<ModCode>()?;
            Ok(Self::ModCode(c))
        } else {
            Err(syn::Error::new(
                Span::call_site(),
                format!(
                    "Error parsing ErrorCodeEnumAttribute({input}): {}",
                    lookahead.error()
                ),
            ))
        }
    }
}

fn parse_syn_attributes<T: Parse>(
    attrs: Vec<Attribute>,
    ident: &'static str,
) -> MultiSynResult<Vec<T>> {
    let mut outs = Vec::new();
    let mut errs = Vec::new();
    for attr in attrs {
        if attr.path().is_ident(ident) {
            match attr.parse_args_with(Punctuated::<T, token::Comma>::parse_terminated) {
                Ok(attrs) => {
                    outs.extend(attrs.into_iter());
                }
                Err(e) => errs.push(syn::Error::new(
                    Span::call_site(),
                    format!(
                        "invalid attribute of '{ident}'({}): {e}",
                        attr.to_token_stream()
                    ),
                )),
            }
        }
    }
    if errs.is_empty() {
        Ok(outs)
    } else {
        Err(errs)
    }
}

fn get_mod_code_from_attribute(attrs: Vec<ErrorCodeEnumAttribute>) -> MultiSynResult<ModCode> {
    if attrs.is_empty() {
        return Ok(ModCode::default());
    }
    let mut res = None;
    for attr in attrs {
        match attr {
            ErrorCodeEnumAttribute::ModCode(code) => {
                if res.is_some() {
                    return Err(vec![syn::Error::new(
                        Span::call_site(),
                        "detected multiple attribute of 'mod_code' used here",
                    )]);
                } else {
                    res = Some(code)
                }
            }
        }
    }
    Ok(res.unwrap())
}

/// Get the type `T` from the fields of a variant 'source: T'.
/// If the variant has no field named "source", return None.
fn get_syn_type_of_source_field(fields: &Fields) -> Option<syn::Type> {
    let mut res = None;
    let named_fields: Vec<Field> = match fields {
        Fields::Named(n_f) => n_f.named.iter().cloned().collect(),
        _ => return None,
    };

    for field in named_fields {
        match field.ident {
            None => {
                panic!("fields of tuple structs is not supported")
            }
            Some(id) if id == "source" => {
                res = Some(field.ty);
            }
            _ => continue,
        }
    }

    res
}
