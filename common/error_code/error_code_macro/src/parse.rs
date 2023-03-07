use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use proc_macro2::Span;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{custom_keyword, token, Attribute, Field, Fields, LitInt, LitStr};

use crate::{default_mode_code, MultiSynResult};

#[derive(Clone)]
pub struct Code {
    pub code_token: code,
    pub eq_token: token::Eq,
    pub code_num: LitInt,
}

impl PartialEq for Code {
    fn eq(&self, other: &Self) -> bool {
        let l: usize = self.code_num.base10_parse().unwrap();
        let r: usize = other.code_num.base10_parse().unwrap();
        l == r
    }
}
impl Eq for Code {}
impl Hash for Code {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let n: usize = self.code_num.base10_parse().unwrap();
        n.hash(state)
    }
}

impl Parse for Code {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(Self {
            code_token: input.parse()?,
            eq_token: input.parse()?,
            code_num: input.parse()?,
        })
    }
}

pub struct FieldContainer {
    pub name: syn::Ident,
    pub fields: Fields,
    pub source_type: Option<syn::Type>,
    pub code: Option<Code>,
}

pub struct EnumInfo {
    pub mod_code: LitStr,
    pub name: syn::Ident,
    pub variants: Vec<FieldContainer>,
}

pub fn check_error_code(enum_info: &EnumInfo) -> MultiSynResult<()> {
    let codes: Vec<u64> = enum_info
        .variants
        .iter()
        .filter_map(|f| f.code.as_ref())
        .map(|code| {
            let num = code.code_num.base10_parse::<u64>();
            match num {
                Ok(n) => n,
                Err(_) => panic!("parse code {} fail", code.code_num),
            }
        })
        .collect();

    if codes.len() != codes.iter().collect::<HashSet<_>>().len() {
        return Err(vec![syn::Error::new(
            Span::call_site(),
            format!(
                "have the same error_code, next code must be greater than {}",
                codes.iter().max().cloned().unwrap_or(1)
            ),
        )]);
    }
    Ok(())
}

pub enum ErrorCodeAttribute {
    ErrorCode(Code),
}

pub struct ModCode {
    pub mod_code_token: mod_code,
    pub eq_token: token::Eq,
    pub code_str: LitStr,
}

impl Parse for ModCode {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(Self {
            mod_code_token: input.parse()?,
            eq_token: input.parse()?,
            code_str: input.parse()?,
        })
    }
}

pub enum ErrorCodeEnumAttribute {
    ModCode(ModCode),
}

fn get_code_attribute(value: Vec<ErrorCodeAttribute>) -> Option<Code> {
    let mut res = None;
    for attr in value {
        match attr {
            ErrorCodeAttribute::ErrorCode(code) => {
                if res.is_some() {
                    panic!("code attribute duplication")
                } else {
                    res = Some(code)
                }
            }
        }
    }
    res
}

pub fn enum_attributes_from_syn(
    attrs: Vec<Attribute>,
) -> MultiSynResult<Vec<ErrorCodeEnumAttribute>> {
    let mut ours = Vec::new();
    let mut errs = Vec::new();
    for attr in attrs {
        if attr.path.is_ident("error_code") {
            let attr_list = Punctuated::<ErrorCodeEnumAttribute, token::Comma>::parse_terminated;
            match attr.parse_args_with(attr_list) {
                Ok(attrs) => {
                    ours.extend(attrs.into_iter().map(Into::into));
                }
                Err(e) => errs.push(e),
            }
        }
    }
    if errs.is_empty() {
        Ok(ours)
    } else {
        Err(errs)
    }
}

fn get_mod_code_attribute(attrs: Vec<ErrorCodeEnumAttribute>) -> MultiSynResult<ModCode> {
    if attrs.is_empty() {
        return Ok(ModCode {
            mod_code_token: mod_code::default(),
            eq_token: token::Eq::default(),
            code_str: LitStr::new(default_mode_code(), Span::call_site()),
        });
    }
    let mut res = None;
    for attr in attrs {
        match attr {
            ErrorCodeEnumAttribute::ModCode(code) => {
                if res.is_some() {
                    return Err(vec![syn::Error::new(
                        Span::call_site(),
                        "there can only be one mod_code",
                    )]);
                } else {
                    res = Some(code)
                }
            }
        }
    }
    Ok(res.unwrap())
}

fn get_source_type(fields: &Fields) -> Option<syn::Type> {
    let mut res = None;
    let named_fields: Vec<Field> = match fields {
        Fields::Named(n_f) => n_f.named.iter().cloned().collect(),
        _ => return None,
    };

    for field in named_fields {
        if field.ident.unwrap().to_string().eq("source") {
            res = Some(field.ty)
        }
    }

    res
}

fn check_mod_code(lit: LitStr) -> MultiSynResult<()> {
    let string = lit.value();
    if string.len() != 2 {
        Err(vec![syn::Error::new(
            Span::call_site(),
            "len of mod_code must be 2 ",
        )])
    } else {
        Ok(())
    }
}

pub fn parse_error_code_enum(
    enum_: syn::DataEnum,
    name: syn::Ident,
    attrs: Vec<Attribute>,
) -> MultiSynResult<EnumInfo> {
    let mut fields = Vec::new();

    let mod_code_attrs = enum_attributes_from_syn(attrs)?;
    let mod_code = get_mod_code_attribute(mod_code_attrs)?.code_str;
    check_mod_code(mod_code.clone())?;

    for i in enum_.variants.iter() {
        let error_code_attrs = variant_attributes_from_syn(i.attrs.clone())?;
        let code = get_code_attribute(error_code_attrs);
        let source_type = get_source_type(&i.fields);

        let field = FieldContainer {
            name: i.ident.clone(),
            fields: i.fields.clone(),
            source_type,
            code,
        };
        fields.push(field)
    }

    let res = EnumInfo {
        mod_code,
        name,
        variants: fields,
    };

    check_error_code(&res)?;
    Ok(res)
}

custom_keyword!(code);
custom_keyword!(mod_code);

impl Parse for ErrorCodeAttribute {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(code) {
            input.parse().map(ErrorCodeAttribute::ErrorCode)
        } else {
            Err(lookahead.error())
        }
    }
}

impl Parse for ErrorCodeEnumAttribute {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(mod_code) {
            input.parse().map(ErrorCodeEnumAttribute::ModCode)
        } else {
            Err(lookahead.error())
        }
    }
}

fn variant_attributes_from_syn(attrs: Vec<Attribute>) -> MultiSynResult<Vec<ErrorCodeAttribute>> {
    let mut ours = Vec::new();
    let mut errs = Vec::new();
    for attr in attrs {
        if attr.path.is_ident("error_code") {
            let attr_list = Punctuated::<ErrorCodeAttribute, token::Comma>::parse_terminated;
            match attr.parse_args_with(attr_list) {
                Ok(attrs) => {
                    ours.extend(attrs.into_iter().map(Into::into));
                }
                Err(e) => errs.push(e),
            }
        }
    }
    if errs.is_empty() {
        Ok(ours)
    } else {
        Err(errs)
    }
}
