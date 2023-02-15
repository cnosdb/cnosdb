use std::fmt::{Display, Formatter};

use protos::models as fb_models;
use serde::{Deserialize, Serialize};

use crate::errors::{Error, Result};
use crate::{FieldId, FieldName, Tag};

const FIELD_NAME_MAX_LEN: usize = 512;

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone, Eq, Hash)]
pub enum ValueType {
    Unknown,
    Float,
    Integer,
    Unsigned,
    Boolean,
    String,
}

impl ValueType {
    pub fn to_fb_type(&self) -> protos::models::FieldType {
        match *self {
            ValueType::Float => protos::models::FieldType::Float,
            ValueType::Integer => protos::models::FieldType::Integer,
            ValueType::Unsigned => protos::models::FieldType::Unsigned,
            ValueType::Boolean => protos::models::FieldType::Boolean,
            ValueType::String => protos::models::FieldType::String,
            ValueType::Unknown => protos::models::FieldType::Unknown,
        }
    }
}

impl Display for ValueType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Unknown => f.write_str("Unknown"),
            ValueType::Float => f.write_str("Float"),
            ValueType::Integer => f.write_str("Integer"),
            ValueType::Unsigned => f.write_str("Unsigned"),
            ValueType::Boolean => f.write_str("Boolean"),
            ValueType::String => f.write_str("String"),
        }
    }
}

impl From<u8> for ValueType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Float,
            1 => Self::Integer,
            2 => Self::Boolean,
            3 => Self::String,
            4 => Self::Unsigned,
            _ => Self::Unknown,
        }
    }
}

impl From<ValueType> for u8 {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Float => 0,
            ValueType::Integer => 1,
            ValueType::Boolean => 2,
            ValueType::String => 3,
            ValueType::Unsigned => 4,
            ValueType::Unknown => 5,
        }
    }
}

impl From<protos::models::FieldType> for ValueType {
    fn from(t: protos::models::FieldType) -> Self {
        match t {
            protos::models::FieldType::Float => ValueType::Float,
            protos::models::FieldType::Integer => ValueType::Integer,
            protos::models::FieldType::Unsigned => ValueType::Unsigned,
            protos::models::FieldType::Boolean => ValueType::Boolean,
            protos::models::FieldType::String => ValueType::String,
            _ => ValueType::Unknown,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FieldInfo {
    id: FieldId,
    name: FieldName,
    value_type: ValueType,

    // high 4 bit for ts code type
    // low 4 bit for val code type
    code_type: u8,
}

impl From<&Tag> for FieldInfo {
    fn from(tag: &Tag) -> Self {
        FieldInfo {
            id: 0,
            name: tag.key.clone(),
            value_type: ValueType::Unknown,
            code_type: 0,
        }
    }
}

impl PartialEq for FieldInfo {
    fn eq(&self, other: &Self) -> bool {
        if self.name == other.name {
            return true;
        }

        false
    }
}

impl FieldInfo {
    pub fn new(id: FieldId, name: FieldName, value_type: ValueType, code_type: u8) -> Self {
        Self {
            id,
            name,
            value_type,
            code_type,
        }
    }

    pub fn from_flatbuffers(field: &fb_models::Field) -> Result<Self> {
        Ok(Self {
            id: 0,
            name: field
                .name()
                .ok_or(Error::InvalidFlatbufferMessage {
                    err: "".to_string(),
                })?
                .bytes()
                .to_vec(),
            value_type: field.type_().into(),
            code_type: 0,
        })
    }

    pub fn check(&self) -> Result<()> {
        if self.name.len() > FIELD_NAME_MAX_LEN {
            return Err(Error::InvalidField {
                err: format!(
                    "TagKey exceeds the FIELD_NAME_MAX_LEN({})",
                    FIELD_NAME_MAX_LEN
                ),
            });
        }
        Ok(())
    }

    pub fn set_field_id(&mut self, id: FieldId) {
        self.id = id;
    }

    pub fn field_id(&self) -> FieldId {
        self.id
    }

    pub fn name(&self) -> &FieldName {
        &self.name
    }

    pub fn value_type(&self) -> ValueType {
        self.value_type
    }

    pub fn code_type(&self) -> u8 {
        self.code_type
    }

    pub fn is_tag(&self) -> bool {
        self.value_type == ValueType::Unknown
    }
}

/// Split a 16 byte FieldId to 8 byte SeriesId and 8 byte FieldHash
// pub fn split_field_id(fid: &FieldId) -> (SeriesId, FieldHash) {
//     ((*fid >> 64 & u64::MAX as u128) as u64, (*fid & u64::MAX as u128) as u64)
// }

#[cfg(test)]
mod test {
    use crate::{FieldInfo, ValueType};

    #[test]
    fn test_field_info_format_check() {
        let field_info = FieldInfo::new(1, Vec::from("hello"), ValueType::Integer, 0);
        field_info.check().unwrap();
    }
}
