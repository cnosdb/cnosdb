use protos::models as fb_models;
use serde::{Deserialize, Serialize};
use std::fmt::{write, Display, Formatter};
use utils::BkdrHasher;

use crate::{
    errors::{Error, Result},
    FieldId, FieldName, SeriesId, Tag,
};

const FIELD_NAME_MAX_LEN: usize = 512;

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
pub enum ValueType {
    Unknown,
    Float,
    Integer,
    Unsigned,
    Boolean,
    String,
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

    /// True if method `finish(series_id)` has been called.
    finished: bool,
}

impl From<&Tag> for FieldInfo {
    fn from(tag: &Tag) -> Self {
        FieldInfo {
            id: 0,
            name: tag.key.clone(),
            finished: false,
            value_type: ValueType::Unknown,
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
    pub fn new(series_id: SeriesId, name: FieldName, value_type: ValueType) -> Self {
        let mut fi = FieldInfo {
            id: 0,
            name,
            value_type,
            finished: true,
        };
        fi.finish(series_id);
        fi
    }

    pub fn from_flatbuffers(field: &fb_models::Field) -> Result<Self> {
        Ok(Self {
            id: 0,
            name: field
                .name()
                .ok_or(Error::InvalidFlatbufferMessage {
                    err: "".to_string(),
                })?
                .to_vec(),
            value_type: field.type_().into(),
            finished: false,
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

    pub fn finish(&mut self, series_id: SeriesId) {
        self.finished = true;
        self.id = generate_field_id(&self.name, series_id);
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

    pub fn is_tag(&self) -> bool {
        self.value_type == ValueType::Unknown
    }
}

pub fn generate_field_id(name: &FieldName, sid: SeriesId) -> FieldId {
    let mut hash = BkdrHasher::with_number(sid);
    hash.hash_with(name.as_slice());
    hash.number()
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
        let field_info = FieldInfo::new(1, Vec::from("hello"), ValueType::Integer);
        field_info.check().unwrap();
    }
}
