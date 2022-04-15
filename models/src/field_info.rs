use super::*;
use utils::bkdr_hash::{Hash, HashWith};

const FIELD_NAME_MAX_LEN: usize = 512;

pub type FieldID = u64;
pub type FieldName = Vec<u8>;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct FieldInfo {
    pub id: FieldID,
    pub name: FieldName,
    pub value_type: ValueType,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum ValueType {
    Unknown,
    Float,
    Integer,
    Unsigned,
    Boolean,
    String,
}

impl FieldInfo {
    pub fn new() -> Self {
        FieldInfo {
            id: 0,
            name: FieldName::new(),
            value_type: ValueType::Unknown,
        }
    }

    pub fn update_id(&mut self, series_id: SeriesID) {
        let mut hash = Hash::from(series_id);
        self.id = hash.hash_with(&self.name).number();
    }

    pub fn format_check(&self) -> Result<(), String> {
        if self.name.len() > FIELD_NAME_MAX_LEN {
            return Err(String::from("TagKey exceeds the FIELD_NAME_MAX_LEN"));
        }
        Ok(())
    }
}

pub trait FieldInfoFromParts<T1, T2> {
    fn from_parts(name: T1, value_type: T2) -> Self;
}

impl FieldInfoFromParts<FieldName, ValueType> for FieldInfo {
    fn from_parts(name: FieldName, value_type: ValueType) -> Self {
        FieldInfo {
            id: 0,
            name,
            value_type,
        }
    }
}

impl FieldInfoFromParts<&str, ValueType> for FieldInfo {
    fn from_parts(name: &str, value_type: ValueType) -> Self {
        FieldInfo {
            id: 0,
            name: name.as_bytes().to_vec(),
            value_type,
        }
    }
}

#[cfg(test)]
mod tests_field_info {
    use crate::{FieldInfo, FieldInfoFromParts, ValueType};

    #[test]
    fn test_field_info_format_check() {
        let field_info = FieldInfo::from_parts(Vec::from("hello"), ValueType::Integer);
        field_info.format_check().unwrap();
    }
}
