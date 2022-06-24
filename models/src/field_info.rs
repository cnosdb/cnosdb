use protos::models as fb_models;
use serde::{Deserialize, Serialize};
use utils::bkdr_hash::BkdrHasher;

use crate::{
    errors::{Error, Result},
    FieldID, FieldName, SeriesID,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FieldInfo {
    id: FieldID,
    name: FieldName,
    value_type: ValueType,

    /// True if method `finish(series_id)` has been called.
    finished: bool,
}

impl FieldInfo {
    pub fn new(series_id: SeriesID, name: FieldName, value_type: ValueType) -> Self {
        let mut fi = FieldInfo { id: 0, name, value_type, finished: true };
        fi.finish(series_id);
        fi
    }

    pub fn from_flatbuffers(field: &fb_models::Field) -> Result<Self> {
        Ok(Self { id: 0,
                  name: field.name()
                             .ok_or(Error::InvalidFlatbufferMessage { err: "".to_string() })?
                             .to_vec(),
                  value_type: field.type_().into(),
                  finished: false })
    }

    pub fn check(&self) -> Result<()> {
        if self.name.len() > FIELD_NAME_MAX_LEN {
            return Err(Error::InvalidField { err: format!("TagKey exceeds the FIELD_NAME_MAX_LEN({})",
                                                          FIELD_NAME_MAX_LEN) });
        }
        Ok(())
    }

    pub fn finish(&mut self, series_id: SeriesID) {
        self.finished = true;
        self.id = generate_field_id(&self.name, series_id);
    }

    pub fn filed_id(&self) -> FieldID {
        self.id
    }

    pub fn name(&self) -> &FieldName {
        &self.name
    }

    pub fn value_type(&self) -> ValueType {
        self.value_type
    }
}

pub fn generate_field_id(name: &FieldName, sid: SeriesID) -> FieldID {
    let mut hash = BkdrHasher::with_number(sid);
    hash.hash_with(name.as_slice());
    hash.number()
}

#[cfg(test)]
mod test {
    use crate::{FieldInfo, ValueType};

    #[test]
    fn test_field_info_format_check() {
        let field_info = FieldInfo::new(1, Vec::from("hello"), ValueType::Integer);
        field_info.check().unwrap();
    }
}
