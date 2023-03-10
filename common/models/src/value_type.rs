use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

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
