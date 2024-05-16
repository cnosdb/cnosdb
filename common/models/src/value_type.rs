use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::gis::data_type::Geometry;

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone, Eq, Hash, Ord, PartialOrd)]
pub enum ValueType {
    Unknown,
    Float,
    Integer,
    Unsigned,
    Boolean,
    String,
    Geometry(Geometry),
}

/// data type for tskv
#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone, Eq, Hash, Ord, PartialOrd)]
pub enum PhysicalDType {
    Unknown,
    Float,
    Integer,
    Unsigned,
    Boolean,
    String,
}

impl ValueType {
    pub fn to_physical_type(&self) -> PhysicalDType {
        match self {
            Self::Unknown => PhysicalDType::Unknown,
            Self::Float => PhysicalDType::Float,
            Self::Integer => PhysicalDType::Integer,
            Self::Unsigned => PhysicalDType::Unsigned,
            Self::Boolean => PhysicalDType::Boolean,
            Self::String => PhysicalDType::String,
            Self::Geometry(_) => PhysicalDType::String,
        }
    }

    pub fn to_sql_type_str(&self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Float => "DOUBLE",
            Self::Integer => "BIGINT",
            Self::Unsigned => "BIGINT UNSIGNED",
            Self::Boolean => "BOOLEAN",
            Self::String => "STRING",
            Self::Geometry(_) => "GEOMETRY",
        }
    }
}

impl Display for PhysicalDType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PhysicalDType::Unknown => f.write_str("Unknown"),
            PhysicalDType::Float => f.write_str("Float"),
            PhysicalDType::Integer => f.write_str("Integer"),
            PhysicalDType::Unsigned => f.write_str("Unsigned"),
            PhysicalDType::Boolean => f.write_str("Boolean"),
            PhysicalDType::String => f.write_str("String"),
        }
    }
}

impl From<protos::models::FieldType> for PhysicalDType {
    fn from(t: protos::models::FieldType) -> Self {
        match t {
            protos::models::FieldType::Float => PhysicalDType::Float,
            protos::models::FieldType::Integer => PhysicalDType::Integer,
            protos::models::FieldType::Unsigned => PhysicalDType::Unsigned,
            protos::models::FieldType::Boolean => PhysicalDType::Boolean,
            protos::models::FieldType::String => PhysicalDType::String,
            _ => PhysicalDType::Unknown,
        }
    }
}
