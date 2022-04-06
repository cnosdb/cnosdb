use serde::{Serialize, Deserialize};

pub type FieldID = u64;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct FieldInfo {
    pub id: FieldID,
    pub value_type: ValueType,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum ValueType {
    Float,
    Integer,
    Unsigned,
    Boolean,
    String,
}