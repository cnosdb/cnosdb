use serde::{Deserialize, Serialize};

pub mod parser;

use parser::Result;

#[derive(Debug, Deserialize, Serialize)]
pub enum JsonType {
    Bulk,
    Ndjson,
    Loki,
}

impl JsonType {
    pub fn try_parse(s: String) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "bulk" => Ok(JsonType::Bulk),
            "ndjson" => Ok(JsonType::Ndjson),
            "loki" => Ok(JsonType::Loki),
            _ => Err(parser::Error::InvalidType { name: s }),
        }
    }
}
