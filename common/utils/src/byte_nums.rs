use std::str::FromStr;

use byte_unit::{Byte, ParseError, UnitType};

pub struct CnosByteNumber {
    byte: Byte,
}

impl CnosByteNumber {
    pub fn parse(text: &str) -> Option<Self> {
        match Byte::from_str(text) {
            Ok(v) => Some(CnosByteNumber { byte: v }),
            Err(_) => None,
        }
    }

    pub fn as_bytes_num(&self) -> u64 {
        self.byte.as_u64()
    }

    pub fn format_bytes_num(bytes: u64) -> String {
        Byte::from_u64(bytes)
            .get_appropriate_unit(UnitType::Both)
            .to_string()
    }

    /// Parses a string (with byte unit e.g. 'k', 'MiB', 'GB') into a `u64` number of bytes.
    pub fn parse_bytes_num(s: &str) -> Result<u64, ParseError> {
        let s = s.chars().filter(|c| *c != '_').collect::<String>();
        Byte::parse_str(&s, true).map(|byte| byte.as_u64())
    }
}
