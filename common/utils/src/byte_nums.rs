use std::str::FromStr;

use byte_unit::{Byte, ParseError, UnitType};

pub const YEAR_SECOND: u64 = 31557600; // 356.25 day

pub struct CnosByteNumber {
    byte: Byte,
}

impl CnosByteNumber {
    pub fn new(text: &str) -> Option<Self> {
        match Byte::from_str(text) {
            Ok(v) => Some(CnosByteNumber { byte: v }),
            Err(_) => None,
        }
    }

    pub fn as_bytes(&self) -> u64 {
        self.byte.as_u64()
    }

    pub fn format_bytes(bytes: u64) -> String {
        Byte::from_u64(bytes)
            .get_appropriate_unit(UnitType::Both)
            .to_string()
    }
    pub fn parse_bytes(s: &str) -> Result<u64, ParseError> {
        let mut str = s.to_string();
        str.retain(|c| c != '_');
        Byte::parse_str(&str, true).map(|byte| byte.as_u64())
    }
}
