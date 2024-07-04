use std::fmt::{self, Display};

use datafusion::arrow::datatypes::TimeUnit;
use serde::{Deserialize, Serialize};

use crate::Timestamp;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Precision {
    MS = 0,
    US,
    NS,
}

impl From<u8> for Precision {
    fn from(value: u8) -> Self {
        match value {
            0 => Precision::MS,
            1 => Precision::US,
            2 => Precision::NS,
            _ => Precision::NS,
        }
    }
}

impl Default for Precision {
    fn default() -> Self {
        Self::NS
    }
}

impl From<TimeUnit> for Precision {
    fn from(value: TimeUnit) -> Self {
        match value {
            TimeUnit::Millisecond => Precision::MS,
            TimeUnit::Microsecond => Precision::US,
            TimeUnit::Nanosecond => Precision::NS,
            _ => Precision::NS,
        }
    }
}

impl From<Precision> for TimeUnit {
    fn from(value: Precision) -> Self {
        match value {
            Precision::MS => TimeUnit::Millisecond,
            Precision::US => TimeUnit::Microsecond,
            Precision::NS => TimeUnit::Nanosecond,
        }
    }
}

impl Precision {
    pub fn new(text: &str) -> Option<Self> {
        match text.to_uppercase().as_str() {
            "MS" => Some(Precision::MS),
            "US" => Some(Precision::US),
            "NS" => Some(Precision::NS),
            _ => None,
        }
    }
}

pub fn timestamp_convert(from: Precision, to: Precision, ts: Timestamp) -> Option<Timestamp> {
    match (from, to) {
        (Precision::NS, Precision::US) | (Precision::US, Precision::MS) => Some(ts / 1_000),
        (Precision::MS, Precision::US) | (Precision::US, Precision::NS) => ts.checked_mul(1_000),
        (Precision::NS, Precision::MS) => Some(ts / 1_000_000),
        (Precision::MS, Precision::NS) => ts.checked_mul(1_000_000),
        _ => Some(ts),
    }
}

impl Display for Precision {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Precision::MS => f.write_str("MS"),
            Precision::US => f.write_str("US"),
            Precision::NS => f.write_str("NS"),
        }
    }
}
