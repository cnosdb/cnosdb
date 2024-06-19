use std::fmt::{self};

use serde::{Deserialize, Serialize};

use crate::schema::database_schema::Precision;
use crate::utils::{
    DAY_MICROS, DAY_MILLS, DAY_NANOS, HOUR_MICROS, HOUR_MILLS, HOUR_NANOS, MINUTES_MICROS,
    MINUTES_MILLS, MINUTES_NANOS,
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum DurationUnit {
    Minutes,
    Hour,
    Day,
    Inf,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Duration {
    pub time_num: u64,
    pub unit: DurationUnit,
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.unit {
            DurationUnit::Minutes => write!(f, "{} Minutes", self.time_num),
            DurationUnit::Hour => write!(f, "{} Hours", self.time_num),
            DurationUnit::Day => write!(f, "{} Days", self.time_num),
            DurationUnit::Inf => write!(f, "INF"),
        }
    }
}

impl Duration {
    pub fn new_with_day(day: u64) -> Self {
        Self {
            time_num: day,
            unit: DurationUnit::Day,
        }
    }

    // with default DurationUnit day
    pub fn new(text: &str) -> Option<Self> {
        if text.is_empty() {
            return None;
        }
        let len = text.len();
        if let Ok(v) = text.parse::<u64>() {
            return Some(Duration {
                time_num: v,
                unit: DurationUnit::Day,
            });
        };

        let time = &text[..len - 1];
        let unit = &text[len - 1..];
        let time_num = match time.parse::<u64>() {
            Ok(v) => v,
            Err(_) => {
                return None;
            }
        };
        let time_unit = match unit.to_uppercase().as_str() {
            "D" => DurationUnit::Day,
            "H" => DurationUnit::Hour,
            "M" => DurationUnit::Minutes,
            _ => return None,
        };
        Some(Duration {
            time_num,
            unit: time_unit,
        })
    }

    pub fn new_inf() -> Self {
        Self {
            time_num: 100000,
            unit: DurationUnit::Day,
        }
    }

    pub fn to_nanoseconds(&self) -> i64 {
        match self.unit {
            DurationUnit::Minutes => (self.time_num as i64).saturating_mul(MINUTES_NANOS),
            DurationUnit::Hour => (self.time_num as i64).saturating_mul(HOUR_NANOS),
            DurationUnit::Day => (self.time_num as i64).saturating_mul(DAY_NANOS),
            DurationUnit::Inf => i64::MAX,
        }
    }

    pub fn to_microseconds(&self) -> i64 {
        match self.unit {
            DurationUnit::Minutes => (self.time_num as i64).saturating_mul(MINUTES_MICROS),
            DurationUnit::Hour => (self.time_num as i64).saturating_mul(HOUR_MICROS),
            DurationUnit::Day => (self.time_num as i64).saturating_mul(DAY_MICROS),
            DurationUnit::Inf => i64::MAX,
        }
    }

    pub fn to_millisecond(&self) -> i64 {
        match self.unit {
            DurationUnit::Minutes => (self.time_num as i64).saturating_mul(MINUTES_MILLS),
            DurationUnit::Hour => (self.time_num as i64).saturating_mul(HOUR_MILLS),
            DurationUnit::Day => (self.time_num as i64).saturating_mul(DAY_MILLS),
            DurationUnit::Inf => i64::MAX,
        }
    }

    pub fn to_precision(&self, pre: Precision) -> i64 {
        match pre {
            Precision::MS => self.to_millisecond(),
            Precision::US => self.to_microseconds(),
            Precision::NS => self.to_nanoseconds(),
        }
    }

    pub fn drop_after_output(&self) -> String {
        match &self.unit {
            DurationUnit::Minutes => format!("{}m", self.time_num),
            DurationUnit::Hour => format!("{}h", self.time_num),
            DurationUnit::Day => format!("{}d", self.time_num),
            _ => String::from(""),
        }
    }
}
