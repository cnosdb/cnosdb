use core::time::Duration;
use std::cmp::min;
use std::fmt::{Display, Formatter};

use humantime::{format_duration, parse_duration};
use serde::{Deserialize, Serialize};

use crate::precision::Precision;

pub const YEAR_SECOND: u64 = 31557600; // 356.25 day

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct CnosDuration {
    duration: Duration,
    is_inf: bool,
}

impl Display for CnosDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.is_inf {
            write!(f, "INF")
        } else {
            write!(f, "{}", format_duration(self.duration))
        }
    }
}

impl CnosDuration {
    pub const fn new_with_day(day: u64) -> Self {
        if u64::MAX / 24 / 60 / 60 < day {
            return Self::new_inf();
        }
        Self {
            duration: Duration::from_secs(day * 24 * 60 * 60),
            is_inf: false,
        }
    }

    pub const fn new_inf() -> Self {
        Self {
            is_inf: true,
            duration: Duration::from_secs(0),
        }
    }

    pub const fn new_with_duration(duration: Duration) -> Self {
        Self {
            duration,
            is_inf: false,
        }
    }

    pub const fn init(duration: Duration, is_inf: bool) -> Self {
        Self { duration, is_inf }
    }

    // with default DurationUnit day
    pub fn new(text: &str) -> Option<Self> {
        if let Ok(v) = text.parse::<u64>() {
            return Some(Self::new_with_day(v));
        }
        if text.to_uppercase().as_str() == "INF" {
            return Some(Self::new_inf());
        }
        let duration = match parse_duration(text) {
            Ok(v) => v,
            Err(_) => return None,
        };
        Some(Self {
            duration,
            is_inf: false,
        })
    }

    pub fn to_nanoseconds(&self) -> i64 {
        if self.is_inf {
            i64::MAX
        } else {
            min(self.duration.as_nanos(), i64::MAX as u128) as i64
        }
    }

    pub fn to_microseconds(&self) -> i64 {
        if self.is_inf {
            i64::MAX
        } else {
            min(self.duration.as_micros(), i64::MAX as u128) as i64
        }
    }

    pub fn to_millisecond(&self) -> i64 {
        if self.is_inf {
            i64::MAX
        } else {
            min(self.duration.as_millis(), i64::MAX as u128) as i64
        }
    }

    pub fn to_precision(&self, pre: Precision) -> i64 {
        match pre {
            Precision::MS => self.to_millisecond(),
            Precision::US => self.to_microseconds(),
            Precision::NS => self.to_nanoseconds(),
        }
    }
}
