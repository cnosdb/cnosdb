#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TimeUnit {
    pub fn parse_duration(&self, duration: std::time::Duration) -> f64 {
        match self {
            Self::Second => duration.as_secs() as f64,
            Self::Millisecond => duration.as_millis() as f64,
            Self::Microsecond => duration.as_micros() as f64,
            Self::Nanosecond => duration.as_nanos() as f64,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Second => "s",
            Self::Millisecond => "ms",
            Self::Microsecond => "us",
            Self::Nanosecond => "ns",
        }
    }
}

impl std::fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
