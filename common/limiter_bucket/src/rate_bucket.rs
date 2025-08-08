use std::fmt::Formatter;
use std::ops::Deref;

use chrono::{DateTime, Utc};
use config::common::RateBucketConfig;
use parking_lot::{Mutex, MutexGuard};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug)]
pub struct RateBucket {
    /// Tokens to add every `per` duration.
    refill: usize,
    /// Interval in milliseconds to add tokens.
    interval: chrono::Duration,
    /// Max number of tokens associated with the rate limiter.
    max: usize,

    critical: Mutex<Critical>,
}

impl PartialEq for RateBucket {
    fn eq(&self, other: &Self) -> bool {
        if self.refill == other.refill && self.interval == other.interval && self.max == other.max {
            let a = *self.critical.lock();
            let b = *other.critical.lock();
            a == b
        } else {
            false
        }
    }
}
impl From<&RateBucketBuilder> for RateBucket {
    fn from(value: &RateBucketBuilder) -> Self {
        value.build()
    }
}

impl From<&RateBucketConfig> for RateBucket {
    fn from(value: &RateBucketConfig) -> Self {
        let builder = RateBucketBuilder::from(value);
        builder.build()
    }
}

impl Serialize for RateBucket {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RateBucket", 5)?;
        state.serialize_field("refill", &self.refill)?;
        state.serialize_field("interval", &self.interval.num_milliseconds())?;
        state.serialize_field("max", &self.max)?;
        let critical = self.critical.lock();
        state.serialize_field("critical", critical.deref())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for RateBucket {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Refill,
            Interval,
            Max,
            Critical,
        }
        const FIELDS: &[&str] = &["refill", "interval", "max", "critical"];

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;
                impl Visitor<'_> for FieldVisitor {
                    type Value = Field;
                    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                        formatter.write_str("`refill` or `interval` or `max` or `critical`")
                    }

                    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        match v {
                            "refill" => Ok(Field::Refill),
                            "interval" => Ok(Field::Interval),
                            "max" => Ok(Field::Max),
                            "critical" => Ok(Field::Critical),
                            _ => Err(serde::de::Error::unknown_field(v, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct RateLimiterVisitor;
        impl<'de> Visitor<'de> for RateLimiterVisitor {
            type Value = RateBucket;
            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("struct RateBucket")
            }
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let refill = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let interval = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let max = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let critical = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(3, &self))?;

                Ok(RateBucket {
                    refill,
                    interval: chrono::Duration::milliseconds(interval),
                    max,
                    critical: Mutex::new(critical),
                })
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut refill = None;
                let mut interval = None;
                let mut max = None;
                let mut critical = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Refill => {
                            if refill.is_some() {
                                return Err(serde::de::Error::duplicate_field("refill"));
                            }
                            refill = Some(map.next_value()?);
                        }
                        Field::Interval => {
                            if interval.is_some() {
                                return Err(serde::de::Error::duplicate_field("interval"));
                            }
                            interval = Some(map.next_value()?);
                        }
                        Field::Max => {
                            if max.is_some() {
                                return Err(serde::de::Error::duplicate_field("max"));
                            }
                            max = Some(map.next_value()?);
                        }
                        Field::Critical => {
                            if critical.is_some() {
                                return Err(serde::de::Error::duplicate_field("critical"));
                            }
                            critical = Some(map.next_value()?);
                        }
                    }
                }
                let refill = refill.ok_or_else(|| serde::de::Error::missing_field("refill"))?;
                let interval =
                    interval.ok_or_else(|| serde::de::Error::missing_field("interval"))?;
                let max = max.ok_or_else(|| serde::de::Error::missing_field("max"))?;
                let critical =
                    critical.ok_or_else(|| serde::de::Error::missing_field("critical"))?;
                Ok(RateBucket {
                    refill,
                    interval: chrono::Duration::milliseconds(interval),
                    max,
                    critical: Mutex::new(critical),
                })
            }
        }

        deserializer.deserialize_struct("RateBucket", FIELDS, RateLimiterVisitor)
    }
}

unsafe impl Send for RateBucket {}
unsafe impl Sync for RateBucket {}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Critical {
    /// current balance of tokens,
    balance: usize,
    /// The deadline for when more tokens can be added
    deadline: chrono::DateTime<Utc>,
}

impl RateBucket {
    pub const DEFAULT_REFILL_MAX_FACTOR: usize = 10;

    #[allow(dead_code)]
    pub fn builder() -> RateBucketBuilder {
        RateBucketBuilder::default()
    }

    #[allow(dead_code)]
    pub fn refill(&self) -> usize {
        self.refill
    }

    #[allow(dead_code)]
    pub fn interval(&self) -> chrono::Duration {
        self.interval
    }

    #[allow(dead_code)]
    pub fn max(&self) -> usize {
        self.max
    }

    #[allow(dead_code)]
    pub fn balance(&self) -> usize {
        self.critical.lock().balance
    }

    pub fn acquire_one(&self) -> Result<(), String> {
        self.acquire(1)
    }
    fn update_critical(&self, critical: &mut MutexGuard<Critical>) {
        if let Some((tokens, deadline)) = calculate_drain(critical.deadline, self.interval) {
            critical.deadline = deadline;
            critical.balance = critical.balance.saturating_add(tokens * self.refill);

            if critical.balance > self.max {
                critical.balance = self.max;
            }
        }
    }

    pub fn acquire_closed(&self, permits: usize) -> usize {
        if permits == 0 {
            return 0;
        }

        let mut critical = self.critical.lock();
        self.update_critical(&mut critical);

        if critical.balance >= permits {
            critical.balance -= permits;
            permits
        } else {
            let res = critical.balance;
            critical.balance = 0;
            res
        }
    }

    pub fn acquire(&self, permits: usize) -> Result<(), String> {
        if permits == 0 {
            return Ok(());
        }

        let mut critical = self.critical.lock();

        self.update_critical(&mut critical);

        if let Some(balance) = critical.balance.checked_sub(permits) {
            critical.balance = balance;
            Ok(())
        } else {
            Err("token is not enough".to_string())
        }
    }

    pub fn to_traffic_string(&self) -> String {
        format!("{}B/{}s", self.refill, self.interval.num_seconds())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct RateBucketBuilder {
    max: Option<usize>,
    initial: usize,
    refill: usize,
    // ms
    interval: i64,
}
impl From<&RateBucketConfig> for RateBucketBuilder {
    fn from(value: &RateBucketConfig) -> Self {
        let RateBucketConfig {
            max,
            initial,
            refill,
            interval,
        } = *value;
        Self {
            max,
            initial,
            refill,
            interval,
        }
    }
}

impl RateBucketBuilder {
    #[allow(dead_code)]
    pub fn max(&mut self, max: usize) -> &mut Self {
        self.max = Some(max);
        self
    }

    #[allow(dead_code)]
    pub fn initial(&mut self, initial: usize) -> &mut Self {
        self.initial = initial;
        self
    }

    #[allow(dead_code)]
    pub fn interval(&mut self, interval: chrono::Duration) -> &mut Self {
        assert_ne! {
            interval.num_milliseconds(),
            0,
            "interval must be non-zero",
        };
        assert! {
            u64::try_from(interval.num_milliseconds()).is_ok(),
            "interval must fit within a 64-bit integer"
        };
        self.interval = interval.num_milliseconds();
        self
    }

    #[allow(dead_code)]
    pub fn refill(&mut self, refill: usize) -> &mut Self {
        assert!(refill > 0, "refill amount cannot be zero");
        self.refill = refill;
        self
    }

    #[allow(dead_code)]
    pub fn build(&self) -> RateBucket {
        let interval = chrono::Duration::milliseconds(self.interval);
        let deadline = Utc::now() + interval;

        let max = match self.max {
            Some(max) => max,
            None => usize::max(self.refill, self.initial)
                .saturating_mul(RateBucket::DEFAULT_REFILL_MAX_FACTOR),
        };

        let initial = usize::min(self.initial, max);

        RateBucket {
            refill: self.refill,
            interval,
            max,
            critical: Mutex::new(Critical {
                balance: initial,
                deadline,
            }),
        }
    }
}

impl Default for RateBucketBuilder {
    fn default() -> Self {
        Self {
            max: None,
            initial: 0,
            refill: 1,
            interval: 100,
        }
    }
}

fn calculate_drain(
    deadline: DateTime<Utc>,
    interval: chrono::Duration,
) -> Option<(usize, DateTime<Utc>)> {
    let now = Utc::now();
    if now < deadline {
        return None;
    }

    // Time elapsed in milliseconds since the last deadline.
    let millis = interval.num_milliseconds();
    let since = now.signed_duration_since(deadline).num_milliseconds();

    let tokens = usize::try_from(since / millis + 1).unwrap_or(usize::MAX);

    let rem = since % millis;

    // Calculated time remaining until the next deadline.
    let deadline = now + (interval - chrono::Duration::milliseconds(rem));
    Some((tokens, deadline))
}
#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::RateBucket;

    #[test]
    fn test_serialize_rate_limiter() {
        let limiter1 = RateBucket::builder()
            .max(5)
            .interval(chrono::Duration::milliseconds(10))
            .initial(5)
            .refill(1)
            .build();
        let data = bincode::serialize(&limiter1).unwrap();
        let limiter2 = bincode::deserialize(data.as_slice()).unwrap();
        let data = serde_json::to_string_pretty(&limiter1).unwrap();
        let limiter3 = serde_json::from_str(&data).unwrap();
        assert_eq!(limiter1, limiter2);
        assert_eq!(limiter1, limiter3);
    }

    #[test]
    fn test_rate_limit_target() {
        const TARGET: usize = 100;
        const INTERVALS: usize = 10;
        const DURATION: u64 = 2000;
        const TARGET_DIFFERENCE: u32 = 20;

        let interval = DURATION / INTERVALS as u64;
        let refill = TARGET / INTERVALS;

        let limiter = RateBucket::builder()
            .refill(refill)
            .interval(chrono::Duration::milliseconds(interval as i64))
            .max(usize::MAX)
            .build();

        let c = AtomicUsize::new(0);
        let start = std::time::Instant::now();
        while c.fetch_add(1, Ordering::SeqCst) < TARGET {
            while limiter.acquire_one().is_err() {}
        }
        let duration = start.elapsed().as_millis();
        let diff = duration as f64 - DURATION as f64;

        assert! {
            diff.abs() < TARGET_DIFFERENCE as f64,
            "diff must be less than {}ms, but was {}ms",
            TARGET_DIFFERENCE,
            diff,
        };
    }
}
