use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct RateLimiter {
    /// Tokens to add every `per` duration.
    refill: usize,
    /// Interval in milliseconds to add tokens.
    interval: chrono::Duration,
    /// Max number of tokens associated with the rate limiter.
    max: usize,

    critical: Mutex<Critical>,
}

impl PartialEq for RateLimiter {
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
impl From<&RateLimiterBuilder> for RateLimiter {
    fn from(value: &RateLimiterBuilder) -> Self {
        value.build()
    }
}

impl Serialize for RateLimiter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RateLimiter", 5)?;
        state.serialize_field("refill", &self.refill)?;
        state.serialize_field("interval", &self.interval.num_milliseconds())?;
        state.serialize_field("max", &self.max)?;
        let critical = self.critical.lock();
        state.serialize_field("critical", critical.deref())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for RateLimiter {
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
                impl<'de> Visitor<'de> for FieldVisitor {
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
            type Value = RateLimiter;
            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("struct RateLimiter")
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

                Ok(RateLimiter {
                    refill,
                    interval: chrono::Duration::milliseconds(interval),
                    max,
                    critical: Mutex::new(critical),
                })
            }
        }

        deserializer.deserialize_struct("RateLimiter", FIELDS, RateLimiterVisitor)
    }
}

unsafe impl Send for RateLimiter {}
unsafe impl Sync for RateLimiter {}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Critical {
    /// current balance of tokens,
    balance: usize,
    /// The deadline for when more tokens can be added
    deadline: chrono::DateTime<Utc>,
}

impl RateLimiter {
    pub const DEFAULT_REFILL_MAX_FACTOR: usize = 10;

    #[allow(dead_code)]
    pub fn builder() -> RateLimiterBuilder {
        RateLimiterBuilder::default()
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

    pub fn acquire(&self, permits: usize) -> Result<(), String> {
        if permits == 0 {
            return Ok(());
        }

        let mut critical = self.critical.lock();
        if let Some((tokens, deadline)) = calculate_drain(critical.deadline, self.interval) {
            critical.deadline = deadline;
            critical.balance = critical.balance.saturating_add(tokens);

            if critical.balance > self.max {
                critical.balance = self.max;
            }
        }

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

#[derive(Default, Copy, Clone, Deserialize, Serialize, Debug)]
pub struct CountLimiterBuilder {
    count: usize,
    max_count: Option<usize>,
}

impl CountLimiterBuilder {
    pub fn initial(&mut self, initial: usize) {
        self.count = initial;
    }
    pub fn max(&mut self, max: usize) {
        self.max_count = Some(max)
    }
    pub fn build(&self) -> CountLimiter {
        CountLimiter {
            count: AtomicUsize::from(self.count),
            max_count: self.max_count,
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct CountLimiter {
    count: AtomicUsize,
    max_count: Option<usize>,
}

impl CountLimiter {
    pub fn new(max_count: Option<usize>) -> Self {
        Self {
            count: AtomicUsize::new(0),
            max_count,
        }
    }

    pub fn new_with_init(init: usize, max_count: Option<usize>) -> Self {
        Self {
            count: AtomicUsize::new(init),
            max_count,
        }
    }

    // pub fn inc(&self, val: usize) {
    //     let val = self.count.fetch_add(val, Ordering::SeqCst);
    //     self.count.store(val, Ordering::SeqCst);
    // }
    //
    // pub fn dec(&self, val: usize) {
    //     let val = self.count.fetch_sub(val, Ordering::SeqCst);
    //     self.count.store(val, Ordering::SeqCst);
    // }

    #[allow(dead_code)]
    pub fn fetch(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub fn max(&self) -> Option<usize> {
        self.max_count
    }

    pub fn try_inc_one(&self) -> Result<(), usize> {
        self.try_inc(1)
    }

    pub fn try_inc(&self, val: usize) -> Result<(), usize> {
        let val = self.count.fetch_add(val, Ordering::SeqCst);
        if let Some(ref max) = self.max_count {
            if val.gt(max) {
                return Err(*max);
            }
        }
        self.count.store(val, Ordering::SeqCst);
        Ok(())
    }
}
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct RateLimiterBuilder {
    max: Option<usize>,
    initial: usize,
    refill: usize,
    // ms
    interval: i64,
}

impl RateLimiterBuilder {
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
    pub fn build(&self) -> RateLimiter {
        let interval = chrono::Duration::milliseconds(self.interval);
        let deadline = Utc::now() + interval;

        let max = match self.max {
            Some(max) => max,
            None => usize::max(self.refill, self.initial)
                .saturating_mul(RateLimiter::DEFAULT_REFILL_MAX_FACTOR),
        };

        let initial = usize::min(self.initial, max);

        RateLimiter {
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

impl Default for RateLimiterBuilder {
    fn default() -> Self {
        Self {
            max: None,
            initial: 0,
            refill: 1,
            interval: 100,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LimiterConfig {
    // add user limit
    pub max_users_number: Option<usize>,
    /// create database limit
    pub max_databases: Option<usize>,
    pub max_shard_number: Option<usize>,
    pub max_replicate_number: Option<usize>,
    pub max_retention_time: Option<usize>,

    pub data_in_rate: RateLimiterBuilder,
    pub data_out_rate: RateLimiterBuilder,
    //
    pub data_in_size: CountLimiterBuilder,
    pub data_out_size: CountLimiterBuilder,
    //
    // storage_size: CountLimiter,
    //
    pub queries_rate: RateLimiterBuilder,
    pub queries: CountLimiterBuilder,
    //
    pub writes_rate: RateLimiterBuilder,
    pub writes: CountLimiterBuilder,
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

#[test]
fn test_serialize_rate_limiter() {
    let limiter1 = RateLimiter::builder()
        .max(5)
        .interval(chrono::Duration::milliseconds(10))
        .initial(5)
        .refill(1)
        .build();
    let data = bincode::serialize(&limiter1).unwrap();
    let limiter2 = bincode::deserialize(data.as_slice()).unwrap();
    assert_eq!(limiter1, limiter2)
}
