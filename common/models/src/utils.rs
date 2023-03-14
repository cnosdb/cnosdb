use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

const LOW_32BIT_MASK: u64 = (0x01 << 32) - 1;
const HIGH_32BIT_MASK: u64 = ((0x01 << 32) - 1) << 32;

pub const SECOND_NANOS: i64 = 1_000_000_000;
pub const MINUTES_NANOS: i64 = 60 * SECOND_NANOS;
pub const HOUR_NANOS: i64 = 60 * MINUTES_NANOS;
pub const DAY_NANOS: i64 = 24 * HOUR_NANOS;

pub const SECOND_MICROS: i64 = 1_000_000;
pub const MINUTES_MICROS: i64 = 60 * SECOND_MICROS;
pub const HOUR_MICROS: i64 = 60 * MINUTES_MICROS;
pub const DAY_MICROS: i64 = 24 * HOUR_MICROS;

pub const SECOND_MILLS: i64 = 1_000;
pub const MINUTES_MILLS: i64 = 60 * SECOND_MILLS;
pub const HOUR_MILLS: i64 = 60 * MINUTES_MILLS;
pub const DAY_MILLS: i64 = 24 * HOUR_MILLS;

pub fn split_id(id: u64) -> (u32, u32) {
    (
        ((id & HIGH_32BIT_MASK) >> 32) as u32,
        (id & LOW_32BIT_MASK) as u32,
    )
}

pub fn unite_id(hash_id: u32, incr_id: u32) -> u64 {
    let high = (hash_id as u64) << 32;
    let low = (incr_id & LOW_32BIT_MASK as u32) as u64;

    high | low
}

pub fn now_timestamp_nanos() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_nanos() as i64,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

pub fn now_timestamp_micros() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_micros() as i64,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

pub fn now_timestamp_millis() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_millis() as i64,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

pub fn to_str(arr: &[u8]) -> String {
    String::from_utf8(arr.to_vec()).unwrap()
}

pub fn min_num<T: std::cmp::PartialOrd>(a: T, b: T) -> T {
    if a < b {
        a
    } else {
        b
    }
}

pub fn max_num<T: std::cmp::PartialOrd>(a: T, b: T) -> T {
    if a > b {
        a
    } else {
        b
    }
}

#[derive(Default)]
pub struct SeqIdGenerator {
    next_id: AtomicU64,
}

impl SeqIdGenerator {
    pub fn new(start: u64) -> Self {
        Self {
            next_id: AtomicU64::new(start),
        }
    }

    pub fn next_id(&self) -> u64 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

#[macro_export]
macro_rules! extensions_options {
    (
     $(#[doc = $struct_d:tt])*
     $vis:vis struct $struct_name:ident {
        $(
        $(#[doc = $d:tt])*
        $field_vis:vis $field_name:ident : $field_type:ty, default = $default:expr
        )*$(,)*
    }
    ) => {
        $(#[doc = $struct_d])*
        #[derive(Debug, Clone)]
        #[non_exhaustive]
        $vis struct $struct_name{
            $(
            $(#[doc = $d])*
            $field_vis $field_name : $field_type,
            )*
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self {
                    $($field_name: $default),*
                }
            }
        }

        impl datafusion::config::ExtensionOptions for $struct_name {
            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn ::std::any::Any {
                self
            }

            fn cloned(&self) -> Box<dyn datafusion::config::ExtensionOptions> {
                Box::new(self.clone())
            }

            fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
                match key {
                    $(
                       stringify!($field_name) => {
                        self.$field_name = value.parse().map_err(|e| {
                            datafusion::error::DataFusionError::Context(
                                format!(concat!("Error parsing {} as ", stringify!($t),), value),
                                Box::new(datafusion::error::DataFusionError::External(Box::new(e))),
                            )
                        })?;
                        Ok(())
                       }
                    )*
                    _ => Err(datafusion::error::DataFusionError::Internal(
                        format!(concat!("Config value \"{}\" not found on ", stringify!($struct_name)), key)
                    ))
                }
            }

            fn entries(&self) -> Vec<datafusion::config::ConfigEntry> {
                vec![
                    $(
                        datafusion::config::ConfigEntry {
                            key: stringify!($field_name).to_owned(),
                            value: (self.$field_name != $default).then(|| self.$field_name.to_string()),
                            description: concat!($($d),*).trim(),
                        },
                    )*
                ]
            }
        }
    }
}
