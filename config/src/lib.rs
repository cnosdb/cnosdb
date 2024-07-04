use std::time::Duration;

use macros::EnvKeys;
use once_cell::sync::Lazy;
mod check;
mod codec;
pub mod common;
pub mod meta;
pub mod tskv;

pub static VERSION: Lazy<String> = Lazy::new(|| {
    format!(
        "{}, revision {}",
        option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN"),
        option_env!("GIT_HASH").unwrap_or("UNKNOWN")
    )
});

trait EnvKeys {
    fn env_keys() -> Vec<String>;
}

macro_rules! impl_primitive_env_keys {
    ($($t:ty),+) => {
        $(impl EnvKeys for $t {
            fn env_keys() -> Vec<String> {
                vec![]
            }
        })+
    };
}

impl_primitive_env_keys!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64, bool, char, String,
    Duration
);

impl<T> EnvKeys for Vec<T>
where
    T: EnvKeys,
{
    fn env_keys() -> Vec<String> {
        T::env_keys()
    }
}

impl<T> EnvKeys for Option<T>
where
    T: EnvKeys,
{
    fn env_keys() -> Vec<String> {
        T::env_keys()
    }
}
