use std::time::Duration;

pub trait FieldKeys {
    fn field_keys() -> Vec<String>;
}

macro_rules! impl_primitive_env_keys {
    ($($t:ty),+) => {
        $(impl FieldKeys for $t {
            fn field_keys() -> Vec<String> {
                vec![]
            }
        })+
    };
}

impl_primitive_env_keys!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64, bool, char, String,
    Duration
);

impl<T> FieldKeys for Vec<T>
where
    T: FieldKeys,
{
    fn field_keys() -> Vec<String> {
        T::field_keys()
    }
}

impl<T> FieldKeys for Option<T>
where
    T: FieldKeys,
{
    fn field_keys() -> Vec<String> {
        T::field_keys()
    }
}
