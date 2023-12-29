use models::PhysicalDType;

pub trait NativeType: std::fmt::Debug + Send + Sync + 'static + Copy + Clone {
    type Bytes: AsRef<[u8]> + for<'a> TryFrom<&'a [u8], Error = std::array::TryFromSliceError>;

    fn to_le_bytes(&self) -> Self::Bytes;

    fn from_le_bytes(bytes: Self::Bytes) -> Self;

    fn ord(&self, other: &Self) -> std::cmp::Ordering;

    const TYPE: PhysicalDType;
}

macro_rules! native {
    ($type:ty, $value_type:expr) => {
        impl NativeType for $type {
            type Bytes = [u8; std::mem::size_of::<Self>()];
            #[inline]
            fn to_le_bytes(&self) -> Self::Bytes {
                Self::to_le_bytes(*self)
            }

            #[inline]
            fn from_le_bytes(bytes: Self::Bytes) -> Self {
                Self::from_le_bytes(bytes)
            }

            #[inline]
            fn ord(&self, other: &Self) -> std::cmp::Ordering {
                self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
            }

            const TYPE: PhysicalDType = $value_type;
        }
    };
}

native!(i64, PhysicalDType::Integer);
native!(u64, PhysicalDType::Unsigned);
native!(f64, PhysicalDType::Float);

/// Returns the ordering of two binary values.
pub fn ord_binary<'a>(a: &'a [u8], b: &'a [u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a.is_empty(), b.is_empty()) {
        (true, true) => return Equal,
        (true, false) => return Less,
        (false, true) => return Greater,
        (false, false) => {}
    }

    for (v1, v2) in a.iter().zip(b.iter()) {
        match v1.cmp(v2) {
            Equal => continue,
            other => return other,
        }
    }
    Equal
}

#[inline]
pub fn decode<T: NativeType>(chunk: &[u8]) -> T {
    let chunk: <T as NativeType>::Bytes = match chunk.try_into() {
        Ok(v) => v,
        Err(_) => panic!(),
    };
    T::from_le_bytes(chunk)
}
