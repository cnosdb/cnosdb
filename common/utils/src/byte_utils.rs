#![allow(clippy::macro_metavars_in_unsafe)]

#[macro_export]
macro_rules! to_bytes {
    ($ty: expr) => {
        $ty.to_le_bytes()
    };
}

#[macro_export]
macro_rules! from_bytes {
    ($primitive_type: ty, $data: expr) => {
        unsafe {
            const LEN: usize = std::mem::size_of::<$primitive_type>();
            <$primitive_type>::from_le_bytes(*($data as *const _ as *const [u8; LEN]))
        }
    };
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        let value = -10244_i64;
        let buffer = to_bytes!(value);
        let data = from_bytes!(i64, &buffer[0..8]);
        println!("Value: {}", data);
    }
}
