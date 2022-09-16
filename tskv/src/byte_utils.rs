#[inline(always)]
pub fn decode_be_u16(key: &[u8]) -> u16 {
    unsafe { u16::from_be_bytes(*(key as *const _ as *const [u8; 2])) }
}

#[inline(always)]
pub fn decode_be_u32(key: &[u8]) -> u32 {
    unsafe { u32::from_be_bytes(*(key as *const _ as *const [u8; 4])) }
}

pub fn decode_be_u64(key: &[u8]) -> u64 {
    unsafe { u64::from_be_bytes(*(key as *const _ as *const [u8; 8])) }
}

pub fn decode_be_u128(key: &[u8]) -> u128 {
    unsafe { u128::from_be_bytes(*(key as *const _ as *const [u8; 16])) }
}

pub fn decode_be_i64(key: &[u8]) -> i64 {
    unsafe { i64::from_be_bytes(*(key as *const _ as *const [u8; 8])) }
}

pub fn decode_be_f64(key: &[u8]) -> f64 {
    unsafe { f64::from_be_bytes(*(key as *const _ as *const [u8; 8])) }
}

pub fn decode_be_bool(key: &[u8]) -> bool {
    let ret = false;
    let val = unsafe { u8::from_be_bytes(*(key as *const _ as *const [u8; 1])) };
    if val > 0 {
        return true;
    }
    ret
}

#[rustfmt::skip]
const BYTE_BIT_ONES_TABLE: [u8; 256] = [
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
];

pub fn one_count_u8(num: u8) -> u8 {
    BYTE_BIT_ONES_TABLE[num as usize]
}

pub fn one_count_u64(num: u64) -> u8 {
    let mut n = 0_u8;
    for part in num.to_be_bytes() {
        n += BYTE_BIT_ONES_TABLE[part as usize];
    }
    n
}

#[cfg(test)]
mod test {
    use crate::byte_utils::decode_be_bool;

    use super::{one_count_u64, one_count_u8};

    #[test]
    fn test_decode_bool() {
        let buf = [1];
        assert!(decode_be_bool(&buf));

        let buf1 = [0];
        assert!(!decode_be_bool(&buf1));

        println!("decode bool {}", decode_be_bool(&buf));
    }

    fn one_count_u32(num: u32) -> usize {
        let mut data = num;
        let mut n = 0_usize;
        while data > 0 {
            n += 1;
            data &= data - 1;
        }
        n
    }

    #[test]
    fn test_one_count() {
        println!("#[rustfmt::skip]");
        print!("const BYTE_BIT_ONES_TABLE: [u8; 256] = [\n    ");
        for i in 0..255 {
            print!("{}", one_count_u32(i));
            if (i + 1) % 16 == 0 {
                print!(",\n    ");
            } else {
                print!(", ")
            }
        }
        println!("{},\n];", one_count_u32(255));
    }

    #[test]
    fn test_one_count_2() {
        let c1 = one_count_u8(255);
        let c2 = one_count_u64(255);
        assert_eq!(c1, c2);
    }
}
