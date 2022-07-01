#[inline(always)]
pub fn decode_be_u32(key: &[u8]) -> u32 {
    unsafe { u32::from_be_bytes(*(key as *const _ as *const [u8; 4])) }
}

pub fn decode_be_u64(key: &[u8]) -> u64 {
    unsafe { u64::from_be_bytes(*(key as *const _ as *const [u8; 8])) }
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

#[test]
fn test_decode_bool() {
    let buf = [1];
    assert!(decode_be_bool(&buf));

    let buf1 = [0];
    assert!(!decode_be_bool(&buf1));

    println!("decode bool {}", decode_be_bool(&buf));
}
