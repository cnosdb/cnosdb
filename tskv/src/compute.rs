#[inline(always)]
pub fn decode_be_u32(key: &[u8]) -> u32 {
    unsafe { u32::from_be_bytes(*(key as *const _ as *const [u8; 4])) }
}
