use std::io::Bytes;
pub struct Points {
    pub series_id: u64,
    pub timestamp: u64,
    pub filed_id: u8,
    pub value: Bytes<u8>,
}
