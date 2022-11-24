#[derive(Debug)]
pub struct Record {
    pub data_type: u8,
    pub data_version: u8,
    pub data: Vec<u8>,
    pub pos: u64,
}
