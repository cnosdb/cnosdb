#[derive(Debug)]
pub struct Record {
    pub data_type: u8,
    pub data_version: u8,
    pub data: Vec<u8>,
    pub pos: u64,
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pos: {}, len: {}, type: {}, version: {}",
            self.pos,
            self.data.len(),
            self.data_type,
            self.data_version
        )
    }
}
