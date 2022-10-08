#[derive(Debug, Clone)]
pub struct CommandHeader {
    pub typ: u32,
    pub len: u32,
}

pub struct CommonResponse {
    pub flag: u32,
    pub data: String,
}
