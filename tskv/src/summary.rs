pub struct CompactMeta{
    file_id: u64, //file id  
    ts_min: u64, 
    ts_max: u64,
}

// use flatbufer to decode/encode
pub struct VersionEdit {
    pub level: u32,
    pub seq_no: u64,
    pub log_seq: u64,
    pub add_files: Vec<CompactMeta>,
    pub del_files: Vec<CompactMeta>,  
}


impl VersionEdit {
    pub fn encode(){
        
    }
    pub fn decode(){

    }
}
