use std::sync::Arc;

use bincode::Result;
use hashbrown::HashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    kv_option::{TseriesFamDesc, TseriesFamOpt},
    Version, VersionSet,
};

#[derive(Serialize, Deserialize, PartialEq, Debug, Default)]
pub struct CompactMeta {
    pub file_id: u64, //file id
    pub ts_min: u64,
    pub ts_max: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct VersionEdit {
    pub level: u32,
    pub seq_no: u64,
    pub log_seq: u64,
    pub add_files: Vec<CompactMeta>,
    pub del_files: Vec<CompactMeta>,
}

impl VersionEdit {
    pub fn new(
        level: u32,
        seq_no: u64,
        log_seq: u64,
        add_files: Vec<CompactMeta>,
        del_files: Vec<CompactMeta>,
    ) -> Self {
        Self {
            level,
            seq_no,
            log_seq,
            add_files,
            del_files,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
    }
    pub fn decode(buf: &Vec<u8>) -> Result<Self> {
        bincode::deserialize(buf)
    }
}

pub struct Summary {
    //writer
    //seq_id
    versions: HashMap<u32, Arc<Version>>,
    c_options: HashMap<u32, Arc<TseriesFamOpt>>,
    file_id: u64,
    version_set: Arc<Mutex<VersionSet>>,
}

impl Summary {
    //create a new summary file
    pub async fn new(tf_desc: &[TseriesFamDesc]) {
        //todo:
        // let mut db = VersionEdit::new(0, 0, 0, vec![], vec![]);
    }
    //recover from summary file
    pub async fn recover() {}
    // apply version edit to summary file
    // and write to memory struct
    pub async fn apply_version_edit() {}
}

#[test]
fn test_version_edit() {
    let mut compact = CompactMeta::default();
    compact.file_id = 100;
    let add_list = vec![compact];
    let mut compact2 = CompactMeta::default();
    compact2.file_id = 101;
    let del_list = vec![compact2];
    let ve = VersionEdit::new(1, 2, 3, add_list, del_list);
    let buf = ve.encode().unwrap();
    let ve2 = VersionEdit::decode(&buf).unwrap();
    assert_eq!(ve2, ve);
}
