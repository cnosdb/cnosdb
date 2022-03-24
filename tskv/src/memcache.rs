use crate::error::Result;
use std::collections::HashMap;
// use protos::models::FieldType;
// pub enum FieldTypeT {}

pub trait PrimitiveType: 'static {
    fn to_bytes() {
        todo!()
    }
}
#[allow(dead_code)]
#[derive(Default, Debug)]
pub struct DataCell<T> {
    ts: u64,
    val: T,
}
pub type U64Cell = DataCell<u64>;
pub type I64Cell = DataCell<i64>;
pub type StrCell = DataCell<String>;
pub type F64Cell = DataCell<f64>;
pub type BoolCell = DataCell<bool>;

#[allow(dead_code)]
#[derive(Default, Debug)]
pub struct MemEntry<T> {
    ts_min: u64,
    ts_max: u64,
    cells: Vec<DataCell<T>>,
}

#[allow(dead_code)]
#[derive(Default, Debug)]
pub struct MemCache<T> {
    // partiton id
    partition_id: u16,
    //wal seq number
    seq_no: u64,
    //max mem buffer size convert to immcache
    max_buf_size: u32,
    //block <filed_id, buffer>
    // filed_id contain the
    data_cache: HashMap<u64, MemEntry<T>>,
    //current size
    cache_size: u32,
}

impl<T> MemCache<T> {
    pub fn new(partiton_id: u16, max_size: u32, seq: u64) -> Self {
        let cache = HashMap::new();
        Self {
            partition_id: partiton_id,
            max_buf_size: max_size,
            data_cache: cache,
            seq_no: seq,
            cache_size: 1 << 20,
        }
    }

    pub fn insert(&mut self, _filed_id: u64, _entry: MemEntry<T>) -> Result<()> {
        // let size = mem::size_of<T>();
        // let x = FieldType::default();
        Ok(())
    }
}
