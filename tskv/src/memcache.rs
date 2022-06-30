use std::{borrow::BorrowMut, collections::HashMap, mem::size_of_val, rc::Rc};

use flatbuffers::Push;
use futures::future::ok;
use models::ValueType;
use protos::models::FieldType;

use crate::{compute, error::Result, tseries_family::TimeRange};

#[allow(dead_code)]
#[derive(Default, Debug, Clone, Copy)]
pub struct DataCell<T> {
    pub ts: i64,
    pub val: T,
}

pub type Byte = Vec<u8>;
pub type U64Cell = DataCell<u64>;
pub type I64Cell = DataCell<i64>;
pub type StrCell = DataCell<Byte>;
pub type F64Cell = DataCell<f64>;
pub type BoolCell = DataCell<bool>;

#[derive(Debug, Clone)]
pub enum DataType {
    U64(U64Cell),
    I64(I64Cell),
    Str(StrCell),
    F64(F64Cell),
    Bool(BoolCell),
}

impl DataType {
    pub fn timestamp(&self) -> i64 {
        match *self {
            DataType::U64(U64Cell { ts, .. }) => ts,
            DataType::I64(I64Cell { ts, .. }) => ts,
            DataType::Str(StrCell { ts, .. }) => ts,
            DataType::F64(F64Cell { ts, .. }) => ts,
            DataType::Bool(BoolCell { ts, .. }) => ts,
        }
    }
}

#[derive(Debug)]
pub struct MemEntry {
    pub ts_min: i64,
    pub ts_max: i64,
    pub field_type: ValueType,
    pub cells: Vec<DataType>,
}
impl Default for MemEntry {
    fn default() -> Self {
        MemEntry { ts_min: i64::MAX,
                   ts_max: i64::MIN,
                   field_type: ValueType::Unknown,
                   cells: Vec::new() }
    }
}

impl MemEntry {
    pub fn read_cell(&self, time_range: &TimeRange) {
        for data in self.cells.iter() {
            if data.timestamp() > time_range.min_ts && data.timestamp() < time_range.max_ts {
                println!("{:?}", data.clone())
            }
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct MemCache {
    // if true,ready be pushed into immemcache
    immutable: bool,
    // partiton id
    tf_id: u32,
    // wal seq number
    pub seq_no: u64,
    // max mem buffer size convert to immcache
    max_buf_size: u64,
    // block <field_id, buffer>
    // field_id contain the field type
    pub data_cache: HashMap<u64, MemEntry>,
    // current size
    cache_size: u64,
}

impl MemCache {
    pub fn new(tf_id: u32, max_size: u64, seq: u64) -> Self {
        let cache = HashMap::new();
        Self { immutable: false,
               tf_id,
               max_buf_size: max_size,
               data_cache: cache,
               seq_no: seq,
               cache_size: 0 }
    }
    pub fn insert_raw(&mut self,
                      seq: u64,
                      field_id: u64,
                      ts: i64,
                      field_type: ValueType,
                      buf: &[u8])
                      -> Result<()> {
        self.seq_no = seq;
        match field_type {
            ValueType::Unsigned => {
                let val = compute::decode_be_u64(buf);
                let data = DataType::U64(U64Cell { ts, val });
                self.insert(field_id, data, ValueType::Unsigned);
            },
            ValueType::Integer => {
                let val = compute::decode_be_i64(buf);
                let data = DataType::I64(I64Cell { ts, val });
                self.insert(field_id, data, ValueType::Integer);
            },
            ValueType::Float => {
                let val = compute::decode_be_f64(buf);
                let data = DataType::F64(F64Cell { ts, val });
                self.insert(field_id, data, ValueType::Float);
            },
            ValueType::String => {
                let val = Vec::from(buf);
                let data = DataType::Str(StrCell { ts, val });
                self.insert(field_id, data, ValueType::String);
            },
            ValueType::Boolean => {
                let val = compute::decode_be_bool(buf);
                let data = DataType::Bool(BoolCell { ts, val });
                self.insert(field_id, data, ValueType::Boolean)
            },
            _ => todo!(),
        };
        Ok(())
    }
    pub fn insert(&mut self, field_id: u64, val: DataType, value_type: ValueType) {
        let ts = val.timestamp();
        let item = self.data_cache.entry(field_id).or_insert(MemEntry::default());
        if item.ts_max < ts {
            item.ts_max = ts;
        }
        if item.ts_min > ts {
            item.ts_min = ts
        }
        item.field_type = value_type;
        self.cache_size += size_of_val(&val) as u64;
        item.cells.push(val);
    }

    // pub fn data_cache(&self) -> HashMap<u64, MemEntry> {
    //     self.data_cache
    // }

    pub fn switch_to_immutable(&mut self) {
        for data in self.data_cache.iter_mut() {
            data.1.cells.sort_by(|a, b| a.timestamp().partial_cmp(&b.timestamp()).unwrap())
        }
        self.immutable = true;
    }

    pub fn flush() -> Result<()> {
        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.cache_size >= self.max_buf_size
    }

    pub fn tf_id(&self) -> u32 {
        self.tf_id
    }

    pub fn max_buf_size(&self) -> u64 {
        self.max_buf_size
    }
}
