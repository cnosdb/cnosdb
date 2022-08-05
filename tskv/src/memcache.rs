use std::{borrow::BorrowMut, cmp::Ordering, collections::HashMap, mem::size_of_val, rc::Rc};

use flatbuffers::Push;
use futures::future::ok;
use models::{FieldId, Timestamp, ValueType};
use protos::models::FieldType;
use trace::{error, info, warn};

use crate::{byte_utils, error::Result, tseries_family::TimeRange};

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
        MemEntry {
            ts_min: i64::MAX,
            ts_max: i64::MIN,
            field_type: ValueType::Unknown,
            cells: Vec::new(),
        }
    }
}

impl MemEntry {
    pub fn read_cell(&self, time_range: &TimeRange) {
        for data in self.cells.iter() {
            if data.timestamp() > time_range.min_ts && data.timestamp() < time_range.max_ts {
                info!("{:?}", data.clone())
            }
        }
    }

    pub fn overlap(&self, time_range: &TimeRange) -> bool {
        !(self.ts_min > time_range.max_ts && self.ts_max < time_range.min_ts)
    }

    pub fn delete_data_cell(&mut self, time_range: &TimeRange) {
        self.cells
            .retain(|x| x.timestamp() < time_range.min_ts || x.timestamp() > time_range.max_ts);
    }
}

pub struct MemRaw<'a> {
    pub seq: u64,
    pub ts: Timestamp,
    pub field_id: FieldId,
    pub field_type: ValueType,
    pub val: &'a [u8],
}

impl MemRaw<'_> {
    pub fn data(&self) -> DataType {
        match self.field_type {
            ValueType::Unsigned => {
                let val = byte_utils::decode_be_u64(self.val);
                DataType::U64(U64Cell { ts: self.ts, val })
            }
            ValueType::Integer => {
                let val = byte_utils::decode_be_i64(self.val);
                DataType::I64(I64Cell { ts: self.ts, val })
            }
            ValueType::Float => {
                let val = byte_utils::decode_be_f64(self.val);
                DataType::F64(F64Cell { ts: self.ts, val })
            }
            ValueType::String => {
                let val = Vec::from(self.val);
                DataType::Str(StrCell { ts: self.ts, val })
            }
            ValueType::Boolean => {
                let val = byte_utils::decode_be_bool(self.val);
                DataType::Bool(BoolCell { ts: self.ts, val })
            }
            _ => todo!(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct MemCache {
    // has been flushed
    pub flushed: bool,
    // flushing
    pub flushing: bool,
    // if true,ready be pushed into immemcache
    immutable: bool,
    // partition id
    tf_id: u32,
    // wal seq number
    pub seq_no: u64,
    // max mem buffer size convert to immemcache
    max_buf_size: u64,
    // block <field_id, buffer>
    // field_id contain the field type
    pub data_cache: HashMap<FieldId, MemEntry>,
    // current size
    cache_size: u64,

    pub is_delta: bool,
}

impl MemCache {
    pub fn new(tf_id: u32, max_size: u64, seq: u64, is_delta: bool) -> Self {
        let cache = HashMap::new();
        Self {
            flushed: false,
            flushing: false,
            immutable: false,
            tf_id,
            max_buf_size: max_size,
            data_cache: cache,
            seq_no: seq,
            cache_size: 0,
            is_delta,
        }
    }

    pub fn insert_raw(&mut self, raw: &mut MemRaw) -> Result<()> {
        self.seq_no = raw.seq;
        let data = raw.data();
        let ts = data.timestamp();
        let item = self
            .data_cache
            .entry(raw.field_id)
            .or_insert_with(MemEntry::default);
        if item.ts_max < ts {
            item.ts_max = ts;
        }
        if item.ts_min > ts {
            item.ts_min = ts
        }
        item.field_type = raw.field_type;
        self.cache_size += size_of_val(&data) as u64;
        item.cells.push(data);
        Ok(())
    }

    pub fn switch_to_immutable(&mut self) {
        for data in self.data_cache.iter_mut() {
            data.1
                .cells
                .sort_by(|a, b| match a.timestamp().partial_cmp(&b.timestamp()) {
                    None => {
                        error!("timestamp is illegal");
                        Ordering::Less
                    }
                    Some(v) => v,
                })
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

    pub fn cache_size(&self) -> u64 {
        self.cache_size
    }
}
