use flatbuffers::Push;
use futures::future::ok;
use models::{utils, FieldId, RwLockRef, Timestamp, ValueType};
use protos::models::{FieldType, Rows};
use std::cmp::Ordering as CmpOrdering;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::{borrow::BorrowMut, collections::HashMap, mem::size_of_val, rc::Rc};
use trace::{error, info, warn};

use crate::tsm::DataBlock;
use crate::{byte_utils, error::Result, tseries_family::TimeRange};
use parking_lot::{RwLock, RwLockWriteGuard};

pub enum FieldVal {
    Float(f64),
    Integer(i64),
    Unsigned(u64),
    Boolean(bool),
    Bytes(Vec<u8>),
}

impl FieldVal {
    pub fn value_type(&self) -> ValueType {
        match *self {
            FieldVal::Float(..) => ValueType::Float,
            FieldVal::Integer(..) => ValueType::Integer,
            FieldVal::Unsigned(..) => ValueType::Unsigned,
            FieldVal::Boolean(..) => ValueType::Boolean,
            FieldVal::Bytes(..) => ValueType::String,
        }
    }

    pub fn data_value(&self, ts: i64) -> DataType {
        match *self {
            FieldVal::Float(val) => DataType::U64(U64Cell { ts, val }),
            FieldVal::Integer(val) => DataType::I64(I64Cell { ts, val }),
            FieldVal::Unsigned(val) => DataType::F64(F64Cell { ts, val }),
            FieldVal::Boolean(val) => DataType::Str(StrCell { ts, val }),
            FieldVal::Bytes(val) => DataType::Bool(BoolCell { ts, val: val.clone() }),
        }
    }
}
pub struct RowData {
    pub ts: u64,
    pub fields: Vec<FieldVal>,
}

pub struct RowGroup {
    pub schema_id: u32,
    pub schema: Vec<u32>,
    pub rows: Vec<RowData>,
}

pub struct SeriesData {
    pub range: TimeRange,
    pub groups: Vec<RowGroup>,
}

impl SeriesData {
    pub fn write(&mut self, group: &mut RowGroup, range: &TimeRange) {
        if range.min_ts < self.range.min_ts {
            self.range.min_ts = range.min_ts;
        }

        if range.max_ts > self.range.max_ts {
            self.range.max_ts = range.max_ts;
        }

        for item in self.groups.iter_mut() {
            if item.schema_id == group.schema_id {
                item.rows.append(&mut group.rows);
                return;
            }
        }

        self.groups.push(group);
    }

    pub fn delete_data(&self, range: &TimeRange) {
        if range.max_ts < self.range.min_ts || range.min_ts > self.range.max_ts {
            return;
        }

        for item in self.groups.iter_mut() {
            item.rows.retain(|&row| row.ts < range.min_ts || row.ts > range.max_ts);
        }
    }

    pub fn read_entry(&self, field_id: &u64) -> Option<Arc<RwLock<MemEntry>>> {
        let mut entry = MemEntry {
            ts_min: self.range.min_ts,
            ts_max: self.range.max_ts,
            field_type: ValueType::Unknown,
            cells: Vec::new(),
        };

        for group in self.groups.iter() {
            let mut index = usize::MAX;
            for i in 0..group.schema.len() {
                if field_id == group.schema[i] {
                    index = i;
                    break;
                }
            }
            if index == usize::MAX {
                continue;
            }

            for row in group.rows.iter() {
                entry.field_type = row.fields[index].value_type();
                entry.cells.push(row.fields[index].data_value(row.ts));
            }
        }

        return Some(Arc::new(RwLock::new(entry)));
    }
}

impl Default for SeriesData {
    fn default() -> Self {
        Self {
            range: (i64::MAX, i64::MIN),
            groups: Vec::with_capacity(4),
        }
    }
}

#[derive(Debug)]
pub struct MemCache {
    tf_id: u32,

    max_size: u64,
    min_seq_no: u64,

    // wal seq number
    seq_no: AtomicU64,
    cache_size: AtomicU64,

    part_count: usize,
    partions: Vec<RwLock<HashMap<u64, RwLockRef<SeriesData>>>>,
}

impl MemCache {
    pub fn new(tf_id: u32, max_size: u64, seq: u64, parts: u32) -> Self {
        let mut partions = Vec::with_capacity(parts);
        for _i in 0..parts {
            partions.push(RwLock::new(HashMap::new()));
        }

        Self {
            tf_id,
            partions,
            max_size,
            min_seq_no: seq,

            part_count: parts as usize,

            seq_no: AtomicU64::new(seq),
            cache_size: AtomicU64::new(0),
        }
    }

    pub fn write_group(&self, sid: u64, seq: u64, range: &TimeRange, group: &mut RowGroup) {
        self.seq_no.store(seq, Ordering::Relaxed);
        self.cache_size.fetch_add(size_of_val(&group) as u64, Ordering::Relaxed);

        let index = (sid as usize) % self.part_count;
        let entry = self.partions[index]
            .write()
            .entry(sid)
            .or_insert_with(|| Arc::new(RwLock::new(SeriesData::default())))
            .clone();

        entry.write().write(group, range);
    }

    pub fn get(&self, field_id: &u64) -> Option<Arc<RwLock<MemEntry>>> {
        let (field_id, sid) = utils::split_id(field_id);

        let index = (sid as usize) % self.part_count;
        let part = self.partions[index].read();
        if let Some(series) = part.get(&sid) {
            return series.read().read_entry(field_id);
        }

        None
    }

    pub fn is_empty(&self) -> bool {
        for part in self.partions.iter() {
            if !part.read().is_empty() {
                return false;
            }
        }

        return true;
    }

    pub fn delete_data(&self, range: &TimeRange) {
        for part in self.partions.iter() {
            let part = part.read();
            for (_, item) in part.iter() {
                item.read().delete_data(range);
            }
        }
    }

    pub fn copy_data(
        &self,
        data_map: &mut HashMap<u64, Vec<Arc<RwLock<MemEntry>>>>,
        size_map: &mut HashMap<u64, usize>,
    ) {
        todo!()
    }

    pub fn is_full(&self) -> bool {
        self.cache_size.load(Ordering::Relaxed) >= self.max_size
    }

    pub fn tf_id(&self) -> u32 {
        self.tf_id
    }

    pub fn seq_no(&self) -> u64 {
        self.seq_no.load(Ordering::Relaxed)
    }

    pub fn min_seq_no(&self) -> u64 {
        self.min_seq_no
    }

    pub fn max_buf_size(&self) -> u64 {
        self.max_size
    }

    pub fn cache_size(&self) -> u64 {
        self.cache_size.load(Ordering::Relaxed)
    }
}

///////////////////////////////////////
#[derive(Debug)]
pub struct MemEntry {
    pub ts_min: i64,
    pub ts_max: i64,
    pub field_type: ValueType,
    pub cells: Vec<DataType>,
}

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
