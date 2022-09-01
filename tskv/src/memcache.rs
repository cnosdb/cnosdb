use flatbuffers::Push;
use futures::future::ok;
use models::{FieldId, Timestamp, ValueType};
use protos::models::FieldType;
use std::cmp::Ordering as CmpOrdering;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::{borrow::BorrowMut, collections::HashMap, mem::size_of_val, rc::Rc};
use trace::{error, info, warn};

use crate::tsm::DataBlock;
use crate::{byte_utils, error::Result, tseries_family::TimeRange};
use parking_lot::{RwLock, RwLockWriteGuard};

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

#[derive(Debug)]
pub struct MemEntry {
    pub ts_min: i64,
    pub ts_max: i64,
    pub field_type: ValueType,
    pub cells: Vec<DataType>,
}

impl MemEntry {
    pub fn new(vtype: ValueType) -> Self {
        MemEntry {
            ts_min: i64::MAX,
            ts_max: i64::MIN,
            field_type: vtype,
            cells: Vec::with_capacity(256),
        }
    }

    pub fn write(&mut self, data: DataType) {
        let ts = data.timestamp();
        if self.ts_max < ts {
            self.ts_max = ts;
        }
        if self.ts_min > ts {
            self.ts_min = ts;
        }

        self.cells.push(data);
    }

    pub fn read_cell(&self, time_range: &TimeRange) -> Vec<DataBlock> {
        let mut data = DataBlock::new(0, self.field_type);
        for datum in self.cells.iter() {
            if datum.timestamp() >= time_range.min_ts && datum.timestamp() <= time_range.max_ts {
                data.insert(datum);
            }
        }
        return vec![data];
    }

    pub fn sort(&mut self) {
        self.cells
            .sort_by(|a, b| match a.timestamp().partial_cmp(&b.timestamp()) {
                None => {
                    error!("timestamp is illegal");
                    CmpOrdering::Less
                }
                Some(v) => v,
            });
    }

    pub fn delete_data_cell(&mut self, time_range: &TimeRange) {
        if !self.overlap(time_range) {
            return;
        }

        self.cells
            .retain(|x| x.timestamp() < time_range.min_ts || x.timestamp() > time_range.max_ts);
    }

    pub fn overlap(&self, time_range: &TimeRange) -> bool {
        !(self.ts_min > time_range.max_ts && self.ts_max < time_range.min_ts)
    }
}

#[derive(Debug)]
pub struct MemCache {
    // has been flushed
    pub flushed: bool,
    // flushing
    pub flushing: bool,
    // if true,ready be pushed into immemcache
    immutable: AtomicBool,

    // partition id
    tf_id: u32,
    // max mem buffer size convert to immemcache
    max_buf_size: u64,
    min_seq_no: u64,

    // wal seq number
    seq_no: AtomicU64,
    cache_size: AtomicU64,

    pub is_delta: bool,

    pub partions: Vec<Arc<RwLock<MemTable>>>,
}

impl MemCache {
    pub fn new(tf_id: u32, max_size: u64, seq: u64, is_delta: bool) -> Self {
        let part_count = 16;
        let mut partions = Vec::with_capacity(part_count);
        for _i in 0..part_count {
            partions.push(Arc::new(RwLock::new(MemTable::new())));
        }

        Self {
            flushed: false,
            flushing: false,

            tf_id,
            is_delta,
            partions,
            min_seq_no: seq,
            max_buf_size: max_size,

            immutable: AtomicBool::new(false),
            seq_no: AtomicU64::new(seq),
            cache_size: AtomicU64::new(0),
        }
    }

    pub fn insert_raw(&self, raw: &mut MemRaw) -> Result<()> {
        let data = raw.data();

        let data_size = size_of_val(&data) as u64;
        self.seq_no.store(raw.seq, Ordering::Relaxed);
        self.cache_size.fetch_add(data_size, Ordering::Relaxed);

        let index = (raw.field_id as usize) % self.partions.len();
        let entry = self.partions[index]
            .write()
            .write_entry(raw.field_id, raw.field_type);

        entry.write().write(data);

        Ok(())
    }

    fn get_partition(&self, id: &u64) -> usize {
        (*id as usize) % self.partions.len()
    }

    pub fn get(&self, id: &u64) -> Option<Arc<RwLock<MemEntry>>> {
        let index = self.get_partition(id);

        self.partions[index].read().read_entry(*id)
    }

    pub fn is_empty(&self) -> bool {
        for table in self.partions.iter() {
            if !table.read().is_empty() {
                return false;
            }
        }

        return true;
    }

    pub fn delete_data(&self, field_ids: &[FieldId], time_range: &TimeRange) {
        for id in field_ids {
            let index = self.get_partition(id);
            self.partions[index].read().delete_data(id, time_range);
        }
    }

    pub fn copy_data(
        &self,
        data_map: &mut HashMap<u64, Vec<Arc<RwLock<MemEntry>>>>,
        size_map: &mut HashMap<u64, usize>,
    ) {
        for part in self.partions.iter() {
            part.write().copy_data(data_map, size_map);
        }
    }

    pub fn switch_to_immutable(&self) {
        self.immutable.store(true, Ordering::Relaxed);
    }

    pub fn is_full(&self) -> bool {
        self.cache_size.load(Ordering::Relaxed) >= self.max_buf_size
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
        self.max_buf_size
    }

    pub fn cache_size(&self) -> u64 {
        self.cache_size.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct MemTable {
    fields: HashMap<u64, Arc<RwLock<MemEntry>>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn write_entry(&mut self, field_id: u64, vtype: ValueType) -> Arc<RwLock<MemEntry>> {
        self.fields
            .entry(field_id)
            .or_insert_with(|| Arc::new(RwLock::new(MemEntry::new(vtype))))
            .clone()
    }

    pub fn read_entry(&self, field_id: u64) -> Option<Arc<RwLock<MemEntry>>> {
        if let Some(v) = self.fields.get(&field_id) {
            return Some(v.clone());
        }

        None
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn copy_data<'a>(
        &'a self,
        data_map: &mut HashMap<u64, Vec<Arc<RwLock<MemEntry>>>>,
        size_map: &mut HashMap<u64, usize>,
    ) {
        for (field_id, entry) in &self.fields {
            entry.write().sort();

            let sum = size_map.entry(*field_id).or_insert(0_usize);
            *sum += entry.read().cells.len();
            let item = data_map.entry(*field_id).or_insert(vec![]);
            item.push(entry.clone());
        }
    }

    pub fn delete_data(&self, id: &u64, time_range: &TimeRange) {
        if let Some(entry) = self.fields.get(id) {
            entry.write().delete_data_cell(time_range);
        }
    }

    pub fn data(&self) -> &HashMap<u64, Arc<RwLock<MemEntry>>> {
        &self.fields
    }
}
