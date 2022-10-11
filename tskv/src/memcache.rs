use flatbuffers::{ForwardsUOffset, Push, Vector};
use futures::future::ok;

use models::{utils, FieldId, RwLockRef, SchemaId, SeriesId, Timestamp, ValueType};
use protos::models::{Field, FieldType, Point};

use std::cmp::Ordering as CmpOrdering;
use std::collections::HashSet;
use std::fmt::Display;
use std::iter::{FromIterator, Peekable};
use std::mem::size_of;
use std::ops::Index;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::{borrow::BorrowMut, collections::HashMap, mem::size_of_val, rc::Rc};

use minivec::{mini_vec, MiniVec};
use trace::{error, info, warn};

use crate::tsm::DataBlock;
use crate::{byte_utils, error::Result, tseries_family::TimeRange};
use models::schema::{TableFiled, TableSchema};
use models::utils::{split_id, unite_id};
use parking_lot::{RwLock, RwLockReadGuard};
use snafu::OptionExt;

use protos::models as fb_models;

#[derive(Debug, Clone, PartialEq)]
pub enum FieldVal {
    Float(f64),
    Integer(i64),
    Unsigned(u64),
    Boolean(bool),
    Bytes(MiniVec<u8>),
}

impl FieldVal {
    pub fn value_type(&self) -> ValueType {
        match self {
            FieldVal::Float(..) => ValueType::Float,
            FieldVal::Integer(..) => ValueType::Integer,
            FieldVal::Unsigned(..) => ValueType::Unsigned,
            FieldVal::Boolean(..) => ValueType::Boolean,
            FieldVal::Bytes(..) => ValueType::String,
        }
    }

    pub fn data_value(&self, ts: i64) -> DataType {
        match self {
            FieldVal::Float(val) => DataType::F64(ts, *val),
            FieldVal::Integer(val) => DataType::I64(ts, *val),
            FieldVal::Unsigned(val) => DataType::U64(ts, *val),
            FieldVal::Boolean(val) => DataType::Bool(ts, *val),
            FieldVal::Bytes(val) => DataType::Str(ts, val.clone()),
        }
    }

    pub fn new(val: MiniVec<u8>, vtype: ValueType) -> FieldVal {
        match vtype {
            ValueType::Unsigned => {
                let val = byte_utils::decode_be_u64(&val);
                FieldVal::Unsigned(val)
            }
            ValueType::Integer => {
                let val = byte_utils::decode_be_i64(&val);
                FieldVal::Integer(val)
            }
            ValueType::Float => {
                let val = byte_utils::decode_be_f64(&val);
                FieldVal::Float(val)
            }
            ValueType::Boolean => {
                let val = byte_utils::decode_be_bool(&val);
                FieldVal::Boolean(val)
            }
            ValueType::String => {
                //let val = Vec::from(val);
                FieldVal::Bytes(val)
            }
            _ => todo!(),
        }
    }

    pub fn heap_size(&self) -> usize {
        if let FieldVal::Bytes(val) = self {
            val.capacity()
        } else {
            0
        }
    }
}

impl Display for FieldVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldVal::Unsigned(val) => write!(f, "{}", val),
            FieldVal::Integer(val) => write!(f, "{}", val),
            FieldVal::Float(val) => write!(f, "{}", val),
            FieldVal::Boolean(val) => write!(f, "{}", val),
            FieldVal::Bytes(val) => write!(f, "{:?})", val),
        }
    }
}

#[derive(Debug)]
pub struct RowData {
    pub ts: i64,
    pub fields: Vec<Option<FieldVal>>,
}

impl RowData {
    pub fn point_to_row_data(
        p: fb_models::Point,
        schema: TableSchema,
        sid: SeriesId,
    ) -> (RowData, Vec<u32>) {
        let mut field_id = vec![];
        let fields = match p.fields() {
            None => {
                let mut fields = Vec::with_capacity(schema.field_fields_num());
                for i in 0..fields.capacity() {
                    fields.push(None);
                    field_id.push(0);
                }
                fields
            }
            Some(fields_inner) => {
                let fields_id = schema.fields_id();
                let mut fields: Vec<Option<FieldVal>> = Vec::with_capacity(fields_id.len());
                for i in 0..fields.capacity() {
                    fields.push(None);
                    field_id.push(0);
                }
                for (i, f) in fields_inner.into_iter().enumerate() {
                    let vtype = f.type_().into();
                    let val = MiniVec::from(f.value().unwrap());
                    match schema.fields.get(
                        String::from_utf8(f.name().unwrap().to_vec())
                            .unwrap()
                            .as_str(),
                    ) {
                        None => {}
                        Some(field) => match fields_id.get(&field.id) {
                            None => {}
                            Some(index) => {
                                fields[*index] = Some(FieldVal::new(val, vtype));
                                field_id[*index] = field.id as u32;
                            }
                        },
                    }
                }
                fields
            }
        };
        let ts = p.timestamp();
        (RowData { ts, fields }, field_id)
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for i in self.fields.iter() {
            match i {
                None => {
                    size += size_of_val(i);
                }
                Some(v) => {
                    size += size_of_val(i) + v.heap_size();
                }
            }
        }
        size += size_of_val(&self.ts);
        size += size_of_val(&self.fields);
        size
    }
}

impl From<fb_models::Point<'_>> for RowData {
    fn from(p: fb_models::Point<'_>) -> Self {
        let fields = match p.fields() {
            Some(fields_inner) => {
                let mut fields = Vec::with_capacity(fields_inner.len());
                for f in fields_inner.into_iter() {
                    let vtype = f.type_().into();
                    let val = MiniVec::from(f.value().unwrap());
                    fields.push(Some(FieldVal::new(val, vtype)));
                }
                fields
            }
            None => vec![],
        };

        let ts = p.timestamp();
        Self { ts, fields }
    }
}

#[derive(Debug)]
pub struct RowGroup {
    pub schema_id: SchemaId,
    pub schema: Vec<u32>,
    pub range: TimeRange,
    pub rows: Vec<RowData>,
    /// total size in stack and heap
    pub size: usize,
}

#[derive(Debug)]
pub struct SeriesData {
    pub range: TimeRange,
    pub groups: Vec<RowGroup>,
}

impl SeriesData {
    pub fn write(&mut self, mut group: RowGroup) {
        self.range.merge(&group.range);

        for item in self.groups.iter_mut() {
            if item.schema_id == group.schema_id {
                item.range.merge(&group.range);
                item.rows.append(&mut group.rows);
                item.schema.append(&mut group.schema);
                item.schema.sort();
                item.schema.dedup();
                return;
            }
        }

        self.groups.push(group);
    }

    pub fn delete_data(&mut self, range: &TimeRange) {
        if range.max_ts < self.range.min_ts || range.min_ts > self.range.max_ts {
            return;
        }

        for item in self.groups.iter_mut() {
            item.rows
                .retain(|row| row.ts < range.min_ts || row.ts > range.max_ts);
        }
    }

    pub fn read_entry(&self, field_id: u32) -> Option<Arc<RwLock<MemEntry>>> {
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
                if let Some(Some(field)) = row.fields.get(index) {
                    entry.field_type = field.value_type();
                    entry.cells.push(field.data_value(row.ts));
                }
            }
        }

        if entry.field_type == ValueType::Unknown || entry.cells.is_empty() {
            return None;
        }

        entry.sort();

        Some(Arc::new(RwLock::new(entry)))
    }

    pub fn flat_groups(&self) -> Vec<(SchemaId, &Vec<u32>, &Vec<RowData>)> {
        self.groups
            .iter()
            .map(|g| (g.schema_id, &g.schema, &g.rows))
            .collect()
    }
}

impl Default for SeriesData {
    fn default() -> Self {
        Self {
            range: TimeRange {
                min_ts: i64::MAX,
                max_ts: i64::MIN,
            },
            groups: Vec::with_capacity(4),
        }
    }
}

#[derive(Debug)]
pub struct MemCache {
    tf_id: u32,

    pub flushed: bool,
    pub flushing: bool,

    max_size: u64,
    min_seq_no: u64,

    // wal seq number
    seq_no: AtomicU64,
    cache_size: AtomicU64,

    part_count: usize,
    partions: Vec<RwLock<HashMap<u64, RwLockRef<SeriesData>>>>,
}

impl MemCache {
    pub fn new(tf_id: u32, max_size: u64, seq: u64) -> Self {
        let parts = 16;
        let mut partions = Vec::with_capacity(parts);
        for _i in 0..parts {
            partions.push(RwLock::new(HashMap::new()));
        }

        Self {
            tf_id,
            partions,
            max_size,
            min_seq_no: seq,

            flushed: false,
            flushing: false,

            part_count: parts as usize,

            seq_no: AtomicU64::new(seq),
            cache_size: AtomicU64::new(0),
        }
    }

    pub fn write_group(&self, sid: u64, seq: u64, group: RowGroup) {
        let (_, sid) = split_id(sid);
        self.seq_no.store(seq, Ordering::Relaxed);
        self.cache_size
            .fetch_add(group.size as u64, Ordering::Relaxed);

        let index = (sid as usize) % self.part_count;
        let entry = self.partions[index]
            .write()
            .entry(sid)
            .or_insert_with(|| Arc::new(RwLock::new(SeriesData::default())))
            .clone();

        entry.write().write(group);
    }

    pub fn get(&self, field_id: &u64) -> Option<Arc<RwLock<MemEntry>>> {
        let (field_id, sid) = utils::split_id(*field_id);

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

        true
    }

    pub fn delete_data(&self, field_ids: &[FieldId], range: &TimeRange) {
        for fid in field_ids {
            let (_, sid) = utils::split_id(*fid);
            let index = (sid as usize) % self.part_count;
            let part = self.partions[index].read();
            if let Some(data) = part.get(&sid) {
                data.write().delete_data(range);
            }
        }
    }

    pub fn read_series_data(&self) -> Vec<(SeriesId, Arc<RwLock<SeriesData>>)> {
        let mut ret = Vec::new();
        self.partions.iter().for_each(|p| {
            let p_rlock = p.read();
            for (k, v) in p_rlock.iter() {
                ret.push((*k, v.clone()));
            }
        });
        ret
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

impl MemEntry {
    pub fn data_block(&self, time_range: &TimeRange) -> DataBlock {
        let mut data = DataBlock::new(0, self.field_type);
        if time_range.is_boundless() {
            for datum in self.cells.iter() {
                data.insert(datum);
            }
        } else {
            for datum in self.cells.iter() {
                if datum.timestamp() >= time_range.min_ts && datum.timestamp() <= time_range.max_ts
                {
                    data.insert(datum);
                }
            }
        }

        data
    }

    pub fn read_cell(&self, time_range: &TimeRange) -> Vec<DataBlock> {
        vec![self.data_block(time_range)]
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    U64(i64, u64),
    I64(i64, i64),
    Str(i64, MiniVec<u8>),
    F64(i64, f64),
    Bool(i64, bool),
}

impl DataType {
    pub fn new(vtype: ValueType, ts: i64) -> Self {
        match vtype {
            ValueType::Unsigned => DataType::U64(ts, 0),
            ValueType::Integer => DataType::I64(ts, 0),
            ValueType::Float => DataType::F64(ts, 0.0),
            ValueType::Boolean => DataType::Bool(ts, false),
            ValueType::String => DataType::Str(ts, mini_vec![]),
            _ => todo!(),
        }
    }
    pub fn timestamp(&self) -> i64 {
        match *self {
            DataType::U64(ts, ..) => ts,
            DataType::I64(ts, ..) => ts,
            DataType::Str(ts, ..) => ts,
            DataType::F64(ts, ..) => ts,
            DataType::Bool(ts, ..) => ts,
        }
    }

    pub fn with_field_val(ts: Timestamp, field_val: FieldVal) -> Self {
        match field_val {
            FieldVal::Float(val) => Self::F64(ts, val),
            FieldVal::Integer(val) => Self::I64(ts, val),
            FieldVal::Unsigned(val) => Self::U64(ts, val),
            FieldVal::Boolean(val) => Self::Bool(ts, val),
            FieldVal::Bytes(val) => Self::Str(ts, val),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::U64(ts, val) => write!(f, "({}, {})", ts, val),
            DataType::I64(ts, val) => write!(f, "({}, {})", ts, val),
            DataType::Str(ts, val) => write!(f, "({}, {:?})", ts, val),
            DataType::F64(ts, val) => write!(f, "({}, {})", ts, val),
            DataType::Bool(ts, val) => write!(f, "({}, {})", ts, val),
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use bytes::buf;
    use models::{SchemaId, SeriesId, Timestamp};
    use std::mem::{size_of, size_of_val};

    use crate::{tsm::DataBlock, TimeRange};

    use super::{DataType, FieldVal, MemCache, RowData, RowGroup};

    pub(crate) fn put_rows_to_cache(
        cache: &mut MemCache,
        series_id: SeriesId,
        schema_id: SchemaId,
        schema_column_ids: Vec<u32>,
        time_range: (Timestamp, Timestamp),
        put_none: bool,
    ) {
        let mut rows = Vec::new();
        let mut size: usize = schema_column_ids.capacity() * size_of::<Vec<u32>>();
        for ts in time_range.0..time_range.1 + 1 {
            let mut fields = Vec::new();
            for _ in 0..schema_column_ids.len() {
                size += size_of::<Option<FieldVal>>();
                if put_none {
                    fields.push(None);
                } else {
                    fields.push(Some(FieldVal::Float(ts as f64)));
                    size += 8;
                }
            }
            size += 8;
            rows.push(RowData { ts, fields });
        }

        let row_group = RowGroup {
            schema_id,
            schema: schema_column_ids,
            range: TimeRange::from(time_range),
            rows,
            size: size_of::<RowGroup>() + size,
        };
        cache.write_group(series_id, 1, row_group);
    }
}
