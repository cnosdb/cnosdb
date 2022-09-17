use flatbuffers::Push;
use futures::future::ok;

use models::{utils, FieldId, RwLockRef, SeriesId, Timestamp, ValueType};
use protos::models::{FieldType, Rows};

use std::cmp::Ordering as CmpOrdering;
use std::collections::HashSet;
use std::fmt::Display;
use std::iter::{FromIterator, Peekable};
use std::ops::Index;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::{borrow::BorrowMut, collections::HashMap, mem::size_of_val, rc::Rc};
use std::mem::size_of;

use minivec::{mini_vec, MiniVec};
use trace::{error, info, warn};

use crate::tsm::{timestamp, DataBlock};
use crate::{byte_utils, error::Result, tseries_family::TimeRange};
use parking_lot::{RwLock, RwLockReadGuard};

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
    pub schema_id: u32,
    pub schema: Vec<u32>,
    pub range: TimeRange,
    pub rows: Vec<RowData>,
    /// total size in stack and heap
    pub size: usize
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
                if let Some(field) = row.fields.get(index) {
                    if let Some(field) = field {
                        entry.field_type = field.value_type();
                        entry.cells.push(field.data_value(row.ts));
                    }
                }
            }
        }

        if entry.field_type == ValueType::Unknown || entry.cells.is_empty() {
            return None;
        }

        entry.sort();

        return Some(Arc::new(RwLock::new(entry)));
    }

    pub fn field_data_block(&self, ids: &[u32], time_range: &TimeRange) -> HashMap<u32, DataBlock> {
        let mut map = HashMap::new();

        for id in ids {
            if let Some(entry) = self.read_entry(*id) {
                map.insert(*id, entry.read().data_block(time_range));
            }
        }

        return map;
    }

    pub fn read_schema_and_column_blocks(&self) -> Option<(Vec<u32>, Vec<ColumnBlock>)> {
        if self.groups.is_empty() {
            return None;
        }

        // 1. Merge schemas:
        //    Since schemas in `RowGroup` are different, we need to init the columns first.
        let mut all_schema: HashSet<u32> = HashSet::new();
        let mut schema_map: HashMap<u32, HashSet<u32>> = HashMap::new();
        let mut group_data: Vec<(&u32, &Vec<u32>, &Timestamp, &Vec<Option<FieldVal>>)> = Vec::new();
        for group in self.groups.iter() {
            if group.schema.is_empty() {
                continue;
            }
            if !schema_map.contains_key(&group.schema_id) {
                let mut group_schema: HashSet<u32> = HashSet::with_capacity(group.schema.len());
                for col in group.schema.iter() {
                    all_schema.insert(*col);
                    group_schema.insert(*col);
                }
                schema_map.insert(group.schema_id, group_schema);
            }

            schema_map
                .entry(group.schema_id)
                .or_insert_with(|| HashSet::from_iter(group.schema.iter().cloned()));
            for row in group.rows.iter() {
                group_data.push((&group.schema_id, &group.schema, &row.ts, &row.fields));
            }
        }

        let mut all_schema: Vec<u32> = all_schema.into_iter().collect();
        let mut all_columns: Vec<ColumnBlock> = Vec::with_capacity(all_schema.len());
        let mut schema_column_map: HashMap<u32, usize> = HashMap::with_capacity(all_schema.len());
        all_schema.sort();
        for (i, col) in all_schema.iter().enumerate() {
            schema_column_map.insert(*col, i);
            all_columns.push(ColumnBlock::new(*col));
        }

        // 2. Merge columns:
        //    Some `RowGroup`s don't have some columns in schema, put nulls in `ColumnBlocks`
        //    of these `RowGroup`s.
        group_data.sort_by_key(|d| d.2);
        let mut column_flags = vec![0; all_columns.len()];
        for (schema_id, schema, ts, fields) in group_data {
            column_flags.fill(0);
            for (v, col) in fields.iter().zip(schema.iter()) {
                if let Some(idx) = schema_column_map.get(col) {
                    column_flags[*idx] = 1;
                    let col_blk = unsafe { all_columns.get_unchecked_mut(*idx) };
                    if let Some(val) = v {
                        col_blk.append_value(*ts, val.clone());
                    } else {
                        col_blk.append_null(*ts);
                    }
                }
            }
            for (i, flag) in column_flags.iter().enumerate() {
                if *flag == 0 {
                    let col_blk = unsafe { all_columns.get_unchecked_mut(i) };
                    col_blk.append_null(*ts)
                }
            }
        }

        Some((all_schema, all_columns))
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

        return true;
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

    pub fn get_series_column_blocks(&self) -> HashMap<SeriesId, (Vec<u32>, Vec<ColumnBlock>)> {
        let mut ret: HashMap<SeriesId, (Vec<u32>, Vec<ColumnBlock>)> = HashMap::new();
        for part in self.partions.iter() {
            let part_rlock = part.read();
            for (sid, series_data) in part_rlock.iter() {
                if let Some(blk) = series_data.read().read_schema_and_column_blocks() {
                    ret.insert(*sid, blk);
                }
            }
        }
        ret
    }

    pub fn iter_series_column_blocks<'a>(&'a self) -> SeriesColumnBlocksIterator<'a> {
        SeriesColumnBlocksIterator::new(self)
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

/// Iterates over partions in a `MemCache`, and partion's inner `SeriesId`->`SeriesIdData` map,
/// by `SeriesId`.
/// For every `SeriesData`, it's `RowGroup`s and inner `RowData`s will be transformed into
/// `ColumnBlock`s that contains nullable field value, sorted by timestamp.
pub struct SeriesColumnBlocksIterator<'a> {
    /// Sorted `SeriesId`s, and a map for `SereisId` to `SeriesIdData`.
    partions: Vec<(
        Vec<SeriesId>,
        RwLockReadGuard<'a, HashMap<u64, RwLockRef<SeriesData>>>,
    )>,

    /// Partion index in `MemCache`.
    partion_idx: usize,
    /// `SeriesIdData` index in a partion.
    series_data_idx: usize,
}

impl<'a> SeriesColumnBlocksIterator<'a> {
    pub fn new(cache: &'a MemCache) -> Self {
        let mut partions = vec![];
        for part in cache.partions.iter() {
            let part_rlock = part.read();
            let mut series_ids: Vec<u64> = part_rlock.keys().cloned().collect();
            series_ids.sort();
            partions.push((series_ids, part_rlock));
        }
        Self {
            partions,
            partion_idx: 0,
            series_data_idx: 0,
        }
    }
}

impl<'a> Iterator for SeriesColumnBlocksIterator<'a> {
    type Item = (SeriesId, Rc<Vec<u32>>, Rc<Vec<ColumnBlock>>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.partion_idx >= self.partions.len() {
                return None;
            }
            let (sids, sid_series_data) = self
                .partions
                .get(self.partion_idx)
                .expect("self.partion_idx < self.partions.len()");
            if sids.is_empty() {
                self.series_data_idx = 0;
                self.partion_idx += 1;
                continue;
            }
            let ret = sids.get(self.series_data_idx).and_then(|sid| {
                match sid_series_data.get(sid).and_then(|series_data| {
                    Some((sid, series_data.read().read_schema_and_column_blocks()))
                }) {
                    Some((sid, Some((schema, column_blocks)))) => {
                        Some((*sid, Rc::new(schema), Rc::new(column_blocks)))
                    }
                    _ => None,
                }
            });

            self.series_data_idx += 1;
            if self.series_data_idx >= sids.len() {
                self.series_data_idx = 0;
                self.partion_idx += 1;
            }

            return ret;
        }
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

        return data;
    }

    pub fn read_cell(&self, time_range: &TimeRange) -> Vec<DataBlock> {
        return vec![self.data_block(time_range)];
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

#[derive(Debug)]
pub struct ColumnBlock {
    column_id: u32,
    timestamps: Vec<Timestamp>,
    field_type: ValueType,
    field_values: Vec<FieldVal>,
    size: usize,
    nulls_bitmap: Vec<u64>,
    /// Element counts in every bitmap u64.
    /// The last bitmap u64 is not in sizes.
    /// len(sizes) = len(nulls_bitmap) - 1
    sizes: Vec<u8>,
}

impl ColumnBlock {
    pub fn new(column_id: u32) -> Self {
        Self {
            column_id,
            timestamps: vec![],
            field_type: ValueType::Unknown,
            field_values: vec![],
            nulls_bitmap: vec![0],
            size: 0,
            sizes: vec![],
        }
    }

    pub fn with_capacity(column_id: u32, capacity: usize) -> Self {
        Self {
            column_id,
            timestamps: Vec::with_capacity(capacity),
            field_type: ValueType::Unknown,
            field_values: Vec::with_capacity(capacity),
            nulls_bitmap: vec![0],
            size: 0,
            sizes: vec![],
        }
    }

    pub fn append_value(&mut self, timestamp: Timestamp, field_value: FieldVal) {
        if self.field_type == ValueType::Unknown {
            self.field_type = match field_value {
                FieldVal::Float(_) => ValueType::Float,
                FieldVal::Integer(_) => ValueType::Integer,
                FieldVal::Unsigned(_) => ValueType::Unsigned,
                FieldVal::Boolean(_) => ValueType::Boolean,
                FieldVal::Bytes(_) => ValueType::String,
            };
        } else if self.field_type != field_value.value_type() {
            return;
        }

        self.timestamps.push(timestamp);
        self.field_values.push(field_value);
        self.append_non_null();
    }

    fn append_non_null(&mut self) {
        if self.nulls_bitmap.is_empty() {
            error!("nulls_bitmap is empty");
        }
        if self.size == 0 {
            // First value, just put 0x0000_0001
            if let Some(n) = self.nulls_bitmap.last_mut() {
                *n = 1_u64;
            }
            self.size += 1;
            return;
        }

        let base = self
            .nulls_bitmap
            .last()
            .cloned()
            .expect("initialized nulls_bitmap: vec![0_u64]");

        let m = self.size % 64;
        if m == 0 {
            // Next u64, just put 0x0000_0001
            self.nulls_bitmap.push(0x01);
            let last_size = self.sizes.last().unwrap_or(&0);
            self.sizes
                .push(*last_size + byte_utils::one_count_u64(base));
        } else {
            let mut b = base;
            // From low to high
            b |= 1 << m;
            if let Some(n) = self.nulls_bitmap.last_mut() {
                *n = b;
            }
        }
        self.size += 1;
    }

    pub fn append_null(&mut self, timestamp: Timestamp) {
        self.timestamps.push(timestamp);

        if self.nulls_bitmap.is_empty() {
            error!("nulls_bitmap is empty");
        }

        if self.size == 0 {
            // First value, do nothing
            self.size += 1;
            return;
        }

        let base = self
            .nulls_bitmap
            .last()
            .cloned()
            .expect("initialized nulls_bitmap: vec![0_u64]");

        let m = self.size % 64;
        if m == 0 {
            // Next u64, just put 0x0000_0001
            self.nulls_bitmap.push(0x00);
            let last_size = self.sizes.last().unwrap_or(&0);
            self.sizes.push(byte_utils::one_count_u64(base));
        }
        self.size += 1;
    }

    pub fn append_nulls(&mut self, timestamps: &[Timestamp]) {
        self.timestamps.resize(self.size + timestamps.len(), 0);
        self.timestamps[self.size..].copy_from_slice(timestamps);

        let n = timestamps.len();
        let m = 64 - (self.size % 64);
        if n <= m {
            self.size += n;
            return;
        }

        let offset = n - m;
        let last_size = *self.sizes.last().unwrap_or(&0);
        for _ in 0..(offset / 64) {
            self.nulls_bitmap.push(0x00);
            self.sizes.push(last_size);
        }
        if offset % 64 > 0 {
            self.nulls_bitmap.push(0x00);
        }
        self.size += n;
    }

    fn is_null(&self, index: usize) -> bool {
        if let Some(base) = self.nulls_bitmap.get(index / 64) {
            *base & 1 << (index % 64) == 0
        } else {
            true
        }
    }

    pub fn iter(&self) -> ColumnBlockValueIterator<'_> {
        ColumnBlockValueIterator::new(self)
    }

    pub fn column_id(&self) -> u32 {
        self.column_id
    }

    pub fn value_type(&self) -> ValueType {
        self.field_type
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

pub struct ColumnBlockValueIterator<'a> {
    inner: &'a ColumnBlock,

    buf: [Option<(&'a Timestamp, Option<&'a FieldVal>)>; 3],
    buf_index: usize,
    iter_index: usize,
    iter_index_d: usize,
}

impl<'a> ColumnBlockValueIterator<'a> {
    pub fn new(column_block: &'a ColumnBlock) -> Self {
        Self {
            inner: column_block,
            buf: [None, None, None],
            buf_index: 0,
            iter_index: 0,
            iter_index_d: 0,
        }
    }

    pub fn next(&mut self) -> bool {
        self.buf_index = (self.buf_index + 1) % 3;
        if self.iter_index >= self.inner.size {
            self.buf[self.buf_index] = None;
            return false;
        }
        if let Some(ts) = self.inner.timestamps.get(self.iter_index) {
            if self.inner.is_null(self.iter_index) {
                self.iter_index += 1;
                self.buf[self.buf_index] = Some((ts, None));
                return true;
            } else {
                let val = self.inner.field_values.get(self.iter_index_d);
                self.iter_index += 1;
                self.iter_index_d += 1;
                self.buf[self.buf_index] = Some((ts, val));
                return true;
            }
        }

        return false;
    }

    pub fn prev(&mut self) {
        self.buf_index = (self.buf_index + 2) % 3;
        self.iter_index -= 1;
        if !self.inner.is_null(self.iter_index) {
            self.iter_index_d -= 1;
        }
    }

    pub fn peek(&mut self) -> &Option<(&'a Timestamp, Option<&'a FieldVal>)> {
        if let Some(item) = self.buf[self.buf_index] {
            return &self.buf[self.buf_index];
        }
        if self.next() {
            &self.buf[self.buf_index]
        } else {
            &self.buf[self.buf_index]
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::mem::{size_of, size_of_val};
    use bytes::buf;
    use models::{SeriesId, Timestamp};

    use crate::{memcache::ColumnBlockValueIterator, tsm::DataBlock, TimeRange};

    use super::{
        ColumnBlock, DataType, FieldVal, MemCache, RowData, RowGroup, SeriesColumnBlocksIterator,
    };

    #[test]
    fn test_column_block() {
        let mut blk = ColumnBlock::new(1);
        blk.append_value(1, FieldVal::Float(1.0));
        blk.append_value(2, FieldVal::Float(2.0));
        // This boolean value and timestamp will be skipped.
        blk.append_value(3, FieldVal::Boolean(false));
        blk.append_nulls(&vec![4, 5]);
        blk.append_value(6, FieldVal::Float(6.0));

        let mut iter = blk.iter();
        assert_eq!(&Some((&1, Some(&FieldVal::Float(1.0)))), iter.peek());
        assert_eq!(&Some((&1, Some(&FieldVal::Float(1.0)))), iter.peek());
        iter.next();
        assert_eq!(&Some((&2, Some(&FieldVal::Float(2.0)))), iter.peek());
        iter.prev();
        assert_eq!(&Some((&1, Some(&FieldVal::Float(1.0)))), iter.peek());
        iter.next();
        assert_eq!(&Some((&2, Some(&FieldVal::Float(2.0)))), iter.peek());
        iter.next();
        assert_eq!(&Some((&4, None)), iter.peek());
        iter.next();
        assert_eq!(&Some((&5, None)), iter.peek());
        iter.next();
        assert_eq!(&Some((&6, Some(&FieldVal::Float(6.0)))), iter.peek());
        iter.prev();
        assert_eq!(&Some((&5, None)), iter.peek());
        iter.next();
        assert_eq!(&Some((&6, Some(&FieldVal::Float(6.0)))), iter.peek());
        iter.next();
        assert_eq!(&None, iter.peek());
    }

    pub(crate) fn put_rows_to_cache(
        cache: &mut MemCache,
        series_id: SeriesId,
        schema_id: u32,
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
            size: size_of::<RowGroup>() + size
        };
        cache.write_group(series_id, 1, row_group);
    }

    pub(crate) fn stringify_series_cache_blocks(
        mut iterator: impl Iterator<
            Item = (SeriesId, impl AsRef<Vec<u32>>, impl AsRef<Vec<ColumnBlock>>),
        >,
    ) -> String {
        let mut buffer = String::new();
        while let Some((sid, schema, blks)) = iterator.next() {
            let schema = schema.as_ref();
            let blks = blks.as_ref();
            buffer.push_str(&format!("| === SeriesId: {} === |\n", sid));
            buffer.push_str(&format!(
                "| Ts\\Col | {} |\n",
                schema
                    .iter()
                    .map(|col| col.to_string())
                    .collect::<Vec<String>>()
                    .join(" | ")
            ));
            let mut iters: Vec<ColumnBlockValueIterator> = blks.iter().map(|b| b.iter()).collect();
            let mut loop_break = false;
            loop {
                let mut line_buf = String::from("|");
                let mut timestamp_added = false;
                for i in 0..iters.len() {
                    let iter = &mut iters[i];
                    if iter.next() {
                        if let Some((ts, d)) = iter.peek() {
                            if !timestamp_added {
                                line_buf.push_str(&format!(" {} |", ts));
                                timestamp_added = true;
                            }
                            match d {
                                Some(rol_col) => line_buf.push_str(&format!(" {}", rol_col)),
                                None => line_buf.push_str(" None"),
                            }
                        } else {
                            loop_break = true;
                            break;
                        }
                    } else {
                        loop_break = true;
                        break;
                    }
                    line_buf.push_str(" |");
                }
                if loop_break {
                    break;
                }
                buffer.push_str(&format!("{}\n", line_buf));
            }
        }
        buffer
    }

    #[test]
    fn test_read_column_block() {
        let mut cache = &mut MemCache::new(1, 1024, 0);
        put_rows_to_cache(&mut cache, 1, 1, vec![0, 1, 2], (3, 4), false);
        put_rows_to_cache(&mut cache, 1, 2, vec![0, 1, 3], (1, 2), false);
        put_rows_to_cache(&mut cache, 1, 3, vec![0, 1, 2, 3], (5, 5), true);
        put_rows_to_cache(&mut cache, 1, 3, vec![0, 1, 2, 3], (5, 6), false);

        let expected: Vec<&str> = vec![
            "| === SeriesId: 1 === |",
            "| Ts\\Col | 0 | 1 | 2 | 3 |",
            "| 1 | 1 | 1 | None | 1 |",
            "| 2 | 2 | 2 | None | 2 |",
            "| 3 | 3 | 3 | 3 | None |",
            "| 4 | 4 | 4 | 4 | None |",
            "| 5 | None | None | None | None |",
            "| 5 | 5 | 5 | 5 | 5 |",
            "| 6 | 6 | 6 | 6 | 6 |",
        ];

        let table = stringify_series_cache_blocks(
            cache
                .get_series_column_blocks()
                .into_iter()
                .map(|(a, (b, c))| (a, b, c)),
        );
        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_series_column_blocks_iterator() {
        let mut cache = &mut MemCache::new(1, 1024, 0);
        put_rows_to_cache(&mut cache, 1, 1, vec![0, 1], (1, 2), false);
        put_rows_to_cache(&mut cache, 1, 2, vec![0, 1, 2], (3, 4), false);
        put_rows_to_cache(&mut cache, 2, 1, vec![0], (1, 2), false);
        put_rows_to_cache(&mut cache, 2, 2, vec![0, 1], (3, 4), false);

        let expected: Vec<&str> = vec![
            "| === SeriesId: 1 === |",
            "| Ts\\Col | 0 | 1 | 2 |",
            "| 1 | 1 | 1 | None |",
            "| 2 | 2 | 2 | None |",
            "| 3 | 3 | 3 | 3 |",
            "| 4 | 4 | 4 | 4 |",
            "| === SeriesId: 2 === |",
            "| Ts\\Col | 0 | 1 |",
            "| 1 | 1 | None |",
            "| 2 | 2 | None |",
            "| 3 | 3 | 3 |",
            "| 4 | 4 | 4 |",
        ];

        let iter = SeriesColumnBlocksIterator::new(&cache);
        let table = stringify_series_cache_blocks(iter);
        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(expected, actual);
    }
}
