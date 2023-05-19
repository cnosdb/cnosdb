use std::collections::HashMap;
use std::fmt::Display;
use std::iter::FromIterator;
use std::mem::size_of_val;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use memory_pool::{MemoryConsumer, MemoryPoolRef, MemoryReservation};
use minivec::{mini_vec, MiniVec};
use models::predicate::domain::TimeRange;
use models::schema::{timestamp_convert, Precision, TableColumn, TskvTableSchema};
use models::utils::split_id;
use models::{ColumnId, FieldId, RwLockRef, SchemaId, SeriesId, Timestamp, ValueType};
use parking_lot::RwLock;
use protos::models as fb_models;
use protos::models::FieldType;
use utils::bitset::BitSet;

use crate::error::Result;
use crate::Error::CommonError;
use crate::{byte_utils, Error, TseriesFamilyId};

#[derive(Debug, Clone)]
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

impl PartialEq for FieldVal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldVal::Unsigned(a), FieldVal::Unsigned(b)) => a == b,
            (FieldVal::Integer(a), FieldVal::Integer(b)) => a == b,
            (FieldVal::Float(a), FieldVal::Float(b)) => a.eq(b),
            (FieldVal::Boolean(a), FieldVal::Boolean(b)) => a == b,
            (FieldVal::Bytes(a), FieldVal::Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for FieldVal {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RowData {
    pub ts: i64,
    pub fields: Vec<Option<FieldVal>>,
}

impl RowData {
    pub fn point_to_row_data(
        p: fb_models::Point,
        schema: &TskvTableSchema,
        from_precision: Precision,
        field_names: &[&str],
        field_type: &[FieldType],
    ) -> Result<RowData> {
        let fields = match p.fields() {
            None => {
                let mut fields = Vec::with_capacity(schema.field_num());
                for _i in 0..fields.capacity() {
                    fields.push(None);
                }
                fields
            }
            Some(fields_inner) => {
                let field_nullbit_buffer = p.fields_nullbit().ok_or(Error::CommonError {
                    reason: "field nullbit missing in point".to_string(),
                })?;
                let len = fields_inner.len();
                let field_nullbit = BitSet::new_without_check(len, field_nullbit_buffer.bytes());
                let fields_id = schema.fields_id();
                let mut fields: Vec<Option<FieldVal>> = Vec::with_capacity(fields_id.len());
                for _i in 0..fields.capacity() {
                    fields.push(None);
                }
                for (idx, ((field, field_name), field_type)) in fields_inner
                    .into_iter()
                    .zip(field_names)
                    .zip(field_type)
                    .enumerate()
                {
                    let val = MiniVec::from(
                        field
                            .value()
                            .ok_or(CommonError {
                                reason: "field missing value".to_string(),
                            })?
                            .bytes(),
                    );
                    match schema.column(field_name) {
                        None => {}
                        Some(field) => match fields_id.get(&field.id) {
                            None => {}
                            Some(index) => {
                                if !field_nullbit.get(idx) {
                                    continue;
                                }
                                fields[*index] = Some(FieldVal::new(val, (*field_type).into()));
                            }
                        },
                    }
                }
                fields
            }
        };
        let to_precision = schema.time_column_precision();
        let ts = timestamp_convert(from_precision, to_precision, p.timestamp()).ok_or(
            Error::CommonError {
                reason: "timestamp overflow".to_string(),
            },
        )?;

        Ok(RowData { ts, fields })
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RowGroup {
    pub schema: Arc<TskvTableSchema>,
    pub range: TimeRange,
    pub rows: Vec<RowData>,
    /// total size in stack and heap
    pub size: usize,
}

#[derive(Debug)]
pub struct SeriesData {
    pub series_id: SeriesId,
    pub range: TimeRange,
    pub groups: Vec<RowGroup>,
}

impl SeriesData {
    fn new(series_id: SeriesId) -> Self {
        Self {
            series_id,
            range: TimeRange {
                min_ts: i64::MAX,
                max_ts: i64::MIN,
            },
            groups: Vec::with_capacity(4),
        }
    }

    pub fn write(&mut self, mut group: RowGroup) {
        self.range.merge(&group.range);

        for item in self.groups.iter_mut() {
            if item.schema.schema_id == group.schema.schema_id {
                item.range.merge(&group.range);
                item.rows.append(&mut group.rows);
                item.schema = group.schema;
                return;
            }
        }

        self.groups.push(group);
    }

    pub fn delete_column(&mut self, column_id: ColumnId) {
        for item in self.groups.iter_mut() {
            let name = match item.schema.column_name(column_id) {
                None => continue,
                Some(name) => name.to_string(),
            };
            let index = match item.schema.fields_id().get(&column_id) {
                None => continue,
                Some(index) => *index,
            };
            for row in item.rows.iter_mut() {
                row.fields.remove(index);
            }
            let mut schema_t = item.schema.as_ref().clone();
            schema_t.drop_column(&name);
            schema_t.schema_id += 1;
            item.schema = Arc::new(schema_t)
        }
    }

    pub fn change_column(&mut self, column_name: &str, new_column: &TableColumn) {
        for item in self.groups.iter_mut() {
            let mut schema_t = item.schema.as_ref().clone();
            schema_t.change_column(column_name, new_column.clone());
            schema_t.schema_id += 1;
            item.schema = Arc::new(schema_t)
        }
    }

    pub fn add_column(&mut self, new_column: &TableColumn) {
        for item in self.groups.iter_mut() {
            let mut schema_t = item.schema.as_ref().clone();
            schema_t.add_column(new_column.clone());
            schema_t.schema_id += 1;
            item.schema = Arc::new(schema_t)
        }
    }

    pub fn delete_series(&mut self, range: &TimeRange) {
        if range.max_ts < self.range.min_ts || range.min_ts > self.range.max_ts {
            return;
        }

        for item in self.groups.iter_mut() {
            item.rows
                .retain(|row| row.ts < range.min_ts || row.ts > range.max_ts);
        }
    }

    pub fn read_data(
        &self,
        column_id: ColumnId,
        mut time_predicate: impl FnMut(Timestamp) -> bool,
        mut value_predicate: impl FnMut(&FieldVal) -> bool,
        mut handle_data: impl FnMut(DataType),
    ) {
        for group in self.groups.iter() {
            let field_index = group.schema.fields_id();
            let index = match field_index.get(&column_id) {
                None => continue,
                Some(v) => v,
            };
            group
                .rows
                .iter()
                .filter(|row| time_predicate(row.ts))
                .for_each(|row| {
                    if let Some(Some(field)) = row.fields.get(*index) {
                        if value_predicate(field) {
                            handle_data(field.data_value(row.ts))
                        }
                    }
                });
        }
    }

    pub fn read_timestamps(
        &self,
        mut time_predicate: impl FnMut(Timestamp) -> bool,
        mut handle_data: impl FnMut(Timestamp),
    ) {
        for group in self.groups.iter() {
            group
                .rows
                .iter()
                .filter(|row| time_predicate(row.ts))
                .for_each(|row| handle_data(row.ts));
        }
    }

    pub fn flat_groups(&self) -> Vec<(SchemaId, Arc<TskvTableSchema>, &Vec<RowData>)> {
        self.groups
            .iter()
            .map(|g| (g.schema.schema_id, g.schema.clone(), &g.rows))
            .collect()
    }
}

#[derive(Debug)]
pub struct MemCache {
    tf_id: TseriesFamilyId,

    flushing: AtomicBool,

    max_size: u64,
    min_seq_no: u64,

    // wal seq number
    seq_no: AtomicU64,
    memory: RwLock<MemoryReservation>,

    part_count: usize,
    // This u64 comes from split_id(SeriesId) % part_count
    partions: Vec<RwLock<HashMap<u32, RwLockRef<SeriesData>>>>,
}

impl MemCache {
    pub fn new(tf_id: TseriesFamilyId, max_size: u64, seq: u64, pool: &MemoryPoolRef) -> Self {
        let part_count = 16;
        let mut partions = Vec::with_capacity(part_count);
        for _i in 0..part_count {
            partions.push(RwLock::new(HashMap::new()));
        }
        let res =
            RwLock::new(MemoryConsumer::new(format!("memcache-{}-{}", tf_id, seq)).register(pool));
        Self {
            tf_id,
            flushing: AtomicBool::new(false),

            max_size,
            min_seq_no: seq,

            part_count,
            partions,

            seq_no: AtomicU64::new(seq),
            memory: res,
        }
    }

    pub fn write_group(&self, sid: SeriesId, seq: u64, group: RowGroup) -> Result<()> {
        self.seq_no.store(seq, Ordering::Relaxed);
        self.memory
            .write()
            .try_grow(group.size)
            .map_err(|_| Error::MemoryExhausted)?;
        let index = (sid as usize) % self.part_count;
        let mut series_map = self.partions[index].write();
        if let Some(series_data) = series_map.get(&sid) {
            let series_data_ptr = series_data.clone();
            let mut series_data_ptr_w = series_data_ptr.write();
            drop(series_map);
            series_data_ptr_w.write(group);
        } else {
            let mut series_data = SeriesData::new(sid);
            series_data.write(group);
            series_map.insert(sid, Arc::new(RwLock::new(series_data)));
        }
        Ok(())
    }

    pub fn read_field_data(
        &self,
        field_id: FieldId,
        time_predicate: impl FnMut(Timestamp) -> bool,
        value_predicate: impl FnMut(&FieldVal) -> bool,
        handle_data: impl FnMut(DataType),
    ) {
        let (column_id, sid) = split_id(field_id);
        let index = (sid as usize) % self.part_count;
        let series_data = self.partions[index].read().get(&sid).cloned();
        if let Some(series_data) = series_data {
            series_data
                .read()
                .read_data(column_id, time_predicate, value_predicate, handle_data)
        }
    }

    pub fn read_series_timestamps(
        &self,
        series_ids: &[SeriesId],
        mut time_predicate: impl FnMut(Timestamp) -> bool,
        mut handle_data: impl FnMut(Timestamp),
    ) {
        for sid in series_ids.iter() {
            let index = (*sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(sid).cloned();
            if let Some(series_data) = series_data {
                series_data
                    .read()
                    .read_timestamps(&mut time_predicate, &mut handle_data);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        for part in self.partions.iter() {
            if !part.read().is_empty() {
                return false;
            }
        }

        true
    }

    pub fn delete_columns(&self, field_ids: &[FieldId]) {
        for fid in field_ids {
            let (column_id, sid) = split_id(*fid);
            let index = (sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(&sid).cloned();
            if let Some(series_data) = series_data {
                series_data.write().delete_column(column_id);
            }
        }
    }

    pub fn change_column(&self, sids: &[SeriesId], column_name: &str, new_column: &TableColumn) {
        for sid in sids {
            let index = (*sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(sid).cloned();
            if let Some(series_data) = series_data {
                series_data.write().change_column(column_name, new_column);
            }
        }
    }

    pub fn add_column(&self, sids: &[SeriesId], new_column: &TableColumn) {
        for sid in sids {
            let index = (*sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(sid).cloned();
            if let Some(series_data) = series_data {
                series_data.write().add_column(new_column);
            }
        }
    }

    pub fn delete_series(&self, sids: &[SeriesId], range: &TimeRange) {
        for sid in sids {
            let index = (*sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(sid).cloned();
            if let Some(series_data) = series_data {
                series_data.write().delete_series(range);
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

    pub fn mark_flushing(&self) -> bool {
        self.flushing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.memory.read().size() >= self.max_size as usize
    }

    pub fn tf_id(&self) -> TseriesFamilyId {
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
        self.memory.read().size() as u64
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

    pub fn to_bytes(&self) -> MiniVec<u8> {
        match self {
            DataType::U64(_, val) => MiniVec::from_iter(val.to_be_bytes()),
            DataType::I64(_, val) => MiniVec::from_iter(val.to_be_bytes()),
            DataType::F64(_, val) => MiniVec::from_iter(val.to_be_bytes()),
            DataType::Str(_, val) => val.clone(),
            DataType::Bool(_, val) => MiniVec::from_iter(if *val { [1_u8] } else { [0_u8] }),
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
    use std::collections::HashMap;
    use std::mem::size_of;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::TimeUnit;
    use memory_pool::{GreedyMemoryPool, MemoryPool};
    use models::predicate::domain::TimeRange;
    use models::schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SchemaId, SeriesId, Timestamp, ValueType};
    use parking_lot::RwLock;

    use super::{FieldVal, MemCache, RowData, RowGroup};

    pub(crate) fn put_rows_to_cache(
        cache: &mut MemCache,
        series_id: SeriesId,
        schema_id: SchemaId,
        mut schema: TskvTableSchema,
        time_range: (Timestamp, Timestamp),
        put_none: bool,
    ) {
        let mut rows = Vec::new();
        let mut size: usize = schema.size();
        for ts in time_range.0..=time_range.1 {
            let mut fields = Vec::new();
            for _ in 0..schema.columns().len() {
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

        schema.schema_id = schema_id;
        let row_group = RowGroup {
            schema: schema.into(),
            range: TimeRange::from(time_range),
            rows,
            size: size_of::<RowGroup>() + size,
        };
        cache.write_group(series_id, 1, row_group).unwrap();
    }

    pub(crate) fn get_one_series_cache_data(
        cache: Arc<RwLock<MemCache>>,
    ) -> HashMap<String, Vec<(Timestamp, FieldVal)>> {
        let mut fname_vals_map: HashMap<String, Vec<(Timestamp, FieldVal)>> = HashMap::new();
        let series_data = cache.read().read_series_data();
        for (_sid, sdata) in series_data {
            let sdata_rlock = sdata.read();
            let schema_groups = sdata_rlock.flat_groups();
            for (_sch_id, sch, row) in schema_groups {
                let fields = sch.fields();
                for r in row {
                    for (i, f) in r.fields.iter().enumerate() {
                        if let Some(fv) = f {
                            if let Some(c) = fields.get(i) {
                                if &c.name != "time" {
                                    fname_vals_map
                                        .entry(c.name.clone())
                                        .or_default()
                                        .push((r.ts, fv.clone()))
                                }
                            };
                        }
                    }
                }
            }
        }

        fname_vals_map
    }

    #[test]
    fn test_write_group() {
        let sid: SeriesId = 1;

        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 1000, 1, &memory_pool);
        {
            let series_part = &mem_cache.partions[sid as usize].read();
            let series_data = series_part.get(&sid);
            assert!(series_data.is_none());
        }

        #[rustfmt::skip]
        let mut schema_1 = TskvTableSchema::new(
            "test_tenant".to_string(), "test_db".to_string(), "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new_tag_column(2, "tag_col_1".to_string()),
                TableColumn::new_tag_column(3, "tag_col_2".to_string()),
                TableColumn::new(4, "f_col_1".to_string(), ColumnType::Field(ValueType::Float), Default::default()),
            ],
        );
        schema_1.schema_id = 1;
        #[rustfmt::skip]
        let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 3),
            rows: vec![
                RowData { ts: 1, fields: vec![Some(FieldVal::Float(1.0))] },
                RowData { ts: 3, fields: vec![Some(FieldVal::Float(3.0))] }
            ],
            size: 10,
        };
        mem_cache.write_group(sid, 1, row_group_1.clone()).unwrap();
        {
            let series_part = &mem_cache.partions[sid as usize].read();
            let series_data = series_part.get(&sid);
            assert!(series_data.is_some());
            let series_data = series_data.unwrap().read();
            assert_eq!(sid, series_data.series_id);
            assert_eq!(TimeRange::new(1, 3), series_data.range);
            assert_eq!(1, series_data.groups.len());
            assert_eq!(row_group_1, series_data.groups[0]);
        }

        #[rustfmt::skip]
        let mut schema_2 = TskvTableSchema::new(
            "test_tenant".to_string(), "test_db".to_string(), "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new_tag_column(2, "tag_col_1".to_string()),
                TableColumn::new_tag_column(3, "tag_col_2".to_string()),
                TableColumn::new(4, "f_col_1".to_string(), ColumnType::Field(ValueType::Float), Default::default()),
                TableColumn::new(5, "f_col_2".to_string(), ColumnType::Field(ValueType::Integer), Default::default()),
            ],
        );
        schema_2.schema_id = 2;
        #[rustfmt::skip]
        let row_group_2 = RowGroup {
            schema: Arc::new(schema_2),
            range: TimeRange::new(3, 5),
            rows: vec![
                RowData { ts: 3, fields: vec![None, Some(FieldVal::Integer(3))] },
                RowData { ts: 5, fields: vec![Some(FieldVal::Float(5.0)), Some(FieldVal::Integer(5))] }
            ],
            size: 10,
        };
        mem_cache.write_group(sid, 2, row_group_2.clone()).unwrap();
        {
            let series_part = &mem_cache.partions[sid as usize].read();
            let series_data = series_part.get(&sid);
            assert!(series_data.is_some());
            let series_data = series_data.unwrap().read();
            assert_eq!(sid, series_data.series_id);
            assert_eq!(TimeRange::new(1, 5), series_data.range);
            assert_eq!(2, series_data.groups.len());
            assert_eq!(row_group_2, series_data.groups[1]);
        }
    }
}
