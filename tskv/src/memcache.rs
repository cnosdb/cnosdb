use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet, LinkedList};
use std::mem::size_of_val;
use std::ops::Bound::Included;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use flatbuffers::{ForwardsUOffset, Vector};
use memory_pool::{MemoryConsumer, MemoryPoolRef, MemoryReservation};
use minivec::MiniVec;
use models::field_value::FieldVal;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::database_schema::{timestamp_convert, Precision};
use models::schema::tskv_table_schema::{TableColumn, TskvTableSchema, TskvTableSchemaRef};
use models::{ColumnId, RwLockRef, SeriesId, SeriesKey, Timestamp};
use parking_lot::RwLock;
use protos::models::{Column, FieldType};
use skiplist::OrderedSkipList;
use snafu::OptionExt;
use trace::error;
use utils::bitset::ImmutBitSet;

use crate::database::FbSchema;
use crate::error::{CommonSnafu, FieldsIsEmptySnafu, MemoryExhaustedSnafu, TskvResult};
use crate::tsm::data_block::{DataBlock, MutableColumn};
use crate::tsm::TsmWriteData;
use crate::TseriesFamilyId;

// use skiplist::ordered_skiplist::OrderedSkipList;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RowData {
    pub ts: i64,
    pub fields: Vec<Option<FieldVal>>,
}

impl RowData {
    pub fn point_to_row_data(
        schema: &TskvTableSchema,
        from_precision: Precision,
        columns: &Vector<ForwardsUOffset<Column>>,
        fb_schema: &FbSchema,
        row_idx: Vec<usize>,
    ) -> TskvResult<Vec<RowData>> {
        let fields_id = schema.fields_id();
        let mut res = Vec::with_capacity(row_idx.len());
        for row_count in row_idx.into_iter() {
            let mut fields = vec![None; fields_id.len()];
            let mut has_fields = false;
            for field_id in fb_schema.field_indexes.iter() {
                let column = columns.get(*field_id);
                let column_name = column.name_ext()?;
                let column_nullbit = column.nullbit_ext()?;
                match column.field_type() {
                    FieldType::Integer => {
                        let len = column.int_values_len()?;
                        let column_nullbits =
                            ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                        if !column_nullbits.get(row_count) {
                            continue;
                        }
                        let val = column.int_values()?.get(row_count);
                        match schema.column(column_name) {
                            None => {
                                error!("column {} not found in schema", column_name);
                            }
                            Some(column) => {
                                let field_id = column.id;
                                let field_idx = fields_id.get(&field_id).unwrap();
                                fields[*field_idx] = Some(FieldVal::Integer(val));
                                has_fields = true;
                            }
                        }
                    }
                    FieldType::Float => {
                        let len = column.float_values_len()?;
                        let column_nullbits =
                            ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                        if !column_nullbits.get(row_count) {
                            continue;
                        }
                        let val = column.float_values()?.get(row_count);
                        match schema.column(column_name) {
                            None => {
                                error!("column {} not found in schema", column_name);
                            }
                            Some(column) => {
                                let field_id = column.id;
                                let field_idx = fields_id.get(&field_id).unwrap();
                                fields[*field_idx] = Some(FieldVal::Float(val));
                                has_fields = true;
                            }
                        }
                    }
                    FieldType::Unsigned => {
                        let len = column.uint_values_len()?;
                        let column_nullbits =
                            ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                        if !column_nullbits.get(row_count) {
                            continue;
                        }
                        let val = column.uint_values()?.get(row_count);
                        match schema.column(column_name) {
                            None => {
                                error!("column {} not found in schema", column_name);
                            }
                            Some(column) => {
                                let field_id = column.id;
                                let field_idx = fields_id.get(&field_id).unwrap();
                                fields[*field_idx] = Some(FieldVal::Unsigned(val));
                                has_fields = true;
                            }
                        }
                    }
                    FieldType::Boolean => {
                        let len = column.bool_values_len()?;
                        let column_nullbits =
                            ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                        if !column_nullbits.get(row_count) {
                            continue;
                        }
                        let val = column.bool_values()?.get(row_count);
                        match schema.column(column_name) {
                            None => {
                                error!("column {} not found in schema", column_name);
                            }
                            Some(column) => {
                                let field_id = column.id;
                                let field_idx = fields_id.get(&field_id).unwrap();
                                fields[*field_idx] = Some(FieldVal::Boolean(val));
                                has_fields = true;
                            }
                        }
                    }
                    FieldType::String => {
                        let len = column.string_values_len()?;
                        let column_nullbits =
                            ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                        if !column_nullbits.get(row_count) {
                            continue;
                        }
                        let val = column.string_values()?.get(row_count);
                        match schema.column(column_name) {
                            None => {
                                error!("column {} not found in schema", column_name);
                            }
                            Some(column) => {
                                let field_id = column.id;
                                let field_idx = fields_id.get(&field_id).unwrap();
                                fields[*field_idx] =
                                    Some(FieldVal::Bytes(MiniVec::from(val.as_bytes())));
                                has_fields = true;
                            }
                        }
                    }
                    _ => {
                        error!("unsupported field type");
                    }
                }
            }

            if !has_fields {
                return Err(FieldsIsEmptySnafu.build());
            }

            let ts_column = columns.get(fb_schema.time_index);
            let ts = ts_column.int_values()?.get(row_count);
            let to_precision = schema.time_column_precision();
            let ts = timestamp_convert(from_precision, to_precision, ts).context(CommonSnafu {
                reason: "timestamp overflow".to_string(),
            })?;
            res.push(RowData { ts, fields });
        }
        Ok(res)
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

impl PartialOrd for RowData {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.ts.cmp(&other.ts))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct OrderedRowsData {
    rows: OrderedSkipList<RowData>,
}

impl OrderedRowsData {
    pub fn new() -> Self {
        let mut rows: OrderedSkipList<RowData> = OrderedSkipList::new();
        unsafe { rows.sort_by(|a: &RowData, b: &RowData| a.partial_cmp(b).unwrap()) }
        Self { rows }
    }

    pub fn get_rows(self) -> OrderedSkipList<RowData> {
        self.rows
    }

    pub fn get_ref_rows(&self) -> &OrderedSkipList<RowData> {
        &self.rows
    }

    pub fn clear(&mut self) {
        self.rows.clear()
    }

    pub fn insert(&mut self, row: RowData) {
        self.rows.insert(row);
    }

    pub fn retain(&mut self, mut f: impl FnMut(&RowData) -> bool) {
        self.rows.retain(|row| f(row));
    }
}

impl Default for OrderedRowsData {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for OrderedRowsData {
    fn clone(&self) -> Self {
        let mut clone_rows: OrderedSkipList<RowData> = OrderedSkipList::new();
        unsafe { clone_rows.sort_by(|a: &RowData, b: &RowData| a.partial_cmp(b).unwrap()) }
        self.rows.iter().for_each(|row| {
            clone_rows.insert(row.clone());
        });
        Self { rows: clone_rows }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RowGroup {
    pub schema: Arc<TskvTableSchema>,
    pub range: TimeRange,
    pub rows: OrderedRowsData,
    /// total size in stack and heap
    pub size: usize,
}

#[derive(Debug)]
pub struct SeriesData {
    pub series_id: SeriesId,
    pub series_key: SeriesKey,
    pub range: TimeRange,
    pub groups: LinkedList<RowGroup>,
}

impl SeriesData {
    fn new(series_id: SeriesId, series_key: SeriesKey) -> Self {
        Self {
            series_id,
            series_key,
            range: TimeRange {
                min_ts: i64::MAX,
                max_ts: i64::MIN,
            },
            groups: LinkedList::new(),
        }
    }

    pub fn write(&mut self, group: RowGroup) {
        self.range.merge(&group.range);

        for item in self.groups.iter_mut().rev() {
            if item.schema.schema_version == group.schema.schema_version {
                item.range.merge(&group.range);
                group.rows.get_rows().into_iter().for_each(|row| {
                    item.rows.insert(row);
                });
                item.schema = group.schema;
                return;
            }
        }

        self.groups.push_back(group);
    }

    pub fn drop_column(&mut self, column_id: ColumnId) {
        for item in self.groups.iter_mut() {
            let name = match item.schema.column_name(column_id) {
                None => continue,
                Some(name) => name.to_string(),
            };
            let index = match item.schema.fields_id().get(&column_id) {
                None => continue,
                Some(index) => *index,
            };
            let mut rowdata_vec: Vec<RowData> = item.rows.get_ref_rows().iter().cloned().collect();
            item.rows.clear();
            for row in rowdata_vec.iter_mut() {
                if index < row.fields.len() {
                    row.fields.remove(index);
                }
                item.rows.insert(row.clone());
            }
            let mut schema_t = item.schema.as_ref().clone();
            schema_t.drop_column(&name);
            //schema_t.schema_id += 1;
            item.schema = Arc::new(schema_t)
        }
    }

    pub fn change_column(&mut self, column_name: &str, new_column: &TableColumn) {
        for item in self.groups.iter_mut() {
            let mut schema_t = item.schema.as_ref().clone();
            schema_t.change_column(column_name, new_column.clone());
            schema_t.schema_version += 1;
            item.schema = Arc::new(schema_t)
        }
    }

    pub fn add_column(&mut self, new_column: &TableColumn) {
        for item in self.groups.iter_mut() {
            let mut schema_t = item.schema.as_ref().clone();
            schema_t.add_column(new_column.clone());
            schema_t.schema_version += 1;
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

    pub fn read_data_v2(
        &self,
        column_ids: &[ColumnId],
        time_ranges: &TimeRanges,
        mut handle_data: impl FnMut(RowData),
    ) {
        match (time_ranges.is_boundless(), time_ranges.is_empty()) {
            (_, false) => {
                for group in self.groups.iter() {
                    let field_index = group.schema.fields_id();
                    for range in time_ranges.time_ranges() {
                        for row in group.rows.get_ref_rows().range(
                            Included(&RowData {
                                ts: range.min_ts,
                                fields: vec![],
                            }),
                            Included(&RowData {
                                ts: range.max_ts,
                                fields: vec![],
                            }),
                        ) {
                            let mut fields = vec![None; column_ids.len()];
                            column_ids.iter().enumerate().for_each(|(i, column_id)| {
                                if let Some(index) = field_index.get(column_id) {
                                    if let Some(Some(field)) = row.fields.get(*index) {
                                        fields[i] = Some(field.clone());
                                    }
                                }
                            });
                            handle_data(RowData { ts: row.ts, fields });
                        }
                    }
                }
            }
            (false, true) => {
                for group in self.groups.iter() {
                    let field_index = group.schema.fields_id();
                    for row in group.rows.get_ref_rows().range(
                        Included(&RowData {
                            ts: time_ranges.min_ts(),
                            fields: vec![],
                        }),
                        Included(&RowData {
                            ts: time_ranges.max_ts(),
                            fields: vec![],
                        }),
                    ) {
                        let mut fields = vec![None; column_ids.len()];
                        column_ids.iter().enumerate().for_each(|(i, column_id)| {
                            if let Some(index) = field_index.get(column_id) {
                                if let Some(Some(field)) = row.fields.get(*index) {
                                    fields[i] = Some(field.clone());
                                }
                            }
                        });
                        handle_data(RowData { ts: row.ts, fields });
                    }
                }
            }
            (true, true) => {
                for group in self.groups.iter() {
                    let field_index = group.schema.fields_id();
                    for row in group.rows.get_ref_rows() {
                        let mut fields = vec![None; column_ids.len()];
                        column_ids.iter().enumerate().for_each(|(i, column_id)| {
                            if let Some(index) = field_index.get(column_id) {
                                if let Some(Some(field)) = row.fields.get(*index) {
                                    fields[i] = Some(field.clone());
                                }
                            }
                        });
                        handle_data(RowData { ts: row.ts, fields });
                    }
                }
            }
        }
    }

    pub fn delete_by_time_ranges(&mut self, time_ranges: &TimeRanges) {
        for time_range in time_ranges.time_ranges() {
            if time_range.max_ts < self.range.min_ts || time_range.min_ts > self.range.max_ts {
                continue;
            }

            for item in self.groups.iter_mut() {
                let mut rows = OrderedRowsData::new();
                item.rows
                    .get_ref_rows()
                    .iter()
                    .rev()
                    .filter(|row| row.ts < time_range.min_ts || row.ts > time_range.max_ts)
                    .for_each(|row| {
                        rows.insert(row.clone());
                    });
                item.rows = rows;
            }
        }
    }

    pub fn read_timestamps(
        &self,
        time_ranges: &TimeRanges,
        mut handle_data: impl FnMut(Timestamp),
    ) {
        match (time_ranges.is_boundless(), time_ranges.is_empty()) {
            (_, false) => {
                for group in self.groups.iter() {
                    for range in time_ranges.time_ranges() {
                        for row in group.rows.get_ref_rows().range(
                            Included(&RowData {
                                ts: range.min_ts,
                                fields: vec![],
                            }),
                            Included(&RowData {
                                ts: range.max_ts,
                                fields: vec![],
                            }),
                        ) {
                            handle_data(row.ts);
                        }
                    }
                }
            }
            (false, true) => {
                for group in self.groups.iter() {
                    for row in group.rows.get_ref_rows().range(
                        Included(&RowData {
                            ts: time_ranges.min_ts(),
                            fields: vec![],
                        }),
                        Included(&RowData {
                            ts: time_ranges.max_ts(),
                            fields: vec![],
                        }),
                    ) {
                        handle_data(row.ts);
                    }
                }
            }
            (true, true) => {
                for group in self.groups.iter() {
                    for row in group.rows.get_ref_rows() {
                        handle_data(row.ts);
                    }
                }
            }
        }
    }

    pub fn flat_groups(&self) -> Vec<(TskvTableSchemaRef, &OrderedRowsData)> {
        self.groups
            .iter()
            .map(|g| (g.schema.clone(), &g.rows))
            .collect()
    }
    pub fn get_schema(&self) -> Option<Arc<TskvTableSchema>> {
        if let Some(item) = self.groups.back() {
            return Some(item.schema.clone());
        }
        None
    }
    pub fn build_data_block(
        &self,
        max_level_ts: i64,
    ) -> TskvResult<Option<(String, DataBlock, DataBlock)>> {
        if let Some(schema) = self.get_schema() {
            let field_ids = schema.fields_id();

            let mut cols = schema
                .fields()
                .iter()
                .map(|col| MutableColumn::empty(col.clone()))
                .collect::<TskvResult<Vec<_>>>()?;
            let mut delta_cols = cols.clone();
            let mut time_array = MutableColumn::empty(schema.time_column())?;

            let mut delta_time_array = time_array.clone();
            let mut cols_desc = vec![None; schema.field_num()];
            for (schema, rows) in self.flat_groups() {
                let values = dedup_and_sort_row_data(rows);
                for row in values {
                    match row.ts.cmp(&max_level_ts) {
                        cmp::Ordering::Greater => {
                            time_array.push(Some(FieldVal::Integer(row.ts)))?;
                        }
                        _ => {
                            delta_time_array.push(Some(FieldVal::Integer(row.ts)))?;
                        }
                    }
                    for col in schema.fields().iter() {
                        if let Some(index) = field_ids.get(&col.id) {
                            let field = row.fields.get(*index).and_then(|v| v.clone());
                            match row.ts.cmp(&max_level_ts) {
                                cmp::Ordering::Greater => {
                                    cols[*index].push(field)?;
                                }
                                _ => {
                                    delta_cols[*index].push(field)?;
                                }
                            }
                            if cols_desc[*index].is_none() {
                                cols_desc[*index] = Some(col.clone());
                            }
                        }
                    }
                }
            }

            let cols_desc = cols_desc.into_iter().flatten().collect::<Vec<_>>();
            if cols_desc.len() != cols.len() {
                return Err(CommonSnafu {
                    reason: "Invalid cols_desc".to_string(),
                }
                .build());
            }

            if !time_array.valid().is_all_set() || !delta_time_array.valid().is_all_set() {
                return Err(CommonSnafu {
                    reason: "Invalid time array in DataBlock".to_string(),
                }
                .build());
            }
            return Ok(Some((
                schema.name.clone(),
                DataBlock::new(schema.clone(), time_array, cols),
                DataBlock::new(schema.clone(), delta_time_array, delta_cols),
            )));
        }
        Ok(None)
    }
}

pub struct MemCacheStatistics {
    _tf_id: TseriesFamilyId,
    /// greater seq mean the last write
    seq_no: u64,
    _statistics: HashMap<SeriesId, TimeRange>,
}

impl MemCacheStatistics {
    pub fn seq_no(&self) -> u64 {
        self.seq_no
    }
}

#[derive(Debug)]
pub struct MemCache {
    tf_id: TseriesFamilyId,

    max_size: u64,
    min_seq_no: u64,

    // wal seq number
    seq_no: AtomicU64,
    memory: RwLock<MemoryReservation>,

    part_count: usize,
    partions: Vec<RwLock<HashMap<SeriesId, RwLockRef<SeriesData>>>>,
}

impl MemCache {
    pub fn to_chunk_group(&self, max_level_ts: i64) -> TskvResult<(TsmWriteData, TsmWriteData)> {
        let partions: HashMap<SeriesId, Arc<RwLock<SeriesData>>> = self
            .partions
            .iter()
            .flat_map(|lock| {
                let inner_map = lock.read();
                let values = inner_map
                    .iter()
                    .map(|(id, rw_lock_ref)| (*id, rw_lock_ref.clone()))
                    .collect::<Vec<_>>();
                values
            })
            .collect();

        let mut chunk_group: TsmWriteData = BTreeMap::new();
        let mut delta_chunk_group: TsmWriteData = BTreeMap::new();
        partions
            .iter()
            .try_for_each(|(series_id, v)| -> TskvResult<()> {
                let data = v.read();
                if let Some((table, datablock, delta_datablock)) =
                    data.build_data_block(max_level_ts)?
                {
                    if !datablock.is_empty() {
                        if let Some(chunk) = chunk_group.get_mut(&table) {
                            chunk.insert(*series_id, (data.series_key.clone(), datablock));
                        } else {
                            let mut chunk = BTreeMap::new();
                            chunk.insert(*series_id, (data.series_key.clone(), datablock));
                            chunk_group.insert(table.clone(), chunk);
                        }
                    }

                    if !delta_datablock.is_empty() {
                        if let Some(chunk) = delta_chunk_group.get_mut(&table) {
                            chunk.insert(*series_id, (data.series_key.clone(), delta_datablock));
                        } else {
                            let mut chunk = BTreeMap::new();
                            chunk.insert(*series_id, (data.series_key.clone(), delta_datablock));
                            delta_chunk_group.insert(table, chunk);
                        }
                    }
                }
                Ok(())
            })?;
        Ok((chunk_group, delta_chunk_group))
    }
    pub fn new(
        tf_id: TseriesFamilyId,
        max_size: u64,
        part_count: usize,
        seq: u64,
        pool: &MemoryPoolRef,
    ) -> Self {
        let mut partions = Vec::with_capacity(part_count);
        for _i in 0..part_count {
            partions.push(RwLock::new(HashMap::new()));
        }
        let res =
            RwLock::new(MemoryConsumer::new(format!("memcache-{}-{}", tf_id, seq)).register(pool));
        Self {
            tf_id,

            max_size,
            min_seq_no: seq,

            part_count,
            partions,

            seq_no: AtomicU64::new(seq),
            memory: res,
        }
    }

    pub fn write_group(
        &self,
        sid: SeriesId,
        series_key: SeriesKey,
        seq: u64,
        group: RowGroup,
    ) -> TskvResult<()> {
        self.seq_no.store(seq, Ordering::Relaxed);
        self.memory
            .write()
            .try_grow(group.size)
            .map_err(|_| MemoryExhaustedSnafu.build())?;
        let index = (sid as usize) % self.part_count;
        let mut series_map = self.partions[index].write();
        if let Some(series_data) = series_map.get(&sid) {
            let series_data_ptr = series_data.clone();
            let mut series_data_ptr_w = series_data_ptr.write();
            drop(series_map);
            series_data_ptr_w.write(group);
        } else {
            let mut series_data = SeriesData::new(sid, series_key);
            series_data.write(group);
            series_map.insert(sid, Arc::new(RwLock::new(series_data)));
        }
        Ok(())
    }

    pub fn read_series_timestamps(
        &self,
        series_ids: &[SeriesId],
        time_ranges: &TimeRanges,
        mut handle_data: impl FnMut(Timestamp),
    ) {
        for sid in series_ids.iter() {
            let index = (*sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(sid).cloned();
            if let Some(series_data) = series_data {
                series_data
                    .read()
                    .read_timestamps(time_ranges, &mut handle_data);
            }
        }
    }

    pub fn statistics(
        &self,
        series_ids: &[SeriesId],
        time_predicate: TimeRange,
    ) -> MemCacheStatistics {
        let mut statistics = HashMap::new();
        for sid in series_ids {
            let index = (*sid as usize) % self.part_count;
            let range = match self.partions[index].read().get(sid) {
                None => continue,
                Some(series_data) => series_data.read().range,
            };
            let time_predicate = match time_predicate.intersect(&range) {
                None => continue,
                Some(time_predicate) => time_predicate,
            };
            statistics.insert(*sid, time_predicate);
        }
        MemCacheStatistics {
            _tf_id: self.tf_id,
            seq_no: self.min_seq_no,
            _statistics: statistics,
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

    pub fn drop_columns(&self, series_ids: &[SeriesId], column_ids: &[ColumnId]) {
        for sid in series_ids {
            for column_id in column_ids {
                let index = (*sid as usize) % self.part_count;
                let series_data = self.partions[index].read().get(sid).cloned();
                if let Some(series_data) = series_data {
                    series_data.write().drop_column(*column_id);
                }
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

    pub fn delete_series_by_time_ranges(&self, sids: &[SeriesId], time_ranges: &TimeRanges) {
        for sid in sids {
            let index = (*sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(sid).cloned();
            if let Some(series_data) = series_data {
                series_data.write().delete_by_time_ranges(time_ranges);
            }
        }
    }

    pub fn read_all_series_data(&self) -> Vec<(SeriesId, Arc<RwLock<SeriesData>>)> {
        let mut ret = Vec::new();
        self.partions.iter().for_each(|p| {
            let p_rlock = p.read();
            for (k, v) in p_rlock.iter() {
                ret.push((*k, v.clone()));
            }
        });
        ret
    }

    pub fn read_series_data_by_id(&self, sid: SeriesId) -> Option<Arc<RwLock<SeriesData>>> {
        let index = (sid as usize) % self.part_count;
        self.partions[index].read().get(&sid).cloned()
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

pub(crate) mod test {
    use std::collections::HashMap;
    use std::mem::size_of;
    use std::sync::Arc;

    use models::field_value::FieldVal;
    use models::predicate::domain::TimeRange;
    use models::schema::tskv_table_schema::TskvTableSchema;
    use models::{SchemaVersion, SeriesId, SeriesKey, Timestamp};
    use parking_lot::RwLock;

    use super::{MemCache, OrderedRowsData, RowData, RowGroup};

    pub fn put_rows_to_cache(
        cache: &MemCache,
        series_id: SeriesId,
        schema_id: SchemaVersion,
        mut schema: TskvTableSchema,
        time_range: (Timestamp, Timestamp),
        put_none: bool,
    ) {
        let mut rows = OrderedRowsData::new();
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
            rows.insert(RowData { ts, fields });
        }

        schema.schema_version = schema_id;
        let row_group = RowGroup {
            schema: schema.into(),
            range: TimeRange::from(time_range),
            rows,
            size: size_of::<RowGroup>() + size,
        };
        cache
            .write_group(series_id, SeriesKey::default(), 1, row_group)
            .unwrap();
    }

    pub fn get_one_series_cache_data(
        cache: Arc<RwLock<MemCache>>,
    ) -> HashMap<String, Vec<(Timestamp, FieldVal)>> {
        let mut fname_vals_map: HashMap<String, Vec<(Timestamp, FieldVal)>> = HashMap::new();
        let series_data = cache.read().read_all_series_data();
        for (_sid, sdata) in series_data {
            let sdata_rlock = sdata.read();
            let schema_groups = sdata_rlock.flat_groups();
            for (sch, row) in schema_groups {
                let fields = sch.fields();
                for r in row.get_ref_rows().iter() {
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
}

pub fn dedup_and_sort_row_data(data: &OrderedRowsData) -> Vec<RowData> {
    // let mut data = data.iter().cloned().collect::<Vec<_>>();
    // data.sort_by(|a, b| a.ts.cmp(&b.ts));
    let mut dedup_ts = HashSet::new();
    data.get_ref_rows().iter().for_each(|row_data| {
        if !dedup_ts.contains(&row_data.ts) {
            dedup_ts.insert(row_data.ts);
        }
    });

    let mut result: Vec<RowData> = Vec::with_capacity(dedup_ts.len());
    for row_data in data.get_ref_rows() {
        if let Some(existing_row) = result.last_mut() {
            if existing_row.ts == row_data.ts {
                for (index, field) in row_data.fields.iter().enumerate() {
                    if existing_row.fields[index].is_none() {
                        existing_row.fields[index] = field.clone();
                    }
                }
            } else {
                result.push(row_data.clone());
            }
        } else {
            result.push(row_data.clone());
        }
    }
    result
}

#[cfg(test)]
mod test_memcache {
    use std::sync::Arc;

    use cache::ShardedAsyncCache;
    use datafusion::arrow::datatypes::TimeUnit;
    use memory_pool::{GreedyMemoryPool, MemoryPool};
    use models::field_value::FieldVal;
    use models::predicate::domain::{TimeRange, TimeRanges};
    use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesId, SeriesKey, ValueType};

    use super::{dedup_and_sort_row_data, MemCache, OrderedRowsData, RowData, RowGroup};
    use crate::file_utils::make_tsm_file;
    use crate::memcache::SeriesData;
    use crate::tseries_family::{ColumnFile, LevelInfo, Version};
    use crate::Options;

    #[test]
    fn test_dedup_and_sort_row_data() {
        let mut rows1 = OrderedRowsData::new();

        rows1.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        rows1.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows1.insert(RowData {
            ts: 7,
            fields: vec![Some(FieldVal::Float(7.0))],
        });
        rows1.insert(RowData {
            ts: 5,
            fields: vec![Some(FieldVal::Float(5.0))],
        });
        rows1.insert(RowData {
            ts: 9,
            fields: vec![Some(FieldVal::Float(9.0))],
        });
        rows1.insert(RowData {
            ts: 7,
            fields: vec![Some(FieldVal::Float(10.0))],
        });
        rows1.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(11.0))],
        });
        rows1.insert(RowData {
            ts: 5,
            fields: vec![None],
        });

        let row_datas = dedup_and_sort_row_data(&rows1);
        let expect_row_datas = vec![
            RowData {
                ts: 1,
                fields: vec![Some(FieldVal::Float(11.0))],
            },
            RowData {
                ts: 3,
                fields: vec![Some(FieldVal::Float(3.0))],
            },
            RowData {
                ts: 5,
                fields: vec![Some(FieldVal::Float(5.0))],
            },
            RowData {
                ts: 7,
                fields: vec![Some(FieldVal::Float(10.0))],
            },
            RowData {
                ts: 9,
                fields: vec![Some(FieldVal::Float(9.0))],
            },
        ];
        assert_eq!(row_datas, expect_row_datas);
    }

    #[test]
    fn test_series_data_write_group() {
        let sid: SeriesId = 1;
        let mut series_data = SeriesData::new(sid, SeriesKey::default());

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
        schema_1.schema_version = 1;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        #[rustfmt::skip]
            let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 3),
            rows,
            size: 10,
        };
        series_data.write(row_group_1.clone());
        {
            assert_eq!(sid, series_data.series_id);
            assert_eq!(TimeRange::new(1, 3), series_data.range);
            assert_eq!(1, series_data.groups.len());
            assert_eq!(row_group_1, series_data.groups.front().unwrap().clone());
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
        schema_2.schema_version = 2;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 3,
            fields: vec![None, Some(FieldVal::Integer(3))],
        });
        rows.insert(RowData {
            ts: 5,
            fields: vec![Some(FieldVal::Float(5.0)), Some(FieldVal::Integer(5))],
        });
        #[rustfmt::skip]
            let row_group_2 = RowGroup {
            schema: Arc::new(schema_2),
            range: TimeRange::new(3, 5),
            rows,
            size: 10,
        };
        series_data.write(row_group_2.clone());
        {
            assert_eq!(sid, series_data.series_id);
            assert_eq!(TimeRange::new(1, 5), series_data.range);
            assert_eq!(2, series_data.groups.len());
            assert_eq!(row_group_2, series_data.groups.back().unwrap().clone());
        }
    }

    #[test]
    fn test_series_data_columns_modify() {
        let sid: SeriesId = 1;
        let mut series_data1 = SeriesData::new(sid, SeriesKey::default());

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
        schema_1.schema_version = 1;
        let mut rows1 = OrderedRowsData::new();
        rows1.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows1.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        #[rustfmt::skip]
            let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 3),
            rows:rows1,
            size: 10,
        };
        series_data1.write(row_group_1.clone());

        series_data1.add_column(&TableColumn::new(
            5,
            "f_col_2".to_string(),
            ColumnType::Field(ValueType::Float),
            Default::default(),
        ));
        {
            let row_group = series_data1.groups.front().unwrap();
            let schema = row_group.schema.clone();
            assert_eq!(5, schema.columns().len());
            assert!(schema.contains_column("f_col_2"));
        }

        series_data1.change_column(
            "f_col_2",
            &TableColumn::new(
                5,
                "i_col_2".to_string(),
                ColumnType::Field(ValueType::Integer),
                Default::default(),
            ),
        );
        {
            let row_group = series_data1.groups.front().unwrap();
            let schema = row_group.schema.clone();
            assert_eq!(5, schema.columns().len());
            assert!(schema.contains_column("i_col_2"));
            assert!(!schema.contains_column("f_col_2"));
        }

        series_data1.drop_column(5);
        {
            let row_group = series_data1.groups.front().unwrap();
            let schema = row_group.schema.clone();
            assert_eq!(4, schema.columns().len());
            assert!(!schema.contains_column("i_col_2"));
            assert!(!schema.contains_column("f_col_2"));
        }

        let mut schema_2 = TskvTableSchema::new(
            "test_tenant".to_string(),
            "test_db".to_string(),
            "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new_tag_column(2, "tag_col_1".to_string()),
                TableColumn::new_tag_column(3, "tag_col_2".to_string()),
                TableColumn::new(
                    4,
                    "f_col_1".to_string(),
                    ColumnType::Field(ValueType::Float),
                    Default::default(),
                ),
                TableColumn::new(
                    5,
                    "i_col_2".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Default::default(),
                ),
            ],
        );

        schema_2.schema_version = 1;
        let mut series_data2 = SeriesData::new(sid, SeriesKey::default());

        let mut rows2 = OrderedRowsData::new();
        rows2.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0)), Some(FieldVal::Integer(2))],
        });
        rows2.insert(RowData {
            ts: 5,
            fields: vec![Some(FieldVal::Float(3.0)), Some(FieldVal::Integer(3))],
        });
        #[rustfmt::skip]
            let row_group_2 = RowGroup {
            schema: Arc::new(schema_2),
            range: TimeRange::new(1, 5),
            rows:rows2,
            size: 10,
        };
        series_data2.write(row_group_2.clone());

        series_data2.drop_column(5);
        {
            let row_group = series_data2.groups.front().unwrap();
            let schema = row_group.schema.clone();
            assert_eq!(4, schema.columns().len());
            assert!(!schema.contains_column("i_col_2"));
            assert_eq!(
                RowData {
                    ts: 5,
                    fields: vec![Some(FieldVal::Float(3.0))]
                },
                row_group.rows.rows[1].clone()
            )
        }
    }

    #[test]
    fn test_series_data_delete_time_ranges() {
        let sid: SeriesId = 1;
        let mut series_data1 = SeriesData::new(sid, SeriesKey::default());

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
        schema_1.schema_version = 1;
        let mut rows1 = OrderedRowsData::new();
        rows1.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows1.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        rows1.insert(RowData {
            ts: 5,
            fields: vec![Some(FieldVal::Float(5.0))],
        });
        rows1.insert(RowData {
            ts: 7,
            fields: vec![Some(FieldVal::Float(7.0))],
        });
        rows1.insert(RowData {
            ts: 9,
            fields: vec![Some(FieldVal::Float(9.0))],
        });

        #[rustfmt::skip]
            let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 9),
            rows:rows1,
            size: 10,
        };
        series_data1.write(row_group_1.clone());

        let time_ranges = TimeRanges::new(vec![TimeRange::new(1, 3), TimeRange::new(7, 9)]);
        series_data1.delete_by_time_ranges(&time_ranges);
        assert_eq!(series_data1.groups.front().unwrap().rows.rows.len(), 1);
    }

    #[test]
    fn test_series_data_build_data_block() {
        let sid: SeriesId = 1;
        let mut series_data1 = SeriesData::new(sid, SeriesKey::default());
        let dir = "/tmp/test/memcache/1";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let mut global_config = config::tskv::get_config_for_test();
        global_config.storage.path = dir.to_string();
        let opt = Arc::new(Options::from(&global_config));

        let database = Arc::new("cnosdb.test".to_string());
        #[rustfmt::skip]
            let levels = [
            LevelInfo::init(database.clone(), 0, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 1, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 2, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 3, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 4, 0, opt.storage.clone()),
        ];
        let tsm_reader_cache = Arc::new(ShardedAsyncCache::create_lru_sharded_cache(16));
        let version = Version::new(
            1,
            database.clone(),
            opt.storage.clone(),
            1,
            levels,
            5,
            tsm_reader_cache,
        );

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
        schema_1.schema_version = 1;
        let mut rows1 = OrderedRowsData::new();

        rows1.insert(RowData {
            ts: 7,
            fields: vec![Some(FieldVal::Float(10.0))],
        });
        rows1.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(10.0))],
        });

        rows1.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows1.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        rows1.insert(RowData {
            ts: 5,
            fields: vec![Some(FieldVal::Float(5.0))],
        });
        rows1.insert(RowData {
            ts: 7,
            fields: vec![Some(FieldVal::Float(7.0))],
        });
        rows1.insert(RowData {
            ts: 9,
            fields: vec![Some(FieldVal::Float(9.0))],
        });

        #[rustfmt::skip]
            let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 10),
            rows:rows1,
            size: 10,
        };
        series_data1.write(row_group_1.clone());
        let result = series_data1.build_data_block(version.max_level_ts());
        match result {
            Ok(opt_blocks) => {
                if let Some((name, main_block, delta_block)) = opt_blocks {
                    assert_eq!("test_table".to_owned(), name);
                    assert_eq!(2, main_block.len());
                    assert_eq!(3, delta_block.len());
                } else {
                    println!("No data blocks were built.");
                }
            }
            Err(error) => {
                eprintln!("Error building data block: {}", error);
            }
        }
    }

    #[test]
    fn test_mem_cache_write_group() {
        let sid: SeriesId = 1;

        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 1000, 2, 1, &memory_pool);
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
        schema_1.schema_version = 1;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        #[rustfmt::skip]
            let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 3),
            rows,
            size: 10,
        };
        mem_cache
            .write_group(sid, SeriesKey::default(), 1, row_group_1.clone())
            .unwrap();
        {
            let series_part = &mem_cache.partions[sid as usize].read();
            let series_data = series_part.get(&sid);
            assert!(series_data.is_some());
            let series_data = series_data.unwrap().read();
            assert_eq!(sid, series_data.series_id);
            assert_eq!(TimeRange::new(1, 3), series_data.range);
            assert_eq!(1, series_data.groups.len());
            assert_eq!(row_group_1, series_data.groups.front().unwrap().clone());
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
        schema_2.schema_version = 2;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 3,
            fields: vec![None, Some(FieldVal::Integer(3))],
        });
        rows.insert(RowData {
            ts: 5,
            fields: vec![Some(FieldVal::Float(5.0)), Some(FieldVal::Integer(5))],
        });
        #[rustfmt::skip]
            let row_group_2 = RowGroup {
            schema: Arc::new(schema_2),
            range: TimeRange::new(3, 5),
            rows,
            size: 10,
        };
        mem_cache
            .write_group(sid, SeriesKey::default(), 2, row_group_2.clone())
            .unwrap();
        {
            let series_part = &mem_cache.partions[sid as usize].read();
            let series_data = series_part.get(&sid);
            assert!(series_data.is_some());
            let series_data = series_data.unwrap().read();
            assert_eq!(sid, series_data.series_id);
            assert_eq!(TimeRange::new(1, 5), series_data.range);
            assert_eq!(2, series_data.groups.len());
            assert_eq!(row_group_2, series_data.groups.back().unwrap().clone());
        }
    }

    #[test]
    fn test_mem_cache_to_chunk_group() {
        let sid: SeriesId = 1;
        let dir = "/tmp/test/memcache/2";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let mut global_config = config::tskv::get_config_for_test();
        global_config.storage.path = dir.to_string();
        let opt = Arc::new(Options::from(&global_config));

        let owner = Arc::new("cnosdb.test".to_string());
        let ts_family_id = 1;
        let tsm_dir = opt.storage.tsm_dir(&owner, ts_family_id);
        #[rustfmt::skip]
            let levels = [
            LevelInfo::init(owner.clone(), 0, 0, opt.storage.clone()),
            LevelInfo {
                files: vec![
                    Arc::new(ColumnFile::new(3, 1, TimeRange::new(3001, 3100), 100, false, make_tsm_file(&tsm_dir, 3))),
                ],
                owner: owner.clone(),
                tsf_id: 1,
                storage_opt: opt.storage.clone(),
                level: 1,
                cur_size: 100,
                max_size: 1000,
                time_range: TimeRange::new(3001, 3100),
            },
            LevelInfo {
                files: vec![
                    Arc::new(ColumnFile::new(1, 2, TimeRange::new(1, 1000), 1000, false, make_tsm_file(&tsm_dir, 1))),
                    Arc::new(ColumnFile::new(2, 2, TimeRange::new(1001, 2000), 1000, false, make_tsm_file(&tsm_dir, 2))),
                ],
                owner: owner.clone(),
                tsf_id: 1,
                storage_opt: opt.storage.clone(),
                level: 2,
                cur_size: 2000,
                max_size: 10000,
                time_range: TimeRange::new(1, 2000),
            },
            LevelInfo::init(owner.clone(), 3, 0, opt.storage.clone()),
            LevelInfo::init(owner.clone(), 4, 0, opt.storage.clone()),
        ];
        let tsm_reader_cache = Arc::new(ShardedAsyncCache::create_lru_sharded_cache(16));
        let version = Version::new(
            1,
            owner.clone(),
            opt.storage.clone(),
            1,
            levels,
            5,
            tsm_reader_cache,
        );
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 1000, 2, 1, &memory_pool);
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
        schema_1.schema_version = 1;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        rows.insert(RowData {
            ts: 6,
            fields: vec![Some(FieldVal::Float(6.0))],
        });
        #[rustfmt::skip]
            let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 3),
            rows,
            size: 10,
        };
        mem_cache
            .write_group(sid, SeriesKey::default(), 1, row_group_1.clone())
            .unwrap();
        let (chunk_group, delta_chunk_group) =
            mem_cache.to_chunk_group(version.max_level_ts()).unwrap();

        assert_eq!(
            1,
            chunk_group
                .get("test_table")
                .unwrap()
                .get(&1)
                .unwrap()
                .1
                .len()
        );
        assert_eq!(
            2,
            delta_chunk_group
                .get("test_table")
                .unwrap()
                .get(&1)
                .unwrap()
                .1
                .len()
        );
    }

    #[test]
    fn test_mem_cache_columns_modify() {
        let sid: SeriesId = 1;
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 1000, 2, 1, &memory_pool);
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
        schema_1.schema_version = 1;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        rows.insert(RowData {
            ts: 6,
            fields: vec![Some(FieldVal::Float(6.0))],
        });
        #[rustfmt::skip]
            let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 3),
            rows,
            size: 10,
        };
        mem_cache
            .write_group(sid, SeriesKey::default(), 1, row_group_1.clone())
            .unwrap();
        let series_ids = [1];
        let sids: &[SeriesId] = &series_ids;
        mem_cache.add_column(
            sids,
            &TableColumn::new(
                5,
                "f_col_2".to_string(),
                ColumnType::Field(ValueType::Float),
                Default::default(),
            ),
        );
        {
            let schema = mem_cache.partions[1]
                .read()
                .get(&1)
                .unwrap()
                .read()
                .get_schema()
                .unwrap();
            assert!(schema.contains_column("f_col_2"));
        }
        mem_cache.change_column(
            sids,
            "f_col_2",
            &TableColumn::new(
                5,
                "i_col_2".to_string(),
                ColumnType::Field(ValueType::Integer),
                Default::default(),
            ),
        );
        {
            let schema = mem_cache.partions[1]
                .read()
                .get(&1)
                .unwrap()
                .read()
                .get_schema()
                .unwrap();
            assert!(schema.contains_column("i_col_2"));
            assert!(!schema.contains_column("f_col_2"));
        }
        mem_cache.drop_columns(sids, &[5]);
        {
            let schema = mem_cache.partions[1]
                .read()
                .get(&1)
                .unwrap()
                .read()
                .get_schema()
                .unwrap();
            assert!(!schema.contains_column("i_col_2"));
            assert!(!schema.contains_column("f_col_2"));
        }
    }

    #[test]
    fn test_mem_cache_read_series_data() {
        let sid: SeriesId = 1;
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 1000, 2, 1, &memory_pool);
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
        schema_1.schema_version = 1;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        rows.insert(RowData {
            ts: 6,
            fields: vec![Some(FieldVal::Float(6.0))],
        });
        #[rustfmt::skip]
            let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 3),
            rows:rows.clone(),
            size: 10,
        };
        mem_cache
            .write_group(sid, SeriesKey::default(), 1, row_group_1.clone())
            .unwrap();
        mem_cache
            .write_group(2, SeriesKey::default(), 2, row_group_1.clone())
            .unwrap();
        let series_data = mem_cache.read_all_series_data();

        assert_eq!(2, series_data.len());
        assert_eq!(
            rows,
            series_data
                .get(1)
                .unwrap()
                .1
                .read()
                .groups
                .front()
                .unwrap()
                .rows
        );
    }

    #[test]
    fn test_mem_cache_delete_time_ranges() {
        let sid: SeriesId = 1;
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 1000, 2, 1, &memory_pool);
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
        schema_1.schema_version = 1;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        rows.insert(RowData {
            ts: 6,
            fields: vec![Some(FieldVal::Float(6.0))],
        });
        rows.insert(RowData {
            ts: 7,
            fields: vec![Some(FieldVal::Float(6.0))],
        });
        rows.insert(RowData {
            ts: 9,
            fields: vec![Some(FieldVal::Float(6.0))],
        });
        #[rustfmt::skip]
            let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 9),
            rows:rows.clone(),
            size: 10,
        };
        mem_cache
            .write_group(sid, SeriesKey::default(), 1, row_group_1.clone())
            .unwrap();
        mem_cache
            .write_group(2, SeriesKey::default(), 2, row_group_1.clone())
            .unwrap();
        let series_data = mem_cache.read_all_series_data();
        let sids = &[1, 2];
        let time_ranges = TimeRanges::new(vec![TimeRange::new(1, 3), TimeRange::new(7, 9)]);
        mem_cache.delete_series_by_time_ranges(sids, &time_ranges);
        let mut expected_rows = OrderedRowsData::new();
        expected_rows.insert(RowData {
            ts: 6,
            fields: vec![Some(FieldVal::Float(6.0))],
        });
        assert_eq!(
            expected_rows,
            series_data
                .get(1)
                .unwrap()
                .1
                .read()
                .groups
                .front()
                .unwrap()
                .rows
        );
        assert_eq!(
            expected_rows,
            series_data
                .first()
                .unwrap()
                .1
                .read()
                .groups
                .front()
                .unwrap()
                .rows
        );
    }
}
