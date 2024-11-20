use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use memory_pool::{MemoryConsumer, MemoryPoolRef, MemoryReservation};
use models::meta_data::VnodeId;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::tskv_table_schema::TableColumn;
use models::{ColumnId, RwLockRef, SeriesId, SeriesKey, Timestamp};
use parking_lot::RwLock;

use super::series_data::{RowGroup, SeriesData};
use crate::error::{MemoryExhaustedSnafu, TskvResult};
use crate::ColumnFileId;

pub struct MemCacheStatistics {
    _tf_id: VnodeId,
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
    tf_id: VnodeId,

    flushing: AtomicBool,

    max_size: u64,
    min_seq_no: u64,
    file_id: ColumnFileId,

    // wal seq number
    seq_no: AtomicU64,
    memory: RwLock<MemoryReservation>,

    part_count: usize,
    partions: Vec<RwLock<HashMap<SeriesId, RwLockRef<SeriesData>>>>,
}

pub struct MemCacheSeriesScanIterator {
    index: usize,
    serieses: Vec<RwLockRef<SeriesData>>,
}

impl MemCacheSeriesScanIterator {
    pub fn new(memcache: Arc<RwLock<MemCache>>) -> Self {
        let memcache = memcache.read();
        let serieses: Vec<RwLockRef<SeriesData>> = memcache
            .partions
            .iter()
            .flat_map(|lock| {
                let inner_map = lock.read();
                inner_map.values().cloned().collect::<Vec<_>>()
            })
            .collect();

        Self { index: 0, serieses }
    }

    pub fn series_count(&self) -> u64 {
        self.serieses.len() as u64
    }
}

impl Iterator for MemCacheSeriesScanIterator {
    type Item = RwLockRef<SeriesData>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.serieses.len() {
            let tmp = Some(self.serieses[self.index].clone());
            self.index += 1;
            tmp
        } else {
            None
        }
    }
}

impl MemCache {
    pub fn new(
        tf_id: VnodeId,
        file_id: ColumnFileId,
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
            file_id,

            flushing: AtomicBool::new(false),

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

    pub fn update_tag_value(&self, series: &HashMap<SeriesId, SeriesKey>) {
        for (sid, key) in series {
            let index = (*sid as usize) % self.part_count;
            let partion = self.partions[index].read();
            let series_data = partion.get(sid);
            if let Some(series_data) = series_data {
                series_data.write().update_tag_value(key.clone());
            }
        }
    }

    pub fn mark_flushing(&self) -> bool {
        self.flushing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn erase_flushing(&self) -> bool {
        self.flushing
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.memory.read().size() >= self.max_size as usize
    }

    pub fn tf_id(&self) -> VnodeId {
        self.tf_id
    }

    pub fn file_id(&self) -> u64 {
        self.file_id
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

    use super::MemCache;
    use crate::mem_cache::row_data::{OrderedRowsData, RowData};
    use crate::mem_cache::series_data::RowGroup;

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
            for (sch, _, row) in schema_groups {
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

#[cfg(test)]
mod test_memcache {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::TimeUnit;
    use memory_pool::{GreedyMemoryPool, MemoryPool};
    use models::field_value::FieldVal;
    use models::predicate::domain::{TimeRange, TimeRanges};
    use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesId, SeriesKey, ValueType};

    use super::MemCache;
    use crate::mem_cache::row_data::{OrderedRowsData, RowData, RowDataRef};
    use crate::mem_cache::series_data::{RowGroup, SeriesData, SeriesDedupMergeSortIterator};

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
            let test_rows = row_group.rows.get_ref_rows();
            assert_eq!(
                RowData {
                    ts: 5,
                    fields: vec![Some(FieldVal::Float(3.0))]
                },
                test_rows[1].clone()
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
        assert_eq!(
            series_data1
                .groups
                .front()
                .unwrap()
                .rows
                .get_ref_rows()
                .len(),
            1
        );
    }

    #[test]
    fn test_mem_cache_write_group() {
        let sid: SeriesId = 1;

        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 0, 1000, 2, 1, &memory_pool);
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
    fn test_mem_cache_columns_modify() {
        let sid: SeriesId = 1;
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 0, 1000, 2, 1, &memory_pool);
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
        let mem_cache = MemCache::new(1, 0, 1000, 2, 1, &memory_pool);
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
        let mem_cache = MemCache::new(1, 0, 1000, 2, 1, &memory_pool);
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

    #[test]
    fn test_iterator() {
        //----------line 1-------------------------------------------------
        let mut rows1 = OrderedRowsData::new();
        #[rustfmt::skip]
        rows1.insert(RowData {ts: 1,fields: vec![Some(FieldVal::Integer(11)), Some(FieldVal::Integer(12)),Some(FieldVal::Integer(13))]});
        #[rustfmt::skip]
        rows1.insert(RowData {ts: 2,fields: vec![Some(FieldVal::Integer(21)), Some(FieldVal::Integer(22)),Some(FieldVal::Integer(23))]});
        #[rustfmt::skip]
        rows1.insert(RowData {ts: 3,fields: vec![Some(FieldVal::Integer(31)), Some(FieldVal::Integer(32)),Some(FieldVal::Integer(33))]});

        //----------line 2-------------------------------------------------
        let mut rows2 = OrderedRowsData::new();
        #[rustfmt::skip]
        rows2.insert(RowData {ts: 2,fields: vec![Some(FieldVal::Integer(211)), None,Some(FieldVal::Integer(231))]});
        #[rustfmt::skip]
        rows2.insert(RowData {ts: 3,fields: vec![Some(FieldVal::Integer(321)), Some(FieldVal::Integer(322)),Some(FieldVal::Integer(323))]});
        #[rustfmt::skip]
        rows2.insert(RowData {ts: 3,fields: vec![None, Some(FieldVal::Integer(332)),Some(FieldVal::Integer(333))]});

        let mut series_data = SeriesData::new(1000, SeriesKey::default());
        #[rustfmt::skip]
        let mut schema1 = TskvTableSchema::new(
            "test_tenant".to_string(), "test_db".to_string(), "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new_tag_column(2, "tag_col_1".to_string()),
                TableColumn::new(4, "f_col_4".to_string(), ColumnType::Field(ValueType::Integer), Default::default()),
                TableColumn::new(5, "f_col_5".to_string(), ColumnType::Field(ValueType::Integer), Default::default()),
                TableColumn::new(6, "f_col_6".to_string(), ColumnType::Field(ValueType::Integer), Default::default()), 
            ],
        );
        schema1.schema_version = 10;
        let row_group1 = RowGroup {
            schema: Arc::new(schema1),
            range: TimeRange::new(1, 3),
            rows: rows1,
            size: 10,
        };
        series_data.write(row_group1);

        #[rustfmt::skip]
        let mut schema2 = TskvTableSchema::new(
            "test_tenant".to_string(), "test_db".to_string(), "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new_tag_column(2, "tag_col_1".to_string()),
                TableColumn::new(4, "f_col_4".to_string(), ColumnType::Field(ValueType::Integer), Default::default()),
                TableColumn::new(5, "f_col_5".to_string(), ColumnType::Field(ValueType::Integer), Default::default()),
                TableColumn::new(7, "f_col_6".to_string(), ColumnType::Field(ValueType::Integer), Default::default()), 
            ],
        );
        schema2.schema_version = 20;
        let row_group2 = RowGroup {
            schema: Arc::new(schema2.clone()),
            range: TimeRange::new(1, 3),
            rows: rows2,
            size: 10,
        };
        series_data.write(row_group2);

        let latest_schema = Arc::new(schema2);
        let groups = series_data.flat_groups();
        let mut iter = SeriesDedupMergeSortIterator::new(groups.clone(), latest_schema.clone());

        let line1 = iter.next().unwrap();
        assert_eq!(
            line1,
            RowDataRef {
                ts: 1,
                fields: vec![
                    Some(&FieldVal::Integer(11)),
                    Some(&FieldVal::Integer(12)),
                    None
                ]
            }
        );

        let line2 = iter.next().unwrap();
        assert_eq!(
            line2,
            RowDataRef {
                ts: 2,
                fields: vec![
                    Some(&FieldVal::Integer(211)),
                    Some(&FieldVal::Integer(22)),
                    Some(&FieldVal::Integer(231)),
                ]
            }
        );

        let line3 = iter.next().unwrap();
        assert_eq!(
            line3,
            RowDataRef {
                ts: 3,
                fields: vec![
                    Some(&FieldVal::Integer(321)),
                    Some(&FieldVal::Integer(332)),
                    Some(&FieldVal::Integer(333)),
                ]
            }
        );

        assert_eq!(iter.next(), None);
    }
}
