use std::cmp;
use std::collections::LinkedList;
use std::ops::Bound::Included;
use std::sync::Arc;

use models::field_value::FieldVal;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::tskv_table_schema::{TableColumn, TskvTableSchema, TskvTableSchemaRef};
use models::{ColumnId, SeriesId, SeriesKey, Timestamp};

use super::memcache::dedup_and_sort_row_data;
use super::row_data::{OrderedRowsData, RowData};
use crate::error::{CommonSnafu, TskvResult};
use crate::tsm::data_block::{DataBlock, MutableColumn};

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
    pub fn new(series_id: SeriesId, series_key: SeriesKey) -> Self {
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

    pub fn read_data(
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
