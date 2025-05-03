use std::collections::{HashMap, LinkedList};
use std::sync::Arc;

use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::tskv_table_schema::{TableColumn, TskvTableSchema, TskvTableSchemaRef};
use models::{ColumnId, SeriesId, SeriesKey, Timestamp};
use skiplist::skiplist;

use super::row_data::{OrderedRowsData, RowData, RowDataRef};
use crate::error::{CommonSnafu, TskvResult};
use crate::tsm::mutable_column_ref::MutableColumnRef;
use crate::tsm::page;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RowGroup {
    pub schema: Arc<TskvTableSchema>,
    pub range: TimeRange,
    pub rows: OrderedRowsData,
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
            let name = match item.schema.get_column_name_by_id(column_id) {
                None => continue,
                Some(name) => name.to_string(),
            };
            let index = match item.schema.field_ids_index().get(&column_id) {
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
            schema_t.alter_column(column_name, new_column.clone());
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

    pub fn update_tag_value(&mut self, series_key: SeriesKey) {
        self.series_key = series_key
    }

    pub fn read_data(
        &self,
        column_ids: &[ColumnId],
        time_ranges: &TimeRanges,
        mut handle_data: impl FnMut(&RowData),
    ) {
        let groups = self.flat_groups();
        let latest_schema = match self.get_schema() {
            Some(schema) => schema,
            None => return,
        };

        let iter = SeriesDedupMergeSortIterator::new(groups, latest_schema.clone());

        for row_data in iter {
            if !time_ranges.contains(row_data.ts) {
                continue;
            }

            let field_index = latest_schema.field_ids_index();
            let mut fields = vec![None; column_ids.len()];

            // 遍历所有的 column_ids，按照对应的索引更新字段
            column_ids.iter().enumerate().for_each(|(i, column_id)| {
                if let Some(index) = field_index.get(column_id) {
                    if let Some(Some(field)) = row_data.fields.get(*index) {
                        // 这里将 &FieldVal 转换为 FieldVal 进行存储
                        fields[i] = Some((*field).clone());
                    }
                }
            });

            handle_data(&RowData {
                ts: row_data.ts,
                fields,
            });
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
        let groups = self.flat_groups();
        let latest_schema = match self.get_schema() {
            Some(schema) => schema,
            None => return,
        };

        let iter = SeriesDedupMergeSortIterator::new(groups, latest_schema.clone());

        for row_data in iter {
            if time_ranges.contains(row_data.ts) {
                handle_data(row_data.ts);
            }
        }
    }
    pub fn flat_groups(
        &self,
    ) -> Vec<(
        TskvTableSchemaRef,
        HashMap<ColumnId, usize>,
        &OrderedRowsData,
    )> {
        self.groups
            .iter()
            .map(|g| {
                (
                    g.schema.clone(),
                    g.schema.field_ids_index().clone(),
                    &g.rows,
                )
            })
            .collect()
    }
    pub fn get_schema(&self) -> Option<Arc<TskvTableSchema>> {
        if let Some(item) = self.groups.back() {
            return Some(item.schema.clone());
        }

        None
    }

    #[allow(clippy::type_complexity)]
    pub fn convert_to_page(&self) -> TskvResult<Option<(Arc<TskvTableSchema>, Vec<page::Page>)>> {
        let latest_schema = match self.get_schema() {
            Some(schema) => schema,
            None => return Ok(None),
        };

        let mut time_array = MutableColumnRef::empty(latest_schema.get_time_column())?;

        let fields_schema = latest_schema.build_field_columns_vec_order_by_id();
        let mut fields_array = fields_schema
            .iter()
            .map(|col| MutableColumnRef::empty(col.clone()))
            .collect::<TskvResult<Vec<_>>>()?;

        let iter = SeriesDedupMergeSortIterator::new(self.flat_groups(), latest_schema.clone());
        for row_data in iter {
            time_array.push_ts(row_data.ts)?;
            for (idx, field) in row_data.fields.iter().enumerate() {
                fields_array[idx].push(*field)?;
            }
        }

        if time_array
            .column_data
            .valid
            .finish_cloned()
            .count_set_bits()
            != time_array.column_data.valid.len()
        {
            return Err(CommonSnafu {
                reason: "Invalid time array in DataBlock".to_string(),
            }
            .build());
        }

        let mut pages_data = vec![];
        if !time_array.column_data.valid.is_empty() {
            pages_data.push(page::Page::colref_to_page(time_array)?);
            for field_array in fields_array {
                pages_data.push(page::Page::colref_to_page(field_array)?);
            }
        }

        Ok(Some((latest_schema, pages_data)))
    }
}

#[allow(clippy::type_complexity)]
pub struct SeriesDedupMergeSortIterator<'a> {
    groups: Vec<(
        Arc<TskvTableSchema>,
        HashMap<u32, usize>,
        &'a OrderedRowsData,
    )>,

    vec_pq: Vec<(usize, Option<&'a RowData>, skiplist::Iter<'a, RowData>)>,

    align_fileds: Vec<TableColumn>,
}

impl<'a> SeriesDedupMergeSortIterator<'a> {
    pub fn new(
        groups: Vec<(
            Arc<TskvTableSchema>,
            HashMap<u32, usize>,
            &'a OrderedRowsData,
        )>,
        align_schema: Arc<TskvTableSchema>,
    ) -> SeriesDedupMergeSortIterator<'a> {
        let mut vec_pq = Vec::with_capacity(groups.len());
        for (idx, group) in groups.iter().enumerate() {
            let mut iter = group.2.get_ref_rows().iter();
            let data = iter.next();
            vec_pq.push((idx, data, iter));
        }

        let align_fileds = align_schema.build_field_columns_vec_order_by_id();
        SeriesDedupMergeSortIterator {
            vec_pq,
            groups,
            align_fileds,
        }
    }
}

impl<'a> Iterator for SeriesDedupMergeSortIterator<'a> {
    type Item = RowDataRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut min_ts = i64::MAX;
        for (_, data, _) in self.vec_pq.iter() {
            if let Some(data) = data {
                if min_ts > data.ts {
                    min_ts = data.ts
                }
            }
        }
        if min_ts == i64::MAX {
            return None;
        }

        let mut result = RowDataRef {
            ts: min_ts,
            fields: vec![None; self.align_fileds.len()],
        };

        for (idx, data, it) in self.vec_pq.iter_mut().rev() {
            while let Some(row) = data {
                if row.ts != min_ts {
                    break;
                }

                for (align_idx, col) in self.align_fileds.iter().enumerate() {
                    if let Some(index) = self.groups[*idx].1.get(&col.id) {
                        if let Some(Some(field)) = row.fields.get(*index) {
                            if result.fields[align_idx].is_none() {
                                result.fields[align_idx] = Some(field);
                            }
                        }
                    }
                }

                *data = it.next();
            }
        }

        Some(result)
    }
}

#[cfg(test)]
mod test {
    #[derive(Debug, Clone)]
    pub struct Tuple {
        pub a: i32,
        #[allow(dead_code)]
        pub b: i32,
    }

    impl PartialOrd for Tuple {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.a.cmp(&other.a))
        }
    }

    impl PartialEq for Tuple {
        fn eq(&self, other: &Self) -> bool {
            self.a == other.a
        }
    }

    impl Ord for Tuple {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.a.cmp(&other.a)
        }
    }

    impl Eq for Tuple {}

    #[test]
    #[ignore]
    fn test_ordered_skiplist() {
        let mut list = skiplist::OrderedSkipList::new();
        list.insert(Tuple { a: 1, b: 12 });
        list.insert(Tuple { a: 1, b: 13 });
        list.insert(Tuple { a: 1, b: 11 });
        list.insert(Tuple { a: 1, b: 10 });
        list.insert(Tuple { a: 1, b: 9 });
        list.insert(Tuple { a: 1, b: 8 });
        list.insert(Tuple { a: 1, b: 7 });
        list.insert(Tuple { a: 1, b: 6 });
        list.insert(Tuple { a: 1, b: 14 });
        list.insert(Tuple { a: 1, b: 15 });
        list.insert(Tuple { a: 1, b: 16 });
        list.insert(Tuple { a: 1, b: 17 });
        list.insert(Tuple { a: 1, b: 11 });
        // list.insert(Tuple { a: 2, b: 11 });
        // list.insert(Tuple { a: 3, b: 11 });
        // list.insert(Tuple { a: 4, b: 11 });

        for it in list.iter() {
            println!("------ {:?}", it);
        }
    }

    #[test]
    #[ignore]
    fn test_binary_heap() {
        let mut pq = std::collections::BinaryHeap::new();
        pq.push(Tuple { a: 1, b: 12 });
        pq.push(Tuple { a: 1, b: 13 });
        pq.push(Tuple { a: 1, b: 11 });
        pq.push(Tuple { a: 2, b: 11 });
        pq.push(Tuple { a: 3, b: 11 });
        pq.push(Tuple { a: 4, b: 11 });

        while let Some(it) = pq.pop() {
            println!("------ {:?}", it);
        }
    }

    #[test]
    #[ignore]
    fn test_alloc_perf() {
        //-----------------分配一次，多次用默认值填充--------------------------
        let start = std::time::Instant::now();
        let mut result = crate::mem_cache::row_data::RowDataRef {
            ts: 0,
            fields: vec![None; 32],
        };
        for _ in 0..10000000 {
            //result.fields.fill(None);

            // for it in result.fields.iter_mut() {
            //     *it = None;
            // }

            for i in 0..32 {
                result.fields[i] = None;
            }
        }
        let elapsed = start.elapsed();
        println!("---------- {:?}", elapsed);

        //---------------多次分配，分配时用默认值填充----------------------------
        let start = std::time::Instant::now();
        for _ in 0..10000000 {
            let _result = crate::mem_cache::row_data::RowDataRef {
                ts: 0,
                fields: vec![None; 32],
            };
        }
        let elapsed = start.elapsed();
        println!("---------- {:?}", elapsed);

        //---------------一次分配，通过clone方式构造----------------------------
        let start = std::time::Instant::now();
        let result = crate::mem_cache::row_data::RowDataRef {
            ts: 0,
            fields: vec![None; 32],
        };
        for _ in 0..10000000 {
            let _res = result.clone();
        }
        let elapsed = start.elapsed();
        println!("---------- {:?}", elapsed);
    }

    #[test]
    #[ignore]
    fn test_iter_perf() {
        let result = crate::mem_cache::row_data::RowDataRef {
            ts: 0,
            fields: vec![None; 3],
        };

        //---------------通过迭代器访问----------------------------
        let start = std::time::Instant::now();
        for _ in 0..100000000 {
            let mut it = result.fields.iter();
            let _tmp = it.next();
        }
        let elapsed = start.elapsed();
        println!("---------- {:?}", elapsed);

        //-----------------通过下标访问--------------------------
        let start = std::time::Instant::now();
        for _ in 0..100000000 {
            let _tmp = result.fields[0];
        }
        let elapsed = start.elapsed();
        println!("---------- {:?}", elapsed);
    }
}
