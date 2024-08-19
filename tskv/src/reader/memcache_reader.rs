use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::compute::kernels::cast;
use arrow::datatypes::{Field, Schema};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use futures::Stream;
use models::predicate::domain::TimeRanges;
use models::schema::tskv_table_schema::{ColumnType, TableColumn};
use models::{ColumnId, Timestamp};
use parking_lot::RwLock;
use skiplist::OrderedSkipList;
use snafu::ResultExt;

use super::{
    BatchReader, BatchReaderRef, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::error::ArrowSnafu;
use crate::mem_cache::row_data::RowData;
use crate::mem_cache::series_data::SeriesData;
use crate::reader::array_builder::ArrayBuilderPtr;
use crate::reader::iterator::RowIterator;
use crate::TskvResult;

enum MemcacheReadMode {
    FieldScan,
    TimeScan,
}
pub struct MemCacheReader {
    series_data: Arc<RwLock<SeriesData>>,
    time_ranges: Arc<TimeRanges>,
    batch_size: usize,
    columns: Vec<TableColumn>,
    schema_meta: HashMap<String, String>,
    read_mode: MemcacheReadMode,
}

impl MemCacheReader {
    pub fn try_new(
        series_data: Arc<RwLock<SeriesData>>,
        time_ranges: Arc<TimeRanges>,
        batch_size: usize,
        projection: &[ColumnId],
        schema_meta: HashMap<String, String>,
    ) -> TskvResult<Option<Arc<Self>>> {
        if let Some(tskv_schema) = series_data.read().get_schema() {
            // filter columns by projection
            let mut columns: Vec<TableColumn> = Vec::with_capacity(projection.len());
            for col_id in projection.iter() {
                if let Some(col_name) = tskv_schema.column_name(*col_id) {
                    if let Some(column) = tskv_schema.column(col_name) {
                        if !column.column_type.is_tag() {
                            columns.push(column.clone());
                        }
                    }
                }
            }

            let mut read_mode = MemcacheReadMode::FieldScan;
            if columns.len() == 1 && columns[0].column_type.is_time() {
                read_mode = MemcacheReadMode::TimeScan;
            }
            Ok(Some(Arc::new(Self {
                series_data: series_data.clone(),
                time_ranges,
                batch_size,
                columns,
                schema_meta,
                read_mode,
            })))
        } else {
            Ok(None)
        }
    }

    fn read_data_and_build_array(&self) -> TskvResult<Vec<ArrayBuilderPtr>> {
        let mut builders = Vec::with_capacity(self.columns.len());
        // build builders to RecordBatch
        for item in self.columns.iter() {
            let kv_dt = item.column_type.to_physical_type();
            let builder_item = RowIterator::new_column_builder(&kv_dt, self.batch_size)?;
            builders.push(ArrayBuilderPtr::new(builder_item, kv_dt))
        }

        match self.read_mode {
            MemcacheReadMode::FieldScan => {
                // 1.read all columns data to Vec<RowData>
                let mut row_data_vec: OrderedSkipList<RowData> = OrderedSkipList::new();
                let column_ids: Vec<u32> = self.columns.iter().map(|c| c.id).collect();
                self.series_data
                    .read()
                    .read_data(&column_ids[1..], &self.time_ranges, |d| {
                        row_data_vec.insert(d)
                    });

                // 2.merge RowData by ts
                let mut merge_row_data_vec: Vec<RowData> = Vec::with_capacity(row_data_vec.len());
                for row_data in row_data_vec {
                    if let Some(existing_row) = merge_row_data_vec.last_mut() {
                        if existing_row.ts == row_data.ts {
                            for (index, field) in row_data.fields.iter().enumerate() {
                                if let Some(field) = field {
                                    existing_row.fields[index] = Some(field.clone());
                                }
                            }
                        } else {
                            merge_row_data_vec.push(row_data.clone());
                        }
                    } else {
                        merge_row_data_vec.push(row_data.clone());
                    }
                }

                // 3.by Vec<RowData>, build multi ArrayBuilderPtr
                for row_data in merge_row_data_vec {
                    let mut offset = 0;
                    if let ColumnType::Time(unit) = &self.columns[0].column_type {
                        builders[offset].append_timestamp(unit, row_data.ts);
                        offset += 1;
                    }
                    for (index, field) in row_data.fields.iter().enumerate() {
                        if let Some(field) = field {
                            builders[index + offset].append_value(
                                self.columns[index + offset]
                                    .column_type
                                    .to_physical_data_type(),
                                Some(field.data_value(row_data.ts)),
                                &self.columns[index + offset].name,
                            )?;
                        } else {
                            builders[index + offset].append_value(
                                self.columns[index + offset]
                                    .column_type
                                    .to_physical_data_type(),
                                None,
                                &self.columns[index + offset].name,
                            )?;
                        }
                    }
                }
            }
            MemcacheReadMode::TimeScan => {
                // 1.read all columns data to Vec<Rev<IntoIter<DataType>>>
                let mut columns_data: Vec<Timestamp> = Vec::new();
                self.series_data
                    .read()
                    .read_timestamps(&self.time_ranges, |d| columns_data.push(d));
                columns_data.sort_by_key(|data| *data);
                columns_data.dedup_by_key(|data| *data);

                // 2.by Vec<Vec<Timestamp>>, build multi ArrayBuilderPtr
                if let ColumnType::Time(unit) = &self.columns[0].column_type {
                    for ts in columns_data {
                        builders[0].append_timestamp(unit, ts);
                    }
                }
            }
        }

        Ok(builders)
    }
}

impl BatchReader for MemCacheReader {
    fn process(&self) -> TskvResult<SendableSchemableTskvRecordBatchStream> {
        let builders = self.read_data_and_build_array()?;
        let fields = self.columns.iter().map(Field::from).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new_with_metadata(fields, self.schema_meta.clone()));

        Ok(Box::pin(MemcacheRecordBatchStream {
            schema,
            column_arrays: builders.into_iter().map(|mut b| b.ptr.finish()).collect(),
        }))
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MemCacheReader: series_id:{}, time_ranges:{:?}, batch_size:{}, columns_count:{}",
            self.series_data.read().series_id,
            self.time_ranges.time_ranges().collect::<Vec<_>>(),
            self.batch_size,
            self.columns.len()
        )
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        vec![]
    }
}

struct MemcacheRecordBatchStream {
    schema: SchemaRef,
    column_arrays: Vec<ArrayRef>,
}

impl SchemableTskvRecordBatchStream for MemcacheRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for MemcacheRecordBatchStream {
    type Item = TskvResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let schema = self.schema.clone();
        let column_nums = self.column_arrays.len();
        let mut arrays = Vec::with_capacity(column_nums);

        let mut index = 0;
        while !self.column_arrays.is_empty() {
            let array = self.column_arrays.remove(0);
            let target_type = self.schema.field(index).data_type().clone();
            // 如果类型不匹配，需要进行类型转换
            match convert_data_type_if_necessary(array.clone(), &target_type) {
                Ok(temp_array) => {
                    arrays.push(temp_array);
                }
                Err(e) => {
                    return Poll::Ready(Some(Err(e)));
                }
            }
            index += 1;
        }
        if arrays.len() == column_nums && column_nums > 0 {
            // 可以构造 RecordBatch
            return Poll::Ready(Some(
                RecordBatch::try_new(schema, arrays).context(ArrowSnafu),
            ));
        }
        Poll::Ready(None)
    }
}

fn convert_data_type_if_necessary(
    array: ArrayRef,
    target_type: &arrow_schema::DataType,
) -> TskvResult<ArrayRef> {
    if array.data_type() != target_type {
        cast::cast(&array, target_type).context(ArrowSnafu)
    } else {
        Ok(array)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::TimeUnit;
    use datafusion::assert_batches_eq;
    use futures::TryStreamExt;
    use memory_pool::{GreedyMemoryPool, MemoryPool};
    use models::field_value::FieldVal;
    use models::predicate::domain::{TimeRange, TimeRanges};
    use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesId, SeriesKey, ValueType};

    use super::MemCacheReader;
    use crate::mem_cache::memcache::MemCache;
    use crate::mem_cache::row_data::{OrderedRowsData, RowData};
    use crate::mem_cache::series_data::RowGroup;
    use crate::reader::BatchReader;

    #[tokio::test]
    async fn test_memcache_reader() {
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 1000, 2, 1, &memory_pool);

        let mut schema_1 = TskvTableSchema::new(
            "test_tenant".to_string(),
            "test_db".to_string(),
            "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new(
                    2,
                    "f1".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Default::default(),
                ),
                TableColumn::new(
                    3,
                    "f2".to_string(),
                    ColumnType::Field(ValueType::Float),
                    Default::default(),
                ),
            ],
        );
        schema_1.schema_version = 1;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Integer(1)), Some(FieldVal::Float(1.0))],
        });
        rows.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Integer(3)), Some(FieldVal::Float(3.0))],
        });
        let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 3),
            rows,
            size: 2,
        };

        let sid: SeriesId = 1;
        mem_cache
            .write_group(sid, SeriesKey::default(), 1, row_group_1.clone())
            .unwrap();

        let trs = Arc::new(TimeRanges::new(vec![TimeRange::new(1, 3)]));
        let memcache_reader: Arc<dyn BatchReader> = MemCacheReader::try_new(
            mem_cache.read_all_series_data()[0].1.clone(),
            trs,
            2,
            &[1, 2, 3],
            HashMap::new(),
        )
        .unwrap()
        .unwrap();
        let stream = memcache_reader.process().expect("memcache_reader");
        let result = stream.try_collect::<Vec<_>>().await.unwrap();
        let expected = [
            "+-------------------------------+----+-----+",
            "| time                          | f1 | f2  |",
            "+-------------------------------+----+-----+",
            "| 1970-01-01T00:00:00.000000001 | 1  | 1.0 |",
            "| 1970-01-01T00:00:00.000000003 | 3  | 3.0 |",
            "+-------------------------------+----+-----+",
        ];
        assert_batches_eq!(expected, &result);
    }
}
