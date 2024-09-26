use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::compute::kernels::cast;
use arrow::datatypes::{Field, Schema};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use futures::Stream;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::tskv_table_schema::{ColumnType, TableColumn};
use models::ColumnId;
use parking_lot::RwLock;
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
use crate::reader::utils::TimeRangeProvider;
use crate::{TskvError, TskvResult};

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

impl TimeRangeProvider for MemCacheReader {
    fn time_range(&self) -> TimeRange {
        self.time_ranges
            .max_time_range()
            .intersect(&self.series_data.read().range)
            .unwrap_or_else(TimeRange::none)
    }
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

        // Initialize builders for each column
        for item in self.columns.iter() {
            let kv_dt = item.column_type.to_physical_type();
            let builder_item = RowIterator::new_column_builder(&kv_dt, self.batch_size)?;
            builders.push(ArrayBuilderPtr::new(builder_item, kv_dt));
        }

        match self.read_mode {
            MemcacheReadMode::FieldScan => {
                // Define column ids for all except the first column (assumed to be timestamps)
                let column_ids: Vec<u32> = self.columns.iter().map(|c| c.id).collect();

                // Read data using the optimized `read_data` function and process it directly
                let mut result: Result<(), TskvError> = Ok(());
                self.series_data.read().read_data(
                    &column_ids[1..],
                    &self.time_ranges,
                    |row_data: &RowData| {
                        let mut offset = 0;

                        // Append timestamp for the first column
                        if let ColumnType::Time(unit) = &self.columns[0].column_type {
                            builders[offset].append_timestamp(unit, row_data.ts);
                            offset += 1;
                        }

                        // Append values for other columns
                        for (index, field) in row_data.fields.iter().enumerate() {
                            if let Some(field) = field {
                                if let Err(e) = builders[index + offset].append_value(
                                    self.columns[index + offset]
                                        .column_type
                                        .to_physical_data_type(),
                                    Some(field.data_value(row_data.ts)),
                                    &self.columns[index + offset].name,
                                ) {
                                    result = Err(e);
                                    return;
                                }
                            } else if let Err(e) = builders[index + offset].append_value(
                                self.columns[index + offset]
                                    .column_type
                                    .to_physical_data_type(),
                                None,
                                &self.columns[index + offset].name,
                            ) {
                                result = Err(e);
                                return;
                            }
                        }
                    },
                );

                // Handle the result of the closure
                result?;
            }

            MemcacheReadMode::TimeScan => {
                // 使用 read_timestamps 收集去重后的时间戳并直接写入 builders
                self.series_data
                    .read()
                    .read_timestamps(&self.time_ranges, |ts| {
                        if let ColumnType::Time(unit) = &self.columns[0].column_type {
                            builders[0].append_timestamp(unit, ts);
                        }
                    });
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
        let mem_cache = MemCache::new(1, 0, 1, 1000, 2, 1, &memory_pool);

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
