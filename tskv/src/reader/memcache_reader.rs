use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec::IntoIter;

use arrow::compute::kernels::cast;
use arrow::datatypes::{Field, Schema};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use futures::Stream;
use models::field_value::DataType;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::{ColumnType, TableColumn};
use models::utils::min_num;
use models::{ColumnId, Timestamp};
use parking_lot::RwLock;

use super::{
    ArrayBuilderPtr, BatchReader, BatchReaderRef, RowIterator, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::memcache::SeriesData;
use crate::reader::utils::TimeRangeProvider;
use crate::Result;

enum MemcacheReadMode {
    FieldScan,
    TimeScan,
}
pub struct MemCacheReader {
    series_data: Arc<RwLock<SeriesData>>,
    time_ranges: Arc<TimeRanges>,
    batch_size: usize,
    columns: Vec<TableColumn>,
    read_mode: MemcacheReadMode,
}

impl TimeRangeProvider for MemCacheReader {
    fn time_range(&self) -> TimeRange {
        match self
            .time_ranges
            .max_time_range()
            .intersect(&self.series_data.read().range)
        {
            None => TimeRange::none(),
            Some(tr) => tr,
        }
    }
}

impl MemCacheReader {
    pub fn try_new(
        series_data: Arc<RwLock<SeriesData>>,
        time_ranges: Arc<TimeRanges>,
        batch_size: usize,
        projection: &[ColumnId],
    ) -> Result<Option<Arc<Self>>> {
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
                read_mode,
            })))
        } else {
            Ok(None)
        }
    }

    fn collect_row_data(
        &self,
        builders: &mut [ArrayBuilderPtr],
        columns_data_vec: &mut Vec<IntoIter<DataType>>,
        row_cols: &mut [Option<DataType>],
    ) -> Result<Option<()>> {
        trace::trace!("======collect_row_data=========");

        let mut min_time = i64::MAX;

        // For each column, peek next (timestamp, value), set column_values, and
        // specify the next min_time (if column is a `Field`).
        for (col_cursor, row_col) in columns_data_vec.iter_mut().zip(row_cols.iter_mut()) {
            if row_col.is_none() {
                *row_col = col_cursor.next();
            }
            if let Some(ref d) = row_col {
                min_time = min_num(min_time, d.timestamp());
            }
        }

        // For the specified min_time, fill each column data.
        // If a column data is for later time, set min_time_column_flag.
        let mut min_time_column_flag = vec![false; columns_data_vec.len()];
        for (ts_val, min_flag) in row_cols.iter().zip(min_time_column_flag.iter_mut()) {
            if let Some(d) = ts_val {
                let ts = d.timestamp();
                if ts == min_time {
                    *min_flag = true
                }
            }
        }

        // Step field_scan completed.
        if min_time == i64::MAX {
            return Ok(None);
        }

        for (i, (value, min_flag)) in row_cols
            .iter_mut()
            .zip(min_time_column_flag.iter_mut())
            .enumerate()
        {
            match &self.columns[i].column_type {
                ColumnType::Time(unit) => {
                    builders[i].append_timestamp(unit, min_time);
                }
                ColumnType::Field(_) => {
                    if *min_flag {
                        builders[i].append_value(
                            self.columns[i].column_type.to_physical_data_type(),
                            value.take(),
                            &self.columns[i].name,
                        )?;
                    } else {
                        builders[i].append_value(
                            self.columns[i].column_type.to_physical_data_type(),
                            None,
                            &self.columns[i].name,
                        )?;
                    }
                }
                _ => {}
            }
        }

        Ok(Some(()))
    }

    fn read_data_and_build_array(&self) -> Result<Vec<ArrayBuilderPtr>> {
        let mut builders = Vec::with_capacity(self.columns.len());
        // build builders to RecordBatch
        for item in self.columns.iter() {
            let kv_dt = item.column_type.to_physical_type();
            let builder_item = RowIterator::new_column_builder(&kv_dt, self.batch_size)?;
            builders.push(ArrayBuilderPtr::new(builder_item, kv_dt))
        }

        match self.read_mode {
            MemcacheReadMode::FieldScan => {
                // 1.read all columns data to Vec<Rev<IntoIter<DataType>>>
                let mut columns_data_vec: Vec<IntoIter<DataType>> = Vec::new();
                for column in &self.columns {
                    let mut columns_data: Vec<DataType> = Vec::new();
                    self.series_data.read().read_data(
                        column.id,
                        &self.time_ranges,
                        |_| true,
                        |d| columns_data.push(d),
                    );
                    columns_data.dedup_by_key(|data| data.timestamp());
                    columns_data_vec.push(columns_data.into_iter());
                }

                // 2.by Vec<Vec<DataType>>, build multi ArrayBuilderPtr
                let mut row_cols: Vec<Option<DataType>> = vec![None; self.columns.len()];
                loop {
                    if self
                        .collect_row_data(&mut builders, &mut columns_data_vec, &mut row_cols)?
                        .is_none()
                    {
                        break;
                    }
                }
            },
            MemcacheReadMode::TimeScan => {
                // 1.read all columns data to Vec<Rev<IntoIter<DataType>>>
                let mut columns_data: Vec<Timestamp> = Vec::new();
                self.series_data.read().read_timestamps(
                    &self.time_ranges,
                    |d| columns_data.push(d),
                );
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
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        let builders = self.read_data_and_build_array()?;
        let fields = self.columns.iter().map(Field::from).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));

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
            self.time_ranges.time_ranges(),
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
    type Item = Result<RecordBatch>;
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
                RecordBatch::try_new(schema, arrays).map_err(Into::into),
            ));
        }
        Poll::Ready(None)
    }
}

fn convert_data_type_if_necessary(
    array: ArrayRef,
    target_type: &arrow_schema::DataType,
) -> Result<ArrayRef> {
    if array.data_type() != target_type {
        match cast::cast(&array, target_type) {
            Ok(array) => Ok(array),
            Err(e) => Err(e.into()),
        }
    } else {
        Ok(array)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::TimeUnit;
    use datafusion::assert_batches_eq;
    use futures::TryStreamExt;
    use memory_pool::{GreedyMemoryPool, MemoryPool};
    use models::field_value::FieldVal;
    use models::predicate::domain::{TimeRange, TimeRanges};
    use models::schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesId, ValueType};

    use super::MemCacheReader;
    use crate::memcache::{MemCache, OrderedRowsData, RowData, RowGroup};
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
        schema_1.schema_id = 1;
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
        mem_cache.write_group(sid, 1, row_group_1.clone()).unwrap();

        let trs = Arc::new(TimeRanges::new(vec![TimeRange::new(1, 3)]));
        let memcache_reader: Arc<dyn BatchReader> = MemCacheReader::try_new(
            mem_cache.read_series_data()[0].1.clone(),
            trs,
            2,
            &[1, 2, 3],
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
