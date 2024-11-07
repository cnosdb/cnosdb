use std::sync::Arc;

use arrow_array::ArrayRef;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use models::arrow::stream::BoxStream;
use models::predicate::domain::TimeRange;
use models::SeriesId;

use super::column_group::ColumnGroupReaderMetrics;
use crate::tsm::page::PageWriteSpec;
use crate::tsm::reader::{page_to_arrow_array_with_tomb, TsmReader};
use crate::TskvResult;

pub type PageReaderRef = Arc<dyn PageReader>;
pub trait PageReader {
    fn process(&self) -> TskvResult<BoxStream<TskvResult<ArrayRef>>>;
}

#[derive(Clone)]
pub struct PrimitiveArrayReader {
    reader: Arc<TsmReader>,
    page_meta: PageWriteSpec,

    // only used for tombstone filter
    series_id: SeriesId,
    time_page_meta: Arc<PageWriteSpec>,
    time_range: TimeRange,

    metrics: Arc<ExecutionPlanMetricsSet>,
}

impl PrimitiveArrayReader {
    pub fn new(
        reader: Arc<TsmReader>,
        page_meta: &PageWriteSpec,
        series_id: SeriesId,
        time_page_meta: Arc<PageWriteSpec>,
        time_range: TimeRange,
        metrics: Arc<ExecutionPlanMetricsSet>,
    ) -> Self {
        Self {
            reader,
            // TODO
            page_meta: page_meta.clone(),
            series_id,
            time_page_meta,
            time_range,
            metrics,
        }
    }
}

impl PageReader for PrimitiveArrayReader {
    fn process(&self) -> TskvResult<BoxStream<TskvResult<ArrayRef>>> {
        let metrics = ColumnGroupReaderMetrics::new(self.metrics.as_ref());

        Ok(Box::pin(futures::stream::once(read(
            self.reader.clone(),
            self.page_meta.clone(),
            self.series_id,
            self.time_page_meta.clone(),
            self.time_range,
            metrics,
        ))))
    }
}

async fn read(
    reader: Arc<TsmReader>,
    page_meta: PageWriteSpec,
    series_id: SeriesId,
    time_page_meta: Arc<PageWriteSpec>,
    time_range: TimeRange,
    metrics: ColumnGroupReaderMetrics,
) -> TskvResult<ArrayRef> {
    let page = {
        let _timer = metrics.elapsed_page_scan_time().timer();
        metrics.page_read_count().add(1);
        metrics.page_read_bytes().add(page_meta.size() as usize);
        reader.read_page(&page_meta).await?
    };

    let _timer = metrics.elapsed_page_to_array_time().timer();
    page_to_arrow_array_with_tomb(page, reader, series_id, time_page_meta, time_range).await
}

#[cfg(test)]
pub(crate) mod tests {
    use std::marker::PhantomData;
    use std::sync::Arc;

    use arrow::datatypes::TimeUnit;
    use arrow_array::builder::{BooleanBuilder, StringBuilder};
    use arrow_array::{ArrayRef, Float64Array, Int64Array, UInt64Array};
    use futures::stream;
    use minivec::MiniVec;
    use models::arrow::stream::BoxStream;
    use models::field_value::FieldVal;
    use models::gis::data_type::{Geometry, GeometryType};
    use models::schema::tskv_table_schema::{ColumnType, TableColumn};
    use models::{PhysicalDType, ValueType};
    use utils::bitset::{BitSet, NullBitset};

    use super::PageReader;
    use crate::tsm::mutable_column::MutableColumn;
    use crate::tsm::page::Page;
    use crate::tsm::reader::{data_buf_to_arrow_array, updated_nullbuffer};
    use crate::TskvResult;

    pub struct TestPageReader<T> {
        _phantom_data: PhantomData<T>,
        row_nums: usize,
    }

    impl<T> TestPageReader<T> {
        pub fn new(row_nums: usize) -> Self {
            Self {
                _phantom_data: PhantomData,
                row_nums,
            }
        }
    }

    impl PageReader for TestPageReader<f64> {
        fn process(&self) -> TskvResult<BoxStream<TskvResult<ArrayRef>>> {
            let mut builder = Float64Array::builder(self.row_nums);
            for i in 0..self.row_nums {
                if i % 2 == 0 {
                    builder.append_null();
                } else {
                    builder.append_value(i as f64);
                }
            }
            Ok(Box::pin(stream::once(async move {
                Ok(Arc::new(builder.finish()) as ArrayRef)
            })))
        }
    }

    impl PageReader for TestPageReader<i64> {
        fn process(&self) -> TskvResult<BoxStream<TskvResult<ArrayRef>>> {
            let mut builder = Int64Array::builder(self.row_nums);
            for i in 0..self.row_nums {
                if i % 2 == 0 {
                    builder.append_null();
                } else {
                    builder.append_value(i as i64);
                }
            }
            Ok(Box::pin(stream::once(async move {
                Ok(Arc::new(builder.finish()) as ArrayRef)
            })))
        }
    }

    impl PageReader for TestPageReader<u64> {
        fn process(&self) -> TskvResult<BoxStream<TskvResult<ArrayRef>>> {
            let mut builder = UInt64Array::builder(self.row_nums);
            for i in 0..self.row_nums {
                if i % 2 == 0 {
                    builder.append_null();
                } else {
                    builder.append_value(i as u64);
                }
            }
            Ok(Box::pin(stream::once(async move {
                Ok(Arc::new(builder.finish()) as ArrayRef)
            })))
        }
    }

    impl PageReader for TestPageReader<String> {
        fn process(&self) -> TskvResult<BoxStream<TskvResult<ArrayRef>>> {
            let mut builder = StringBuilder::new();
            for i in 0..self.row_nums {
                if i % 2 == 0 {
                    builder.append_null();
                } else {
                    builder.append_value(format!("str_{}", i));
                }
            }
            Ok(Box::pin(stream::once(async move {
                Ok(Arc::new(builder.finish()) as ArrayRef)
            })))
        }
    }

    impl PageReader for TestPageReader<bool> {
        fn process(&self) -> TskvResult<BoxStream<TskvResult<ArrayRef>>> {
            let mut builder = BooleanBuilder::new();
            for i in 0..self.row_nums {
                if i % 2 == 0 {
                    builder.append_null();
                } else {
                    builder.append_value(i % 3 == 0);
                }
            }
            Ok(Box::pin(stream::once(async move {
                Ok(Arc::new(builder.finish()) as ArrayRef)
            })))
        }
    }

    fn page(row_nums: usize, ct: ColumnType) -> Page {
        let col_schema = TableColumn::new_with_default("col".to_string(), ct.clone());

        let mut col = MutableColumn::empty_with_cap(col_schema, row_nums).unwrap();

        for i in 0..row_nums {
            let val = if i % 2 == 0 {
                None
            } else {
                match ct.to_physical_data_type() {
                    PhysicalDType::Boolean => Some(FieldVal::Boolean(true)),
                    PhysicalDType::Integer => Some(FieldVal::Integer(i as i64)),
                    PhysicalDType::String => Some(FieldVal::Bytes(MiniVec::from(
                        format!("str_{i}").as_bytes(),
                    ))),
                    PhysicalDType::Float => Some(FieldVal::Float(i as f64)),
                    PhysicalDType::Unsigned => Some(FieldVal::Unsigned(i as u64)),
                    _ => None,
                }
            };

            col.push(val).unwrap();
        }

        Page::col_to_page(&col).unwrap()
    }

    #[tokio::test]
    async fn test_time_col() {
        let page = page(11, ColumnType::Time(TimeUnit::Second));
        let array = data_buf_to_arrow_array(&page).unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<Timestamp(Second, None)>\n[\n  null,\n  1970-01-01T00:00:01,\n  null,\n  1970-01-01T00:00:03,\n  null,\n  1970-01-01T00:00:05,\n  null,\n  1970-01-01T00:00:07,\n  null,\n  1970-01-01T00:00:09,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_tag_col() {
        let page = page(11, ColumnType::Tag);
        let array = data_buf_to_arrow_array(&page).unwrap();
        assert_eq!(format!("{array:?}"), "StringArray\n[\n  null,\n  \"str_1\",\n  null,\n  \"str_3\",\n  null,\n  \"str_5\",\n  null,\n  \"str_7\",\n  null,\n  \"str_9\",\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_bool_col() {
        let page = page(11, ColumnType::Field(ValueType::Boolean));
        let array = data_buf_to_arrow_array(&page).unwrap();
        assert_eq!(format!("{array:?}"), "BooleanArray\n[\n  null,\n  true,\n  null,\n  true,\n  null,\n  true,\n  null,\n  true,\n  null,\n  true,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_float_col() {
        let page = page(11, ColumnType::Field(ValueType::Float));
        let array = data_buf_to_arrow_array(&page).unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<Float64>\n[\n  null,\n  1.0,\n  null,\n  3.0,\n  null,\n  5.0,\n  null,\n  7.0,\n  null,\n  9.0,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_int_col() {
        let page = page(11, ColumnType::Field(ValueType::Integer));
        let array = data_buf_to_arrow_array(&page).unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<Int64>\n[\n  null,\n  1,\n  null,\n  3,\n  null,\n  5,\n  null,\n  7,\n  null,\n  9,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_uint_col() {
        let page = page(11, ColumnType::Field(ValueType::Unsigned));
        let array = data_buf_to_arrow_array(&page).unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<UInt64>\n[\n  null,\n  1,\n  null,\n  3,\n  null,\n  5,\n  null,\n  7,\n  null,\n  9,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_string_col() {
        let page = page(11, ColumnType::Field(ValueType::String));
        let array = data_buf_to_arrow_array(&page).unwrap();
        assert_eq!(format!("{array:?}"), "StringArray\n[\n  null,\n  \"str_1\",\n  null,\n  \"str_3\",\n  null,\n  \"str_5\",\n  null,\n  \"str_7\",\n  null,\n  \"str_9\",\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_geom_col() {
        let page = page(
            11,
            ColumnType::Field(ValueType::Geometry(Geometry::new_with_srid(
                GeometryType::Point,
                0,
            ))),
        );
        let array = data_buf_to_arrow_array(&page).unwrap();
        assert_eq!(format!("{array:?}"), "StringArray\n[\n  null,\n  \"str_1\",\n  null,\n  \"str_3\",\n  null,\n  \"str_5\",\n  null,\n  \"str_7\",\n  null,\n  \"str_9\",\n  null,\n]");
    }

    #[test]
    fn test_empty_databuf() {
        let page = {
            let col_schema = TableColumn::new_with_default(
                "col".to_string(),
                ColumnType::Field(ValueType::Integer),
            );
            let mut col = MutableColumn::empty_with_cap(col_schema, 10).unwrap();
            for _ in 0..10 {
                col.push(None).unwrap();
            }
            Page::col_to_page(&col).unwrap()
        };
        let page_null_bits = BitSet::with_size(10);
        let array = data_buf_to_arrow_array(&page).unwrap();
        let array = updated_nullbuffer(array, NullBitset::Own(page_null_bits)).unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<Int64>\n[\n  null,\n  null,\n  null,\n  null,\n  null,\n  null,\n  null,\n  null,\n  null,\n  null,\n]");
    }
}
