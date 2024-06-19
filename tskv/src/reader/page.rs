use std::sync::Arc;

use arrow::array::ArrayData;
use arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer};
use arrow::datatypes::DataType;
use arrow_array::builder::StringBuilder;
use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use minivec::MiniVec;
use models::arrow::stream::BoxStream;
use models::predicate::domain::TimeRange;
use models::schema::tskv_table_schema::PhysicalCType;
use models::{PhysicalDType, SeriesId};
use snafu::{OptionExt, ResultExt};
use utils::bitset::NullBitset;

use super::column_group::ColumnGroupReaderMetrics;
use crate::error::{CommonSnafu, DecodeSnafu, UnsupportedDataTypeSnafu};
use crate::tsm::codec::{
    get_bool_codec, get_encoding, get_f64_codec, get_i64_codec, get_str_codec, get_ts_codec,
    get_u64_codec,
};
use crate::tsm::page::{Page, PageMeta, PageWriteSpec};
use crate::tsm::reader::TsmReader;
use crate::{TskvError, TskvResult};

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

async fn page_to_arrow_array_with_tomb(
    page: Page,
    reader: Arc<TsmReader>,
    series_id: SeriesId,
    time_page_meta: Arc<PageWriteSpec>,
    time_range: TimeRange,
) -> TskvResult<ArrayRef> {
    let tombstone = reader.tombstone();
    let null_bitset = if tombstone.overlaps(series_id, page.meta.column.id, &time_range) {
        let time_page = reader.read_page(&time_page_meta).await?;
        let column = time_page.to_column()?;
        let time_ranges =
            tombstone.get_overlapped_time_ranges(series_id, page.meta.column.id, &time_range);
        let mut null_bitset = page.null_bitset().to_bitset();
        for time_range in time_ranges {
            let start_index = column
                .data()
                .binary_search_for_i64_col(time_range.min_ts)
                .map_err(|e| TskvError::ColumnDataError { source: e })?
                .unwrap_or_else(|x| x);
            let end_index = column
                .data()
                .binary_search_for_i64_col(time_range.max_ts)
                .map_err(|e| TskvError::ColumnDataError { source: e })?
                .map(|x| x + 1)
                .unwrap_or_else(|x| x);
            null_bitset.clear_bits(start_index, end_index);
        }
        NullBitset::Own(null_bitset)
    } else {
        NullBitset::Ref(page.null_bitset())
    };

    let meta = page.meta();
    let data_buffer = page.data_buffer();
    data_buf_to_arrow_array(data_buffer, meta, null_bitset)
}

fn data_buf_to_arrow_array(
    data_buffer: &[u8],
    meta: &PageMeta,
    null_bitset: NullBitset,
) -> TskvResult<ArrayRef> {
    let data_type = meta.column.column_type.to_physical_type();
    let num_values = meta.num_values as usize;

    let null_bitset_slice = null_bitset.null_bitset_slice();
    let null_buffer = Buffer::from_vec(null_bitset_slice);
    let null_mutable_buffer = NullBuffer::new(BooleanBuffer::new(null_buffer, 0, num_values));

    let array: ArrayRef = match data_type {
        PhysicalCType::Tag | PhysicalCType::Field(PhysicalDType::String) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_str_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .context(DecodeSnafu)?;
            let mut iter_target = target.into_iter();
            let mut target = Vec::with_capacity(null_bitset.len());
            for i in 0..null_bitset.len() {
                if null_bitset.get(i) {
                    target.push(iter_target.next().context(CommonSnafu {
                        reason: "target is not enough".to_string(),
                    })?);
                } else {
                    target.push(MiniVec::new());
                }
            }

            let mut builder = StringBuilder::with_capacity(num_values, 128);

            for (i, v) in target.iter().enumerate() {
                if null_bitset.get(i) {
                    let data = String::from_utf8_lossy(v.as_ref());
                    builder.append_value(data)
                } else {
                    builder.append_null()
                }
            }

            Arc::new(builder.finish())
        }
        PhysicalCType::Time(_) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_ts_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .context(DecodeSnafu)?;

            let mut iter_target = target.into_iter();
            let mut target = Vec::with_capacity(null_bitset.len());
            for i in 0..null_bitset.len() {
                if null_bitset.get(i) {
                    target.push(iter_target.next().context(CommonSnafu {
                        reason: "target is not enough".to_string(),
                    })?);
                } else {
                    target.push(0);
                }
            }

            let data_mutable_buffer = MutableBuffer::from_vec(target);

            let nulls = Some(null_mutable_buffer);
            let builder = ArrayData::builder(DataType::Int64)
                .len(num_values)
                .add_buffer(data_mutable_buffer.into())
                .nulls(nulls);

            let array_data = unsafe { builder.build_unchecked() };
            Arc::new(PrimitiveArray::<Int64Type>::from(array_data))
        }
        PhysicalCType::Field(PhysicalDType::Float) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_f64_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .context(DecodeSnafu)?;

            let mut iter_target = target.into_iter();
            let mut target = Vec::with_capacity(null_bitset.len());
            for i in 0..null_bitset.len() {
                if null_bitset.get(i) {
                    target.push(iter_target.next().context(CommonSnafu {
                        reason: "target is not enough".to_string(),
                    })?);
                } else {
                    target.push(0.0);
                }
            }

            let data_mutable_buffer = MutableBuffer::from_vec(target);

            let nulls = Some(null_mutable_buffer);
            let builder = ArrayData::builder(DataType::Float64)
                .len(num_values)
                .add_buffer(data_mutable_buffer.into())
                .nulls(nulls);

            let array_data = unsafe { builder.build_unchecked() };
            Arc::new(PrimitiveArray::<Float64Type>::from(array_data))
        }
        PhysicalCType::Field(PhysicalDType::Integer) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_i64_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .context(DecodeSnafu)?;

            let mut iter_target = target.into_iter();
            let mut target = Vec::with_capacity(null_bitset.len());
            for i in 0..null_bitset.len() {
                if null_bitset.get(i) {
                    target.push(iter_target.next().context(CommonSnafu {
                        reason: "target is not enough".to_string(),
                    })?);
                } else {
                    target.push(0);
                }
            }

            let data_mutable_buffer = MutableBuffer::from_vec(target);

            let nulls = Some(null_mutable_buffer);
            let builder = ArrayData::builder(DataType::Int64)
                .len(num_values)
                .add_buffer(data_mutable_buffer.into())
                .nulls(nulls);

            let array_data = unsafe { builder.build_unchecked() };
            Arc::new(PrimitiveArray::<Int64Type>::from(array_data))
        }
        PhysicalCType::Field(PhysicalDType::Unsigned) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_u64_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .context(DecodeSnafu)?;

            let mut iter_target = target.into_iter();
            let mut target = Vec::with_capacity(null_bitset.len());
            for i in 0..null_bitset.len() {
                if null_bitset.get(i) {
                    target.push(iter_target.next().context(CommonSnafu {
                        reason: "target is not enough".to_string(),
                    })?);
                } else {
                    target.push(0);
                }
            }

            let data_mutable_buffer = MutableBuffer::from_vec(target);

            let nulls = Some(null_mutable_buffer);
            let builder = ArrayData::builder(DataType::UInt64)
                .len(num_values)
                .add_buffer(data_mutable_buffer.into())
                .nulls(nulls);

            let array_data = unsafe { builder.build_unchecked() };
            Arc::new(PrimitiveArray::<UInt64Type>::from(array_data))
        }
        PhysicalCType::Field(PhysicalDType::Boolean) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_bool_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .context(DecodeSnafu)?;

            let mut iter_target = target.into_iter();
            let mut target = Vec::with_capacity(null_bitset.len());
            for i in 0..null_bitset.len() {
                if null_bitset.get(i) {
                    target.push(iter_target.next().context(CommonSnafu {
                        reason: "target is not enough".to_string(),
                    })?);
                } else {
                    target.push(false);
                }
            }

            // TODO target 应为 bitset
            let data_mutable_buffer = BooleanBuffer::from(target);

            let array = BooleanArray::new(data_mutable_buffer, Some(null_mutable_buffer));
            Arc::new(array)
        }
        PhysicalCType::Field(PhysicalDType::Unknown) => {
            return Err(UnsupportedDataTypeSnafu {
                dt: "Unknown".to_string(),
            }
            .build())
        }
    };

    Ok(array)
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
    use utils::bitset::BitSet;

    use super::{NullBitset, PageReader};
    use crate::tsm::data_block::MutableColumn;
    use crate::tsm::page::{Page, PageMeta, PageStatistics};
    use crate::tsm::statistics::ValueStatistics;
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
        let meta = page.meta();
        let data_buffer = page.data_buffer();
        let page_null_bits = page.null_bitset();
        let array =
            super::data_buf_to_arrow_array(data_buffer, meta, NullBitset::Ref(page_null_bits))
                .unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<Int64>\n[\n  null,\n  1,\n  null,\n  3,\n  null,\n  5,\n  null,\n  7,\n  null,\n  9,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_tag_col() {
        let page = page(11, ColumnType::Tag);
        let meta = page.meta();
        let data_buffer = page.data_buffer();
        let page_null_bits = page.null_bitset();
        let array =
            super::data_buf_to_arrow_array(data_buffer, meta, NullBitset::Ref(page_null_bits))
                .unwrap();
        assert_eq!(format!("{array:?}"), "StringArray\n[\n  null,\n  \"str_1\",\n  null,\n  \"str_3\",\n  null,\n  \"str_5\",\n  null,\n  \"str_7\",\n  null,\n  \"str_9\",\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_bool_col() {
        let page = page(11, ColumnType::Field(ValueType::Boolean));
        let meta = page.meta();
        let data_buffer = page.data_buffer();
        let page_null_bits = page.null_bitset();
        let array =
            super::data_buf_to_arrow_array(data_buffer, meta, NullBitset::Ref(page_null_bits))
                .unwrap();
        assert_eq!(format!("{array:?}"), "BooleanArray\n[\n  null,\n  true,\n  null,\n  true,\n  null,\n  true,\n  null,\n  true,\n  null,\n  true,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_float_col() {
        let page = page(11, ColumnType::Field(ValueType::Float));
        let meta = page.meta();
        let data_buffer = page.data_buffer();
        let page_null_bits = page.null_bitset();
        let array =
            super::data_buf_to_arrow_array(data_buffer, meta, NullBitset::Ref(page_null_bits))
                .unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<Float64>\n[\n  null,\n  1.0,\n  null,\n  3.0,\n  null,\n  5.0,\n  null,\n  7.0,\n  null,\n  9.0,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_int_col() {
        let page = page(11, ColumnType::Field(ValueType::Integer));
        let meta = page.meta();
        let data_buffer = page.data_buffer();
        let page_null_bits = page.null_bitset();
        let array =
            super::data_buf_to_arrow_array(data_buffer, meta, NullBitset::Ref(page_null_bits))
                .unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<Int64>\n[\n  null,\n  1,\n  null,\n  3,\n  null,\n  5,\n  null,\n  7,\n  null,\n  9,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_uint_col() {
        let page = page(11, ColumnType::Field(ValueType::Unsigned));
        let meta = page.meta();
        let data_buffer = page.data_buffer();
        let page_null_bits = page.null_bitset();
        let array =
            super::data_buf_to_arrow_array(data_buffer, meta, NullBitset::Ref(page_null_bits))
                .unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<UInt64>\n[\n  null,\n  1,\n  null,\n  3,\n  null,\n  5,\n  null,\n  7,\n  null,\n  9,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_string_col() {
        let page = page(11, ColumnType::Field(ValueType::String));
        let meta = page.meta();
        let data_buffer = page.data_buffer();
        let page_null_bits = page.null_bitset();
        let array =
            super::data_buf_to_arrow_array(data_buffer, meta, NullBitset::Ref(page_null_bits))
                .unwrap();
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
        let meta = page.meta();
        let data_buffer = page.data_buffer();
        let page_null_bits = page.null_bitset();
        let array =
            super::data_buf_to_arrow_array(data_buffer, meta, NullBitset::Ref(page_null_bits))
                .unwrap();
        assert_eq!(format!("{array:?}"), "StringArray\n[\n  null,\n  \"str_1\",\n  null,\n  \"str_3\",\n  null,\n  \"str_5\",\n  null,\n  \"str_7\",\n  null,\n  \"str_9\",\n  null,\n]");
    }

    #[test]
    fn test_empty_databuf() {
        let data_buffer = vec![];
        let meta = PageMeta {
            column: TableColumn::new_with_default(
                "col".to_string(),
                ColumnType::Field(ValueType::Integer),
            ),
            num_values: 10,
            statistics: PageStatistics::I64(ValueStatistics::new(None, None, None, 10)),
        };
        let page_null_bits = BitSet::with_size(10);
        let array =
            super::data_buf_to_arrow_array(&data_buffer, &meta, NullBitset::Own(page_null_bits))
                .unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<Int64>\n[\n  null,\n  null,\n  null,\n  null,\n  null,\n  null,\n  null,\n  null,\n  null,\n  null,\n]");
    }
}
