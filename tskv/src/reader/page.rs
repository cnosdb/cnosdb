use std::sync::Arc;

use arrow::array::ArrayData;
use arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer};
use arrow::datatypes::DataType;
use arrow_array::builder::StringBuilder;
use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use models::arrow::stream::BoxStream;
use models::schema::PhysicalCType;
use models::PhysicalDType;

use super::column_group::ColumnGroupReaderMetrics;
use crate::tsm::codec::{
    get_bool_codec, get_encoding, get_f64_codec, get_i64_codec, get_str_codec, get_ts_codec,
    get_u64_codec,
};
use crate::tsm2::page::{Page, PageWriteSpec};
use crate::tsm2::reader::TSM2Reader;
use crate::{Error, Result};

pub type PageReaderRef = Arc<dyn PageReader>;
pub trait PageReader {
    fn process(&self) -> Result<BoxStream<Result<ArrayRef>>>;
}

#[derive(Clone)]
pub struct PrimitiveArrayReader {
    date_type: PhysicalDType,
    reader: Arc<TSM2Reader>,
    page_meta: PageWriteSpec,
    metrics: Arc<ExecutionPlanMetricsSet>,
}

impl PrimitiveArrayReader {
    pub fn new(
        date_type: PhysicalDType,
        reader: Arc<TSM2Reader>,
        page_meta: &PageWriteSpec,
        metrics: Arc<ExecutionPlanMetricsSet>,
    ) -> Self {
        Self {
            date_type,
            reader,
            // TODO
            page_meta: page_meta.clone(),
            metrics,
        }
    }
}

impl PageReader for PrimitiveArrayReader {
    fn process(&self) -> Result<BoxStream<Result<ArrayRef>>> {
        let metrics = ColumnGroupReaderMetrics::new(self.metrics.as_ref());

        Ok(Box::pin(futures::stream::once(read(
            self.reader.clone(),
            self.page_meta.clone(),
            metrics,
        ))))
    }
}

async fn read(
    reader: Arc<TSM2Reader>,
    page_meta: PageWriteSpec,
    metrics: ColumnGroupReaderMetrics,
) -> Result<ArrayRef> {
    let page = {
        let _timer = metrics.elapsed_page_scan_time().timer();
        metrics.page_read_count().add(1);
        metrics.page_read_bytes().add(page_meta.size());
        reader.read_page(&page_meta).await?
    };

    let _timer = metrics.elapsed_page_to_array_time().timer();
    page_to_arrow_array(page).await
}

async fn page_to_arrow_array(page: Page) -> Result<ArrayRef> {
    let data_type = page.meta().column.column_type.to_physical_type();
    let num_values = page.meta().num_values as usize;
    let data_buffer = page.data_buffer();

    let null_bitset = page.null_bitset();
    let null_bitset_slice = page.null_bitset_slice();
    let null_buffer = Buffer::from_vec(null_bitset_slice.to_vec());
    let null_mutable_buffer = NullBuffer::new(BooleanBuffer::new(null_buffer, 0, num_values));

    let array: ArrayRef = match data_type {
        PhysicalCType::Tag | PhysicalCType::Field(PhysicalDType::String) => {
            let encoding = get_encoding(&data_buffer[0..1]);
            let ts_codec = get_str_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .map_err(|e| Error::Decode { source: e })?;

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
            let encoding = get_encoding(&data_buffer[0..1]);
            let ts_codec = get_ts_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .map_err(|e| Error::Decode { source: e })?;

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
            let encoding = get_encoding(&data_buffer[0..1]);
            let ts_codec = get_f64_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .map_err(|e| Error::Decode { source: e })?;

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
            let encoding = get_encoding(&data_buffer[0..1]);
            let ts_codec = get_i64_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .map_err(|e| Error::Decode { source: e })?;

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
            let encoding = get_encoding(&data_buffer[0..1]);
            let ts_codec = get_u64_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .map_err(|e| Error::Decode { source: e })?;

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
            let encoding = get_encoding(&data_buffer[0..1]);
            let ts_codec = get_bool_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .map_err(|e| Error::Decode { source: e })?;
            // TODO target 应为 bitset
            let data_mutable_buffer = BooleanBuffer::from(target);

            let array = BooleanArray::new(data_mutable_buffer, Some(null_mutable_buffer));
            Arc::new(array)
        }
        PhysicalCType::Field(PhysicalDType::Unknown) => {
            return Err(Error::UnsupportedDataType {
                dt: "Unknown".to_string(),
            })
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
    use models::schema::{ColumnType, TableColumn};
    use models::{PhysicalDType, ValueType};

    use super::PageReader;
    use crate::tsm2::page::Page;
    use crate::tsm2::writer::Column;
    use crate::Result;

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
        fn process(&self) -> Result<BoxStream<Result<ArrayRef>>> {
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
        fn process(&self) -> Result<BoxStream<Result<ArrayRef>>> {
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
        fn process(&self) -> Result<BoxStream<Result<ArrayRef>>> {
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
        fn process(&self) -> Result<BoxStream<Result<ArrayRef>>> {
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
        fn process(&self) -> Result<BoxStream<Result<ArrayRef>>> {
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
        let mut col = Column::empty_with_cap(ct.clone(), row_nums).unwrap();

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

            col.push(val);
        }

        let col_schema = TableColumn::new_with_default("col".to_string(), ct);

        col.col_to_page(&col_schema).unwrap()
    }

    #[tokio::test]
    async fn test_time_col() {
        let page = page(11, ColumnType::Time(TimeUnit::Second));
        let array = super::page_to_arrow_array(page).await.unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<Int64>\n[\n  null,\n  1,\n  null,\n  3,\n  null,\n  5,\n  null,\n  7,\n  null,\n  9,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_tag_col() {
        let page = page(11, ColumnType::Tag);
        let array = super::page_to_arrow_array(page).await.unwrap();
        assert_eq!(format!("{array:?}"), "StringArray\n[\n  null,\n  \"str_1\",\n  null,\n  \"str_3\",\n  null,\n  \"str_5\",\n  null,\n  \"str_7\",\n  null,\n  \"str_9\",\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_bool_col() {
        let page = page(11, ColumnType::Field(ValueType::Boolean));
        let array = super::page_to_arrow_array(page).await.unwrap();
        assert_eq!(format!("{array:?}"), "BooleanArray\n[\n  null,\n  true,\n  null,\n  true,\n  null,\n  true,\n  null,\n  true,\n  null,\n  true,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_float_col() {
        let page = page(11, ColumnType::Field(ValueType::Float));
        let array = super::page_to_arrow_array(page).await.unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<Float64>\n[\n  null,\n  1.0,\n  null,\n  3.0,\n  null,\n  5.0,\n  null,\n  7.0,\n  null,\n  9.0,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_int_col() {
        let page = page(11, ColumnType::Field(ValueType::Integer));
        let array = super::page_to_arrow_array(page).await.unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<Int64>\n[\n  null,\n  1,\n  null,\n  3,\n  null,\n  5,\n  null,\n  7,\n  null,\n  9,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_uint_col() {
        let page = page(11, ColumnType::Field(ValueType::Unsigned));
        let array = super::page_to_arrow_array(page).await.unwrap();
        assert_eq!(format!("{array:?}"), "PrimitiveArray<UInt64>\n[\n  null,\n  1,\n  null,\n  3,\n  null,\n  5,\n  null,\n  7,\n  null,\n  9,\n  null,\n]");
    }

    #[tokio::test]
    async fn test_field_string_col() {
        let page = page(11, ColumnType::Field(ValueType::String));
        let array = super::page_to_arrow_array(page).await.unwrap();
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
        let array = super::page_to_arrow_array(page).await.unwrap();
        assert_eq!(format!("{array:?}"), "StringArray\n[\n  null,\n  \"str_1\",\n  null,\n  \"str_3\",\n  null,\n  \"str_5\",\n  null,\n  \"str_7\",\n  null,\n  \"str_9\",\n  null,\n]");
    }
}
