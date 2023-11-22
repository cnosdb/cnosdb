use std::sync::Arc;

use arrow::array::ArrayData;
use arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer};
use arrow::datatypes::DataType;
use arrow_array::builder::StringBuilder;
use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray};
use models::arrow::stream::BoxStream;
use models::PhysicalDType;

use crate::tsm::codec::{
    get_bool_codec, get_encoding, get_f64_codec, get_i64_codec, get_str_codec, get_u64_codec,
};
use crate::tsm2::page::PageWriteSpec;
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
}

impl PrimitiveArrayReader {
    pub fn new(
        date_type: PhysicalDType,
        reader: Arc<TSM2Reader>,
        page_meta: &PageWriteSpec,
    ) -> Self {
        Self {
            date_type,
            reader,
            // TODO
            page_meta: page_meta.clone(),
        }
    }
}

impl PageReader for PrimitiveArrayReader {
    fn process(&self) -> Result<BoxStream<Result<ArrayRef>>> {
        Ok(Box::pin(futures::stream::once(read(
            self.date_type,
            self.reader.clone(),
            self.page_meta.clone(),
        ))))
    }
}

async fn read(
    date_type: PhysicalDType,
    reader: Arc<TSM2Reader>,
    page_meta: PageWriteSpec,
) -> Result<ArrayRef> {
    let page = reader.read_page(&page_meta).await?;

    let num_values = page.meta().num_values as usize;
    let data_buffer = page.data_buffer();

    let null_bitset = page.null_bitset();
    let null_bitset_slice = page.null_bitset_slice();
    let null_buffer = Buffer::from_vec(null_bitset_slice.to_vec());
    let null_mutable_buffer = NullBuffer::new(BooleanBuffer::new(null_buffer, 0, num_values));

    let array: ArrayRef = match date_type {
        PhysicalDType::Integer => {
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
        PhysicalDType::Unsigned => {
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
        PhysicalDType::Float => {
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
        PhysicalDType::Boolean => {
            let encoding = get_encoding(&data_buffer[0..1]);
            let ts_codec = get_bool_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .map_err(|e| Error::Decode { source: e })?;

            let data_mutable_buffer = BooleanBuffer::from(target);

            let array = BooleanArray::new(data_mutable_buffer, Some(null_mutable_buffer));
            Arc::new(array)
        }
        PhysicalDType::String => {
            let encoding = get_encoding(&data_buffer[0..1]);
            let ts_codec = get_str_codec(encoding);
            let mut target = Vec::new();
            ts_codec
                .decode(data_buffer, &mut target)
                .map_err(|e| Error::Decode { source: e })?;

            let mut builder = StringBuilder::with_capacity(num_values, 128);

            for (i, v) in target.iter().enumerate() {
                if null_bitset.get(i) {
                    builder.append_value(String::from_utf8_lossy(v.as_slice()))
                } else {
                    builder.append_null()
                }
            }

            Arc::new(builder.finish())
        }
        PhysicalDType::Unknown => {
            return Err(Error::UnsupportedDataType {
                dt: "Unknown".to_string(),
            })
        }
    };
    Ok(array)
}
