use std::sync::Arc;

use arrow::array::ArrayData;
use arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer};
use arrow_array::builder::StringBuilder;
use arrow_array::types::{
    Float64Type, Int64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt64Type,
};
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray};
use arrow_schema::{DataType, TimeUnit};
use models::column_data::{ColumnData, PrimaryColumnData};
use models::errors::InternalSnafu;
use models::field_value::FieldVal;
use models::schema::tskv_table_schema::{ColumnType, PhysicalCType, TableColumn};
use models::{ModelResult, PhysicalDType, ValueType};
use snafu::{OptionExt, ResultExt};
use utils::bitset::{BitSet, NullBitset};

use crate::error::{DecodeSnafu, TsmPageSnafu, UnsupportedDataTypeSnafu};
use crate::tsm::codec::{
    get_bool_codec, get_encoding, get_f64_codec, get_i64_codec, get_str_codec, get_u64_codec,
};
use crate::tsm::page::PageMeta;
use crate::{TskvError, TskvResult};

#[derive(Debug, Clone, PartialEq)]
pub struct MutableColumn {
    column_desc: TableColumn,
    column_data: ColumnData,
}

impl MutableColumn {
    pub fn empty(column_desc: TableColumn) -> TskvResult<MutableColumn> {
        let column_type = column_desc.column_type.to_physical_data_type();
        let data =
            ColumnData::new(column_type).map_err(|e| TskvError::ColumnDataError { source: e })?;
        let column = Self {
            column_desc,
            column_data: data,
        };
        Ok(column)
    }

    pub fn empty_with_cap(column_desc: TableColumn, cap: usize) -> TskvResult<MutableColumn> {
        let column_type = column_desc.column_type.to_physical_data_type();
        let data = ColumnData::with_cap(column_type, cap)
            .map_err(|e| TskvError::ColumnDataError { source: e })?;
        let column = Self {
            column_desc,
            column_data: data,
        };
        Ok(column)
    }

    pub fn chunk(&self, start: usize, end: usize) -> TskvResult<MutableColumn> {
        let column = self
            .column_data
            .chunk(start, end)
            .map_err(|e| TskvError::ColumnDataError { source: e })?;
        Ok(MutableColumn {
            column_desc: self.column_desc.clone(),
            column_data: column,
        })
    }

    pub fn valid(&self) -> &BitSet {
        &self.column_data.valid
    }

    pub fn mut_valid(&mut self) -> &mut BitSet {
        &mut self.column_data.valid
    }

    pub fn data(&self) -> &PrimaryColumnData {
        &self.column_data.primary_data
    }

    pub fn into_data(self) -> ColumnData {
        self.column_data
    }

    pub fn column_desc(&self) -> &TableColumn {
        &self.column_desc
    }

    pub fn push(&mut self, value: Option<FieldVal>) -> TskvResult<()> {
        self.column_data
            .push(value)
            .map_err(|e| TskvError::ColumnDataError { source: e })
    }

    pub fn data_buf_to_column(
        data_buffer: &[u8],
        meta: &PageMeta,
        bitset: &NullBitset,
    ) -> TskvResult<MutableColumn> {
        let col_type = meta.column.column_type.to_physical_type();
        let mut col = MutableColumn::empty_with_cap(meta.column.clone(), meta.num_values as usize)?;
        match col_type {
            PhysicalCType::Time(_) | PhysicalCType::Field(PhysicalDType::Integer) => {
                let encoding = get_encoding(data_buffer);
                let ts_codec = get_i64_codec(encoding);
                let mut target = Vec::new();
                ts_codec
                    .decode(data_buffer, &mut target)
                    .context(DecodeSnafu)?;

                let mut target = target.into_iter();
                for i in 0..bitset.len() {
                    if bitset.get(i) {
                        col.push(Some(FieldVal::Integer(target.next().context(
                            TsmPageSnafu {
                                reason: "data buffer not enough".to_string(),
                            },
                        )?)))?;
                    } else {
                        col.push(None)?;
                    }
                }
            }
            PhysicalCType::Field(PhysicalDType::Float) => {
                let encoding = get_encoding(data_buffer);
                let ts_codec = get_f64_codec(encoding);
                let mut target = Vec::new();
                ts_codec
                    .decode(data_buffer, &mut target)
                    .context(DecodeSnafu)?;

                let mut target = target.into_iter();
                for i in 0..bitset.len() {
                    if bitset.get(i) {
                        col.push(Some(FieldVal::Float(target.next().context(
                            TsmPageSnafu {
                                reason: "data buffer not enough".to_string(),
                            },
                        )?)))?;
                    } else {
                        col.push(None)?;
                    }
                }
            }
            PhysicalCType::Field(PhysicalDType::Unsigned) => {
                let encoding = get_encoding(data_buffer);
                let ts_codec = get_u64_codec(encoding);
                let mut target = Vec::new();
                ts_codec
                    .decode(data_buffer, &mut target)
                    .context(DecodeSnafu)?;
                let mut target = target.into_iter();
                for i in 0..bitset.len() {
                    if bitset.get(i) {
                        col.push(Some(FieldVal::Unsigned(target.next().context(
                            TsmPageSnafu {
                                reason: "data buffer not enough".to_string(),
                            },
                        )?)))?;
                    } else {
                        col.push(None)?;
                    }
                }
            }
            PhysicalCType::Field(PhysicalDType::Boolean) => {
                let encoding = get_encoding(data_buffer);
                let ts_codec = get_bool_codec(encoding);
                let mut target = Vec::new();
                ts_codec
                    .decode(data_buffer, &mut target)
                    .context(DecodeSnafu)?;

                let mut target = target.into_iter();
                for i in 0..bitset.len() {
                    if bitset.get(i) {
                        col.push(Some(FieldVal::Boolean(target.next().context(
                            TsmPageSnafu {
                                reason: "data buffer not enough".to_string(),
                            },
                        )?)))?;
                    } else {
                        col.push(None)?;
                    }
                }
            }
            PhysicalCType::Field(PhysicalDType::String) | PhysicalCType::Tag => {
                let encoding = get_encoding(data_buffer);
                let ts_codec = get_str_codec(encoding);
                let mut target = Vec::new();
                ts_codec
                    .decode(data_buffer, &mut target)
                    .context(DecodeSnafu)?;

                let mut target = target.into_iter();
                for i in 0..bitset.len() {
                    if bitset.get(i) {
                        col.push(Some(FieldVal::Bytes(target.next().context(
                            TsmPageSnafu {
                                reason: "data buffer not enough".to_string(),
                            },
                        )?)))?;
                    } else {
                        col.push(None)?;
                    }
                }
            }
            PhysicalCType::Field(PhysicalDType::Unknown) => {
                return Err(UnsupportedDataTypeSnafu {
                    dt: "unknown".to_string(),
                }
                .build());
            }
        }
        Ok(col)
    }

    pub fn to_arrow_array(self, null_bitset: Option<NullBitset>) -> ModelResult<ArrayRef> {
        let num_values = self.column_data.primary_data.len();
        let null_bitset = if let Some(null_bitset) = null_bitset {
            null_bitset
        } else {
            NullBitset::Own(self.column_data.valid)
        };
        let null_buffer = Buffer::from_vec(null_bitset.null_bitset_slice());
        let null_mutable_buffer = NullBuffer::new(BooleanBuffer::new(null_buffer, 0, num_values));

        let array: ArrayRef = match self.column_data.primary_data {
            PrimaryColumnData::F64(value, _, _) => {
                let data_mutable_buffer = MutableBuffer::from_vec(value);
                let nulls = Some(null_mutable_buffer);
                let builder = ArrayData::builder(DataType::Float64)
                    .len(num_values)
                    .add_buffer(data_mutable_buffer.into())
                    .nulls(nulls);

                let array_data = unsafe { builder.build_unchecked() };
                Arc::new(PrimitiveArray::<Float64Type>::from(array_data))
            }
            PrimaryColumnData::I64(value, _, _) => {
                let data_mutable_buffer = MutableBuffer::from_vec(value);
                let nulls = Some(null_mutable_buffer);
                let array: ArrayRef = match self.column_desc.column_type {
                    ColumnType::Time(TimeUnit::Second) => {
                        let builder =
                            ArrayData::builder(DataType::Timestamp(TimeUnit::Second, None))
                                .len(num_values)
                                .add_buffer(data_mutable_buffer.into())
                                .nulls(nulls);
                        let array_data = unsafe { builder.build_unchecked() };
                        Arc::new(PrimitiveArray::<TimestampSecondType>::from(array_data))
                    }
                    ColumnType::Time(TimeUnit::Millisecond) => {
                        let builder =
                            ArrayData::builder(DataType::Timestamp(TimeUnit::Millisecond, None))
                                .len(num_values)
                                .add_buffer(data_mutable_buffer.into())
                                .nulls(nulls);
                        let array_data = unsafe { builder.build_unchecked() };
                        Arc::new(PrimitiveArray::<TimestampMillisecondType>::from(array_data))
                    }
                    ColumnType::Time(TimeUnit::Microsecond) => {
                        let builder =
                            ArrayData::builder(DataType::Timestamp(TimeUnit::Microsecond, None))
                                .len(num_values)
                                .add_buffer(data_mutable_buffer.into())
                                .nulls(nulls);
                        let array_data = unsafe { builder.build_unchecked() };
                        Arc::new(PrimitiveArray::<TimestampMicrosecondType>::from(array_data))
                    }
                    ColumnType::Time(TimeUnit::Nanosecond) => {
                        let builder =
                            ArrayData::builder(DataType::Timestamp(TimeUnit::Nanosecond, None))
                                .len(num_values)
                                .add_buffer(data_mutable_buffer.into())
                                .nulls(nulls);
                        let array_data = unsafe { builder.build_unchecked() };
                        Arc::new(PrimitiveArray::<TimestampNanosecondType>::from(array_data))
                    }
                    ColumnType::Field(ValueType::Integer) => {
                        let builder = ArrayData::builder(DataType::Int64)
                            .len(num_values)
                            .add_buffer(data_mutable_buffer.into())
                            .nulls(nulls);
                        let array_data = unsafe { builder.build_unchecked() };
                        Arc::new(PrimitiveArray::<Int64Type>::from(array_data))
                    }
                    _ => {
                        return Err(InternalSnafu {
                            err: format!("expect column type biginteger or timestamp, but got column type: {:?}", self.column_desc.column_type),
                        }.build());
                    }
                };
                array
            }
            PrimaryColumnData::U64(value, _, _) => {
                let data_mutable_buffer = MutableBuffer::from_vec(value);
                let nulls = Some(null_mutable_buffer);
                let builder = ArrayData::builder(DataType::UInt64)
                    .len(num_values)
                    .add_buffer(data_mutable_buffer.into())
                    .nulls(nulls);

                let array_data = unsafe { builder.build_unchecked() };
                Arc::new(PrimitiveArray::<UInt64Type>::from(array_data))
            }
            PrimaryColumnData::String(value, _, _) => {
                let mut builder = StringBuilder::with_capacity(num_values, 128);
                for i in 0..null_bitset.len() {
                    if null_bitset.get(i) {
                        builder.append_value(value.get(i).ok_or_else(|| {
                            InternalSnafu {
                                err: "value is not enough".to_string(),
                            }
                            .build()
                        })?);
                    } else {
                        builder.append_null()
                    }
                }
                Arc::new(builder.finish())
            }
            PrimaryColumnData::Bool(value, _, _) => {
                let data_mutable_buffer = BooleanBuffer::from(value);
                let array = BooleanArray::new(data_mutable_buffer, Some(null_mutable_buffer));
                Arc::new(array)
            }
        };

        Ok(array)
    }
}
