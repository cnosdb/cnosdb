use arrow_schema::{Field, Schema};
use datafusion::arrow::array::{
    ArrayBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::error::ArrowError;

use crate::Error;

pub trait WriteArrow {
    fn write(self, builder: &mut Box<dyn ArrayBuilder>) -> Result<(), ArrowError>;
}

impl WriteArrow for Vec<Option<Vec<u8>>> {
    fn write(self, builder: &mut Box<dyn ArrayBuilder>) -> Result<(), ArrowError> {
        let builder = builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| {
                ArrowError::SchemaError(
                    "Cast failed for ListBuilder<StringBuilder> during nested data parsing"
                        .to_string(),
                )
            })?;

        for arrow in self {
            let val = if let Some(ref vec) = arrow {
                Some(
                    std::str::from_utf8(vec)
                        .map_err(|_| Error::EncodingError)
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?,
                )
            } else {
                None
            };

            builder.append_option(val);
        }

        Ok(())
    }
}

/// Create array builder from schema
pub fn build_arrow_array_builders(
    schema: &Schema,
    batch_size: usize,
) -> Result<Vec<Box<dyn ArrayBuilder>>, ArrowError> {
    schema
        .fields()
        .iter()
        .map(|f: &Field| build_arrow_array_builder(f.data_type(), batch_size))
        .collect::<Result<Vec<_>, ArrowError>>()
}

pub fn build_arrow_array_builder(
    data_type: &DataType,
    batch_size: usize,
) -> Result<Box<dyn ArrayBuilder>, ArrowError> {
    match data_type {
        // cnosdb used data type
        DataType::Utf8 => {
            let values_builder = StringBuilder::with_capacity(batch_size, 128);
            Ok(Box::new(values_builder))
        }
        DataType::UInt64 => {
            let values_builder = UInt64Builder::with_capacity(batch_size);
            Ok(Box::new(values_builder))
        }
        DataType::Int64 => {
            let values_builder = Int64Builder::with_capacity(batch_size);
            Ok(Box::new(values_builder))
        }
        DataType::Float64 => {
            let values_builder = Float64Builder::with_capacity(batch_size);
            Ok(Box::new(values_builder))
        }
        DataType::Boolean => {
            let values_builder = BooleanBuilder::with_capacity(batch_size);
            Ok(Box::new(values_builder))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let values_builder = TimestampSecondBuilder::with_capacity(batch_size);
            Ok(Box::new(values_builder))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values_builder = TimestampMillisecondBuilder::with_capacity(batch_size);
            Ok(Box::new(values_builder))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let values_builder = TimestampMicrosecondBuilder::with_capacity(batch_size);
            Ok(Box::new(values_builder))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let values_builder = TimestampNanosecondBuilder::with_capacity(batch_size);
            Ok(Box::new(values_builder))
        }
        // cnosdb not use
        // DataType::Int8 => {
        //     let values_builder = Int8Builder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Int16 => {
        //     let values_builder = Int16Builder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Int32 => {
        //     let values_builder = Int32Builder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::UInt8 => {
        //     let values_builder = UInt8Builder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::UInt16 => {
        //     let values_builder = UInt16Builder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::UInt32 => {
        //     let values_builder = UInt32Builder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Float32 => {
        //     let values_builder = Float32Builder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Date32 => {
        //     let values_builder = Date32Builder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Date64 => {
        //     let values_builder = Date64Builder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Time32(TimeUnit::Second) => {
        //     let values_builder = Time32SecondBuilder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Time32(TimeUnit::Millisecond) => {
        //     let values_builder = Time32MillisecondBuilder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Time64(TimeUnit::Microsecond) => {
        //     let values_builder = Time64MicrosecondBuilder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Time64(TimeUnit::Nanosecond) => {
        //     let values_builder = Time64NanosecondBuilder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Interval(IntervalUnit::YearMonth) => {
        //     let values_builder = IntervalYearMonthBuilder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Interval(IntervalUnit::DayTime) => {
        //     let values_builder = IntervalDayTimeBuilder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Interval(IntervalUnit::MonthDayNano) => {
        //     let values_builder = IntervalMonthDayNanoBuilder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Duration(TimeUnit::Second) => {
        //     let values_builder = DurationSecondBuilder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Duration(TimeUnit::Millisecond) => {
        //     let values_builder = DurationMillisecondBuilder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Duration(TimeUnit::Microsecond) => {
        //     let values_builder = DurationMicrosecondBuilder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        // DataType::Duration(TimeUnit::Nanosecond) => {
        //     let values_builder = DurationNanosecondBuilder::with_capacity(batch_size);
        //     Ok(Box::new(values_builder))
        // }
        _ => Err(ArrowError::NotYetImplemented(format!(
            "found data type: {}",
            data_type
        ))),
    }
}
