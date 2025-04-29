pub mod stream;
pub use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
pub use datafusion::common::file_options::file_type::FileType;
pub use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
pub use datafusion::sql::sqlparser::ast::DataType as SqlDataType;
use datafusion::sql::sqlparser::ast::{ExactNumberInfo, TimezoneInfo};

use crate::errors::CommonSnafu;

pub fn arrow_data_type_to_sql_data_type(
    data_type: &DataType,
) -> Result<SqlDataType, crate::ModelError> {
    let res = match data_type {
        DataType::Boolean => SqlDataType::Boolean,
        DataType::Int8 => SqlDataType::TinyInt(None),
        DataType::Int16 => SqlDataType::SmallInt(None),
        DataType::Int32 => SqlDataType::Int(None),
        DataType::Int64 => SqlDataType::BigInt(None),
        DataType::UInt8 => SqlDataType::TinyIntUnsigned(None),
        DataType::UInt16 => SqlDataType::SmallIntUnsigned(None),
        DataType::UInt32 => SqlDataType::IntUnsigned(None),
        DataType::UInt64 => SqlDataType::BigIntUnsigned(None),
        DataType::Float16 => SqlDataType::Float(None),
        DataType::Float32 => SqlDataType::Float(None),
        DataType::Float64 => SqlDataType::Float64,
        DataType::Timestamp(_, _) => SqlDataType::Timestamp(None, TimezoneInfo::None),
        DataType::Date32 => SqlDataType::Date,
        DataType::Date64 => SqlDataType::Date,
        DataType::Time32(_) => SqlDataType::Time(Some(32), TimezoneInfo::None),
        DataType::Time64(_) => SqlDataType::Time(None, TimezoneInfo::None),
        DataType::Interval(_) => SqlDataType::Interval,
        DataType::Binary => SqlDataType::Varbinary(None),
        DataType::FixedSizeBinary(a) => SqlDataType::Binary(Some(*a as u64)),
        DataType::LargeBinary => SqlDataType::Blob(None),
        DataType::BinaryView => SqlDataType::Blob(None),
        DataType::Utf8 => SqlDataType::String(None),
        DataType::Decimal128(p, s) => {
            SqlDataType::Decimal(ExactNumberInfo::PrecisionAndScale(*p as u64, *s as u64))
        }
        DataType::Decimal256(p, s) => {
            SqlDataType::BigDecimal(ExactNumberInfo::PrecisionAndScale(*p as u64, *s as u64))
        }
        _ => {
            return Err(CommonSnafu {
                msg: format!("Not implement {} to sql type", data_type),
            }
            .build());
        }
    };
    Ok(res)
}
