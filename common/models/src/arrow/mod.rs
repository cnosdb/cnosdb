pub mod stream;
pub use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
pub use datafusion::common::file_options::file_type::FileType;
pub use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
pub use datafusion::sql::sqlparser::ast::DataType as SQLDataType;
use datafusion::sql::sqlparser::ast::{ExactNumberInfo, TimezoneInfo};

use crate::errors::CommonSnafu;

pub fn arrow_data_type_to_sql_data_type(
    data_type: &DataType,
) -> Result<SQLDataType, crate::ModelError> {
    let res = match data_type {
        DataType::Boolean => SQLDataType::Boolean,
        DataType::Int8 => SQLDataType::TinyInt(None),
        DataType::Int16 => SQLDataType::SmallInt(None),
        DataType::Int32 => SQLDataType::Int(None),
        DataType::Int64 => SQLDataType::BigInt(None),
        DataType::UInt8 => SQLDataType::TinyIntUnsigned(None),
        DataType::UInt16 => SQLDataType::SmallIntUnsigned(None),
        DataType::UInt32 => SQLDataType::IntUnsigned(None),
        DataType::UInt64 => SQLDataType::BigIntUnsigned(None),
        DataType::Float16 => SQLDataType::Float(None),
        DataType::Float32 => SQLDataType::Float(None),
        DataType::Float64 => SQLDataType::Float64,
        DataType::Timestamp(_, _) => SQLDataType::Timestamp(None, TimezoneInfo::None),
        DataType::Date32 => SQLDataType::Date,
        DataType::Date64 => SQLDataType::Date,
        DataType::Time32(_) => SQLDataType::Time(Some(32), TimezoneInfo::None),
        DataType::Time64(_) => SQLDataType::Time(None, TimezoneInfo::None),
        DataType::Interval(_) => SQLDataType::Interval,
        DataType::Binary => SQLDataType::Varbinary(None),
        DataType::FixedSizeBinary(a) => SQLDataType::Binary(Some(*a as u64)),
        DataType::LargeBinary => SQLDataType::Blob(None),
        DataType::Utf8 => SQLDataType::String(None),
        DataType::Decimal128(p, s) => {
            SQLDataType::Decimal(ExactNumberInfo::PrecisionAndScale(*p as u64, *s as u64))
        }
        DataType::Decimal256(p, s) => {
            SQLDataType::BigDecimal(ExactNumberInfo::PrecisionAndScale(*p as u64, *s as u64))
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
