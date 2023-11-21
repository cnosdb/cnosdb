pub mod stream;
pub use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
pub use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
pub use datafusion::sql::sqlparser::ast::DataType as SQLDataType;
use datafusion::sql::sqlparser::ast::{ExactNumberInfo, TimezoneInfo};

pub fn arrow_data_type_to_sql_data_type(data_type: &DataType) -> Result<SQLDataType, crate::Error> {
    let res = match data_type {
        DataType::Boolean => SQLDataType::Boolean,
        DataType::Int8 => SQLDataType::TinyInt(None),
        DataType::Int16 => SQLDataType::SmallInt(None),
        DataType::Int32 => SQLDataType::Int(None),
        DataType::Int64 => SQLDataType::BigInt(None),
        DataType::UInt8 => SQLDataType::UnsignedTinyInt(None),
        DataType::UInt16 => SQLDataType::UnsignedSmallInt(None),
        DataType::UInt32 => SQLDataType::UnsignedInt(None),
        DataType::UInt64 => SQLDataType::UnsignedBigInt(None),
        DataType::Float16 => SQLDataType::Float(None),
        DataType::Float32 => SQLDataType::Float(None),
        DataType::Float64 => SQLDataType::Double,
        DataType::Timestamp(_, _) => SQLDataType::Timestamp(None, TimezoneInfo::None),
        DataType::Date32 => SQLDataType::Date,
        DataType::Date64 => SQLDataType::Date,
        DataType::Time32(_) => SQLDataType::Time(Some(32), TimezoneInfo::None),
        DataType::Time64(_) => SQLDataType::Time(None, TimezoneInfo::None),
        DataType::Interval(_) => SQLDataType::Interval,
        DataType::Binary => SQLDataType::Varbinary(None),
        DataType::FixedSizeBinary(a) => SQLDataType::Binary(Some(*a as u64)),
        DataType::LargeBinary => SQLDataType::Blob(None),
        DataType::Utf8 => SQLDataType::String,
        DataType::Decimal128(p, s) => {
            SQLDataType::Decimal(ExactNumberInfo::PrecisionAndScale(*p as u64, *s as u64))
        }
        DataType::Decimal256(p, s) => {
            SQLDataType::BigDecimal(ExactNumberInfo::PrecisionAndScale(*p as u64, *s as u64))
        }
        _ => {
            return Err(crate::Error::Common {
                msg: format!("Not implement {} to sql type", data_type),
            });
        }
    };
    Ok(res)
}
