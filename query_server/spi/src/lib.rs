use std::error;

use coordinator::errors::CoordinatorError;
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use datafusion::sql::sqlparser::parser::ParserError;
use error_code::ErrorCoder;
use meta::error::MetaError;
use models::auth::AuthError;
use models::codec::Encoding;
use models::define_result;
use models::error_code::ErrorCode;
use models::schema::TIME_FIELD_NAME;
use snafu::Snafu;

use crate::service::protocol::QueryId;

pub mod query;
pub mod server;
pub mod service;

define_result!(QueryError);

pub type GenericError = Box<dyn error::Error + Send + Sync>;

#[derive(Debug, Snafu, ErrorCoder)]
#[snafu(visibility(pub))]
#[error_code(mod_code = "01")]
pub enum QueryError {
    TsKv {
        source: tskv::Error,
    },

    Meta {
        source: MetaError,
    },

    Coordinator {
        source: CoordinatorError,
    },

    #[snafu(display(
        "Internal error: {}. This was likely caused by a bug in Cnosdb's \
    code and we would welcome that you file an bug report in our issue tracker",
        reason
    ))]
    #[error_code(code = 9998)]
    Internal {
        reason: String,
    },

    Models {
        source: models::Error,
    },

    #[error_code(code = 9999)]
    Unimplement {
        msg: String,
    },

    #[error_code(code = 1)]
    Datafusion {
        source: DataFusionError,
    },

    #[error_code(code = 2)]
    Arrow {
        source: ArrowError,
    },

    #[snafu(display("Insufficient privileges, expected [{}]", privilege))]
    #[error_code(code = 4)]
    InsufficientPrivileges {
        privilege: String,
    },

    #[snafu(display("This feature is not implemented: {}", err))]
    #[error_code(code = 5)]
    NotImplemented {
        err: String,
    },

    #[snafu(display("Failed to build QueryDispatcher. err: {}", err))]
    #[error_code(code = 6)]
    BuildQueryDispatcher {
        err: String,
    },

    #[snafu(display("Udf already exists, name:{}.", name))]
    #[error_code(code = 7)]
    FunctionExists {
        name: String,
    },

    #[snafu(display("Udf not exists, name:{}.", name))]
    #[error_code(code = 8)]
    FunctionNotExists {
        name: String,
    },

    #[snafu(display("{}", source))]
    #[error_code(code = 9)]
    Parser {
        source: ParserError,
    },

    #[snafu(display("Failed to do analyze. err: {}", err))]
    #[error_code(code = 10)]
    Analyzer {
        err: String,
    },

    #[snafu(display("Query not found: {:?}", query_id))]
    #[error_code(code = 11)]
    QueryNotFound {
        query_id: QueryId,
    },

    #[snafu(display("Concurrent query request limit exceeded"))]
    #[error_code(code = 12)]
    RequestLimit,

    #[snafu(display("Multi-statement not allow, found num:{}, sql:{}", num, sql))]
    #[error_code(code = 13)]
    MultiStatement {
        num: usize,
        sql: String,
    },

    #[snafu(display("The query has been canceled"))]
    #[error_code(code = 14)]
    Cancel,

    #[snafu(display("The query server has been closed"))]
    #[error_code(code = 15)]
    Closed,

    #[snafu(display("Auth error: {}", source))]
    #[error_code(code = 16)]
    Auth {
        source: AuthError,
    },

    #[snafu(display("Invalid flatbuffers: {}", source))]
    #[error_code(code = 17)]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display(
        "Common error: {}. This was likely caused by a bug in CnosDB's \
    code and we would welcome that you file an bug report in our issue tracker",
        msg
    ))]
    #[error_code(code = 18)]
    CommonError {
        msg: String,
    },

    #[snafu(display("Failed to write points flat buffer, err: {}", err))]
    #[error_code(code = 19)]
    ToPointsFlatBuffer {
        err: String,
    },

    #[snafu(display("Invalid array type, expected: {}, found: {}", expected, found))]
    #[error_code(code = 20)]
    InvalidArrayType {
        expected: String,
        found: String,
    },

    #[snafu(display("Column {} not found.", col))]
    #[error_code(code = 21)]
    ColumnNotFound {
        col: String,
    },

    #[snafu(display("Data type {} not support.", type_))]
    #[error_code(code = 22)]
    PointErrorDataTypeNotSupport {
        type_: String,
    },

    #[snafu(display("Column {} cannot be null.", col))]
    #[error_code(code = 23)]
    PointErrorNotNullConstraint {
        col: String,
    },

    #[snafu(display("Invalid parameter : {}", reason))]
    #[error_code(code = 24)]
    InvalidParam {
        reason: String,
    },

    #[snafu(display("File has no footer"))]
    #[error_code(code = 25)]
    NoFooter,

    #[snafu(display("Read/Write record file block: {}", reason))]
    #[error_code(code = 26)]
    RecordFileIo {
        reason: String,
    },

    #[snafu(display("Semantic error: {}", err))]
    #[error_code(code = 3)]
    Semantic {
        err: String,
    },

    #[snafu(display("Semantic error: Field or Tag have the same name {}", column))]
    #[error_code(code = 27)]
    SameColumnName {
        column: String,
    },

    #[snafu(display("Semantic error:  column {} already exists in table {}", column, table))]
    #[error_code(code = 28)]
    ColumnAlreadyExists {
        table: String,
        column: String,
    },

    #[snafu(display("Semantic error: Column {} not exists in table {}", column, table))]
    #[error_code(code = 29)]
    ColumnNotExists {
        table: String,
        column: String,
    },

    #[snafu(display("Semantic error: Can't drop tag column {}.", column))]
    #[error_code(code = 30)]
    DropTag {
        column: String,
    },

    #[snafu(display("Semantic error: Can't drop column {}.", TIME_FIELD_NAME))]
    #[error_code(code = 31)]
    DropTime,

    #[snafu(display("Semantic error: There must be at least one field column."))]
    #[error_code(code = 32)]
    AtLeastOneField,

    #[snafu(display("Semantic error: There must be at least one tag column. "))]
    #[error_code(code = 33)]
    AtLeastOneTag,

    #[snafu(display("Semantic error: Tag does not support compression"))]
    #[error_code(code = 34)]
    TagNotSupportCompression,

    #[snafu(display(
        "Semantic error: Column {} does not support modification",
        TIME_FIELD_NAME
    ))]
    #[error_code(code = 35)]
    TimeColumnAlter,

    #[snafu(display("Semantic error: DB {} conflict with table {}", db, table))]
    #[error_code(code = 36)]
    DBTableConflict {
        db: String,
        table: String,
    },

    #[snafu(display("Semantic error: OFFSET must be >= 0, '{}' was provided.", provide))]
    #[error_code(code = 37)]
    OffsetBtZero {
        provide: i64,
    },

    #[snafu(display("Semantic error: The OFFSET clause must be a constant of BIGINT type"))]
    #[error_code(code = 38)]
    OffsetConstant,

    #[snafu(display("Semantic error: Limit must be >= 0, '{}' was provided.", provide))]
    #[error_code(code = 39)]
    LimitBtZero {
        provide: i64,
    },

    #[snafu(display("Semantic error: The LIMIT clause must be a constant of BIGINT type"))]
    #[error_code(code = 40)]
    LimitConstant,

    #[snafu(display("Semantic error: Unexpected data type {} of {}", data_type, column))]
    #[error_code(code = 41)]
    DataType {
        data_type: String,
        column: String,
    },

    #[snafu(display(
        "Semantic error: Unsupported encoding type {:?} for {}",
        encoding_type,
        data_type
    ))]
    #[error_code(code = 42)]
    EncodingType {
        encoding_type: Encoding,
        data_type: String,
    },

    #[snafu(display("Semantic error: System roles are not allowed to be modified"))]
    #[error_code(code = 43)]
    SystemRoleModification,

    #[snafu(display(
        "Semantic error: Insert column '{}' does not exist in target table, expect {}",
        insert_col,
        fields
    ))]
    #[error_code(code = 44)]
    MissingColumn {
        insert_col: String,
        fields: String,
    },

    #[snafu(display("Semantic error: Insert columns and Source columns not match"))]
    #[error_code(code = 45)]
    MismatchColumns,

    #[snafu(display(
        "Semantic error: SHOW SERIES does not support where clause contains field {}",
        column
    ))]
    #[error_code(code = 46)]
    ShowSeriesWhereContainsField {
        column: String,
    },

    #[error_code(code = 47)]
    ObjectStore {
        source: object_store::Error,
    },

    #[error_code(code = 48)]
    #[snafu(display("Failed to close parquet writer, error: {}", source))]
    CloseParquetWriter {
        source: ParquetError,
    },

    #[error_code(code = 49)]
    #[snafu(display("Failed to serialize data to parquet bytes, error: {}", source))]
    SerializeParquet {
        source: ParquetError,
    },

    #[error_code(code = 50)]
    #[snafu(display("Failed to build parquet writer, error: {}", source))]
    BuildParquetArrowWriter {
        source: ParquetError,
    },

    #[error_code(code = 51)]
    #[snafu(display("Failed to serialize data to csv bytes, error: {}", source))]
    SerializeCsv {
        source: ArrowError,
    },

    #[error_code(code = 52)]
    #[snafu(display("Failed to serialize data to json bytes, error: {}", source))]
    SerializeJson {
        source: ArrowError,
    },

    #[error_code(code = 53)]
    #[snafu(display("{}", source))]
    StdIoError {
        source: std::io::Error,
    },

    #[error_code(code = 54)]
    #[snafu(display("{}", source))]
    SerdeJsonError {
        source: serde_json::Error,
    },

    #[error_code(code = 55)]
    #[snafu(display("{}", source))]
    SnappyError {
        source: snap::Error,
    },

    #[error_code(code = 56)]
    #[snafu(display("Invalid prom remote read request, error: {}", source))]
    InvalidRemoteReadReq {
        source: GenericError,
    },

    #[error_code(code = 57)]
    #[snafu(display("Invalid prom remote write requeset, error: {}", source))]
    InvalidRemoteWriteReq {
        source: GenericError,
    },

    #[snafu(display("Invalid TimeWindow parameter : {}", reason))]
    #[error_code(code = 58)]
    InvalidTimeWindowParam {
        reason: String,
    },

    #[snafu(display("Unsupported stream type: {}", stream_type))]
    #[error_code(code = 59)]
    UnsupportedStreamType {
        stream_type: String,
    },

    #[snafu(display("Stream source factory already exists: {}", stream_type))]
    #[error_code(code = 60)]
    StreamSourceFactoryAlreadyExists {
        stream_type: String,
    },

    #[snafu(display("Event time column not specified of table {}", name))]
    #[error_code(code = 61)]
    EventTimeColumnNotSpecified {
        name: String,
    },

    #[snafu(display("Missing option {} of table {}", option_name, table_name))]
    #[error_code(code = 62)]
    MissingTableOptions {
        option_name: String,
        table_name: String,
    },

    #[snafu(display("Invalid option {} of table {}: {}", option_name, table_name, reason))]
    #[error_code(code = 63)]
    InvalidTableOption {
        option_name: String,
        table_name: String,
        reason: String,
    },
}

impl From<ParserError> for QueryError {
    fn from(value: ParserError) -> Self {
        QueryError::Parser { source: value }
    }
}

impl From<DataFusionError> for QueryError {
    fn from(value: DataFusionError) -> Self {
        match value {
            DataFusionError::External(e) if e.downcast_ref::<QueryError>().is_some() => {
                *e.downcast::<QueryError>().unwrap()
            }

            DataFusionError::External(e) if e.downcast_ref::<MetaError>().is_some() => {
                QueryError::Meta {
                    source: *e.downcast::<MetaError>().unwrap(),
                }
            }

            DataFusionError::External(e) if e.downcast_ref::<tskv::Error>().is_some() => {
                QueryError::TsKv {
                    source: *e.downcast::<tskv::Error>().unwrap(),
                }
            }

            DataFusionError::External(e) if e.downcast_ref::<CoordinatorError>().is_some() => {
                QueryError::Coordinator {
                    source: *e.downcast::<CoordinatorError>().unwrap(),
                }
            }
            DataFusionError::ArrowError(e) => e.into(),
            v => QueryError::Datafusion { source: v },
        }
    }
}

impl From<MetaError> for QueryError {
    fn from(value: MetaError) -> Self {
        QueryError::Meta { source: value }
    }
}

impl From<CoordinatorError> for QueryError {
    fn from(value: CoordinatorError) -> Self {
        QueryError::Coordinator { source: value }
    }
}

impl From<tskv::Error> for QueryError {
    fn from(value: tskv::Error) -> Self {
        QueryError::TsKv { source: value }
    }
}

impl From<models::Error> for QueryError {
    fn from(value: models::Error) -> Self {
        QueryError::Models { source: value }
    }
}

impl From<ArrowError> for QueryError {
    fn from(value: ArrowError) -> Self {
        match value {
            ArrowError::ExternalError(e) if e.downcast_ref::<QueryError>().is_some() => {
                *e.downcast::<QueryError>().unwrap()
            }
            ArrowError::ExternalError(e) if e.downcast_ref::<MetaError>().is_some() => {
                QueryError::Meta {
                    source: *e.downcast::<MetaError>().unwrap(),
                }
            }
            ArrowError::ExternalError(e) if e.downcast_ref::<tskv::Error>().is_some() => {
                QueryError::TsKv {
                    source: *e.downcast::<tskv::Error>().unwrap(),
                }
            }
            ArrowError::ExternalError(e) if e.downcast_ref::<DataFusionError>().is_some() => {
                let df_error = *e.downcast::<DataFusionError>().unwrap();
                df_error.into()
            }
            ArrowError::ExternalError(e) if e.downcast_ref::<CoordinatorError>().is_some() => {
                QueryError::Coordinator {
                    source: *e.downcast::<CoordinatorError>().unwrap(),
                }
            }
            ArrowError::ExternalError(e) if e.downcast_ref::<ArrowError>().is_some() => {
                let arrow_error = *e.downcast::<ArrowError>().unwrap();
                arrow_error.into()
            }
            other => QueryError::Arrow { source: other },
        }
    }
}

impl From<object_store::Error> for QueryError {
    fn from(source: object_store::Error) -> Self {
        QueryError::ObjectStore { source }
    }
}

impl From<std::io::Error> for QueryError {
    fn from(source: std::io::Error) -> Self {
        QueryError::StdIoError { source }
    }
}

impl From<serde_json::Error> for QueryError {
    fn from(source: serde_json::Error) -> Self {
        QueryError::SerdeJsonError { source }
    }
}

impl From<snap::Error> for QueryError {
    fn from(source: snap::Error) -> Self {
        QueryError::SnappyError { source }
    }
}

impl QueryError {
    pub fn error_code(&self) -> &dyn ErrorCode {
        match self {
            QueryError::Meta { source } => source.error_code(),
            QueryError::TsKv { source } => source.error_code(),
            QueryError::Coordinator { source } => source.error_code(),
            _ => self,
        }
    }
}

#[test]
fn test_mod_code() {
    let e = QueryError::LimitConstant;
    assert!(e.error_code().code().starts_with("01"));
}
