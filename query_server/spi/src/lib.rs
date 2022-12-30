use crate::service::protocol::QueryId;
use coordinator::errors::CoordinatorError;
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::sql::sqlparser::parser::ParserError;
use error_code::ErrorCoder;
use meta::error::MetaError;
use models::auth::AuthError;
use models::define_result;
use models::error_code::ErrorCode;
use snafu::Snafu;

pub mod query;
pub mod server;
pub mod service;

define_result!(QueryError);

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

    Datafusion {
        source: DataFusionError,
    },

    Arrow {
        source: ArrowError,
    },

    #[snafu(display("Semantic err: {}", err))]
    Semantic {
        err: String,
    },

    #[snafu(display("Insufficient privileges, expected [{}]", privilege))]
    InsufficientPrivileges {
        privilege: String,
    },

    #[snafu(display("This feature is not implemented: {}", err))]
    NotImplemented {
        err: String,
    },

    #[snafu(display("Failed to build QueryDispatcher. err: {}", err))]
    BuildQueryDispatcher {
        err: String,
    },

    #[snafu(display("Udf already exists, name:{}.", name))]
    FunctionExists {
        name: String,
    },

    #[snafu(display("Udf not exists, name:{}.", name))]
    FunctionNotExists {
        name: String,
    },

    #[snafu(display("{}", source))]
    Parser {
        source: ParserError,
    },

    #[snafu(display("Failed to do analyze. err: {}", err))]
    Analyzer {
        err: String,
    },

    #[snafu(display("Query not found: {:?}", query_id))]
    QueryNotFound {
        query_id: QueryId,
    },

    #[snafu(display("Concurrent query request limit exceeded"))]
    RequestLimit,

    #[snafu(display("Multi-statement not allow, found num:{}, sql:{}", num, sql))]
    MultiStatement {
        num: usize,
        sql: String,
    },

    #[snafu(display("The query has been canceled"))]
    Cancel,

    #[snafu(display("The query server has been closed"))]
    Closed,

    // #[snafu(display("Failed to do execute statement, err:{}", source))]
    // Query { source: QueryError },
    #[snafu(display("Auth error: {}", source))]
    Auth {
        source: AuthError,
    },

    #[snafu(display("invalid flatbuffers: {}", source))]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("{}", msg))]
    CommonError {
        msg: String,
    },

    #[snafu(display("Failed to write points flat buffer, err: {}", err))]
    ToPointsFlatBuffer {
        err: String,
    },

    #[snafu(display("Invalid array type, expected: {}, found: {}", expected, found))]
    InvalidArrayType {
        expected: String,
        found: String,
    },

    #[snafu(display("Column {} not found.", col))]
    ColumnNotFound {
        col: String,
    },

    #[snafu(display("Data type {} not support.", type_))]
    PointErrorDataTypeNotSupport {
        type_: String,
    },

    #[snafu(display("Column {} cannot be null.", col))]
    PointErrorNotNullConstraint {
        col: String,
    },

    #[snafu(display("Invalid parameter : {}", reason))]
    InvalidParam {
        reason: String,
    },

    #[snafu(display("file has no footer"))]
    NoFooter,

    #[snafu(display("read/write record file block: {}", reason))]
    RecordFileIo {
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
