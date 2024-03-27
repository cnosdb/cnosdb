use std::path::PathBuf;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use error_code::{ErrorCode, ErrorCoder};
use http_protocol::response::ErrorResponse;
use meta::error::MetaError;
use models::meta_data::VnodeId;
use protos::PointsError;
use snafu::Snafu;
use tonic::{Code, Status};

use crate::file_system::error::FileSystemError;
use crate::index::IndexError;
use crate::record_file;
use crate::schema::error::SchemaError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug, ErrorCoder)]
#[snafu(visibility(pub))]
#[error_code(mod_code = "02")]
pub enum Error {
    #[snafu(display("Invalid http error response: {}", error))]
    ErrorResponse {
        error: ErrorResponse,
    },

    Network {
        source: Status,
    },

    Meta {
        source: MetaError,
    },

    OutOfSpec {
        reason: String, // When the tsm file is known to be out of spec.
    },

    #[error_code(code = 9999)]
    Unimplemented {
        msg: String,
    },

    #[snafu(display("Invalid flatbuffers: {}", source))]
    #[error_code(code = 1)]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("Fields can't be empty"))]
    #[error_code(code = 2)]
    FieldsIsEmpty,

    #[snafu(display("{}", reason))]
    #[error_code(code = 3)]
    CommonError {
        reason: String,
    },

    #[snafu(display("DataSchemaError: {}", source))]
    #[error_code(code = 4)]
    Schema {
        source: SchemaError,
    },

    #[snafu(display("Memory Exhausted Retry Later"))]
    #[error_code(code = 5)]
    MemoryExhausted,

    #[error_code(code = 6)]
    Arrow {
        source: ArrowError,
    },

    #[snafu(display("read tsm block file error: {}", reason))]
    #[error_code(code = 7)]
    ReadTsm {
        reason: String,
        // source: ReadTsmError,
    },

    #[snafu(display("write tsm block file error: {}", reason))]
    #[error_code(code = 9)]
    WriteTsm {
        reason: String,
        // source: WriteTsmError,
    },

    #[snafu(display("Unsupported datatype {}", dt))]
    #[error_code(code = 10)]
    UnsupportedDataType {
        dt: String,
    },

    #[snafu(display("Mismatched schema {}", msg))]
    #[error_code(code = 11)]
    MismatchedSchema {
        msg: String,
    },

    #[error_code(code = 12)]
    DatafusionError {
        source: DataFusionError,
    },

    #[snafu(display("ColumnGroupError: {}", reason))]
    #[error_code(code = 13)]
    TsmColumnGroupError {
        reason: String,
    },

    #[snafu(display("TsmPageError: {}", reason))]
    #[error_code(code = 14)]
    TsmPageError {
        reason: String,
    },

    #[snafu(display("DataBlockError: {}", reason))]
    #[error_code(code = 15)]
    DataBlockError {
        reason: String,
    },

    #[snafu(display("TagError: {}", reason))]
    #[error_code(code = 16)]
    TagError {
        reason: String,
    },

    #[snafu(display("FileSystemError: {}", source))]
    #[error_code(code = 17)]
    FileSystemError {
        source: FileSystemError,
    },

    // Internal Error
    #[snafu(display("{}", source))]
    IO {
        source: std::io::Error,
    },

    #[snafu(display("Unable to open file '{}': {}", path.display(), source))]
    OpenFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to create file '{}': {}", path.display(), source))]
    CreateFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error with read file '{}': {}", path.display(), source))]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to write file '{}': {}", path.display(), source))]
    WriteFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to sync file: {}", source))]
    SyncFile {
        source: std::io::Error,
    },

    #[snafu(display("Unable to delete file '{}': {}", path.display(), source))]
    DeleteFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("File {} has wrong name format: {}", file_name, message))]
    InvalidFileName {
        file_name: String,
        message: String,
    },

    #[snafu(display("File '{}' has wrong format: {}", path.display(), message))]
    InvalidFileFormat {
        path: PathBuf,
        message: String,
    },

    /// Failed to send someting to a channel
    #[snafu(display("{source}"))]
    ChannelSend {
        source: ChannelSendError,
    },

    /// Failed to receive something from a channel
    #[snafu(display("{source}"))]
    ChannelReceive {
        source: ChannelReceiveError,
    },

    /// WAL file is truncated, it's because CnosDB didn't shutdown properly.
    ///
    /// This error is handled by WAL module:
    /// just stop the current WAL file reading, go to the next WAL file.
    #[snafu(display("Internal handled: WAL truncated"))]
    WalTruncated,

    #[snafu(display("read/write record file block: {}", reason))]
    RecordFileIo {
        reason: String,
    },

    #[snafu(display("Unexpected eof"))]
    Eof,

    #[snafu(display("Failed to encode record file block: {}", source))]
    RecordFileEncode {
        source: bincode::Error,
    },

    #[snafu(display("Faield to decode record file block: {}", source))]
    RecordFileDecode {
        source: bincode::Error,
    },

    /// This error is handled by the caller of record_file::Reader::read_record()
    #[snafu(display("Record data at [{pos}..{}] is invalid", pos + * len as u64))]
    RecordFileInvalidDataSize {
        pos: u64,
        len: u32,
    },

    /// This error is handled by the caller of record_file::Reader::read_record()
    #[snafu(display(
        "Record CRC not match at [{}..{}], expected: {crc}, calculated: {crc_calculated}",
        record.pos,
        record.pos + record.data.len() as u64 + record_file::RECORD_HEADER_LEN as u64,
    ))]
    RecordFileHashCheckFailed {
        crc: u32,
        crc_calculated: u32,
        record: record_file::Record,
    },

    #[snafu(display("Failed to do encode: {}", source))]
    Encode {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Faield to do decode: {}", source))]
    Decode {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Index: {}", source))]
    IndexErr {
        source: crate::index::IndexError,
    },

    #[snafu(display("error apply edits to summary"))]
    ErrApplyEdit,

    #[snafu(display("character set error"))]
    ErrCharacterSet,

    #[snafu(display("Invalid parameter : {}", reason))]
    InvalidParam {
        reason: String,
    },

    #[snafu(display("file has no footer"))]
    NoFooter,

    #[snafu(display("unable to transform: {}", reason))]
    Transform {
        reason: String,
    },

    #[snafu(display("invalid points : '{}'", source))]
    Points {
        source: PointsError,
    },

    #[snafu(display("non-UTF-8 string '{message}': {source}"))]
    InvalidUtf8 {
        message: String,
        source: std::str::Utf8Error,
    },

    #[snafu(display("Vnode {vnode_id} not found"))]
    VnodeNotFound {
        vnode_id: VnodeId,
    },

    #[snafu(display("Tenant {tenant} not found"))]
    #[error_code(code = 50)]
    TenantNotFound {
        tenant: String,
    },

    #[snafu(display("Table {table} not found"))]
    #[error_code(code = 51)]
    TableNotFound {
        table: String,
    },

    #[snafu(display("Create TskvDatabase Failed, {msg}"))]
    CreateDatabase {
        msg: String,
    },
    #[error_code(code = 52)]
    #[snafu(display("Column {} not found", column))]
    ColumnNotFound {
        column: String,
    },

    #[snafu(display("Table name can't be empty"))]
    #[error_code(code = 53)]
    InvalidPointTable,

    #[error_code(code = 54)]
    #[snafu(display("ColumnId {} not found", column))]
    ColumnIdNotFound {
        column: String,
    },

    #[snafu(display("Columns of FlatBufferTable is missing"))]
    FlatBufColumnsMiss,

    // Internal Error
    #[snafu(display("{}", source))]
    Serialize {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // Internal Error
    #[snafu(display("{}", source))]
    Deserialize {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl From<PointsError> for Error {
    fn from(value: PointsError) -> Self {
        Error::Points { source: value }
    }
}

impl From<SchemaError> for Error {
    fn from(value: SchemaError) -> Self {
        match value {
            SchemaError::Meta { source } => Self::Meta { source },
            other => Error::Schema { source: other },
        }
    }
}

impl From<IndexError> for Error {
    fn from(value: IndexError) -> Self {
        Error::IndexErr { source: value }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::IO { source: value }
    }
}

impl From<MetaError> for Error {
    fn from(source: MetaError) -> Self {
        Error::Meta { source }
    }
}

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        Error::Arrow { source }
    }
}

impl From<DataFusionError> for Error {
    fn from(source: DataFusionError) -> Self {
        match source {
            DataFusionError::External(e) if e.downcast_ref::<MetaError>().is_some() => Self::Meta {
                source: *e.downcast::<MetaError>().unwrap(),
            },

            DataFusionError::External(e) if e.downcast_ref::<Self>().is_some() => {
                match *e.downcast::<Self>().unwrap() {
                    Self::Arrow { source } => Self::from(source),
                    Self::DatafusionError { source } => Self::from(source),
                    e => e,
                }
            }

            DataFusionError::External(e) if e.downcast_ref::<ArrowError>().is_some() => {
                let arrow_error = *e.downcast::<ArrowError>().unwrap();
                arrow_error.into()
            }

            DataFusionError::External(e) if e.downcast_ref::<DataFusionError>().is_some() => {
                let datafusion_error = *e.downcast::<DataFusionError>().unwrap();
                Self::from(datafusion_error)
            }

            DataFusionError::ArrowError(e) => e.into(),
            v => Self::DatafusionError { source: v },
        }
    }
}

impl Error {
    pub fn error_code(&self) -> &dyn ErrorCode {
        match self {
            Error::Meta { source } => source.error_code(),
            Error::ErrorResponse { error } => error,
            _ => self,
        }
    }

    pub fn vnode_broken_code(code: &str) -> bool {
        let e = Self::ReadTsm {
            // source: ReadTsmError::CrcCheck,
            reason: "crc check failed".to_string(),
        };

        e.code() == code
    }

    pub fn is_file_not_found_error(&self) -> bool {
        match self {
            Self::OpenFile { source, .. } => source.kind() == std::io::ErrorKind::NotFound,
            _ => false,
        }
    }
}

// default conversion from CoordinatorError to tonic treats everything
// other than `Status` as an internal error
impl From<Error> for Status {
    fn from(value: Error) -> Self {
        let error_resp = ErrorResponse::new(value.error_code());

        match serde_json::to_string(&error_resp) {
            Ok(err) => Status::internal(err),
            Err(err) => {
                let error_str = format!("Serialize TskvError, error: {}", err);
                trace::error!(error_str);
                Status::unknown(error_str)
            }
        }
    }
}

impl From<Status> for Error {
    fn from(source: Status) -> Self {
        let error_message = source.message();
        match source.code() {
            Code::Internal => {
                match serde_json::from_str::<ErrorResponse>(error_message) {
                    Ok(error) => Error::ErrorResponse { error },
                    Err(err) => {
                        // Deserialization tskv error failed, maybe a bug
                        Error::CommonError {
                            reason: err.to_string(),
                        }
                    }
                }
            }
            Code::Unknown => {
                // The server will return this exception if serialization of tskv error fails, maybe a bug
                trace::error!("The server will return this exception if serialization of tskv error fails, maybe a bug");
                Error::CommonError {
                    reason: error_message.to_string(),
                }
            }
            _ => Self::Network { source },
        }
    }
}

#[derive(Snafu, Debug)]
pub enum ChannelSendError {
    #[snafu(display("Failed to send a WAL task"))]
    WalTask,
}

#[derive(Snafu, Debug)]
pub enum ChannelReceiveError {
    #[snafu(display("Failed to receive write WAL result: {source}"))]
    WriteWalResult {
        source: tokio::sync::oneshot::error::RecvError,
    },
}

#[test]
fn test_mod_code() {
    let e = Error::Schema {
        source: SchemaError::TableNotFound {
            database: String::new(),
            table: String::new(),
        },
    };
    assert_eq!(e.code(), "020004");
}
