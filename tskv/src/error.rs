use crate::wal;
use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("{}", source))]
    IO { source: std::io::Error },
    #[snafu(display("async file system stopped"))]
    Cancel,
    #[snafu(display("invalid flatbuffers: {}", source))]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },
    #[snafu(display("parse flatbuffers: {}", source))]
    ParseFlatbuffer { source: ParseFlatbufferError },
    #[snafu(display("unable to write wal: {}", source))]
    WriteAheadLog { source: wal::WalError },
}

#[derive(Snafu, Debug)]
pub enum ParseFlatbufferError {
    Error,
}
