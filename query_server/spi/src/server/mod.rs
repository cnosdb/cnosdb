use crate::catalog::MetadataError;
use crate::query::{function, QueryError};
use models::define_result;
use snafu::{Backtrace, Snafu};

pub mod dbms;

define_result!(ServerError);

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ServerError {
    #[snafu(display("Failed to do execute statement, err:{}", source))]
    Query {
        source: QueryError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to build server, err:{}", source))]
    Build {
        source: QueryError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to load functions, err:{}", source))]
    LoadFunction {
        source: function::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed init meta data, err :{}", source))]
    MetaData {
        source: MetadataError,
        backtrace: Backtrace,
    },
}
