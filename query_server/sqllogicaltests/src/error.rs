use arrow::error::ArrowError;
use snafu::Snafu;
use sqllogictest::TestError;

pub type Result<T, E = SqlError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum SqlError {
    #[snafu(display("SqlLogicTest error: {source}"))]
    SqlLogicTest { source: TestError },

    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: ArrowError },

    #[snafu(display("Other Error: {reason}"))]
    Other { reason: String },
}

impl From<TestError> for SqlError {
    fn from(source: TestError) -> Self {
        SqlError::SqlLogicTest { source }
    }
}

impl From<ArrowError> for SqlError {
    fn from(source: ArrowError) -> Self {
        SqlError::Arrow { source }
    }
}

impl From<String> for SqlError {
    fn from(reason: String) -> Self {
        SqlError::Other { reason }
    }
}
