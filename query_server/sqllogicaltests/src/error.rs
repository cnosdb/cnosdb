use std::fmt::Display;

use arrow::error::ArrowError;
use snafu::Snafu;
use sqllogictest::TestError;
use url::ParseError;

pub type Result<T, E = SqlError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum SqlError {
    #[snafu(display("SqlLogicTest error: {}", error_fmt(source)))]
    SqlLogicTest { source: TestError },

    #[snafu(display("Arrow error: {}", error_fmt(source)))]
    Arrow { source: ArrowError },

    #[snafu(display("Http error: {}", error_fmt(err)))]
    Http { err: String },

    #[snafu(display("Other Error: {}", error_fmt(reason)))]
    Other { reason: String },
}

fn error_fmt(e: impl Display) -> String {
    e.to_string()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
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

impl From<reqwest::Error> for SqlError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http {
            err: value.to_string(),
        }
    }
}

impl From<url::ParseError> for SqlError {
    fn from(value: ParseError) -> Self {
        Self::Http {
            err: value.to_string(),
        }
    }
}
