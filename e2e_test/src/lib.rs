#[cfg(feature = "coordinator_e2e_test")]
#[cfg(test)]
mod coordinator_tests;
mod flight_sql;
mod http_api_tests;
mod kv_service_tests;
mod prom;
#[cfg(test)]
mod utils;

#[derive(Debug)]
pub enum E2eError {
    Http(String),
    MetaWrite(String),
    DataWrite(String),
}

impl std::fmt::Display for E2eError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            E2eError::Http(msg) => write!(f, "HTTP error: {msg}"),
            E2eError::MetaWrite(msg) => write!(f, "Meta write error: {msg}"),
            E2eError::DataWrite(msg) => write!(f, "Data write error: {msg}"),
        }
    }
}

impl std::error::Error for E2eError {}

pub type E2eResult<T> = std::result::Result<T, E2eError>;
