/// Common end-to-end test case executor, and some predefined test cases.
mod case;

/// CnosDB cluster definition, used to initialize CnosDB cluster.
mod cluster_def;

/// Independent test cases, CnosDB cluster is managed by the test case itself.
mod independent;

/// Test cases reliant on running CnosDB cluster.
mod reliant;

/// Common utilities for end-to-end tests.
mod utils;

/// Errors during e2e tests, some of the variants may contains optional fields to compare with expected errors.
#[derive(Debug)]
pub enum E2eError {
    Connect(String),
    Http {
        status: reqwest::StatusCode,
        url: Option<String>,
        req: Option<String>,
        err: Option<String>,
    },
    Api {
        status: reqwest::StatusCode,
        url: Option<String>,
        req: Option<String>,
        resp: Option<String>,
    },
    MetaWrite(String),
    DataWrite(String),
    Command(String),
    Ignored,
}

impl std::fmt::Display for E2eError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            E2eError::Connect(msg) => write!(f, "Connect error: {msg}"),
            E2eError::Http {
                status,
                url,
                req,
                err,
            } => {
                write!(f, "HTTP error: status: {status}")?;
                if let Some(u) = url {
                    write!(f, ", url: {u}")?;
                }
                if let Some(r) = req {
                    write!(f, ", req: {r}")?;
                }
                if let Some(e) = err {
                    write!(f, ", err: {e}")?;
                }
                Ok(())
            }
            E2eError::Api {
                status,
                url,
                req,
                resp,
            } => {
                write!(f, "API error: status: {status}")?;
                if let Some(u) = url {
                    write!(f, ", url: {u}")?;
                }
                if let Some(r) = req {
                    write!(f, ", req: {r}")?;
                }
                if let Some(r) = resp {
                    write!(f, ", err: {r}")?;
                }
                Ok(())
            }
            E2eError::MetaWrite(msg) => write!(f, "Meta write error: {msg}"),
            E2eError::DataWrite(msg) => write!(f, "Data write error: {msg}"),
            E2eError::Command(msg) => write!(f, "Execute program error: {msg}"),
            E2eError::Ignored => write!(f, "Ignored"),
        }
    }
}

impl std::error::Error for E2eError {}

impl PartialEq for E2eError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (E2eError::Connect(msg1), E2eError::Connect(msg2)) => msg1 == msg2,
            (
                E2eError::Http {
                    status: status1,
                    url: url1,
                    req: req1,
                    err: err1,
                },
                E2eError::Http {
                    status: status2,
                    url: url2,
                    req: req2,
                    err: err2,
                },
            ) => {
                status1 == status2
                    && match (url1, url2) {
                        (None, _) => true,
                        (Some(u1), Some(u2)) => u1 == u2,
                        (Some(_), None) => false,
                    }
                    && match (req1, req2) {
                        (None, _) => true,
                        (Some(r1), Some(r2)) => r1 == r2,
                        (Some(_), None) => false,
                    }
                    && match (err1, err2) {
                        (None, _) => true,
                        (Some(e1), Some(e2)) => e1 == e2,
                        (Some(_), None) => false,
                    }
            }
            (
                E2eError::Api {
                    status: status1,
                    url: url1,
                    req: req1,
                    resp: resp1,
                },
                E2eError::Api {
                    status: status2,
                    url: url2,
                    req: req2,
                    resp: resp2,
                },
            ) => {
                status1 == status2
                    && match (url1, url2) {
                        (None, _) => true,
                        (Some(u1), Some(u2)) => u1 == u2,
                        (Some(_), None) => false,
                    }
                    && match (req1, req2) {
                        (None, _) => true,
                        (Some(r1), Some(r2)) => r1 == r2,
                        (Some(_), None) => false,
                    }
                    && match (resp1, resp2) {
                        (None, _) => true,
                        (Some(r1), Some(r2)) => r1 == r2,
                        (Some(_), None) => false,
                    }
            }
            _ => false,
        }
    }
}

pub type E2eResult<T> = std::result::Result<T, E2eError>;
