mod case;
mod cluster_def;
mod independent;
mod reliant;
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
    ANY,
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
            E2eError::ANY => write!(f, "Any error"),
        }
    }
}

impl std::error::Error for E2eError {}

pub type E2eResult<T> = std::result::Result<T, E2eError>;
