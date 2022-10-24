use reqwest::header::InvalidHeaderValue;
use snafu::Snafu;
#[derive(Clone, Debug, Snafu)]
pub enum Error {
    #[snafu(display("Case new fail\n"))]
    CaseNew,
    #[snafu(display("Case search fail\n"))]
    CaseSearch,
    #[snafu(display("Case not match\n"))]
    CaseNotMatch,
    #[snafu(display("Case path not found\n"))]
    CasePathNotFound,
    CaseFail,
    HttpRequestBuildFail,
    IO,
}

impl From<walkdir::Error> for Error {
    fn from(_: walkdir::Error) -> Self {
        Self::CaseSearch
    }
}

impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Error::IO
    }
}

impl From<InvalidHeaderValue> for Error {
    fn from(_: InvalidHeaderValue) -> Self {
        Self::HttpRequestBuildFail
    }
}

impl From<reqwest::Error> for Error {
    fn from(_: reqwest::Error) -> Self {
        Self::HttpRequestBuildFail
    }
}

pub type Result<T> = std::result::Result<T, Error>;
