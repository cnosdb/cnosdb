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

pub type Result<T> = std::result::Result<T, Error>;
