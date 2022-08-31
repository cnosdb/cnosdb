use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("stmt to datafusion plan error"))]
    DFPlanError,
    #[snafu(display("cannot find table name:{}", name))]
    TableNameError { name: String },
    #[snafu(display("cannot find database name:{}", name))]
    DatabaseNameError { name: String },
}
