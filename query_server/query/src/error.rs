use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("stmt to datafusion plan error"))]
    DFPlan,
    #[snafu(display("cannot find table name:{}", name))]
    TableName { name: String },
    #[snafu(display("cannot find database name:{}", name))]
    DatabaseName { name: String },
}
