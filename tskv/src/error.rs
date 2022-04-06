use snafu::Snafu;
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("{}", source))]
    IO { source: std::io::Error },
    #[snafu(display("async file system stopped"))]
    Cancel,
}
