use snafu::Snafu;

pub type FileSystemResult<T> = Result<T, FileSystemError>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum FileSystemError {
    #[snafu(display("File error: {:?}", source))]
    StdIOError { source: std::io::Error },
}
