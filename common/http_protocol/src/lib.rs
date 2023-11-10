pub mod blocking;
pub mod encoding;
pub mod header;
pub mod http_client;
pub mod parameter;
pub mod response;
pub mod status_code;

use std::path::PathBuf;

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to read certificate from path '{}': {source}", path.display()))]
    LoadCertificate {
        source: std::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Failed to parse certificate '{}': {source}", path.display()))]
    ParseCertificate {
        source: reqwest::Error,
        path: PathBuf,
    },

    #[snafu(display("Failed to build http client: {source}"))]
    BuildHttpClient { source: reqwest::Error },
}
