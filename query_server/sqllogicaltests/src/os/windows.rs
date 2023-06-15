use std::error::Error;
use std::path::{Path, PathBuf};

use trace::info;

use crate::instance::{CnosDBClient, SqlClientOptions};

pub async fn run_test_file(
    path: &Path,
    relative_path: PathBuf,
    options: SqlClientOptions,
) -> Result<(), Box<dyn Error>> {
    info!("Running with DataFusion runner: {}", path.display());

    let _client = CnosDBClient::new(relative_path, options);
    Ok(())
}

pub async fn run_complete_file(
    path: &Path,
    relative_path: PathBuf,
    options: SqlClientOptions,
) -> Result<(), Box<dyn Error>> {
    info!("Running with DataFusion runner: {}", path.display());

    let _client = CnosDBClient::new(relative_path, options);
    Ok(())
}
