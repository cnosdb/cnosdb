use std::error::Error;
use std::path::{Path, PathBuf};

use sqllogictest::{default_column_validator, default_validator};
use trace::info;

use crate::instance::{CnosDBClient, SqlClientOptions};

pub async fn run_test_file(
    path: &Path,
    relative_path: PathBuf,
    options: SqlClientOptions,
) -> Result<(), Box<dyn Error>> {
    info!("Running with DataFusion runner: {}", path.display());
    let mut runner = sqllogictest::Runner::new(|| async {
        CnosDBClient::new(relative_path.clone(), options.clone())
    });
    runner.run_file_async(path).await?;
    Ok(())
}

pub async fn run_complete_file(
    path: &Path,
    relative_path: PathBuf,
    options: SqlClientOptions,
) -> Result<(), Box<dyn Error>> {
    info!("Using complete mode to complete: {}", path.display());

    // let mut data = 3;
    let mut runner = sqllogictest::Runner::new(|| async {
        CnosDBClient::new(relative_path.clone(), options.clone())
    });
    let col_separator = " ";
    let validator = default_validator;
    let column_validator = default_column_validator;
    runner
        .update_test_file(path, col_separator, validator, column_validator)
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}
