use std::error::Error;
use std::path::{Path, PathBuf};

use sqllogictest::{default_column_validator, default_validator};
use trace::info;

use crate::instance::{CnosdbClient, CnosdbTestEngine};

pub async fn run_test_file(
    engine_name: String,
    path: &Path,
    relative_path: PathBuf,
    client: CnosdbClient,
) -> Result<(), Box<dyn Error>> {
    info!("Running test mode: {}", path.display());
    let mut runner = sqllogictest::Runner::new(|| async {
        CnosdbTestEngine::new(engine_name.clone(), relative_path.clone(), client.clone())
    });
    runner.run_file_async(path).await?;
    Ok(())
}

pub async fn update_test_file(
    engine_name: String,
    path: &Path,
    relative_path: PathBuf,
    client: CnosdbClient,
) -> Result<(), Box<dyn Error>> {
    info!("Running complete mode: {}", path.display());

    let mut runner = sqllogictest::Runner::new(|| async {
        CnosdbTestEngine::new(engine_name.clone(), relative_path.clone(), client.clone())
    });
    let col_separator = " ";
    let validator = default_validator;
    let column_validator = default_column_validator;
    runner
        .update_test_file(
            path,
            col_separator,
            validator,
            |s| s.clone(),
            column_validator,
        )
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}
