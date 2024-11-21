use std::path::Path;
use std::process::Command;

use crate::utils::get_workspace_dir;

#[test]
fn test_version_check() {
    build();
    let workspace_dir = get_workspace_dir();
    let cnosdb_path = workspace_dir.join("target/debug/cnosdb");
    let meta_path = workspace_dir.join("target/debug/cnosdb-meta");
    let cli_path = workspace_dir.join("target/debug/cnosdb-cli");

    let cnosdb_version = get_version(&cnosdb_path);
    let meta_version = get_version(&meta_path);
    let cli_version = get_version(&cli_path);

    assert_eq!(cnosdb_version, meta_version);
    assert_eq!(cnosdb_version, cli_version);
}

fn build() {
    Command::new("cargo")
        .args([
            "build",
            "--package",
            "main",
            "--package",
            "meta",
            "--package",
            "client",
        ])
        .status()
        .expect("failed to build");
}

fn get_version(path: &Path) -> String {
    String::from_utf8(
        Command::new(path)
            .arg("--version")
            .output()
            .unwrap_or_else(|_| panic!("failed to get version of {:?}", path))
            .stdout,
    )
    .unwrap()
    .split_once(' ')
    .unwrap()
    .1
    .to_string()
}
