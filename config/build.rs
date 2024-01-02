use std::process::Command;

fn main() {
    let mut rustc_env_git_hash = false;
    // Get the GIT_HASH by 'git rev-parse HEAD'.
    // https://stackoverflow.com/questions/43753491/include-git-commit-hash-as-string-into-rust-program
    if let Ok(output) = Command::new("git").args(["rev-parse", "HEAD"]).output() {
        if output.status.success() {
            if let Ok(git_hash) = String::from_utf8(output.stdout) {
                println!("cargo:rustc-env=GIT_HASH={}", git_hash);
                rustc_env_git_hash = true;
            }
        } else {
            eprintln!("Failed to run 'git rev-parse HEAD'");
        }
    } else {
        eprintln!("Failed to start process 'git rev-parse HEAD'");
    }
    if !rustc_env_git_hash {
        // Get the GIT_HASH from environment variable 'CNOSDB_GIT_HASH'
        // when using Github CI to build.
        if let Ok(git_hash) = std::env::var("CNOSDB_GIT_HASH") {
            println!("cargo:rustc-env=GIT_HASH={}", git_hash);
            rustc_env_git_hash = true;
        }
    }
    if !rustc_env_git_hash {
        eprintln!(
            "Failed to get commit hash by 'git rev-parse HEAD' or from env 'CNOSDB_GIT_HASH'"
        );
    }
}
