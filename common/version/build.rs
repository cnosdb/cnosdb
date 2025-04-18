use std::process::Command;
use std::time::SystemTime;

fn main() {
    let build_time = humantime::format_rfc3339_seconds(SystemTime::now()).to_string();
    println!("cargo:rustc-env=BUILD_TIME={build_time}");

    let mut rustc_env_git_hash = false;

    // Get the GIT_HASH by 'git rev-parse HEAD'.
    // https://stackoverflow.com/questions/43753491/include-git-commit-hash-as-string-into-rust-program
    match Command::new("git").args(["rev-parse", "HEAD"]).output() {
        Ok(output) => {
            if output.status.success() {
                let rev_head = String::from_utf8_lossy(&output.stdout);
                let git_hash: String = rev_head.chars().take(7).collect();
                println!("cargo:rustc-env=GIT_HASH={git_hash}");
                rustc_env_git_hash = true;
            } else {
                println!(
                    "cargo:warning=Failed to get git rev-parse HEAD, stderr:\n{}\nUse environment variable '$CNOSDB_GIT_HASH' instead",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
        Err(e) => {
            println!("cargo:warning=Failed to execute git command: {e}, use environment variable '$CNOSDB_GIT_HASH' instead");
        }
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
        println!(
            "cargo:warning=Failed to get commit hash by 'git rev-parse HEAD' or from env 'CNOSDB_GIT_HASH', use 'UNKNOWN' instead"
        );
    }
}
