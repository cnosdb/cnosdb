// Include the GIT_HASH
// https://stackoverflow.com/questions/43753491/include-git-commit-hash-as-string-into-rust-program
use std::process::Command;
fn main() {
    let output = Command::new("git").args(["rev-parse", "HEAD"]).output();

    if let Ok(output) = output {
        if let Ok(git_hash) = String::from_utf8(output.stdout) {
            println!("cargo:rustc-env=GIT_HASH={}", git_hash);
        }
    }
    let res = Command::new("sh").arg("-c").arg("pwd").output();
    if let Ok(output) = res {
        if let Ok(pwd) = String::from_utf8(output.stdout) {
            println!("pwd：{}", pwd);
        }
    }

    if std::env::var("BACKTRACE").ok().is_some() {
        let mut cmd = "";
        if cfg!(target_os = "macos") {
            cmd = "sed -i '' -e '/^ *pub async fn/s/pub async fn/ #[async_backtrace::framed] pub async fn/g'  \
            -e '/^ *async fn/s/async fn/ #[async_backtrace::framed] async fn/g' \
            `grep async .. -rl --exclude-dir=generated --exclude-dir=spi --exclude-dir=target --exclude=mod.rs --exclude=lib.rs`";
        } else if cfg!(target_os = "linux") {
            cmd = "sed -i -e '/^ *pub async fn/s/pub async fn/ #[async_backtrace::framed] pub async fn/g'  \
            -e '/^ *async fn/s/async fn/ #[async_backtrace::framed] async fn/g' \
            `grep async .. -rl --exclude-dir=generated --exclude-dir=spi --exclude-dir=target --exclude=mod.rs --exclude=lib.rs`";
        };
        let _ = Command::new("sh").arg("-c").arg(cmd).output();
    }
}
