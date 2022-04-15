use std::path::PathBuf;

pub fn make_summary_file(path: &str, number: u64) -> PathBuf {
    let p = format!("{}/summary-{:06}", path, number);
    PathBuf::from(p)
}
