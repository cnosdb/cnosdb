//! ProgressBar with a relatively uniform style.

use indicatif::{ProgressBar, ProgressStyle};

const PROGRESS_BAR_WITH_SIZE_TEMPLATE: &str =
    "{spinner:.green} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})";

// Create a new progress bar with a given size (bytes)
pub fn new_with_size(size: u64) -> ProgressBar {
    let pb = ProgressBar::new(size);
    pb.set_style(ProgressStyle::with_template(PROGRESS_BAR_WITH_SIZE_TEMPLATE).unwrap());
    pb
}
