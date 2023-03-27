use std::time::Instant;

use crate::ctx::ResultSet;
use crate::print_format::PrintFormat;
use crate::Result;

#[derive(Debug, Clone)]
pub struct PrintOptions {
    pub format: PrintFormat,
    pub quiet: bool,
}

fn print_timing_info(_row_count: usize, now: Instant) {
    println!("Query took {:.3} seconds.", now.elapsed().as_secs_f64());
    // println!(
    //     "{} {} in set. Query took {:.3} seconds.",
    //     row_count,
    //     if row_count == 1 { "row" } else { "rows" },
    //     now.elapsed().as_secs_f64()
    // );
}

impl PrintOptions {
    /// print the batches to stdout using the specified format
    pub fn print_batches(&self, result_set: &ResultSet, now: Instant) -> Result<()> {
        result_set.print_fmt(&self.format)?;

        if !self.quiet {
            print_timing_info(result_set.row_count(), now);
        }
        Ok(())
    }
}
