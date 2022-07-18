use std::env;

use tskv;

const ARG_PRINT: &str = "print";
const ARG_TSM: &str = "--tsm";

/// # Example
/// tskv print --tsm <tsm_path>
fn main() {
    let mut args = env::args();
    let mut tsm_path: Option<String> = None;

    while let Some(arg) = args.next() {
        // --print [--tsm <path>]
        if &arg == ARG_PRINT {
            match args.next() {
                Some(a) if &a == ARG_TSM => {
                    tsm_path = args.next();
                },
                _ => break,
            }
        }
    }

    if let Some(p) = tsm_path {
        print!("Path: {}", p);
        tskv::print_tsm_statistics(p);
    }
}
