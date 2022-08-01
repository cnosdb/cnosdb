use std::env;

const ARG_PRINT: &str = "print";
const ARG_TSM: &str = "--tsm";
const ARG_TOMBSTONE: &str = "--tombstone";

/// # Example
/// tskv print [--tsm <tsm_path>] [--tombstone]
///
/// - --tsm <tsm_path> print statistics for .tsm file at <tsm_path> .
/// - --tombstone also print tombstone for every field_id in .tsm file.
fn main() {
    let mut args = env::args().peekable();
    let mut tsm_path: Option<String> = None;
    let mut show_tombstone = false;

    while let Some(arg) = args.peek() {
        // --print [--tsm <path>]
        if arg.as_str() == ARG_PRINT {
            while let Some(print_arg) = args.next() {
                match print_arg.as_str() {
                    ARG_TSM => {
                        tsm_path = args.next();
                        if tsm_path.is_none() {
                            println!("Invalid arguments: --tsm <tsm_path>");
                        }
                    }
                    ARG_TOMBSTONE => {
                        show_tombstone = true;
                    }
                    _ => {}
                }
            }
        }
        args.next();
    }

    if let Some(p) = tsm_path {
        println!("Path: {}, ShowTombstone: {}", p, show_tombstone);
        tskv::print_tsm_statistics(p, show_tombstone);
    }
}
