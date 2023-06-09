use std::env;

const ARG_PRINT: &str = "print"; // To print something
const ARG_REPAIR: &str = "repair"; // To repair something
const ARG_TSM: &str = "--tsm"; // To print a .tsm file
const ARG_TOMBSTONE: &str = "--tombstone"; // To print a .tsm file with tombsotne
const ARG_SUMMARY: &str = "--summary"; // To print a summary file
const ARG_WAL: &str = "--wal"; // To print a wal file
const ARG_INDEX: &str = "--index"; // To print a wal file

/// # Example
/// tskv print [--tsm <tsm_path>] [--tombstone]
/// tskv print [--summary <summary_path>]
/// tskv print [--wal <wal_path>]
/// tskv repair [--index <file_name>]
/// - --tsm <tsm_path> print statistics for .tsm file at <tsm_path> .
/// - --tombstone also print tombstone for every field_id in .tsm file.
#[tokio::main]
async fn main() {
    let mut args = env::args().peekable();

    let mut show_tsm = false;
    let mut tsm_path: Option<String> = None;
    let mut show_tombstone = false;

    let mut show_summary = false;
    let mut summary_path: Option<String> = None;

    let mut show_wal = false;
    let mut wal_path: Option<String> = None;

    let mut repair_index = false;
    let mut index_file: Option<String> = None;

    while let Some(arg) = args.peek() {
        // --print [--tsm <path>]
        if arg.as_str() == ARG_PRINT {
            while let Some(print_arg) = args.next() {
                match print_arg.as_str() {
                    ARG_TSM => {
                        show_tsm = true;
                        tsm_path = args.next();
                        if tsm_path.is_none() {
                            println!("Invalid arguments: --tsm <tsm_path>");
                        }
                    }
                    ARG_TOMBSTONE => {
                        show_tombstone = true;
                    }
                    ARG_SUMMARY => {
                        show_summary = true;
                        summary_path = args.next();
                        if summary_path.is_none() {
                            println!("Invalid arguments: --summary <summary_path>")
                        }
                    }
                    ARG_WAL => {
                        show_wal = true;
                        wal_path = args.next();
                        if wal_path.is_none() {
                            println!("Invalid arguments: --wal <wal_path>")
                        }
                    }
                    _ => {}
                }
            }
        } else if arg.as_str() == ARG_REPAIR {
            while let Some(repair_arg) = args.next() {
                if repair_arg.as_str() == ARG_INDEX {
                    repair_index = true;
                    index_file = args.next();
                    if index_file.is_none() {
                        println!("Invalid arguments: --index <index file>");
                    }
                }
            }
        }
        args.next();
    }

    if show_tsm {
        if let Some(p) = tsm_path {
            println!("TSM Path: {}, ShowTombstone: {}", p, show_tombstone);
            tskv::print_tsm_statistics(p, show_tombstone).await;
        }
    }

    if show_summary {
        if let Some(p) = summary_path {
            println!("Summary Path: {}", p);
            tskv::print_summary_statistics(p).await;
        }
    }

    if show_wal {
        if let Some(p) = wal_path {
            println!("Wal Path: {}", p);
            tskv::print_wal_statistics(p).await;
        }
    }

    if repair_index {
        if let Some(name) = index_file {
            println!("repair index: {}", name);
            let result = tskv::index::binlog::repair_index_file(&name).await;
            println!("repair index result: {:?}", result);
        }
    }
}
