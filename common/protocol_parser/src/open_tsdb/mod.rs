use crate::open_tsdb::parser::Parser;
use crate::{Line, Result};

pub mod parser;

pub fn open_tsdb_to_lines(lines: &str, default_time: i64) -> Result<Vec<Line>> {
    let parser = Parser::new(default_time);
    parser.parse(lines)
}
