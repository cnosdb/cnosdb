use crate::line_protocol::parser::Parser;
use crate::{Line, Result};

pub mod parser;

pub fn line_protocol_to_lines(lines: &str, default_time: i64) -> Result<Vec<Line>> {
    let parser = Parser::new(default_time);
    parser.parse(lines)
}
