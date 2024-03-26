use self::parser::{Parser, Result};
use crate::Line;

pub mod parser;

pub fn es_log_to_lines(
    lines: &str,
    table: String,
    time_alias: Option<String>,
    msg_alias: Option<String>,
) -> Result<Vec<Line>> {
    let parser = Parser::new(table);
    parser.parse(lines, time_alias, msg_alias)
}
