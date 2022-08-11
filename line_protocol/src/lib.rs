use parser::{Line, Parser};
use snafu::Snafu;

mod parser;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Some error occured"))]
    Something,
    #[snafu(display("Error: pos: {}, in: '{}'", pos, content))]
    Parse { pos: usize, content: String },
}

pub fn line_protocol_to_lines(lines: &str) -> Result<Vec<Line>> {
    let parser = Parser::new();
    parser.parse(lines)
}
