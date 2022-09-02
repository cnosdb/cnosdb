use crate::sql::parser::ExtParser;
use snafu::ResultExt;
use spi::query::ast::ExtStatement;
use spi::query::parser::Parser;
use spi::query::ParserSnafu;
use spi::query::Result;
use std::collections::VecDeque;

#[derive(Default)]
pub struct DefaultParser {}

impl Parser for DefaultParser {
    fn parse(&self, sql: &str) -> Result<VecDeque<ExtStatement>> {
        ExtParser::parse_sql(sql).context(ParserSnafu)
    }
}
