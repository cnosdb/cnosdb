use std::collections::VecDeque;

use super::{ast::ExtStatement, Result};

pub trait Parser {
    fn parse(&self, sql: &str) -> Result<VecDeque<ExtStatement>>;
}
