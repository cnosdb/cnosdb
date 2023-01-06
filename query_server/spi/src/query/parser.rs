use std::collections::VecDeque;

use super::ast::ExtStatement;
use crate::Result;

pub trait Parser {
    fn parse(&self, sql: &str) -> Result<VecDeque<ExtStatement>>;
}
