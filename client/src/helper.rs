//! Helper that helps with interactive editing, including multi-line parsing and validation,
//! and auto-completion for file name during creating external table.

use datafusion::sql::parser::{DFParser, Statement};
use rustyline::completion::{Completer, FilenameCompleter, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Helper, Result};

#[derive(Default)]
pub struct CliHelper {
    completer: FilenameCompleter,
}

impl Highlighter for CliHelper {}

impl Hinter for CliHelper {
    type Hint = String;
}

/// returns true if the current position is after the open quote for
/// creating an external table.
fn is_open_quote_for_location(line: &str, pos: usize) -> bool {
    let mut sql = line[..pos].to_string();
    sql.push('\'');
    if let Ok(stmts) = DFParser::parse_sql(&sql) {
        if let Some(Statement::CreateExternalTable(_)) = stmts.back() {
            return true;
        }
    }
    false
}

impl Completer for CliHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> std::result::Result<(usize, Vec<Pair>), ReadlineError> {
        if is_open_quote_for_location(line, pos) {
            self.completer.complete(line, pos, ctx)
        } else {
            Ok((0, Vec::with_capacity(0)))
        }
    }
}

impl Validator for CliHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> Result<ValidationResult> {
        let input = ctx.input().trim_end();
        if let Some(sql) = input.strip_suffix(';') {
            if sql.trim().is_empty() {
                return Ok(ValidationResult::Invalid(Some(
                    "  🤔 You entered an empty statement".to_string(),
                )));
            }

            Ok(ValidationResult::Valid(None))
        } else if input.starts_with('\\') {
            // command
            Ok(ValidationResult::Valid(None))
        } else {
            Ok(ValidationResult::Incomplete)
        }
    }
}

impl Helper for CliHelper {}
