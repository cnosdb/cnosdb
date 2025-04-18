#![allow(unused)]

mod executor;
pub mod step;

use std::path::PathBuf;

use backtrace::Backtrace;
pub use executor::*;

use crate::{E2eError, E2eResult};

// TODO(zipper): This module also needs test.

pub enum CaseFlowControl {
    Continue,
    Break,
    Error(String),
}

#[derive(Debug)]
pub struct CnosdbAuth {
    pub username: String,
    pub password: Option<String>,
}

impl std::fmt::Display for CnosdbAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{username}", username = self.username)?;
        if let Some(password) = &self.password {
            write!(f, ":{password}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct Location {
    pub file: Option<PathBuf>,
    pub line: u32,
    pub column: u32,
}

impl Location {
    fn try_new(mut trace_deepth: usize) -> Option<Self> {
        let bt = Backtrace::new();
        let mut deepth = 0_usize;
        for frame in bt.frames() {
            for symbol in frame.symbols() {
                // if let Some(Some(name)) = symbol.name().map(|s| s.as_str()) {
                if let Some(name) = symbol.name() {
                    if name.to_string().starts_with("e2e_test") {
                        deepth += 1;
                        if deepth > trace_deepth {
                            return Some(Location {
                                file: symbol.filename().map(|p| p.to_path_buf()),
                                line: symbol.lineno().unwrap_or(0),
                                column: symbol.colno().unwrap_or(0),
                            });
                        }
                    }
                }
            }
        }

        None
    }
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(file) = &self.file {
            write!(f, "'{}':{}:{}", file.display(), self.line, self.column)
        } else {
            write!(f, "<unknown location>")
        }
    }
}

fn check_e2e_error<T>(
    result_response: &E2eResult<T>,
    expect_error: &E2eError,
    message: &str,
) -> CaseFlowControl {
    match result_response {
        Ok(_) => return CaseFlowControl::Error(format!("Response is ok: {message}")),
        Err(resp_err) => match expect_error {
            E2eError::Ignored => {
                // Do not check error message.
            }
            exp_err => {
                if exp_err != resp_err {
                    let message = format!("assertion failed: (left == right), {message}\n left: {resp_err:?}\nright: {exp_err:?}");
                    return CaseFlowControl::Error(message);
                }
            }
        },
    }
    CaseFlowControl::Continue
}
