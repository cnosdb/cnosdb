use std::fmt::Display;
use std::sync::Arc;

use crate::Config;

// const NONE: &str = "/033[m";
// const RED: &str = "/033[0;32;31m";
// const BLUE: &str = "/033[0;32;34m";

fn write_error(f: &mut std::fmt::Formatter<'_>, message: &str) -> std::fmt::Result {
    write!(f, "    {}", message)
}

fn write_warn(f: &mut std::fmt::Formatter<'_>, message: &str) -> std::fmt::Result {
    write!(f, "    {}", message)
}

pub trait CheckConfig {
    fn check(&self, all_config: &Config) -> Option<CheckConfigResult>;
}

#[derive(Default)]
pub struct CheckConfigResult {
    warn: Vec<CheckConfigItemResult>,
    error: Vec<CheckConfigItemResult>,
    pub show_warnings: bool,
}

impl CheckConfigResult {
    pub fn add_warn(&mut self, ret: CheckConfigItemResult) {
        self.warn.push(ret);
    }

    pub fn add_error(&mut self, ret: CheckConfigItemResult) {
        self.error.push(ret);
    }

    pub fn add_all(&mut self, mut other: Self) {
        self.warn.append(&mut other.warn);
        self.error.append(&mut other.error);
    }

    pub fn is_empty(&self) -> bool {
        self.warn.is_empty() && self.error.is_empty()
    }

    pub fn introspect(&mut self) {
        self.warn.sort();
        self.error.sort();
    }
}

impl Display for CheckConfigResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.error.is_empty() {
            let item_type = if self.show_warnings && self.warn.is_empty() {
                "errors / warnings"
            } else {
                "errors"
            };
            writeln!(f, "There are no {} in the configuration file.", item_type)?;
        }
        for e in self.error.iter() {
            writeln!(f, "Error in entry '{}.{}':", e.config, e.item)?;
            write_error(f, &e.message)?;
            write!(f, "\n\n")?;
        }

        if self.show_warnings {
            for w in self.warn.iter() {
                writeln!(f, "Warn in entry '{}.{}':", w.config, w.item)?;
                write_warn(f, &w.message)?;
                write!(f, "\n\n")?;
            }
        }

        Ok(())
    }
}

#[derive(PartialEq, Eq)]
pub struct CheckConfigItemResult {
    pub config: Arc<String>,
    pub item: String,
    pub message: String,
}

impl PartialOrd for CheckConfigItemResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CheckConfigItemResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.config.cmp(&other.config) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match self.item.cmp(&other.item) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        std::cmp::Ordering::Equal
    }
}

#[test]
fn test_print() {
    let r = CheckConfigResult {
        warn: vec![CheckConfigItemResult {
            config: Arc::new(String::from("cache")),
            item: "memory".to_string(),
            message: "warning message".to_string(),
        }],
        error: vec![CheckConfigItemResult {
            config: Arc::new(String::from("storage")),
            item: "path".to_string(),
            message: "error message".to_string(),
        }],
        show_warnings: true,
    };

    print!("{}", r);
}
