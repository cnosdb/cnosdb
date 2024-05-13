#![allow(dead_code)]

mod executor;
pub mod step;

pub use executor::*;

pub enum CaseFlowControl {
    Continue,
    Break,
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
