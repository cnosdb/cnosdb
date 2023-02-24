use serde::{Deserialize, Serialize};

pub mod meta_service;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Infallible {}
