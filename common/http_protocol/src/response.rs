use models::error_code::ErrorCode;
use serde::{Deserialize, Serialize};

pub use reqwest::Response;

#[derive(Debug, Serialize)]
pub struct EmptyResponse {}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ErrorResponse {
    error_code: String,
    error_message: String,
}

impl ErrorResponse {
    pub fn new(error_code: ErrorCode, error_message: String) -> ErrorResponse {
        Self {
            error_code: error_code.as_str().to_string(),
            error_message,
        }
    }
}
