use serde::{Deserialize, Serialize};

use models::error_code::ErrorCode;
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
    pub fn new(error_code: &dyn ErrorCode) -> ErrorResponse {
        Self {
            error_code: error_code.code().to_string(),
            error_message: error_code.message(),
        }
    }
}
