use std::fmt::Display;

use models::error_code::ErrorCode;
pub use reqwest::Response;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct EmptyResponse {}

#[derive(Debug, Deserialize, Serialize)]
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

impl std::error::Error for ErrorResponse {}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{\"error_code\": \"{}\", \"error_message\": \"{}\"}}",
            self.error_code, self.error_message
        )
    }
}

impl ErrorCode for ErrorResponse {
    fn code(&self) -> &'static str {
        Box::leak(self.error_code.clone().into_boxed_str())
    }

    fn message(&self) -> String {
        self.error_message.clone()
    }
}
