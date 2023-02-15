mod test;

use std::fmt::{Debug, Display, Formatter};

pub use error_code_macro::ErrorCoder;

pub trait ErrorCode: std::error::Error {
    /// mod_code 2 , variant code 4, e.g. 010001
    fn code(&self) -> &'static str;

    /// Display of Error
    fn message(&self) -> String;

    fn source_error_code(&self) -> &dyn ErrorCode
    where
        Self: Sized,
    {
        self
    }
}

pub struct UnknownCode;

pub struct UnknownCodeWithMessage(pub String);

impl Debug for UnknownCodeWithMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown error {}", self.0)
    }
}

impl Display for UnknownCodeWithMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Debug for UnknownCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown error")
    }
}

impl Display for UnknownCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl std::error::Error for UnknownCode {}
impl std::error::Error for UnknownCodeWithMessage {}

impl ErrorCode for UnknownCode {
    fn code(&self) -> &'static str {
        "0000000"
    }
    fn message(&self) -> String {
        "unknow_error".to_string()
    }
}

impl ErrorCode for UnknownCodeWithMessage {
    fn code(&self) -> &'static str {
        "000000"
    }
    fn message(&self) -> String {
        self.0.to_owned()
    }
}

// cnosdb provides an error code mechanism to help you quickly
// locate information such as error types, severity levels,
// and causes of errors, helping you quickly locate and solve problems.
//
// It is used to determine the category, cause and severity of the error code. The format is as follows.
//
// ```
// MMCCCCX
// ```
//
// MM：Indicates the module number. 2 bit integer. Number values are as follows:
//
//     00: Indicates an unknown module, meaningless, and serves as a placeholder
//     01: query engine
//     02: tskv engine
//
// CCCC：Indicates the error code, a 4-digit integer.
//
//     0000: Indicates an unknown exception, meaningless, and space occupying
//
// X：Indicates the severity of the error. 1-bit integer. The higher the value, the higher the severity. The value is 0~9. 0 is a placeholder, 1 is the smallest error, such as input error. 9 is the highest level error, such as atomic error.
//
// The following is the error code format and error code list of cnosdb.
// define_error_codes! {
//     /// The sql state code needs to be developed later
//     /// and is currently used as a placeholder
//     (Unknown, b"0000000");
//     /// The sql state code needs to be developed later
//     /// and is currently used as a placeholder
//     (QueryUnknown, b"0100000");
//     /// The sql state code needs to be developed later
//     /// and is currently used as a placeholder
//     (TskvUnknown, b"0200000");
//     /// The sql state code needs to be developed later
//     /// and is currently used as a placeholder
//     (CoordinatorUnknown, b"0300000");
//     (TenantNotFound, b"0300001");
// }
