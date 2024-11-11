mod boolean;
mod float;
mod instance;
mod integer;
mod simple8b;
mod string;
mod timestamp;
mod unsigned;

use std::error::Error;

pub use instance::*;
use models::codec::Encoding;

/// Max number of bytes needed to store a varint-encoded 32-bit integer.
const MAX_VAR_INT_32: usize = 5;

/// Max number of bytes needed to store a varint-encoded 64-bit integer.
const MAX_VAR_INT_64: usize = 10;

pub type CodecError = Box<dyn Error + Send + Sync>;
