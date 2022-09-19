use crate::tsm::timestamp;
use crate::tsm::timestamp::{
    ts_q_compress_decode, ts_q_compress_encode, ts_without_compress_decode,
    ts_without_compress_encode, ts_zigzag_simple8b_decode, ts_zigzag_simple8b_encode,
};
use std::error::Error;

#[derive(Copy, Clone, PartialEq)]
pub enum CodeType {
    Null = 0,
    Delta = 1,
    Quantile = 2,
    Gzip = 3,
    Bzip = 4,
    Gorilla = 5,
    Snappy = 6,
    Zstd = 7,
    Zlib = 8,
    Lz4 = 9,
    Unknown = 10,
}

impl Default for CodeType {
    fn default() -> Self {
        CodeType::Unknown
    }
}

impl TryFrom<u8> for CodeType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CodeType::Null),
            1 => Ok(CodeType::Delta),
            2 => Ok(CodeType::Quantile),
            3 => Ok(CodeType::Gzip),
            4 => Ok(CodeType::Bzip),
            5 => Ok(CodeType::Gorilla),
            6 => Ok(CodeType::Snappy),
            7 => Ok(CodeType::Zstd),
            8 => Ok(CodeType::Zlib),
            9 => Ok(CodeType::Lz4),
            _ => Ok(CodeType::Unknown),
        }
    }
}

pub trait TimestampCoder {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullTimestampCoder();

impl TimestampCoder for NullTimestampCoder {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_without_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_without_compress_decode(src, dst)
    }
}

struct DeltaTimestampCoder();

impl TimestampCoder for DeltaTimestampCoder {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_zigzag_simple8b_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_zigzag_simple8b_decode(src, dst)
    }
}

struct QuantileTimestampCoder();

impl TimestampCoder for QuantileTimestampCoder {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_q_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_q_compress_decode(src, dst)
    }
}

pub trait IntegerCoder {
    fn encode(src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub trait FloatCoder {
    fn encode(src: &[f64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(src: &[u8], dst: &mut Vec<f64>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

/// unsigned coder algorithm same integer
pub trait UnsignedCoder {
    fn encode(src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(src: &[u8], dst: &mut Vec<u64>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

/// boolean use bitpack
pub trait BooleanCoder {
    fn encode(src: &[bool], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(src: &[u8], dst: &mut Vec<bool>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub trait StringCoder {
    fn encode(src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(src: &[u8], dst: &mut Vec<Vec<u8>>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub fn get_code_type(src: &[u8]) -> CodeType {
    if src.is_empty() {
        return CodeType::Unknown;
    }
    return CodeType::try_from(src[0]).unwrap();
}

pub fn get_ts_coder(algo: CodeType) -> Box<dyn TimestampCoder> {
    match algo {
        CodeType::Null => Box::new(NullTimestampCoder()),
        CodeType::Delta => Box::new(DeltaTimestampCoder()),
        CodeType::Quantile => Box::new(QuantileTimestampCoder()),
        _ => Box::new(DeltaTimestampCoder()),
    }
}
