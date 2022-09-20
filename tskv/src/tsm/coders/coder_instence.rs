use crate::tsm::boolean::{
    bool_bitpack_decode, bool_bitpack_encode, bool_without_compress_decode,
    bool_without_compress_encode,
};
use crate::tsm::float::{
    f64_gorilla_decode, f64_gorilla_encode, f64_q_compress_decode, f64_q_compress_encode,
    f64_without_compress_decode, f64_without_compress_encode,
};
use crate::tsm::integer::{
    i64_q_compress_decode, i64_q_compress_encode, i64_without_compress_decode,
    i64_without_compress_encode, i64_zigzag_simple8b_decode, i64_zigzag_simple8b_encode,
};
use crate::tsm::string::{
    str_bzip_decode, str_bzip_encode, str_gzip_decode, str_gzip_encode, str_snappy_decode,
    str_snappy_encode, str_without_compress_decode, str_without_compress_encode, str_zlib_decode,
    str_zlib_encode, str_zstd_decode, str_zstd_encode,
};
use crate::tsm::timestamp;
use crate::tsm::timestamp::{
    ts_q_compress_decode, ts_q_compress_encode, ts_without_compress_decode,
    ts_without_compress_encode, ts_zigzag_simple8b_decode, ts_zigzag_simple8b_encode,
};
use crate::tsm::unsigned::{
    u64_q_compress_decode, u64_q_compress_encode, u64_without_compress_decode,
    u64_without_compress_encode, u64_zigzag_simple8b_decode, u64_zigzag_simple8b_encode,
};
use libc::max_align_t;
use std::error::Error;

#[derive(Copy, Clone, PartialEq, Debug)]
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
    BitPack = 9,
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
            9 => Ok(CodeType::BitPack),
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
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullIntegerCoder();

impl IntegerCoder for NullIntegerCoder {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_without_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_without_compress_decode(src, dst)
    }
}

struct DeltaIntegerCoder();

impl IntegerCoder for DeltaIntegerCoder {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_zigzag_simple8b_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_zigzag_simple8b_decode(src, dst)
    }
}

struct QuantileIntegerCoder();

impl IntegerCoder for QuantileIntegerCoder {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_q_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_q_compress_decode(src, dst)
    }
}

pub trait FloatCoder {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(&self, src: &[u8], dst: &mut Vec<f64>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullFloatCoder();

impl FloatCoder for NullFloatCoder {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_without_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<f64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_without_compress_decode(src, dst)
    }
}

struct GorillaFloatCoder();

impl FloatCoder for GorillaFloatCoder {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_gorilla_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<f64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_gorilla_decode(src, dst)
    }
}

struct QuantileFloatCoder();

impl FloatCoder for QuantileFloatCoder {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_q_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<f64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_q_compress_decode(src, dst)
    }
}

pub trait UnsignedCoder {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(&self, src: &[u8], dst: &mut Vec<u64>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullUnsignedCoder();

impl UnsignedCoder for NullUnsignedCoder {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_without_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<u64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_without_compress_decode(src, dst)
    }
}

struct DeltaUnsignedCoder();

impl UnsignedCoder for DeltaUnsignedCoder {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_zigzag_simple8b_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<u64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_zigzag_simple8b_decode(src, dst)
    }
}

struct QuantileUnsignedCoder();

impl UnsignedCoder for QuantileUnsignedCoder {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_q_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<u64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_q_compress_decode(src, dst)
    }
}

pub trait BooleanCoder {
    fn encode(&self, src: &[bool], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(&self, src: &[u8], dst: &mut Vec<bool>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullBooleanCoder();

impl BooleanCoder for NullBooleanCoder {
    fn encode(&self, src: &[bool], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        bool_without_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<bool>) -> Result<(), Box<dyn Error + Send + Sync>> {
        bool_without_compress_decode(src, dst)
    }
}

struct BitPackBooleanCoder();

impl BooleanCoder for BitPackBooleanCoder {
    fn encode(&self, src: &[bool], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        bool_bitpack_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<bool>) -> Result<(), Box<dyn Error + Send + Sync>> {
        bool_bitpack_decode(src, dst)
    }
}

pub trait StringCoder {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<Vec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullStringCoder();

impl StringCoder for NullStringCoder {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_without_compress_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<Vec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_without_compress_decode(src, dst)
    }
}

struct SnappyStringCoder();

impl StringCoder for SnappyStringCoder {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_snappy_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<Vec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_snappy_decode(src, dst)
    }
}

struct GzipStringCoder();

impl StringCoder for GzipStringCoder {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_gzip_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<Vec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_gzip_decode(src, dst)
    }
}

struct BzipStringCoder();

impl StringCoder for BzipStringCoder {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_bzip_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<Vec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_bzip_decode(src, dst)
    }
}

struct ZstdStringCoder();

impl StringCoder for ZstdStringCoder {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_zstd_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<Vec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_zstd_decode(src, dst)
    }
}

struct ZlibStringCoder();

impl StringCoder for ZlibStringCoder {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_zlib_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<Vec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_zlib_decode(src, dst)
    }
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

pub fn get_i64_coder(algo: CodeType) -> Box<dyn IntegerCoder> {
    match algo {
        CodeType::Null => Box::new(NullIntegerCoder()),
        CodeType::Delta => Box::new(DeltaIntegerCoder()),
        CodeType::Quantile => Box::new(QuantileIntegerCoder()),
        _ => Box::new(DeltaIntegerCoder()),
    }
}

pub fn get_u64_coder(algo: CodeType) -> Box<dyn UnsignedCoder> {
    match algo {
        CodeType::Null => Box::new(NullUnsignedCoder()),
        CodeType::Delta => Box::new(DeltaUnsignedCoder()),
        CodeType::Quantile => Box::new(QuantileUnsignedCoder()),
        _ => Box::new(DeltaUnsignedCoder()),
    }
}

pub fn get_f64_coder(algo: CodeType) -> Box<dyn FloatCoder> {
    match algo {
        CodeType::Null => Box::new(NullFloatCoder()),
        CodeType::Gorilla => Box::new(GorillaFloatCoder()),
        CodeType::Quantile => Box::new(QuantileFloatCoder()),
        _ => Box::new(GorillaFloatCoder()),
    }
}

pub fn get_str_coder(algo: CodeType) -> Box<dyn StringCoder> {
    match algo {
        CodeType::Null => Box::new(NullStringCoder()),
        CodeType::Gzip => Box::new(GzipStringCoder()),
        CodeType::Bzip => Box::new(BzipStringCoder()),
        CodeType::Snappy => Box::new(SnappyStringCoder()),
        CodeType::Zstd => Box::new(ZstdStringCoder()),
        CodeType::Zlib => Box::new(ZlibStringCoder()),
        _ => Box::new(SnappyStringCoder()),
    }
}

pub fn get_bool_coder(algo: CodeType) -> Box<dyn BooleanCoder> {
    match algo {
        CodeType::Null => Box::new(NullBooleanCoder()),
        CodeType::BitPack => Box::new(BitPackBooleanCoder()),
        _ => Box::new(BitPackBooleanCoder()),
    }
}
