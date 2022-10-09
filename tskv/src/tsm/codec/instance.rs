use crate::tsm::codec::boolean::{
    bool_bitpack_decode, bool_bitpack_encode, bool_without_compress_decode,
    bool_without_compress_encode,
};
use crate::tsm::codec::float::{
    f64_gorilla_decode, f64_gorilla_encode, f64_q_compress_decode, f64_q_compress_encode,
    f64_without_compress_decode, f64_without_compress_encode,
};
use crate::tsm::codec::integer::{
    i64_q_compress_decode, i64_q_compress_encode, i64_without_compress_decode,
    i64_without_compress_encode, i64_zigzag_simple8b_decode, i64_zigzag_simple8b_encode,
};
use crate::tsm::codec::string::{
    str_bzip_decode, str_bzip_encode, str_gzip_decode, str_gzip_encode, str_snappy_decode,
    str_snappy_encode, str_without_compress_decode, str_without_compress_encode, str_zlib_decode,
    str_zlib_encode, str_zstd_decode, str_zstd_encode,
};
use crate::tsm::codec::timestamp;
use crate::tsm::codec::timestamp::{
    ts_q_compress_decode, ts_q_compress_encode, ts_without_compress_decode,
    ts_without_compress_encode, ts_zigzag_simple8b_decode, ts_zigzag_simple8b_encode,
};
use crate::tsm::codec::unsigned::{
    u64_q_compress_decode, u64_q_compress_encode, u64_without_compress_decode,
    u64_without_compress_encode, u64_zigzag_simple8b_decode, u64_zigzag_simple8b_encode,
};
use datafusion::physical_plan::expressions::Min;
use libc::max_align_t;
use minivec::MiniVec;
use models::codec::Encoding;
use std::error::Error;

pub trait TimestampCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullTimestampCodec();

impl TimestampCodec for NullTimestampCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_without_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_without_compress_decode(src, dst)
    }
}

struct DeltaTimestampCodec();

impl TimestampCodec for DeltaTimestampCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_zigzag_simple8b_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_zigzag_simple8b_decode(src, dst)
    }
}

struct QuantileTimestampCodec();

impl TimestampCodec for QuantileTimestampCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_q_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        ts_q_compress_decode(src, dst)
    }
}

pub trait IntegerCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullIntegerCodec();

impl IntegerCodec for NullIntegerCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_without_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_without_compress_decode(src, dst)
    }
}

struct DeltaIntegerCodec();

impl IntegerCodec for DeltaIntegerCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_zigzag_simple8b_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_zigzag_simple8b_decode(src, dst)
    }
}

struct QuantileIntegerCodec();

impl IntegerCodec for QuantileIntegerCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_q_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        i64_q_compress_decode(src, dst)
    }
}

pub trait FloatCodec {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(&self, src: &[u8], dst: &mut Vec<f64>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullFloatCodec();

impl FloatCodec for NullFloatCodec {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_without_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<f64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_without_compress_decode(src, dst)
    }
}

struct GorillaFloatCodec();

impl FloatCodec for GorillaFloatCodec {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_gorilla_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<f64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_gorilla_decode(src, dst)
    }
}

struct QuantileFloatCodec();

impl FloatCodec for QuantileFloatCodec {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_q_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<f64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        f64_q_compress_decode(src, dst)
    }
}

pub trait UnsignedCodec {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(&self, src: &[u8], dst: &mut Vec<u64>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullUnsignedCodec();

impl UnsignedCodec for NullUnsignedCodec {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_without_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<u64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_without_compress_decode(src, dst)
    }
}

struct DeltaUnsignedCodec();

impl UnsignedCodec for DeltaUnsignedCodec {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_zigzag_simple8b_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<u64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_zigzag_simple8b_decode(src, dst)
    }
}

struct QuantileUnsignedCodec();

impl UnsignedCodec for QuantileUnsignedCodec {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_q_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<u64>) -> Result<(), Box<dyn Error + Send + Sync>> {
        u64_q_compress_decode(src, dst)
    }
}

pub trait BooleanCodec {
    fn encode(&self, src: &[bool], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(&self, src: &[u8], dst: &mut Vec<bool>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullBooleanCodec();

impl BooleanCodec for NullBooleanCodec {
    fn encode(&self, src: &[bool], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        bool_without_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<bool>) -> Result<(), Box<dyn Error + Send + Sync>> {
        bool_without_compress_decode(src, dst)
    }
}

struct BitPackBooleanCodec();

impl BooleanCodec for BitPackBooleanCodec {
    fn encode(&self, src: &[bool], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        bool_bitpack_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<bool>) -> Result<(), Box<dyn Error + Send + Sync>> {
        bool_bitpack_decode(src, dst)
    }
}

pub trait StringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<MiniVec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}

struct NullStringCodec();

impl StringCodec for NullStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_without_compress_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<MiniVec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_without_compress_decode(src, dst)
    }
}

struct SnappyStringCodec();

impl StringCodec for SnappyStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_snappy_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<MiniVec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_snappy_decode(src, dst)
    }
}

struct GzipStringCodec();

impl StringCodec for GzipStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_gzip_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<MiniVec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_gzip_decode(src, dst)
    }
}

struct BzipStringCodec();

impl StringCodec for BzipStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_bzip_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<MiniVec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_bzip_decode(src, dst)
    }
}

struct ZstdStringCodec();

impl StringCodec for ZstdStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_zstd_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<MiniVec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_zstd_decode(src, dst)
    }
}

struct ZlibStringCodec();

impl StringCodec for ZlibStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_zlib_encode(src, dst)
    }

    fn decode(
        &self,
        src: &[u8],
        dst: &mut Vec<MiniVec<u8>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        str_zlib_decode(src, dst)
    }
}

pub fn get_encoding(src: &[u8]) -> Encoding {
    if src.is_empty() {
        return Encoding::Unknown;
    }
    Encoding::from(src[0])
}

pub fn get_ts_codec(algo: Encoding) -> Box<dyn TimestampCodec> {
    match algo {
        Encoding::Null => Box::new(NullTimestampCodec()),
        Encoding::Delta => Box::new(DeltaTimestampCodec()),
        Encoding::Quantile => Box::new(QuantileTimestampCodec()),
        _ => Box::new(DeltaTimestampCodec()),
    }
}

pub fn get_i64_codec(algo: Encoding) -> Box<dyn IntegerCodec> {
    match algo {
        Encoding::Null => Box::new(NullIntegerCodec()),
        Encoding::Delta => Box::new(DeltaIntegerCodec()),
        Encoding::Quantile => Box::new(QuantileIntegerCodec()),
        _ => Box::new(DeltaIntegerCodec()),
    }
}

pub fn get_u64_codec(algo: Encoding) -> Box<dyn UnsignedCodec> {
    match algo {
        Encoding::Null => Box::new(NullUnsignedCodec()),
        Encoding::Delta => Box::new(DeltaUnsignedCodec()),
        Encoding::Quantile => Box::new(QuantileUnsignedCodec()),
        _ => Box::new(DeltaUnsignedCodec()),
    }
}

pub fn get_f64_codec(algo: Encoding) -> Box<dyn FloatCodec> {
    match algo {
        Encoding::Null => Box::new(NullFloatCodec()),
        Encoding::Gorilla => Box::new(GorillaFloatCodec()),
        Encoding::Quantile => Box::new(QuantileFloatCodec()),
        _ => Box::new(GorillaFloatCodec()),
    }
}

pub fn get_str_codec(algo: Encoding) -> Box<dyn StringCodec> {
    match algo {
        Encoding::Null => Box::new(NullStringCodec()),
        Encoding::Gzip => Box::new(GzipStringCodec()),
        Encoding::Bzip => Box::new(BzipStringCodec()),
        Encoding::Snappy => Box::new(SnappyStringCodec()),
        Encoding::Zstd => Box::new(ZstdStringCodec()),
        Encoding::Zlib => Box::new(ZlibStringCodec()),
        _ => Box::new(SnappyStringCodec()),
    }
}

pub fn get_bool_codec(algo: Encoding) -> Box<dyn BooleanCodec> {
    match algo {
        Encoding::Null => Box::new(NullBooleanCodec()),
        Encoding::BitPack => Box::new(BitPackBooleanCodec()),
        _ => Box::new(BitPackBooleanCodec()),
    }
}
