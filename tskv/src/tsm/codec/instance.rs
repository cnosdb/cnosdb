use arrow::buffer::NullBuffer;
use arrow_array::ArrayRef;
use minivec::MiniVec;
use models::codec::Encoding;

use super::CodecError;
use crate::tsm::codec::boolean::{
    bool_bitpack_decode, bool_bitpack_encode, bool_without_compress_decode,
    bool_without_compress_encode,
};
use crate::tsm::codec::float::{
    f64_gorilla_decode, f64_gorilla_encode, f64_pco_decode, f64_pco_encode,
    f64_without_compress_decode, f64_without_compress_encode,
};
use crate::tsm::codec::integer::{
    i64_pco_decode_to_array, i64_pco_encode, i64_without_compress_decode_to_array,
    i64_without_compress_encode, i64_zigzag_simple8b_decode_to_array, i64_zigzag_simple8b_encode,
};
use crate::tsm::codec::string::{
    str_bzip_decode, str_bzip_decode_to_array, str_bzip_encode, str_gzip_decode,
    str_gzip_decode_to_array, str_gzip_encode, str_snappy_decode, str_snappy_decode_to_array,
    str_snappy_encode, str_without_compress_decode, str_without_compress_decode_to_array,
    str_without_compress_encode, str_zlib_decode, str_zlib_decode_to_array, str_zlib_encode,
    str_zstd_decode, str_zstd_decode_to_array, str_zstd_encode,
};
use crate::tsm::codec::timestamp::{
    ts_pco_decode_to_array, ts_pco_encode, ts_without_compress_decode_to_array,
    ts_without_compress_encode, ts_zigzag_simple8b_decode_to_array, ts_zigzag_simple8b_encode,
};
use crate::tsm::codec::unsigned::{
    u64_pco_decode, u64_pco_encode, u64_without_compress_decode, u64_without_compress_encode,
    u64_zigzag_simple8b_decode, u64_zigzag_simple8b_encode,
};

pub trait TimestampCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), CodecError>;

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError>;
}

struct NullTimestampCodec();

impl TimestampCodec for NullTimestampCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        ts_without_compress_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        ts_without_compress_decode_to_array(src, bit_set)
    }
}

struct DeltaTsTimestampCodec();

impl TimestampCodec for DeltaTsTimestampCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        ts_zigzag_simple8b_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        ts_zigzag_simple8b_decode_to_array(src, bit_set)
    }
}

impl IntegerCodec for DeltaTsTimestampCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        let codec = self as &dyn TimestampCodec;
        codec.encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        let codec = self as &dyn TimestampCodec;
        codec.decode_to_array(src, bit_set)
    }
}

struct QuantileTimestampCodec();

impl TimestampCodec for QuantileTimestampCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        ts_pco_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        ts_pco_decode_to_array(src, bit_set)
    }
}

pub trait IntegerCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), CodecError>;

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError>;
}

struct NullIntegerCodec();

impl IntegerCodec for NullIntegerCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        i64_without_compress_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        i64_without_compress_decode_to_array(src, bit_set)
    }
}

struct DeltaIntegerCodec();

impl IntegerCodec for DeltaIntegerCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        i64_zigzag_simple8b_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        i64_zigzag_simple8b_decode_to_array(src, bit_set)
    }
}

impl TimestampCodec for DeltaIntegerCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        let codec = self as &dyn IntegerCodec;
        codec.encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        let codec = self as &dyn IntegerCodec;
        codec.decode_to_array(src, bit_set)
    }
}

struct QuantileIntegerCodec();

impl IntegerCodec for QuantileIntegerCodec {
    fn encode(&self, src: &[i64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        i64_pco_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        i64_pco_decode_to_array(src, bit_set)
    }
}

pub trait FloatCodec {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), CodecError>;
    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError>;
}

struct NullFloatCodec();

impl FloatCodec for NullFloatCodec {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        f64_without_compress_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        f64_without_compress_decode(src, bit_set)
    }
}

struct GorillaFloatCodec();

impl FloatCodec for GorillaFloatCodec {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        f64_gorilla_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        f64_gorilla_decode(src, bit_set)
    }
}

struct QuantileFloatCodec();

impl FloatCodec for QuantileFloatCodec {
    fn encode(&self, src: &[f64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        f64_pco_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        f64_pco_decode(src, bit_set)
    }
}

pub trait UnsignedCodec {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), CodecError>;

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError>;
}

struct NullUnsignedCodec();

impl UnsignedCodec for NullUnsignedCodec {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        u64_without_compress_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        u64_without_compress_decode(src, bit_set)
    }
}

struct DeltaUnsignedCodec();

impl UnsignedCodec for DeltaUnsignedCodec {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        u64_zigzag_simple8b_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        u64_zigzag_simple8b_decode(src, bit_set)
    }
}

struct QuantileUnsignedCodec();

impl UnsignedCodec for QuantileUnsignedCodec {
    fn encode(&self, src: &[u64], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        u64_pco_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        u64_pco_decode(src, bit_set)
    }
}

pub trait BooleanCodec {
    fn encode(&self, src: &[bool], dst: &mut Vec<u8>) -> Result<(), CodecError>;
    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError>;
}

struct NullBooleanCodec();

impl BooleanCodec for NullBooleanCodec {
    fn encode(&self, src: &[bool], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        bool_without_compress_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        bool_without_compress_decode(src, bit_set)
    }
}

struct BitPackBooleanCodec();

impl BooleanCodec for BitPackBooleanCodec {
    fn encode(&self, src: &[bool], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        bool_bitpack_encode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        bool_bitpack_decode(src, bit_set)
    }
}

pub trait StringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), CodecError>;
    fn decode(&self, src: &[u8], dst: &mut Vec<MiniVec<u8>>) -> Result<(), CodecError>;

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError>;
}

struct NullStringCodec();

impl StringCodec for NullStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        str_without_compress_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<MiniVec<u8>>) -> Result<(), CodecError> {
        str_without_compress_decode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        str_without_compress_decode_to_array(src, bit_set)
    }
}

struct SnappyStringCodec();

impl StringCodec for SnappyStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        str_snappy_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<MiniVec<u8>>) -> Result<(), CodecError> {
        str_snappy_decode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bitset: &NullBuffer) -> Result<ArrayRef, CodecError> {
        str_snappy_decode_to_array(src, bitset)
    }
}

struct GzipStringCodec();

impl StringCodec for GzipStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        str_gzip_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<MiniVec<u8>>) -> Result<(), CodecError> {
        str_gzip_decode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bitset: &NullBuffer) -> Result<ArrayRef, CodecError> {
        str_gzip_decode_to_array(src, bitset)
    }
}

struct BzipStringCodec();

impl StringCodec for BzipStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        str_bzip_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<MiniVec<u8>>) -> Result<(), CodecError> {
        str_bzip_decode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        str_bzip_decode_to_array(src, bit_set)
    }
}

struct ZstdStringCodec();

impl StringCodec for ZstdStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        str_zstd_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<MiniVec<u8>>) -> Result<(), CodecError> {
        str_zstd_decode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        str_zstd_decode_to_array(src, bit_set)
    }
}

struct ZlibStringCodec();

impl StringCodec for ZlibStringCodec {
    fn encode(&self, src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), CodecError> {
        str_zlib_encode(src, dst)
    }

    fn decode(&self, src: &[u8], dst: &mut Vec<MiniVec<u8>>) -> Result<(), CodecError> {
        str_zlib_decode(src, dst)
    }

    fn decode_to_array(&self, src: &[u8], bit_set: &NullBuffer) -> Result<ArrayRef, CodecError> {
        str_zlib_decode_to_array(src, bit_set)
    }
}

pub fn get_encoding(src: &[u8]) -> Encoding {
    if src.is_empty() {
        return Encoding::Unknown;
    }
    Encoding::from(src[0])
}

pub fn get_ts_codec(algo: Encoding) -> Box<dyn TimestampCodec + Send + Sync> {
    match algo {
        Encoding::Null => Box::new(NullTimestampCodec()),
        Encoding::Delta => Box::new(DeltaIntegerCodec()),
        Encoding::DeltaTs => Box::new(DeltaTsTimestampCodec()),
        Encoding::Quantile => Box::new(QuantileTimestampCodec()),
        _ => Box::new(DeltaTsTimestampCodec()),
    }
}

pub fn get_i64_codec(algo: Encoding) -> Box<dyn IntegerCodec + Send + Sync> {
    match algo {
        Encoding::Null => Box::new(NullIntegerCodec()),
        Encoding::Delta => Box::new(DeltaIntegerCodec()),
        Encoding::DeltaTs => Box::new(DeltaTsTimestampCodec()),
        Encoding::Quantile => Box::new(QuantileIntegerCodec()),
        _ => Box::new(DeltaIntegerCodec()),
    }
}

pub fn get_u64_codec(algo: Encoding) -> Box<dyn UnsignedCodec + Send + Sync> {
    match algo {
        Encoding::Null => Box::new(NullUnsignedCodec()),
        Encoding::Delta => Box::new(DeltaUnsignedCodec()),
        Encoding::Quantile => Box::new(QuantileUnsignedCodec()),
        _ => Box::new(DeltaUnsignedCodec()),
    }
}

pub fn get_f64_codec(algo: Encoding) -> Box<dyn FloatCodec + Send + Sync> {
    match algo {
        Encoding::Null => Box::new(NullFloatCodec()),
        Encoding::Gorilla => Box::new(GorillaFloatCodec()),
        Encoding::Quantile => Box::new(QuantileFloatCodec()),
        _ => Box::new(GorillaFloatCodec()),
    }
}

pub fn get_str_codec(algo: Encoding) -> Box<dyn StringCodec + Send + Sync> {
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

pub fn get_bool_codec(algo: Encoding) -> Box<dyn BooleanCodec + Send + Sync> {
    match algo {
        Encoding::Null => Box::new(NullBooleanCodec()),
        Encoding::BitPack => Box::new(BitPackBooleanCodec()),
        _ => Box::new(BitPackBooleanCodec()),
    }
}
