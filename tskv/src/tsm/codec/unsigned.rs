use std::error::Error;
use std::sync::Arc;

use arrow::buffer::NullBuffer;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, ArrayRef, PrimitiveArray, UInt64Array};
use arrow_schema::DataType::UInt64;

// note: encode/decode adapted from influxdb_iox
// https://github.com/influxdata/influxdb_iox/tree/main/influxdb_tsm/src/encoders

/// Encodes a slice of unsigned 64-bit integers into `dst`.
///
/// Deltas between the integers in the input are first calculated, then the
/// deltas are further compressed if possible, either via bit-packing using
/// simple8b or by run-length encoding the deltas if they're all the same.
pub fn u64_zigzag_simple8b_encode(
    src: &[u64],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let signed = u64_to_i64_vector(src);
    super::integer::i64_zigzag_simple8b_encode(&signed, dst)
}

pub fn u64_pco_encode(src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let signed = u64_to_i64_vector(src);
    super::integer::i64_pco_encode(&signed, dst)
}

pub fn u64_without_compress_encode(
    src: &[u64],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let signed = u64_to_i64_vector(src);
    super::integer::i64_without_compress_encode(&signed, dst)
}

/// Decodes a slice of bytes into a destination vector of unsigned integers.
pub fn u64_zigzag_simple8b_decode(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        let null_value: Vec<Option<u64>> = vec![None; bit_set.len()];
        let array = UInt64Array::from(null_value);
        return Ok(Arc::new(array));
    }
    let array_ref = super::integer::i64_zigzag_simple8b_decode_to_array(src, bit_set)?;
    let builder = array_ref.to_data().into_builder().data_type(UInt64);
    let u64_array: PrimitiveArray<UInt64Type> =
        PrimitiveArray::from(unsafe { builder.build_unchecked() });
    Ok(Arc::new(u64_array))
}

pub fn u64_pco_decode(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        let null_value: Vec<Option<u64>> = vec![None; bit_set.len()];
        let array = UInt64Array::from(null_value);
        return Ok(Arc::new(array));
    }
    let array_ref = super::integer::i64_pco_decode_to_array(src, bit_set)?;
    let builder = array_ref.to_data().into_builder().data_type(UInt64);
    let u64_array: PrimitiveArray<UInt64Type> =
        PrimitiveArray::from(unsafe { builder.build_unchecked() });
    Ok(Arc::new(u64_array))
}

pub fn u64_without_compress_decode(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        let null_value: Vec<Option<u64>> = vec![None; bit_set.len()];
        let array = UInt64Array::from(null_value);
        return Ok(Arc::new(array));
    }

    let array_ref = super::integer::i64_without_compress_decode_to_array(src, bit_set)?;
    let builder = array_ref.to_data().into_builder().data_type(UInt64);
    let u64_array: PrimitiveArray<UInt64Type> =
        PrimitiveArray::from(unsafe { builder.build_unchecked() });
    Ok(Arc::new(u64_array))
}

// Converts a slice of `u64` values to a `Vec<i64>`.
// TODO(edd): this is expensive as it copies. There are cheap
// but unsafe alternatives to look into such as std::mem::transmute
fn u64_to_i64_vector(src: &[u64]) -> Vec<i64> {
    src.iter().map(|&x| x as i64).collect()
}

#[cfg(test)]
#[allow(clippy::unreadable_literal)]
mod tests {

    use super::super::integer::DeltaEncoding;
    use super::super::simple8b;
    use super::*;
    use crate::tsm::codec::{get_encoding, Encoding};

    #[test]
    fn encode_no_values() {
        let src: Vec<u64> = vec![];
        let mut dst = vec![];

        // check for error
        u64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        assert_eq!(dst.len(), 0);
    }

    #[test]
    fn encode_uncompressed() {
        let src: Vec<u64> = vec![1000, 0, simple8b::MAX_VALUE, 213123421];
        let mut dst = vec![];

        u64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");

        // verify uncompressed encoding used
        assert_eq!(&dst[0] >> 4, DeltaEncoding::Uncompressed as u8);

        let null_bitset = NullBuffer::new_valid(src.len());
        let array_ref = u64_zigzag_simple8b_decode(&dst, &null_bitset).expect("failed to decode");
        let array = array_ref.as_any().downcast_ref::<UInt64Array>().unwrap();

        let expected = UInt64Array::from_iter(src.iter().cloned());
        // verify got same values back
        assert_eq!(*array, expected);
    }

    #[test]
    fn encode_pco_and_uncompress() {
        let src: Vec<u64> = vec![1000, 0, simple8b::MAX_VALUE, 213123421];
        let mut dst = vec![];

        u64_pco_encode(&src, &mut dst).unwrap();
        let exp_code_type = Encoding::Quantile;
        let got_code_type = get_encoding(&dst);
        assert_eq!(exp_code_type, got_code_type);

        let null_bitset = NullBuffer::new_valid(src.len());
        let array_ref = u64_pco_decode(&dst, &null_bitset).unwrap();
        let array = array_ref.as_any().downcast_ref::<UInt64Array>().unwrap();

        let expected = UInt64Array::from_iter(src.iter().cloned());
        assert_eq!(*array, expected);

        dst.clear();

        u64_without_compress_encode(&src, &mut dst).unwrap();
        let exp_code_type = Encoding::Null;
        let got_code_type = get_encoding(&dst);
        assert_eq!(exp_code_type, got_code_type);

        let array_ref = u64_without_compress_decode(&dst, &null_bitset).unwrap();
        let array = array_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(*array, expected);
    }

    struct Test {
        name: String,
        input: Vec<u64>,
    }

    #[test]
    fn encode_rle() {
        let tests = vec![
            Test {
                name: String::from("no delta"),
                input: vec![123; 8],
            },
            Test {
                name: String::from("delta increasing"),
                input: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            },
            Test {
                name: String::from("delta decreasing"),
                input: vec![350, 200, 50],
            },
        ];

        for test in tests {
            let mut dst = vec![];
            let src = test.input.clone();
            u64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");

            // verify RLE encoding used
            assert_eq!(
                &dst[1] >> 4,
                DeltaEncoding::Rle as u8,
                "didn't use rle on {:?}",
                src
            );
            let null_bitset = NullBuffer::new_valid(src.len());
            let array_ref =
                u64_zigzag_simple8b_decode(&dst, &null_bitset).expect("failed to decode");
            let array = array_ref.as_any().downcast_ref::<UInt64Array>().unwrap();

            let expected = UInt64Array::from_iter(src.iter().cloned());
            // verify got same values back
            assert_eq!(*array, expected, "{}", test.name);
        }
    }

    #[test]
    fn encode_rle_byte_for_byte_with_go() {
        let mut dst = vec![];
        let src = vec![1232342341234u64; 1000];
        u64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");

        let expected_encoded = vec![32, 0, 0, 2, 61, 218, 167, 172, 228, 0, 231, 7];
        assert_eq!(dst[1..], expected_encoded);

        assert_eq!(&dst[1] >> 4, DeltaEncoding::Rle as u8);

        let null_bitset = NullBuffer::new_valid(src.len());
        let array_ref = u64_zigzag_simple8b_decode(&dst, &null_bitset).expect("failed to decode");
        let array = array_ref.as_any().downcast_ref::<UInt64Array>().unwrap();

        let expected = UInt64Array::from_iter(src.iter().cloned());
        assert_eq!(*array, expected);
    }

    #[test]
    fn encode_simple8b() {
        let tests = vec![Test {
            name: String::from("positive"),
            input: vec![1, 11, 3124, 123543256, 2398567984273478],
        }];

        for test in tests {
            let mut dst = vec![];
            let src = test.input.clone();
            u64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");
            // verify Simple8b encoding used
            assert_eq!(&dst[1] >> 4, DeltaEncoding::Simple8b as u8);

            let null_bitset = NullBuffer::new_valid(src.len());
            let array_ref =
                u64_zigzag_simple8b_decode(&dst, &null_bitset).expect("failed to decode");
            let array = array_ref.as_any().downcast_ref::<UInt64Array>().unwrap();

            let expected = UInt64Array::from_iter(src.iter().cloned());
            // verify got same values back
            assert_eq!(*array, expected, "{}", test.name);
        }
    }

    #[test]
    // This tests against a defect found when decoding a TSM block from InfluxDB.
    fn rle_regression() {
        let values = vec![809201799168u64; 509];
        let mut enc = vec![];
        u64_zigzag_simple8b_encode(&values, &mut enc).expect("encoding failed");

        // this is a compressed rle integer block representing 509 identical
        // 809201799168 values.
        let enc_influx = [32, 0, 0, 1, 120, 208, 95, 32, 0, 0, 252, 3];

        // ensure that encoder produces same bytes as InfluxDB encoder.
        assert_eq!(enc[1..], enc_influx);

        let null_bitset = NullBuffer::new_valid(values.len());
        let array_ref = u64_zigzag_simple8b_decode(&enc, &null_bitset).expect("failed to decode");
        let array = array_ref.as_any().downcast_ref::<UInt64Array>().unwrap();

        let expected = UInt64Array::from_iter(values.iter().cloned());
        // verify got same values back
        assert_eq!(*array, expected);
    }
}
