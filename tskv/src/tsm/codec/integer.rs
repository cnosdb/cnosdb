use std::error::Error;
use std::sync::Arc;

use arrow::buffer::NullBuffer;
use arrow_array::builder::Int64Builder;
use arrow_array::{ArrayRef, Int64Array};
use integer_encoding::*;

use super::simple8b;
use crate::tsm::codec::timestamp::{
    ts_pco_decode_to_array, ts_pco_encode, ts_without_compress_decode_to_array,
    ts_without_compress_encode,
};
use crate::tsm::codec::Encoding;

// note: encode/decode adapted from influxdb_iox
// https://github.com/influxdata/influxdb_iox/tree/main/influxdb_tsm/src/encoders

/// Encoding describes the type of encoding used by an encoded integer block.
#[derive(Debug, Clone, Copy)]
pub enum DeltaEncoding {
    Uncompressed = 0,
    Simple8b = 1,
    Rle = 2,
}

pub fn i64_pco_encode(src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
    ts_pco_encode(src, dst)
}

pub fn i64_without_compress_encode(
    src: &[i64],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    ts_without_compress_encode(src, dst)
}

/// encode encodes a vector of signed integers into dst.
///
/// Deltas between the integers in the vector are first calculated, and these
/// deltas are then zig-zag encoded. The resulting zig-zag encoded deltas are
/// further compressed if possible, either via bit-packing using simple8b or by
/// run-length encoding the deltas if they're all the same.
pub fn i64_zigzag_simple8b_encode(
    src: &[i64],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }
    dst.push(Encoding::Delta as u8);

    let mut max: u64 = 0;
    let mut deltas = i64_to_u64_vector(src);
    for i in (1..deltas.len()).rev() {
        deltas[i] = zig_zag_encode(deltas[i].wrapping_sub(deltas[i - 1]) as i64);
        if deltas[i] > max {
            max = deltas[i];
        }
    }

    // deltas[0] is the first value in the sequence.
    deltas[0] = zig_zag_encode(src[0]);

    if deltas.len() > 2 {
        let mut use_rle = true;
        for i in 2..deltas.len() {
            if deltas[1] != deltas[i] {
                use_rle = false;
                break;
            }
        }

        // Encode with RLE if possible.
        if use_rle {
            // count is the number of deltas repeating excluding first value.
            encode_rle(deltas[0], deltas[1], deltas.len() as u64 - 1, dst);
            // 4 high bits of first byte used for the encoding type
            dst[1] |= (DeltaEncoding::Rle as u8) << 4;
            return Ok(());
        }
    }

    // write block uncompressed
    if max > simple8b::MAX_VALUE {
        let cap = 2 + (deltas.len() * 8); // 8 bytes per value plus header byte
        if dst.capacity() < cap {
            dst.reserve_exact(cap - dst.capacity());
        }
        dst.push((DeltaEncoding::Uncompressed as u8) << 4);
        for delta in &deltas {
            dst.extend_from_slice(&delta.to_be_bytes());
        }
        return Ok(());
    }

    // Compress with simple8b
    // first 4 high bits used for encoding type
    dst.push((DeltaEncoding::Simple8b as u8) << 4);
    dst.extend_from_slice(&deltas[0].to_be_bytes()); // encode first value
    simple8b::encode(&deltas[1..], dst)?;
    Ok(())
}

// zig_zag_encode converts a signed integer into an unsigned one by zig zagging
// negative and positive values across even and odd numbers.
//
// Eg. [0,-1,1,-2] becomes [0, 1, 2, 3].
fn zig_zag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

// zig_zag_decode converts a zig zag encoded unsigned integer into an signed
// integer.
fn zig_zag_decode(v: u64) -> i64 {
    ((v >> 1) ^ ((((v & 1) as i64) << 63) >> 63) as u64) as i64
}

// Converts a slice of `i64` values to a `Vec<u64>`.
// TODO(edd): this is expensive as it copies. There are cheap
// but unsafe alternatives to look into such as std::mem::transmute
fn i64_to_u64_vector(src: &[i64]) -> Vec<u64> {
    src.iter().map(|&x| x as u64).collect()
}

// encode_rle encodes the value v, delta and count into dst.
//
// v should be the first element of a sequence, delta the difference that each
// value in the sequence differs by, and count the number of times that the
// delta is repeated.
fn encode_rle(v: u64, delta: u64, count: u64, dst: &mut Vec<u8>) {
    use super::MAX_VAR_INT_64;
    dst.push(0); // save a byte for encoding type
    dst.extend_from_slice(&v.to_be_bytes()); // write the first value in as a byte array.
    let mut n = 10;

    if dst.len() - n <= MAX_VAR_INT_64 {
        dst.resize(n + MAX_VAR_INT_64, 0);
    }
    n += delta.encode_var(&mut dst[n..]); // encode delta between values

    if dst.len() - n <= MAX_VAR_INT_64 {
        dst.resize(n + MAX_VAR_INT_64, 0);
    }
    n += count.encode_var(&mut dst[n..]); // encode count of values
    dst.truncate(n);
}

pub fn i64_zigzag_simple8b_decode_to_array(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        let null_value: Vec<Option<i64>> = vec![None; bit_set.len()];
        let array = Int64Array::from(null_value);
        return Ok(Arc::new(array));
    }
    let src = &src[1..];
    let encoding = &src[0] >> 4;
    match encoding {
        encoding if encoding == DeltaEncoding::Uncompressed as u8 => {
            decode_uncompressed_to_array(&src[1..], bit_set) // first byte not used
        }
        encoding if encoding == DeltaEncoding::Rle as u8 => decode_rle_to_array(&src[1..], bit_set),
        encoding if encoding == DeltaEncoding::Simple8b as u8 => {
            decode_simple8b_to_array(&src[1..], bit_set)
        }
        _ => Err(From::from("invalid block encoding")),
    }
}

fn decode_uncompressed_to_array(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.is_empty() || src.len() & 0x7 != 0 {
        return Err(From::from("invalid uncompressed block length"));
    }
    let mut i = 0;
    let mut prev: i64 = 0;
    let mut buf: [u8; 8] = [0; 8];
    let mut builder = Int64Builder::with_capacity(bit_set.len());
    for is_valid in bit_set.iter() {
        if !is_valid {
            builder.append_null();
            continue;
        }
        buf.copy_from_slice(&src[i..i + 8]);
        prev = prev.wrapping_add(zig_zag_decode(u64::from_be_bytes(buf)));
        builder.append_value(prev); // N.B - signed integer...
        i += 8;
    }
    Ok(Arc::new(builder.finish()))
}

fn decode_rle_to_array(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.len() < 8 {
        return Err(From::from("not enough data to decode using RLE"));
    }

    let (delta, _n) = u64::decode_var(&src[8..]).ok_or("unable to decode delta")?;

    let mut a: [u8; 8] = [0; 8];
    a.copy_from_slice(&src[0..8]);
    let mut first = zig_zag_decode(u64::from_be_bytes(a));
    let delta_z = zig_zag_decode(delta);
    let mut is_first = true;
    let mut builder = Int64Builder::with_capacity(bit_set.len());
    for is_valid in bit_set.iter() {
        if !is_valid {
            builder.append_null();
            continue;
        }
        if is_first {
            // first values stored raw
            builder.append_value(first);
            is_first = false;
            continue;
        }
        first = first.wrapping_add(delta_z);
        builder.append_value(first);
    }
    Ok(Arc::new(builder.finish()))
}

fn decode_simple8b_to_array(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.len() < 8 {
        return Err(From::from("not enough data to decode packed integer."));
    }

    // TODO(edd): pre-allocate res by counting bytes in encoded slice?
    let mut res = vec![];
    let mut buf: [u8; 8] = [0; 8];
    buf.copy_from_slice(&src[0..8]);
    let mut first_val = true;
    let mut next = 0;
    let mut iter = res.iter();
    let mut builder = Int64Builder::with_capacity(bit_set.len());
    for is_valid in bit_set.iter() {
        if !is_valid {
            builder.append_null();
            continue;
        }
        if first_val {
            next = zig_zag_decode(u64::from_be_bytes(buf));
            builder.append_value(next);
            simple8b::decode(&src[8..], &mut res);
            first_val = false;
            iter = res.iter();
            continue;
        }
        if let Some(v) = iter.next() {
            next += zig_zag_decode(*v);
            builder.append_value(next);
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn i64_without_compress_decode_to_array(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    ts_without_compress_decode_to_array(src, bit_set)
}

pub fn i64_pco_decode_to_array(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    ts_pco_decode_to_array(src, bit_set)
}

#[cfg(test)]
#[allow(clippy::unreadable_literal)]
mod tests {
    use arrow_array::Array;

    use super::*;
    use crate::tsm::codec::{get_encoding, Encoding};

    #[test]
    fn zig_zag_encoding() {
        let input = [-2147483648, -2, -1, 0, 1, 2147483647];
        let exp = [4294967295, 3, 1, 0, 2, 4294967294];
        for (i, v) in input.iter().enumerate() {
            let encoded = zig_zag_encode(*v);
            assert_eq!(encoded, exp[i]);

            let decoded = zig_zag_decode(encoded);
            assert_eq!(decoded, input[i]);
        }
    }

    #[test]
    fn encode_no_values() {
        let src: Vec<i64> = vec![];
        let mut dst = vec![];

        // check for error
        i64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode src");
        i64_pco_encode(&src, &mut dst).unwrap();
        i64_without_compress_encode(&src, &mut dst).unwrap();

        // verify encoded no values.
        assert_eq!(dst.to_vec().len(), 0);
    }

    #[test]
    fn encode_uncompressed() {
        let src: Vec<i64> = vec![-1000, 0, simple8b::MAX_VALUE as i64, 213123421];
        let mut dst = vec![];

        i64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");

        // verify uncompressed encoding used
        assert_eq!(&dst[0] >> 4, DeltaEncoding::Uncompressed as u8);

        let null_bitset = NullBuffer::new_valid(src.len());
        let array_ref =
            i64_zigzag_simple8b_decode_to_array(&dst, &null_bitset).expect("failed to decode");
        let array = array_ref.as_any().downcast_ref::<Int64Array>().unwrap();

        let expected = Int64Array::from_iter(src.iter().cloned());

        // verify got same values back
        assert_eq!(*array, expected);
    }

    #[test]
    fn encode_pco_and_uncompress() {
        let src: Vec<i64> = vec![-1000, 0, simple8b::MAX_VALUE as i64, 213123421];
        let mut dst = vec![];

        i64_pco_encode(&src, &mut dst).unwrap();
        let exp_code_type = Encoding::Quantile;
        let got_code_type = get_encoding(&dst);
        assert_eq!(exp_code_type, got_code_type);

        let null_bitset = NullBuffer::new_valid(src.len());
        let array_ref = i64_pco_decode_to_array(&dst, &null_bitset).unwrap();
        let array = array_ref.as_any().downcast_ref::<Int64Array>().unwrap();

        let expected = Int64Array::from_iter(src.iter().cloned());
        assert_eq!(*array, expected);

        dst.clear();

        i64_without_compress_encode(&src, &mut dst).unwrap();
        let exp_code_type = Encoding::Null;
        let got_code_type = get_encoding(&dst);
        assert_eq!(exp_code_type, got_code_type);

        let array_ref = i64_without_compress_decode_to_array(&dst, &null_bitset).unwrap();
        let array = array_ref.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(*array, expected);
    }

    #[test]
    fn encode_rle() {
        struct Test {
            name: String,
            input: Vec<i64>,
        }

        let tests = vec![
            Test {
                name: String::from("no delta positive"),
                input: vec![123; 8],
            },
            Test {
                name: String::from("no delta negative"),
                input: vec![-345632452354; 1000],
            },
            Test {
                name: String::from("delta positive"),
                input: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            },
            Test {
                name: String::from("delta negative"),
                input: vec![-350, -200, -50],
            },
            Test {
                name: String::from("delta mixed"),
                input: vec![-35000, -5000, 25000, 55000],
            },
            Test {
                name: String::from("delta descending"),
                input: vec![100, 50, 0, -50, -100, -150],
            },
        ];

        for test in tests {
            let mut dst = vec![];
            let src = test.input.clone();
            i64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");

            // verify RLE encoding used
            assert_eq!(&dst[1] >> 4, DeltaEncoding::Rle as u8);

            let null_bitset = NullBuffer::new_valid(src.len());
            let array_ref = i64_zigzag_simple8b_decode_to_array(&dst, &null_bitset).unwrap();
            let array = array_ref.as_any().downcast_ref::<Int64Array>().unwrap();

            let expected = Int64Array::from_iter(src.iter().cloned());
            assert_eq!(*array, expected, "{}", test.name);
        }
    }

    #[test]
    fn encode_simple8b() {
        struct Test {
            name: String,
            input: Vec<i64>,
        }

        let tests = vec![
            Test {
                name: String::from("positive"),
                input: vec![1, 11, 3124, 123543256, 2398567984273478],
            },
            Test {
                name: String::from("negative"),
                input: vec![-109290, -1234, -123, -12],
            },
            Test {
                name: String::from("mixed"),
                input: vec![-109290, -1234, -123, -12, 0, 0, 0, 1234, 44444, 4444444],
            },
        ];

        for test in tests {
            let mut dst = vec![];
            let src = test.input.clone();
            i64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");
            // verify Simple8b encoding used
            assert_eq!(&dst[1] >> 4, DeltaEncoding::Simple8b as u8);

            let null_bitset = NullBuffer::new_valid(src.len());
            let array_ref = i64_zigzag_simple8b_decode_to_array(&dst, &null_bitset).unwrap();
            let array = array_ref.as_any().downcast_ref::<Int64Array>().unwrap();

            let expected = Int64Array::from_iter(src.iter().cloned());
            assert_eq!(*array, expected, "{}", test.name);
        }
    }

    #[test]
    // This tests against a defect found when decoding a TSM block from InfluxDB.
    fn rle_regression() {
        let values = vec![809201799168i64; 509];
        let mut enc = vec![];
        i64_zigzag_simple8b_encode(&values, &mut enc).expect("encoding failed");

        // this is a compressed rle integer block representing 509 identical
        // 809201799168 values.
        let enc_influx = [32, 0, 0, 1, 120, 208, 95, 32, 0, 0, 252, 3];

        // ensure that encoder produces same bytes as InfluxDB encoder.
        assert_eq!(enc[1..], enc_influx);

        let null_bitset = NullBuffer::new_valid(values.len());
        let array_ref =
            i64_zigzag_simple8b_decode_to_array(&enc, &null_bitset).expect("failed to decode");
        let array = array_ref.as_any().downcast_ref::<Int64Array>().unwrap();

        let expected = Int64Array::from_iter(values.iter().cloned());

        // verify got same values back
        assert_eq!(*array, expected);
    }

    #[test]
    // This tests against a defect found when decoding a TSM block from InfluxDB.
    fn simple8b_short_regression() {
        let values = vec![346];
        let mut enc = vec![];
        i64_zigzag_simple8b_encode(&values, &mut enc).expect("encoding failed");

        // this is a compressed simple8b integer block representing the value 346.
        let enc_influx = [16, 0, 0, 0, 0, 0, 0, 2, 180];

        // ensure that encoder produces same bytes as InfluxDB encoder.
        assert_eq!(enc[1..], enc_influx);

        let null_bitset = NullBuffer::new_valid(values.len());
        let array_ref =
            i64_zigzag_simple8b_decode_to_array(&enc, &null_bitset).expect("failed to decode");
        let array = array_ref.as_any().downcast_ref::<Int64Array>().unwrap();

        let expected = Int64Array::from_iter(values.iter().cloned());

        // verify got same values back
        assert_eq!(*array, expected);
    }
}
