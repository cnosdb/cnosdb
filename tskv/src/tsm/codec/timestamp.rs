use std::error::Error;
use std::sync::Arc;

use arrow::buffer::NullBuffer;
use arrow_array::builder::Int64Builder;
use arrow_array::{ArrayRef, Int64Array};
use integer_encoding::*;
use pco::standalone::{simple_decompress, simpler_compress};
use pco::DEFAULT_COMPRESSION_LEVEL;

use super::simple8b;
use crate::byte_utils::decode_be_i64;
use crate::tsm::codec::Encoding;

// note: encode/decode adapted from influxdb_iox
// https://github.com/influxdata/influxdb_iox/tree/main/influxdb_tsm/src/encoders

// Encoding describes the type of encoding used by an encoded timestamp block.
enum DeltaEncoding {
    Uncompressed = 0,
    Simple8b = 1,
    Rle = 2,
}

pub fn ts_without_compress_encode(
    src: &[i64],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }
    dst.push(Encoding::Null as u8);

    for i in src.iter() {
        dst.extend_from_slice(i.to_be_bytes().as_slice());
    }
    Ok(())
}

pub fn ts_pco_encode(src: &[i64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }

    dst.push(Encoding::Quantile as u8);

    dst.append(&mut simpler_compress(src, DEFAULT_COMPRESSION_LEVEL)?);
    Ok(())
}

/// encode encodes a vector of signed integers into a slice of bytes.
///
/// To maximise compression, the provided vector should be sorted in ascending
/// order. First deltas between the integers are determined, then further
/// encoding is potentially carried out. If all the deltas are the same the
/// block can be encoded using RLE. If not, as long as the deltas are not bigger
/// than simple8b::MAX_VALUE they can be encoded using simple8b.
pub fn ts_zigzag_simple8b_encode(
    src: &[i64],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }
    dst.push(Encoding::DeltaTs as u8);

    let mut max: u64 = 0;
    let mut deltas = i64_to_u64_vector(src);
    if deltas.len() > 1 {
        for i in (1..deltas.len()).rev() {
            deltas[i] = deltas[i].wrapping_sub(deltas[i - 1]);
            if deltas[i] > max {
                max = deltas[i];
            }
        }
        let mut use_rle = true;
        for i in 2..deltas.len() {
            if deltas[1] != deltas[i] {
                use_rle = false;
                break;
            }
        }

        // Encode with RLE if possible.
        if use_rle {
            encode_rle(deltas[0], deltas[1], deltas.len() as u64, dst);
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
    // First find the divisor for the deltas.
    let mut div: u64 = 1_000_000_000_000;
    for delta in deltas.iter().skip(1) {
        if div <= 1 {
            break;
        }
        while div > 1 && delta % div != 0 {
            div /= 10;
        }
    }

    if div > 1 {
        // apply only if expense of division is warranted
        for delta in deltas.iter_mut().skip(1) {
            *delta /= div
        }
    }

    // first 4 high bits used for encoding type
    dst.push((DeltaEncoding::Simple8b as u8) << 4);
    dst[1] |= ((div as f64).log10()) as u8; // 4 low bits used for log10 divisor
    dst.extend_from_slice(&deltas[0].to_be_bytes()); // encode first value
    simple8b::encode(&deltas[1..], dst)?;
    Ok(())
}

// i64_to_u64_vector converts a Vec<i64> to Vec<u64>.
// TODO(edd): this is expensive as it copies. There are cheap
// but unsafe alternatives to look into such as std::mem::transmute
fn i64_to_u64_vector(src: &[i64]) -> Vec<u64> {
    src.iter().map(|x| *x as u64).collect::<Vec<u64>>()
}

// encode_rle encodes the value v, delta and count into dst.
//
// v should be the first element of a sequence, delta the difference that each
// value in the sequence differs by, and count the total number of values in the
// sequence.
fn encode_rle(v: u64, delta: u64, count: u64, dst: &mut Vec<u8>) {
    use super::MAX_VAR_INT_64;

    // Keep a byte back for the scaler.
    dst.push(0);
    let mut n = 2;
    // write the first value in as a byte array.
    dst.extend_from_slice(&v.to_be_bytes());
    n += 8;

    // check delta's divisor
    let mut div: u64 = 1_000_000_000_000;
    while div > 1 && delta % div != 0 {
        div /= 10;
    }

    if dst.len() <= n + MAX_VAR_INT_64 {
        dst.resize(n + MAX_VAR_INT_64, 0);
    }

    // 4 low bits are the log10 divisor.
    if div > 1 {
        // calculate and store the number of trailing 0s in the divisor.
        // e.g., 100_000 would be stored as 5.
        let scaler = ((div as f64).log10()) as u8;
        assert!(scaler <= 15);

        dst[1] |= scaler; // Set the scaler on low 4 bits of first byte.
        n += (delta / div).encode_var(&mut dst[n..]);
    } else {
        n += delta.encode_var(&mut dst[n..]);
    }

    if dst.len() - n <= MAX_VAR_INT_64 {
        dst.resize(n + MAX_VAR_INT_64, 0);
    }
    // finally, encode the number of times the delta is repeated.
    n += count.encode_var(&mut dst[n..]);
    dst.truncate(n);
}

pub fn ts_zigzag_simple8b_decode_to_array(
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
        encoding if encoding == DeltaEncoding::Rle as u8 => decode_rle_to_array(src, bit_set),
        encoding if encoding == DeltaEncoding::Simple8b as u8 => {
            decode_simple8b_to_array(src, bit_set)
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
    let mut prev = 0;
    let mut buf: [u8; 8] = [0; 8];

    let mut builder = Int64Builder::new();
    for is_valid in bit_set.iter() {
        if !is_valid {
            builder.append_null();
            continue;
        }
        if i < src.len() {
            buf.copy_from_slice(&src[i..i + 8]);
            prev += i64::from_be_bytes(buf);
            builder.append_value(prev);
            i += 8;
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn decode_rle_to_array(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.len() < 9 {
        return Err(From::from("not enough data to decode using RLE"));
    }

    // calculate the scaler from the lower 4 bits of the first byte.
    let scaler = 10_u64.pow((src[0] & 0b0000_1111) as u32);
    let mut i = 1;

    let mut a: [u8; 8] = [0; 8];
    a.copy_from_slice(&src[i..i + 8]);
    i += 8;
    let (mut delta, _n) = u64::decode_var(&src[i..]).ok_or("unable to decode delta")?;
    delta *= scaler;

    let mut is_first = true;
    let mut first = 0;
    let mut builder = Int64Builder::with_capacity(bit_set.len());
    for is_valid in bit_set.iter() {
        if !is_valid {
            builder.append_null();
            continue;
        }
        if is_first {
            first = i64::from_be_bytes(a);
            builder.append_value(first);
            is_first = false;
            continue;
        }
        first = first.wrapping_add(delta as i64);
        builder.append_value(first);
    }
    Ok(Arc::new(builder.finish()))
}

fn decode_simple8b_to_array(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.len() < 9 {
        return Err(From::from("not enough data to decode packed timestamp"));
    }

    let scaler = 10_u64.pow((src[0] & 0b0000_1111) as u32);

    let mut res = vec![];
    let mut buf: [u8; 8] = [0; 8];
    buf.copy_from_slice(&src[1..9]);
    let mut next = i64::from_be_bytes(buf);
    let mut builder = Int64Builder::with_capacity(bit_set.len());
    let mut idx = 0;
    if bit_set.is_valid(idx) {
        builder.append_value(next);
    } else {
        builder.append_null();
    }
    idx += 1;
    simple8b::decode(&src[9..], &mut res);

    let mut value_iter = res.into_iter();
    while idx < bit_set.len() {
        if bit_set.is_valid(idx) {
            if let Some(value) = value_iter.next() {
                next += if scaler > 1 {
                    (value * scaler) as i64
                } else {
                    value as i64
                };
                builder.append_value(next);
            }
        } else {
            builder.append_null();
        }
        idx += 1;
    }
    Ok(Arc::new(builder.finish()))
}

pub fn ts_without_compress_decode_to_array(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        let null_value: Vec<Option<i64>> = vec![None; bit_set.len()];
        let array = Int64Array::from(null_value);
        return Ok(Arc::new(array));
    }
    let src = &src[1..];
    let mut iter = src.chunks(8);
    let mut builder = Int64Builder::with_capacity(bit_set.len());
    for is_valid in bit_set.iter() {
        if !is_valid {
            builder.append_null();
            continue;
        }
        if let Some(i) = iter.next() {
            builder.append_value(decode_be_i64(i));
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn ts_pco_decode_to_array(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        let null_value: Vec<Option<i64>> = vec![None; bit_set.len()];
        let array = Int64Array::from(null_value);
        return Ok(Arc::new(array));
    }

    let src = &src[1..];
    let decode: Vec<i64> = simple_decompress(src)?;
    let mut iter = decode.iter();
    let mut builder = Int64Builder::with_capacity(bit_set.len());
    for is_valid in bit_set.iter() {
        if !is_valid {
            builder.append_null();
            continue;
        }
        if let Some(val) = iter.next() {
            builder.append_value(*val);
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
#[allow(clippy::unreadable_literal)]
mod tests {

    use super::*;
    use crate::tsm::codec::get_encoding;

    #[test]
    fn encode_no_values() {
        let src: Vec<i64> = vec![];
        let mut dst = vec![];

        // check for error
        ts_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst.len(), 0);
        ts_pco_encode(&src, &mut dst).unwrap();
        assert_eq!(dst.len(), 0);
        ts_without_compress_encode(&src, &mut dst).unwrap();
        assert_eq!(dst.len(), 0);
    }

    #[test]
    fn encode_uncompressed() {
        let src: Vec<i64> = vec![-1000, 0, simple8b::MAX_VALUE as i64, 213123421];
        let mut dst = vec![];

        ts_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");

        // verify uncompressed encoding used
        assert_eq!(&dst[1] >> 4, DeltaEncoding::Uncompressed as u8);

        let null_bitset = NullBuffer::new_valid(src.len());
        let array_ref = ts_zigzag_simple8b_decode_to_array(&dst, &null_bitset).unwrap();
        let array = array_ref.as_any().downcast_ref::<Int64Array>().unwrap();

        let expected = Int64Array::from_iter(src.iter().cloned());
        // verify got same values back
        assert_eq!(*array, expected);
    }

    #[test]
    fn encode_pco_and_uncompress() {
        let src: Vec<i64> = vec![-1000, 0, simple8b::MAX_VALUE as i64, 213123421];
        let mut dst = vec![];

        ts_pco_encode(&src, &mut dst).unwrap();
        let exp_code_type = Encoding::Quantile;
        let got_code_type = get_encoding(&dst);
        assert_eq!(exp_code_type, got_code_type);

        dst.clear();

        ts_without_compress_encode(&src, &mut dst).unwrap();
        let exp_code_type = Encoding::Null;
        let got_code_type = get_encoding(&dst);
        assert_eq!(exp_code_type, got_code_type);

        let null_bitset = NullBuffer::new_valid(src.len());
        let array_ref = ts_without_compress_decode_to_array(&dst, &null_bitset).unwrap();
        let array = array_ref.as_any().downcast_ref::<Int64Array>().unwrap();

        let expected = Int64Array::from_iter(src.iter().cloned());
        // verify got same values back
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
                input: vec![-2398749823764923; 10000],
            },
            Test {
                name: String::from("no delta negative"),
                input: vec![-345632452354; 1000],
            },
            Test {
                name: String::from("delta positive 1"),
                input: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            },
            Test {
                name: String::from("delta positive 2000"),
                input: vec![100, 2100, 4100, 6100, 8100, 10100, 12100, 14100],
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
            ts_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");

            // verify RLE encoding used
            assert_eq!(&dst[1] >> 4, DeltaEncoding::Rle as u8);

            let null_bitset = NullBuffer::new_valid(src.len());
            let array_ref = ts_zigzag_simple8b_decode_to_array(&dst, &null_bitset).unwrap();
            let array = array_ref.as_any().downcast_ref::<Int64Array>().unwrap();

            let expected = Int64Array::from_iter(src.iter().cloned());
            // verify got same values back
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
            ts_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");
            // verify Simple8b encoding used
            assert_eq!(&dst[1] >> 4, DeltaEncoding::Simple8b as u8);

            let null_bitset = NullBuffer::new_valid(src.len());
            let array_ref =
                ts_zigzag_simple8b_decode_to_array(&dst, &null_bitset).expect("failed to decode");
            let array = array_ref.as_any().downcast_ref::<Int64Array>().unwrap();

            let expected = Int64Array::from_iter(src.iter().cloned());
            // verify got same values back
            assert_eq!(*array, expected, "{}", test.name);
        }
    }
}
