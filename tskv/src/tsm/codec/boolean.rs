use std::convert::TryInto;
use std::error::Error;
use std::sync::Arc;

use arrow::buffer::NullBuffer;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{ArrayRef, BooleanArray};
use integer_encoding::VarInt;

use crate::tsm::codec::Encoding;
// note: encode/decode adapted from influxdb_iox
// https://github.com/influxdata/influxdb_iox/tree/main/influxdb_tsm/src/encoders

/// The header consists of one byte indicating the compression type.
const HEADER_LEN: usize = 1;
/// A bit packed format using 1 bit per boolean. This is the only available
/// boolean compression format at this time.
const BOOLEAN_COMPRESSED_BIT_PACKED: u8 = 1;

/// Encodes a slice of booleans into `dst`.
///
/// Boolean encoding uses 1 bit per value. Each compressed byte slice contains a
/// 1 byte header indicating the compression type, followed by a variable byte
/// encoded length indicating how many booleans are packed in the slice. The
/// remaining bytes contain 1 byte for every 8 boolean values encoded.
pub fn bool_bitpack_encode(
    src: &[bool],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }
    dst.push(Encoding::BitPack as u8);

    let size = HEADER_LEN + 8 + ((src.len() + 7) / 8); // Header + Num bools + bool data.
    dst.resize(size + 1, 0);

    // Store the encoding type in the 4 high bits of the first byte
    dst[1] = BOOLEAN_COMPRESSED_BIT_PACKED << 4;

    let mut n = 8u64; // Current bit in current byte.

    // Encode the number of booleans written.
    let len_u64: u64 = src.len().try_into()?;
    let i = len_u64.encode_var(&mut dst[2..]);
    let step: u64 = (i * 8).try_into()?;
    n += step;

    for &v in src {
        let index: usize = (n >> 3).try_into()?;
        if v {
            dst[index + 1] |= 128 >> (n & 7); // Set current bit on current byte.
        } else {
            dst[index + 1] &= !(128 >> (n & 7)); // Clear current bit on current
                                                 // byte.
        }
        n += 1;
    }

    let mut length = n >> 3;
    if n & 7 > 0 {
        length += 1; // Add an extra byte to capture overflowing bits.
    }
    let length: usize = length.try_into()?;

    dst.truncate(length + 1);

    Ok(())
}

pub fn bool_without_compress_encode(
    src: &[bool],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.push(Encoding::Null as u8);
    for i in src {
        if *i {
            dst.push(1);
        } else {
            dst.push(0);
        }
    }
    Ok(())
}

/// Decodes a slice of bytes into a destination vector of `bool`s.
pub fn bool_bitpack_decode(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(Arc::new(BooleanArray::from(vec![None; bit_set.len()])));
    }

    let src = &src[1..]; // 跳过第一个字节（编码类型）
    assert_eq!(src[0], BOOLEAN_COMPRESSED_BIT_PACKED << 4);
    let src = &src[HEADER_LEN..];

    let (count, num_bytes_read) = u64::decode_var(src).ok_or("boolean decoder: invalid count")?;
    let count: usize = count.try_into()?;
    let src = &src[num_bytes_read..];

    let mut builder = BooleanBuilder::with_capacity(bit_set.len());
    let mut bit_index = 0;

    for is_valid in bit_set.iter() {
        if is_valid {
            if bit_index >= count {
                return Err("Insufficient data for decoding".into());
            }
            let byte = src[bit_index / 8];
            let bit = (byte >> (7 - (bit_index % 8))) & 1;
            builder.append_value(bit == 1);
            bit_index += 1;
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}
pub fn bool_without_compress_decode(
    src: &[u8],
    bit_set: &NullBuffer,
) -> Result<ArrayRef, Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(Arc::new(BooleanArray::from(vec![] as Vec<bool>)));
    }

    let src = &src[1..];
    let mut iter = src.iter();
    let mut builder = BooleanBuilder::with_capacity(bit_set.len());
    for is_valid in bit_set.iter() {
        if is_valid {
            if let Some(v) = iter.next() {
                if *v == 1 {
                    builder.append_value(true);
                } else {
                    builder.append_value(false);
                }
            }
        } else {
            builder.append_null();
        }
    }
    let array = builder.finish();
    Ok(Arc::new(array))
}

#[cfg(test)]
mod tests {
    use arrow::buffer::BooleanBuffer;
    use arrow_array::Array;

    use super::*;

    #[test]
    fn encode_no_values() {
        let src: Vec<bool> = vec![];
        let mut dst = vec![];

        // check for error
        bool_bitpack_encode(&src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        assert_eq!(dst.len(), 0);
    }

    #[test]
    fn encode_single_true() {
        let src = vec![true];
        let mut dst = vec![];

        bool_bitpack_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst[1..], vec![16, 1, 128]);
    }

    #[test]
    fn encode_single_false() {
        let src = vec![false];
        let mut dst = vec![];

        bool_bitpack_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst[1..], vec![16, 1, 0]);
    }

    #[test]
    fn encode_multi_compressed() {
        let src: Vec<_> = (0..10).map(|i| i % 2 == 0).collect();
        let mut dst = vec![];

        bool_bitpack_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst[1..], vec![16, 10, 170, 128]);
    }

    #[test]
    fn decode_no_values() {
        let src: Vec<u8> = vec![];
        let null_bitset = NullBuffer::new(BooleanBuffer::from(vec![]));

        let array = bool_bitpack_decode(&src, &null_bitset).unwrap();

        assert_eq!(array.len(), 0);
    }

    #[test]
    fn decode_single_true() {
        let src = vec![0, 16, 1, 128];
        let null_bitset = NullBuffer::new_valid(1);

        let array_ref = bool_bitpack_decode(&src, &null_bitset).expect("failed to decode src");
        let array = array_ref.as_any().downcast_ref::<BooleanArray>().unwrap();
        let expected = BooleanArray::new(BooleanBuffer::from(vec![true]), Some(null_bitset));
        assert_eq!(*array, expected);
    }

    #[test]
    fn decode_single_false() {
        let src = vec![0, 16, 1, 0];
        let null_bitset = NullBuffer::new(BooleanBuffer::from(vec![true]));

        let array_ref = bool_bitpack_decode(&src, &null_bitset).expect("failed to decode src");
        let array = array_ref.as_any().downcast_ref::<BooleanArray>().unwrap();
        let expected = BooleanArray::new(BooleanBuffer::from(vec![false]), Some(null_bitset));
        assert_eq!(*array, expected);
    }

    #[test]
    fn decode_multi_compressed() {
        let src = vec![0, 16, 10, 170, 128];
        let null_bitset = NullBuffer::new(BooleanBuffer::from(vec![true; 10]));

        let array_ref = bool_bitpack_decode(&src, &null_bitset).expect("failed to decode src");
        let array = array_ref.as_any().downcast_ref::<BooleanArray>().unwrap();
        let expected: Vec<_> = (0..10).map(|i| i % 2 == 0).collect();
        let expected_array = BooleanArray::new(BooleanBuffer::from(expected), Some(null_bitset));
        assert_eq!(*array, expected_array);
    }

    #[test]
    fn test_bool_encode_decode() {
        let mut data = vec![];
        for _ in 0..100 {
            data.push(rand::random::<bool>());
        }

        let mut dst = vec![];

        let null_bitset = NullBuffer::new(BooleanBuffer::from(vec![true; 100]));

        bool_bitpack_encode(&data, &mut dst).unwrap();
        let array_ref = bool_bitpack_decode(&dst, &null_bitset).unwrap();
        let array = array_ref.as_any().downcast_ref::<BooleanArray>().unwrap();

        let expected = BooleanArray::new(
            BooleanBuffer::from_iter(data.iter().cloned()),
            Some(null_bitset.clone()),
        );

        assert_eq!(*array, expected);
        dst.clear();

        bool_without_compress_encode(&data, &mut dst).unwrap();
        let array_ref = bool_without_compress_decode(&dst, &null_bitset).unwrap();
        let array = array_ref.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(*array, expected);
    }
}
