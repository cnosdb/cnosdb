use std::convert::TryInto;
use std::error::Error;
use std::io::Write;

use bzip2::write::{BzDecoder, BzEncoder};
use bzip2::Compression as CompressionBzip;
use flate2::write::{GzDecoder, GzEncoder, ZlibDecoder, ZlibEncoder};
use flate2::Compression as CompressionFlate;
use integer_encoding::VarInt;
use minivec::MiniVec;

use crate::byte_utils::decode_be_u64;
use crate::tsm::codec::Encoding;
// note: encode/decode adapted from influxdb_iox
// https://github.com/influxdata/influxdb_iox/tree/main/influxdb_tsm/src/encoders

/// A compressed encoding using Snappy compression. Snappy is the only available
/// string compression format at this time.
const STRING_COMPRESSED_SNAPPY: u8 = 1;
/// The header consists of one byte indicating the compression type.
const HEADER_LEN: usize = 1;
/// Store `i32::MAX` as a `usize` for comparing with lengths in assertions
const MAX_I32: usize = i32::MAX as usize;

/// zstd compress level, select from -5 ~ 17
const ZSTD_COMPRESS_LEVEL: i32 = 3;

/// Encodes a slice of byte slices representing string data into a vector of
/// bytes. Currently uses Snappy compression.
pub fn str_snappy_encode(
    src: &[&[u8]],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.clear(); // reset buffer
    if src.is_empty() {
        return Ok(());
    }

    // strings shouldn't be longer than 64kb
    let length_of_lengths = src.len() * super::MAX_VAR_INT_32;
    let sum_of_lengths: usize = src
        .iter()
        .map(|s| {
            let len = s.len();
            assert!(len < MAX_I32);
            len
        })
        .sum();
    let source_size = 2 + length_of_lengths + sum_of_lengths;

    // determine the maximum possible length needed for the buffer, which
    // includes the compressed size
    let max_encoded_len = snap::raw::max_compress_len(source_size);
    if max_encoded_len == 0 {
        return Err("source length too large".into());
    }
    let compressed_size = max_encoded_len + HEADER_LEN;
    let total_size = source_size + compressed_size;

    if dst.len() < total_size {
        dst.resize(total_size, 0);
    }

    // write the data to be compressed *after* the space needed for snappy
    // compression. The compressed data is at the start of the allocated buffer,
    // ensuring the entire capacity is returned and available for subsequent use.
    let (compressed_data, data) = dst.split_at_mut(compressed_size);
    let mut n = 0;
    for s in src {
        let len = s.len();
        let len_u64: u64 = len.try_into()?;
        n += len_u64.encode_var(&mut data[n..]);
        data[n..][..len].copy_from_slice(s);
        n += len;
    }
    let data = &data[..n];

    let (header, compressed_data) = compressed_data.split_at_mut(HEADER_LEN);

    header[0] = STRING_COMPRESSED_SNAPPY << 4; // write compression type

    // TODO: snap docs say it is beneficial to reuse an `Encoder` when possible
    let mut encoder = snap::raw::Encoder::new();
    let actual_compressed_size = encoder.compress(data, compressed_data)?;

    dst.truncate(HEADER_LEN + actual_compressed_size);
    dst.insert(0, Encoding::Gorilla as u8);

    Ok(())
}

pub fn str_zstd_encode(
    src: &[&[u8]],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.clear(); // reset buffer
    if src.is_empty() {
        return Ok(());
    }

    let mut data = vec![];
    for s in src {
        let len = s.len() as u64;
        data.extend_from_slice(len.to_be_bytes().as_slice());
        data.extend_from_slice(s);
    }

    dst.push(Encoding::Zstd as u8);
    zstd::stream::copy_encode(data.as_slice(), dst, ZSTD_COMPRESS_LEVEL)?;
    Ok(())
}

pub fn str_gzip_encode(
    src: &[&[u8]],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.clear(); // reset buffer
    if src.is_empty() {
        return Ok(());
    }

    let mut data = vec![];
    for s in src {
        let len = s.len() as u64;
        data.extend_from_slice(len.to_be_bytes().as_slice());
        data.extend_from_slice(s);
    }

    let mut encoder = GzEncoder::new(vec![], CompressionFlate::default());
    encoder.write_all(&data)?;

    dst.push(Encoding::Gzip as u8);
    dst.append(&mut encoder.finish()?);
    Ok(())
}

pub fn str_bzip_encode(
    src: &[&[u8]],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.clear(); // reset buffer
    if src.is_empty() {
        return Ok(());
    }

    let mut data = vec![];
    for s in src {
        let len = s.len() as u64;
        data.extend_from_slice(len.to_be_bytes().as_slice());
        data.extend_from_slice(s);
    }

    let mut encoder = BzEncoder::new(vec![], CompressionBzip::default());
    encoder.write_all(&data).unwrap();

    dst.push(Encoding::Bzip as u8);
    dst.append(&mut encoder.finish()?);
    Ok(())
}

pub fn str_zlib_encode(
    src: &[&[u8]],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.clear(); // reset buffer
    if src.is_empty() {
        return Ok(());
    }

    let mut data = vec![];
    for s in src {
        let len = s.len() as u64;
        data.extend_from_slice(len.to_be_bytes().as_slice());
        data.extend_from_slice(s);
    }
    let mut encoder = ZlibEncoder::new(vec![], CompressionFlate::default());
    encoder.write_all(&data).unwrap();
    dst.push(Encoding::Zlib as u8);
    dst.append(&mut encoder.finish()?);
    Ok(())
}

pub fn str_without_compress_encode(
    src: &[&[u8]],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.clear(); // reset buffer
    if src.is_empty() {
        return Ok(());
    }

    dst.push(Encoding::Null as u8);

    for s in src {
        let len = s.len() as u64;
        dst.extend_from_slice(len.to_be_bytes().as_slice());
        dst.extend_from_slice(s);
    }

    Ok(())
}

/// Decodes a slice of bytes representing Snappy-compressed data into a vector
/// of vectors of bytes representing string data, which may or may not be valid
/// UTF-8.
pub fn str_snappy_decode(
    src: &[u8],
    dst: &mut Vec<MiniVec<u8>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }
    let src = &src[1..];

    let mut decoder = snap::raw::Decoder::new();
    // First byte stores the encoding type, only have snappy format
    // currently so ignore for now.
    let decoded_bytes = decoder.decompress_vec(&src[HEADER_LEN..])?;

    if dst.capacity() == 0 {
        dst.reserve_exact(64);
    }

    let num_decoded_bytes = decoded_bytes.len();
    let mut i = 0;

    while i < num_decoded_bytes {
        let (length, num_bytes_read) =
            u64::decode_var(&decoded_bytes[i..]).ok_or("invalid encoded string length")?;
        let length: usize = length.try_into()?;

        let lower = i + num_bytes_read;
        let upper = lower + length;

        if upper < lower {
            return Err("length overflow".into());
        }
        if upper > num_decoded_bytes {
            return Err("short buffer".into());
        }

        dst.push(MiniVec::from(&decoded_bytes[lower..upper]));

        // The length of this string plus the length of the variable byte encoded length
        i += length + num_bytes_read;
    }

    Ok(())
}

fn split_stream(
    data: &[u8],
    dst: &mut Vec<MiniVec<u8>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let len = data.len();
    let mut i = 0;

    while i < len {
        let str_len = decode_be_u64(&data[i..i + 8]);
        i += 8;
        let str_len: usize = str_len.try_into()?;
        dst.push(MiniVec::from(&data[i..i + str_len]));
        i += str_len;
    }
    Ok(())
}

pub fn str_zstd_decode(
    src: &[u8],
    dst: &mut Vec<MiniVec<u8>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }

    let src = &src[1..];
    let mut data = vec![];

    zstd::stream::copy_decode(src, &mut data)?;

    split_stream(&data, dst)?;
    Ok(())
}

pub fn str_bzip_decode(
    src: &[u8],
    dst: &mut Vec<MiniVec<u8>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }

    let src = &src[1..];

    let mut decoder = BzDecoder::new(vec![]);
    decoder.write_all(src).unwrap();
    let data = decoder.finish()?;

    split_stream(&data, dst)?;

    Ok(())
}

pub fn str_gzip_decode(
    src: &[u8],
    dst: &mut Vec<MiniVec<u8>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }

    let src = &src[1..];

    let mut decoder = GzDecoder::new(vec![]);
    decoder.write_all(src).unwrap();
    let data = decoder.finish()?;

    split_stream(&data, dst)?;

    Ok(())
}

pub fn str_zlib_decode(
    src: &[u8],
    dst: &mut Vec<MiniVec<u8>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }
    let src = &src[1..];

    let mut decoder = ZlibDecoder::new(vec![]);

    decoder.write_all(src).unwrap();
    let data = decoder.finish()?;

    split_stream(&data, dst)?;

    Ok(())
}

pub fn str_without_compress_decode(
    src: &[u8],
    dst: &mut Vec<MiniVec<u8>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }
    let src = &src[1..];
    split_stream(src, dst)?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn encode_no_values() {
        let src: Vec<&[u8]> = vec![];
        let mut dst = vec![];

        // check for error
        str_snappy_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst.to_vec().len(), 0);
        str_zlib_encode(&src, &mut dst).unwrap();
        assert_eq!(dst.to_vec().len(), 0);
        str_gzip_encode(&src, &mut dst).unwrap();
        assert_eq!(dst.to_vec().len(), 0);
        str_zstd_encode(&src, &mut dst).unwrap();
        assert_eq!(dst.to_vec().len(), 0);
        str_bzip_encode(&src, &mut dst).unwrap();
        assert_eq!(dst.to_vec().len(), 0);
        str_without_compress_encode(&src, &mut dst).unwrap();
        assert_eq!(dst.to_vec().len(), 0);

        // verify encoded no values.
    }

    #[test]
    fn encode_single() {
        let v1_bytes = b"v1";
        let src = vec![&v1_bytes[..]];
        let mut dst = vec![];

        str_snappy_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst[1..], vec![16, 3, 8, 2, 118, 49]);
    }

    #[test]
    fn encode_multi_compressed() {
        let src_strings: Vec<_> = (0..10).map(|i| format!("value {}", i)).collect();
        let src: Vec<_> = src_strings.iter().map(|s| s.as_bytes()).collect();
        let mut dst = vec![];

        str_snappy_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(
            dst[1..],
            vec![
                16, 80, 28, 7, 118, 97, 108, 117, 101, 32, 48, 13, 8, 0, 49, 13, 8, 0, 50, 13, 8,
                0, 51, 13, 8, 0, 52, 13, 8, 0, 53, 13, 8, 0, 54, 13, 8, 0, 55, 13, 8, 32, 56, 7,
                118, 97, 108, 117, 101, 32, 57
            ]
        );
    }

    #[test]
    fn encode_unicode() {
        let src = vec!["☃".as_bytes()];
        let mut dst = vec![];

        str_snappy_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst[1..], vec![16, 4, 12, 3, 226, 152, 131]);
    }

    #[test]
    fn encode_invalid_utf8() {
        let src = vec![&[b'\xC0'][..]];
        let mut dst = vec![];

        str_snappy_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst[1..], vec![16, 2, 4, 1, 192]);
    }

    #[test]
    fn decode_no_values() {
        let src: Vec<u8> = vec![];
        let mut dst = vec![];

        str_snappy_decode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst.to_vec().len(), 0);
        str_zlib_decode(&src, &mut dst).unwrap();
        assert_eq!(dst.to_vec().len(), 0);
        str_gzip_decode(&src, &mut dst).unwrap();
        assert_eq!(dst.to_vec().len(), 0);
        str_zstd_decode(&src, &mut dst).unwrap();
        assert_eq!(dst.to_vec().len(), 0);
        str_bzip_decode(&src, &mut dst).unwrap();
        assert_eq!(dst.to_vec().len(), 0);
        str_without_compress_decode(&src, &mut dst).unwrap();
        assert_eq!(dst.to_vec().len(), 0);
    }

    #[test]
    fn decode_single() {
        let src = vec![0, 16, 3, 8, 2, 118, 49];
        let mut dst = vec![];

        str_snappy_decode(&src, &mut dst).expect("failed to decode src");

        let dst_as_strings: Vec<_> = dst
            .iter()
            .map(|s| std::str::from_utf8(s).unwrap())
            .collect();
        assert_eq!(dst_as_strings, vec!["v1"]);
    }

    #[test]
    fn decode_multi_compressed() {
        let src = vec![
            0, 16, 80, 28, 7, 118, 97, 108, 117, 101, 32, 48, 13, 8, 0, 49, 13, 8, 0, 50, 13, 8, 0,
            51, 13, 8, 0, 52, 13, 8, 0, 53, 13, 8, 0, 54, 13, 8, 0, 55, 13, 8, 32, 56, 7, 118, 97,
            108, 117, 101, 32, 57,
        ];
        let mut dst = vec![];

        str_snappy_decode(&src, &mut dst).expect("failed to decode src");

        let dst_as_strings: Vec<_> = dst
            .iter()
            .map(|s| std::str::from_utf8(s).unwrap())
            .collect();
        let expected: Vec<_> = (0..10).map(|i| format!("value {}", i)).collect();
        assert_eq!(dst_as_strings, expected);
    }

    #[test]
    fn decode_unicode() {
        let src = vec![0, 16, 4, 12, 3, 226, 152, 131];
        let mut dst = vec![];

        str_snappy_decode(&src, &mut dst).expect("failed to decode src");

        let dst_as_strings: Vec<_> = dst
            .iter()
            .map(|s| std::str::from_utf8(s).unwrap())
            .collect();
        assert_eq!(dst_as_strings, vec!["☃"]);
    }

    #[test]
    fn decode_invalid_utf8() {
        let src = vec![0, 16, 2, 4, 1, 192];
        let mut dst = vec![];

        str_snappy_decode(&src, &mut dst).expect("failed to decode src");
        assert_eq!(dst, vec![&[b'\xC0'][..]]);
    }

    static ALLSTR: [&str; 10] = [
        "beijing",
        "shanghai",
        "guangzhou",
        "shenzhen",
        "wuhan",
        "qingdao",
        "beihai",
        "nanjing",
        "chengdu",
        "shijiazhuang",
    ];

    #[test]
    fn test_encode_decode() {
        let mut dst = vec![];
        let mut got = vec![];

        let mut data = vec![];
        let mut data_exp: Vec<MiniVec<u8>> = vec![];
        for i in ALLSTR {
            data.push(i.as_bytes());
            data_exp.push(MiniVec::from(i.as_bytes()));
        }

        str_snappy_encode(&data, &mut dst).unwrap();
        str_snappy_decode(&dst, &mut got).unwrap();
        assert_eq!(data_exp, got);
        dst.clear();
        got.clear();

        str_gzip_encode(&data, &mut dst).unwrap();
        str_gzip_decode(&dst, &mut got).unwrap();
        assert_eq!(data_exp, got);
        dst.clear();
        got.clear();

        str_zstd_encode(&data, &mut dst).unwrap();
        str_zstd_decode(&dst, &mut got).unwrap();
        assert_eq!(data_exp, got);
        dst.clear();
        got.clear();

        str_zlib_encode(&data, &mut dst).unwrap();
        str_zlib_decode(&dst, &mut got).unwrap();
        assert_eq!(data_exp, got);
        dst.clear();
        got.clear();

        str_bzip_encode(&data, &mut dst).unwrap();
        str_bzip_decode(&dst, &mut got).unwrap();
        assert_eq!(data_exp, got);
        dst.clear();
        got.clear();

        str_without_compress_encode(&data, &mut dst).unwrap();
        str_without_compress_decode(&dst, &mut got).unwrap();
        assert_eq!(data_exp, got);
    }
}
