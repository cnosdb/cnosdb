use std::io::{self, Cursor, Write};
use std::slice::Iter;

use brotli::enc::BrotliEncoderParams;
use bytes::Bytes;
use flate2::write::{DeflateDecoder, DeflateEncoder, GzDecoder, GzEncoder};
use flate2::Compression;
use fly_accept_encoding::Encoding;
use zstd::DEFAULT_COMPRESSION_LEVEL;

use crate::header::{BROTLI, DEFLATE, GZIP, IDENTITY, ZSTD};

pub trait EncodingExt {
    fn encode(&self, bytes: Vec<u8>) -> io::Result<Vec<u8>>;
    fn decode(&self, bytes: Bytes) -> io::Result<Bytes>;
    fn from_str(s: &str) -> Option<Encoding> {
        match s.to_ascii_lowercase().as_str() {
            GZIP => Some(Encoding::Gzip),
            DEFLATE => Some(Encoding::Deflate),
            BROTLI => Some(Encoding::Brotli),
            ZSTD => Some(Encoding::Zstd),
            IDENTITY => Some(Encoding::Identity),
            _ => None,
        }
    }
    fn iterator() -> Iter<'static, Encoding> {
        const ENCODINGS: [Encoding; 5] = [
            Encoding::Brotli,
            Encoding::Deflate,
            Encoding::Gzip,
            Encoding::Identity,
            Encoding::Zstd,
        ];
        ENCODINGS.iter()
    }
}

impl EncodingExt for Encoding {
    fn encode(&self, bytes: Vec<u8>) -> io::Result<Vec<u8>> {
        Ok(match self {
            Encoding::Gzip => {
                let mut encoder = GzEncoder::new(vec![], Compression::default());
                encoder.write_all(&bytes)?;
                encoder.finish()?
            }
            Encoding::Deflate => {
                let mut encoder = DeflateEncoder::new(vec![], Compression::default());
                encoder.write_all(&bytes)?;
                encoder.finish()?
            }
            Encoding::Brotli => {
                let params = BrotliEncoderParams::default();
                let mut compressed_bytes = vec![];
                let mut cursor = Cursor::new(bytes);
                brotli::BrotliCompress(&mut cursor, &mut compressed_bytes, &params)
                    .map(|_| compressed_bytes)?
            }
            Encoding::Zstd => {
                let mut compressed_bytes = vec![];
                zstd::stream::copy_encode(
                    &bytes[..],
                    &mut compressed_bytes,
                    DEFAULT_COMPRESSION_LEVEL,
                )?;
                compressed_bytes
            }
            Encoding::Identity => bytes,
        })
    }

    fn decode(&self, bytes: Bytes) -> io::Result<Bytes> {
        Ok(match self {
            Encoding::Gzip => {
                let mut decoder = GzDecoder::new(vec![]);
                decoder.write_all(&bytes)?;
                let decompressed_bytes = decoder.finish()?;
                Bytes::from(decompressed_bytes)
            }
            Encoding::Deflate => {
                let mut decoder = DeflateDecoder::new(vec![]);
                decoder.write_all(&bytes)?;
                let decompressed_bytes = decoder.finish()?;
                Bytes::from(decompressed_bytes)
            }
            Encoding::Brotli => {
                let mut decompressed_bytes = vec![];
                let mut cursor = Cursor::new(bytes);
                brotli::BrotliDecompress(&mut cursor, &mut decompressed_bytes)
                    .map(|_| Bytes::from(decompressed_bytes))?
            }
            Encoding::Zstd => {
                let mut decompressed_bytes = vec![];
                zstd::stream::copy_decode(&bytes[..], &mut decompressed_bytes)?;
                Bytes::from(decompressed_bytes)
            }
            Encoding::Identity => bytes,
        })
    }
}
