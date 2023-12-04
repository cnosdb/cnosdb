use std::cell::RefCell;
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

thread_local! {
    pub static BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
}

impl EncodingExt for Encoding {
    fn encode(&self, bytes: Vec<u8>) -> io::Result<Vec<u8>> {
        Ok(match self {
            Encoding::Gzip => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                let mut encoder = GzEncoder::new(buf, Compression::default());
                encoder.write_all(&bytes)?;
                encoder.finish().map(|res| res.clone())
            })?,
            Encoding::Deflate => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                let mut encoder = DeflateEncoder::new(buf, Compression::default());
                encoder.write_all(&bytes)?;
                encoder.finish().map(|res| res.clone())
            })?,
            Encoding::Brotli => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                let params = BrotliEncoderParams::default();
                let mut cursor = Cursor::new(bytes);
                brotli::BrotliCompress(&mut cursor, buf, &params).map(|_| buf.clone())
            })?,
            Encoding::Zstd => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                zstd::stream::copy_encode(&bytes[..], &mut *buf, DEFAULT_COMPRESSION_LEVEL)?;
                io::Result::Ok(buf.clone())
            })?,
            Encoding::Identity => bytes,
        })
    }

    fn decode(&self, bytes: Bytes) -> io::Result<Bytes> {
        Ok(match self {
            Encoding::Gzip => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                let mut decoder = GzDecoder::new(buf);
                decoder.write_all(&bytes)?;
                decoder.finish().map(|res| Bytes::from(res.clone()))
            })?,
            Encoding::Deflate => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                let mut decoder = DeflateDecoder::new(buf);
                decoder.write_all(&bytes)?;
                decoder.finish().map(|res| Bytes::from(res.clone()))
            })?,
            Encoding::Brotli => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                let mut cursor = Cursor::new(bytes);
                brotli::BrotliDecompress(&mut cursor, buf).map(|_| Bytes::from(buf.clone()))
            })?,
            Encoding::Zstd => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                zstd::stream::copy_decode(&bytes[..], &mut *buf)?;
                io::Result::Ok(Bytes::from(buf.clone()))
            })?,
            Encoding::Identity => bytes,
        })
    }
}
