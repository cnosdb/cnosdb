use std::cell::RefCell;
use std::io::{self, Cursor, Read, Write};

use brotli::enc::BrotliEncoderParams;
use bytes::Bytes;
use flate2::write::{DeflateDecoder, DeflateEncoder, GzDecoder, GzEncoder};
use flate2::Compression;
use reqwest::header::HeaderValue;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use zstd::DEFAULT_COMPRESSION_LEVEL;

use crate::header::{BROTLI, DEFLATE, GZIP, IDENTITY, LZ4, SNAPPY, ZSTD};

thread_local! {
    pub static BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, EnumIter)]
pub enum Encoding {
    Gzip,
    Deflate,
    Brotli,
    Zstd,
    Snappy,
    LZ4,
    Identity,
}

impl Encoding {
    pub fn from_str_opt(s: &str) -> Option<Encoding> {
        Encoding::iter().find(|e| e.as_str() == s)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Encoding::Gzip => GZIP,
            Encoding::Deflate => DEFLATE,
            Encoding::Brotli => BROTLI,
            Encoding::Zstd => ZSTD,
            Encoding::Snappy => SNAPPY,
            Encoding::LZ4 => LZ4,
            Encoding::Identity => IDENTITY,
        }
    }

    pub fn iterator() -> EncodingIter {
        Encoding::iter()
    }

    pub fn to_header_value(self) -> HeaderValue {
        match self {
            Encoding::Gzip => HeaderValue::from_str(GZIP).unwrap(),
            Encoding::Deflate => HeaderValue::from_str(DEFLATE).unwrap(),
            Encoding::Brotli => HeaderValue::from_str(BROTLI).unwrap(),
            Encoding::Zstd => HeaderValue::from_str(ZSTD).unwrap(),
            Encoding::Snappy => HeaderValue::from_str(SNAPPY).unwrap(),
            Encoding::LZ4 => HeaderValue::from_str(LZ4).unwrap(),
            Encoding::Identity => HeaderValue::from_str(IDENTITY).unwrap(),
        }
    }

    pub fn encode(&self, bytes: Vec<u8>) -> io::Result<Vec<u8>> {
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
            Encoding::Snappy => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                let mut encoder = snap::read::FrameEncoder::new(&bytes[..]);
                encoder.read_to_end(buf)?;
                io::Result::Ok(buf.clone())
            })?,
            Encoding::LZ4 => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                let mut encoder = lz4_flex::frame::FrameEncoder::new(buf);
                encoder
                    .write_all(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                encoder.finish().map(|res| res.clone())
            })?,
            Encoding::Identity => bytes,
        })
    }

    pub fn decode(&self, bytes: Bytes) -> io::Result<Bytes> {
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
            Encoding::Snappy => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                snap::read::FrameDecoder::new(&bytes[..]).read_to_end(buf)?;
                io::Result::Ok(Bytes::from(buf.clone()))
            })?,
            Encoding::LZ4 => BUFFER.with(|buf| {
                let buf = &mut *buf.borrow_mut();
                buf.clear();
                lz4_flex::frame::FrameDecoder::new(&bytes[..])
                    .read_to_end(buf)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                io::Result::Ok(Bytes::from(buf.clone()))
            })?,
            Encoding::Identity => bytes,
        })
    }
}
