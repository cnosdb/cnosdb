use fly_accept_encoding::Encoding;
use http_protocol::encoding::EncodingExt;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING};
use warp::reject::{self, Rejection};

use super::header::Header;
use super::Error as HttpError;

pub fn get_accept_encoding_from_header(header: &Header) -> Result<Option<Encoding>, Rejection> {
    match header.get_accept_encoding() {
        Some(s) => {
            let mut header_map = HeaderMap::new();
            header_map.insert(
                ACCEPT_ENCODING,
                HeaderValue::from_str(s).map_err(|e| HttpError::InvalidHeader {
                    reason: format!("{}", e),
                })?,
            );
            let encoding =
                fly_accept_encoding::parse(&header_map).map_err(|e| HttpError::InvalidHeader {
                    reason: format!("{}", e),
                })?;
            Ok(encoding)
        }
        None => Ok(None),
    }
}

pub fn get_content_encoding_from_header(header: &Header) -> Result<Option<Encoding>, Rejection> {
    match header.get_content_encoding() {
        Some(s) => match Encoding::from_str(s) {
            Some(encoding) => Ok(Some(encoding)),
            None => Err(reject::custom(HttpError::InvalidHeader {
                reason: format!("content encoding not support: {}", s),
            })),
        },
        None => Ok(None),
    }
}
