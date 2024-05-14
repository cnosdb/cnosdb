use http_protocol::encoding::Encoding;
use trace::error;
use warp::reject::{self, Rejection};

use super::header::Header;
use super::Error as HttpError;

pub fn get_accept_encoding_from_header(header: &Header) -> Result<Option<Encoding>, Rejection> {
    match header.get_accept_encoding() {
        Some(s) => {
            let encodings = s.split(',').map(|s| s.trim());
            let mut encoding = None;
            for e in encodings {
                match Encoding::from_str_opt(e) {
                    Some(enc) => {
                        encoding = Some(enc);
                        break;
                    }
                    None => continue,
                }
            }
            match encoding {
                Some(enc) => Ok(Some(enc)),
                None => Err(reject::custom(HttpError::InvalidHeader {
                    reason: format!(
                        "accept encoding not support: {}, only support: {}",
                        s,
                        Encoding::iterator()
                            .map(|e| e.as_str())
                            .collect::<Vec<&str>>()
                            .join(", ")
                    ),
                })),
            }
        }
        None => Ok(None),
    }
}

pub fn get_content_encoding_from_header(header: &Header) -> Result<Option<Encoding>, Rejection> {
    match header.get_content_encoding() {
        Some(s) => match Encoding::from_str_opt(s) {
            Some(encoding) => Ok(Some(encoding)),
            None => {
                let e = HttpError::InvalidHeader {
                    reason: format!("content encoding not support: {}", s),
                };
                error!("get_content_encoding_from_header: {:?}", e);
                Err(reject::custom(e))
            }
        },
        None => Ok(None),
    }
}
