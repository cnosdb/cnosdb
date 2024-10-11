use base64::prelude::{Engine, BASE64_STANDARD};
use http_protocol::header::{APPLICATION_CSV, BASIC_PREFIX};
use models::auth::user::UserInfo;
use warp::http::header::{HeaderName, HeaderValue};

use super::Error as HttpError;

#[derive(Debug, Clone)]
pub struct Header {
    accept: Option<String>,
    accept_encoding: Option<String>,
    content_encoding: Option<String>,
    authorization: String,
    private_key: Option<String>,
    tenant: Option<String>,
    db: Option<String>,
    table: Option<String>,
}

impl Header {
    pub fn with(
        accept: Option<String>,
        accept_encoding: Option<String>,
        content_encoding: Option<String>,
        authorization: String,
    ) -> Self {
        Self {
            accept,
            accept_encoding,
            content_encoding,
            authorization,
            private_key: None,
            tenant: None,
            db: None,
            table: None,
        }
    }

    pub fn with_private_key(
        accept: Option<String>,
        accept_encoding: Option<String>,
        content_encoding: Option<String>,
        authorization: String,
        private_key: Option<String>,
        tenant: Option<String>,
        db: Option<String>,
        table: Option<String>,
    ) -> Self {
        Self {
            accept,
            accept_encoding,
            content_encoding,
            authorization,
            private_key,
            tenant,
            db,
            table,
        }
    }

    pub fn get_accept(&self) -> &str {
        self.accept.as_deref().unwrap_or(APPLICATION_CSV)
    }

    pub fn get_accept_encoding(&self) -> Option<&str> {
        self.accept_encoding.as_deref()
    }

    pub fn get_content_encoding(&self) -> Option<&str> {
        self.content_encoding.as_deref()
    }

    pub fn get_tenant(&self) -> Option<String> {
        self.tenant.clone()
    }

    pub fn get_db(&self) -> Option<String> {
        self.db.clone()
    }

    pub fn get_table(&self) -> Option<String> {
        self.table.clone()
    }

    pub fn try_get_raw_basic_auth(&self) -> Result<&str, HttpError> {
        let auth = &self.authorization;

        let get_err = || {
            Err(HttpError::ParseAuth {
                reason: auth.to_string(),
            })
        };

        if auth.len() < BASIC_PREFIX.len() {
            return get_err();
        }

        let basic_in_auth = &auth[0..BASIC_PREFIX.len()];

        if basic_in_auth != BASIC_PREFIX {
            return get_err();
        }

        Ok(&auth[BASIC_PREFIX.len()..])
    }

    pub fn try_get_basic_auth(&self) -> Result<UserInfo, HttpError> {
        let private_key = self
            .private_key
            .as_ref()
            .map(|e| {
                let content = BASE64_STANDARD
                    .decode(e)
                    .map_err(|_| HttpError::InvalidHeader {
                        reason: format!("Can not parse private_key with base64: {}", e),
                    })?;
                String::from_utf8(content).map_err(|err| HttpError::InvalidHeader {
                    reason: err.to_string(),
                })
            })
            .transpose()?;

        let content_in_auth = self.try_get_raw_basic_auth()?;

        if let Ok(content) = BASE64_STANDARD.decode(content_in_auth) {
            if let Ok(str) = String::from_utf8(content) {
                if let Some(idx) = str.find(':') {
                    return Ok(UserInfo {
                        user: str[0..idx].to_string(),
                        password: str[idx + 1..].to_string(),
                        private_key,
                    });
                }
            }
        }

        Err(HttpError::ParseAuth {
            reason: self.authorization.to_string(),
        })
    }
}

pub trait IntoHeaderValue: Sized {
    fn into_value(self) -> HeaderValue;
}

pub trait IntoHeaderPair: Sized {
    fn into_pair(self) -> (HeaderName, HeaderValue);
}

impl<V> IntoHeaderPair for (HeaderName, V)
where
    V: IntoHeaderValue,
{
    fn into_pair(self) -> (HeaderName, HeaderValue) {
        let (name, value) = self;
        let value = value.into_value();
        (name, value)
    }
}

impl IntoHeaderValue for &'static str {
    fn into_value(self) -> HeaderValue {
        HeaderValue::from_static(self)
    }
}

impl IntoHeaderValue for &HeaderValue {
    fn into_value(self) -> HeaderValue {
        self.clone()
    }
}

impl IntoHeaderValue for HeaderValue {
    fn into_value(self) -> HeaderValue {
        self
    }
}

#[cfg(test)]
mod tests {
    use base64::prelude::{Engine, BASE64_STANDARD};

    use super::*;

    #[test]
    fn test_into_head_value() {
        let _: HeaderValue = "test".into_value();
        let _: HeaderValue = HeaderValue::from_static("test").into_value();
        let _: HeaderValue = (&HeaderValue::from_static("test")).into_value();
    }

    #[test]
    fn test_into_pair() {
        let _: (HeaderName, HeaderValue) = (HeaderName::from_static("test"), "test").into_pair();
        let _: (HeaderName, HeaderValue) = (
            HeaderName::from_static("test"),
            HeaderValue::from_static("test"),
        )
            .into_pair();
        let _: (HeaderName, HeaderValue) = (
            HeaderName::from_static("test"),
            &HeaderValue::from_static("test"),
        )
            .into_pair();
    }

    #[test]
    fn test_header_auth() {
        let auth = BASE64_STANDARD.encode("xx:");
        let valid_auth_without_passwd = format!("{}{}", BASIC_PREFIX, auth);
        let header = Header::with(None, None, None, valid_auth_without_passwd);
        let user_info = header.try_get_basic_auth().unwrap();
        assert_eq!(&user_info.user, "xx");
        assert_eq!(&user_info.password, "");

        let auth = BASE64_STANDARD.encode("xx:xx");
        let valid_auth_with_passwd = format!("{}{}", BASIC_PREFIX, auth);
        let header = Header::with(None, None, None, valid_auth_with_passwd);
        let user_info = header.try_get_basic_auth().unwrap();
        assert_eq!(&user_info.user, "xx");
        assert_eq!(&user_info.password, "xx");

        let auth = BASE64_STANDARD.encode("xx");
        let invalid_auth_1 = format!("{}{}", BASIC_PREFIX, auth);
        let header = Header::with(None, None, None, invalid_auth_1);
        assert!(header.try_get_basic_auth().is_err());

        let auth = BASE64_STANDARD.encode("xx");
        let header = Header::with(None, None, None, auth);
        assert!(header.try_get_basic_auth().is_err());
    }
}
