use spi::server::dbms::DBMSRef;
use tonic::{metadata::MetadataMap, Status};
use trace::debug;

use crate::{flight_sql::utils, http::header::Header};

use super::{CallHeaderAuthenticator, CommonAuthResult};

#[derive(Clone)]
pub struct BasicCallHeaderAuthenticator {
    instance: DBMSRef,
}

impl BasicCallHeaderAuthenticator {
    pub fn new(instance: DBMSRef) -> Self {
        Self { instance }
    }
}

impl CallHeaderAuthenticator for BasicCallHeaderAuthenticator {
    type AuthResult = CommonAuthResult;

    fn authenticate(&self, req_headers: &MetadataMap) -> Result<Self::AuthResult, Status> {
        debug!("authenticate, request headers: {:?}", req_headers);

        let authorization = utils::get_value_from_auth_header(req_headers, "")
            .ok_or_else(|| Status::unauthenticated("authorization field not present"))?;

        let user_info = Header::with(None, authorization)
            .try_get_basic_auth()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let _ = self
            .instance
            .authenticate(&user_info)
            .map_err(|e| Status::unauthenticated(e.to_string()))?;

        debug!("authenticate success, user: {}", user_info.user);

        Ok(CommonAuthResult { user_info })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use http_protocol::header::AUTHORIZATION;
    use spi::server::dbms::DatabaseManagerSystemMock;
    use tonic::metadata::{AsciiMetadataValue, MetadataMap};

    use crate::flight_sql::auth_middleware::{AuthResult, CallHeaderAuthenticator};

    use super::BasicCallHeaderAuthenticator;

    #[test]
    fn test() {
        let instance = Arc::new(DatabaseManagerSystemMock {});
        let authenticator = BasicCallHeaderAuthenticator::new(instance);

        let mut req_headers = MetadataMap::default();

        assert!(authenticator.authenticate(&req_headers).is_err());

        let val = AsciiMetadataValue::from_static("Basic eHg6eHgK");

        req_headers.insert(AUTHORIZATION.as_str(), val);

        let auth_result = authenticator
            .authenticate(&req_headers)
            .expect("authenticate");

        auth_result
            .append_to_outgoing_headers(&mut req_headers)
            .expect("append_to_outgoing_headers");

        assert_eq!(req_headers.len(), 1)
    }
}
