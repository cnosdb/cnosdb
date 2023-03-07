use http_protocol::header;
use spi::server::dbms::DBMSRef;
use tonic::metadata::MetadataMap;
use tonic::Status;
use trace::debug;

use super::{CallHeaderAuthenticator, CommonAuthResult};
use crate::flight_sql::utils;
use crate::http::header::Header;

#[derive(Clone)]
pub struct BasicCallHeaderAuthenticator {
    instance: DBMSRef,
}

impl BasicCallHeaderAuthenticator {
    pub fn new(instance: DBMSRef) -> Self {
        Self { instance }
    }
}

#[async_trait::async_trait]
impl CallHeaderAuthenticator for BasicCallHeaderAuthenticator {
    type AuthResult = CommonAuthResult;

    async fn authenticate(&self, req_headers: &MetadataMap) -> Result<Self::AuthResult, Status> {
        debug!("authenticate, request headers: {:?}", req_headers);

        let authorization = utils::get_value_from_auth_header(req_headers, "")
            .ok_or_else(|| Status::unauthenticated("authorization field not present"))?;

        let user_info = Header::with(None, authorization)
            .try_get_basic_auth()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let tenant = utils::get_value_from_header(req_headers, header::TENANT, "");

        let user = self
            .instance
            .authenticate(&user_info, tenant.as_deref())
            .await
            .map_err(|e| Status::unauthenticated(e.to_string()))?;

        debug!("authenticate success, user: {}", user_info.user);

        Ok(CommonAuthResult { user })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use http_protocol::header::AUTHORIZATION;
    use spi::server::dbms::DatabaseManagerSystemMock;
    use tonic::metadata::{AsciiMetadataValue, MetadataMap};

    use super::BasicCallHeaderAuthenticator;
    use crate::flight_sql::auth_middleware::{AuthResult, CallHeaderAuthenticator};

    #[tokio::test]
    async fn test() {
        let instance = Arc::new(DatabaseManagerSystemMock {});
        let authenticator = BasicCallHeaderAuthenticator::new(instance);

        let mut req_headers = MetadataMap::default();

        assert!(authenticator.authenticate(&req_headers).await.is_err());

        let val = AsciiMetadataValue::from_static("Basic eHg6eHgK");

        req_headers.insert(AUTHORIZATION.as_str(), val);

        let auth_result = authenticator
            .authenticate(&req_headers)
            .await
            .expect("authenticate");

        auth_result
            .append_to_outgoing_headers(&mut req_headers)
            .expect("append_to_outgoing_headers");

        assert_eq!(req_headers.len(), 1)
    }
}
