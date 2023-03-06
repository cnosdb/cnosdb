use std::time::Duration;

use http_protocol::header::BEARER_PREFIX;
use models::auth::user::User;
use models::oid::UuidGenerator;
use moka::sync::Cache;
use tonic::metadata::MetadataMap;
use tonic::Status;
use trace::debug;

use super::{AuthResult, CallHeaderAuthenticator};
use crate::flight_sql::utils;

/// Generates and caches bearer tokens from user credentials.
#[derive(Clone)]
pub struct GeneratedBearerTokenAuthenticator<T>
where
    T: Clone,
{
    bearer_to_identifier: Cache<String, User>,

    initial_authenticator: T,
    id_generator: UuidGenerator,
}

impl<T> GeneratedBearerTokenAuthenticator<T>
where
    T: Clone,
{
    pub fn new(initial_authenticator: T) -> Self {
        let bearer_to_identifier = Cache::builder()
            .thread_pool_enabled(false)
            // Time to idle (TTL): 10 minutes
            // If bearer is not used within 10 minutes, it will expire
            .time_to_idle(Duration::from_secs(10 * 60))
            .build();

        Self {
            bearer_to_identifier,
            initial_authenticator,
            id_generator: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<T> CallHeaderAuthenticator for GeneratedBearerTokenAuthenticator<T>
where
    T: CallHeaderAuthenticator + std::marker::Sync + std::marker::Send,
    T: Clone,
{
    type AuthResult = GeneratedBearerTokenAuthResult;

    async fn authenticate(&self, req_headers: &MetadataMap) -> Result<Self::AuthResult, Status> {
        debug!("authenticate, request headers: {:?}", req_headers);

        // Check if headers contain a bearer token and if so, validate the token.
        if let Some(bearer_token) = utils::get_value_from_auth_header(req_headers, BEARER_PREFIX) {
            // get user_info from cache by token
            let user = self
                .bearer_to_identifier
                .get(&bearer_token)
                .ok_or_else(|| Status::unauthenticated("token has expired or not exist"))?;

            debug!("authenticate success, bearer_token exists");

            return Ok(GeneratedBearerTokenAuthResult {
                user,
                bearer_token: Some(bearer_token),
            });
        }

        debug!("bearer_token not exists, delegate to initial_authenticator");

        // Delegate to the basic auth handler to do the validation.
        let auth_result = self.initial_authenticator.authenticate(req_headers).await?;
        self.process_auth_result(auth_result)
    }
}

impl<T> GeneratedBearerTokenAuthenticator<T>
where
    T: Clone,
{
    fn process_auth_result(
        &self,
        auth_result: impl AuthResult,
    ) -> Result<GeneratedBearerTokenAuthResult, Status> {
        let user = auth_result.identity();
        // After the user authentication is successful,
        // Generate a new bearer token and return an AuthResult that can write it.
        let bearer_token = self.id_generator.next_id().to_string();
        // And cache the mapping between bearer and user information on the server side
        self.bearer_to_identifier
            .insert(bearer_token.clone(), user.clone());

        debug!("authenticate success, generated new bearer_token");

        Ok(GeneratedBearerTokenAuthResult {
            user,
            bearer_token: Some(bearer_token),
        })
    }
}

pub struct GeneratedBearerTokenAuthResult {
    user: User,
    bearer_token: Option<String>,
}

impl AuthResult for GeneratedBearerTokenAuthResult {
    fn identity(&self) -> User {
        self.user.clone()
    }

    fn append_to_outgoing_headers(&self, resp_headers: &mut MetadataMap) -> Result<(), Status> {
        if let Some(ref bearer_token) = self.bearer_token {
            utils::insert_bearer_auth(resp_headers, bearer_token).map_err(Status::internal)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use http_protocol::header::{AUTHORIZATION, BEARER_PREFIX};
    use models::auth::role::UserRole;
    use models::auth::user::{User, UserDesc, UserOptionsBuilder};
    use tonic::metadata::{AsciiMetadataValue, MetadataMap};

    use crate::flight_sql::auth_middleware::generated_bearer_token_authenticator::GeneratedBearerTokenAuthenticator;
    use crate::flight_sql::auth_middleware::{
        AuthResult, CallHeaderAuthenticator, CommonAuthResult,
    };
    use crate::flight_sql::utils;

    #[derive(Clone)]
    struct CallHeaderAuthenticatorMock {}

    #[async_trait::async_trait]
    impl CallHeaderAuthenticator for CallHeaderAuthenticatorMock {
        type AuthResult = CommonAuthResult;

        async fn authenticate(
            &self,
            _req_headers: &MetadataMap,
        ) -> Result<Self::AuthResult, tonic::Status> {
            let options = unsafe {
                UserOptionsBuilder::default()
                    .password("123456")
                    .build()
                    .unwrap_unchecked()
            };
            let mock_desc = UserDesc::new(0_u128, "name".to_string(), options, false);
            let mock_user = User::new(mock_desc, UserRole::Dba.to_privileges());
            Ok(CommonAuthResult { user: mock_user })
        }
    }

    #[tokio::test]
    async fn test() {
        let authenticator = GeneratedBearerTokenAuthenticator::new(CallHeaderAuthenticatorMock {});

        let mut req_headers = MetadataMap::default();
        let val = AsciiMetadataValue::from_static("Basic eHg6eHgK");
        req_headers.insert(AUTHORIZATION.as_str(), val);

        assert_eq!(req_headers.len(), 1);

        authenticator
            .authenticate(&req_headers)
            .await
            .expect("authenticate")
            .append_to_outgoing_headers(&mut req_headers)
            .expect("append_to_outgoing_headers");

        assert!(utils::get_value_from_auth_header(&req_headers, BEARER_PREFIX).is_some());
    }
}
