pub mod basic_call_header_authenticator;
pub mod generated_bearer_token_authenticator;

use async_trait::async_trait;
use models::auth::user::User;
use tonic::metadata::MetadataMap;
use tonic::Status;

/// Interface for Server side authentication handlers.
#[async_trait]
pub trait CallHeaderAuthenticator {
    type AuthResult: AuthResult + Send + Sync;
    /// Implementations of CallHeaderAuthenticator should
    /// take care not to provide leak confidential details
    /// for security reasons when reporting errors back to clients.
    async fn authenticate(&self, req_headers: &MetadataMap) -> Result<Self::AuthResult, Status>;
}

pub trait AuthResult {
    fn identity(&self) -> User;
    fn append_to_outgoing_headers(&self, resp_headers: &mut MetadataMap) -> Result<(), Status>;
}

pub struct CommonAuthResult {
    user: User,
}

impl CommonAuthResult {
    pub fn new(user: User) -> Self {
        Self { user }
    }
}

impl AuthResult for CommonAuthResult {
    fn identity(&self) -> User {
        self.user.clone()
    }

    fn append_to_outgoing_headers(&self, _resp_headers: &mut MetadataMap) -> Result<(), Status> {
        Ok(())
    }
}
