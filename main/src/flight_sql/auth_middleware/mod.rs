pub mod basic_call_header_authenticator;
pub mod generated_bearer_token_authenticator;

use models::auth::user::UserInfo;
use tonic::{metadata::MetadataMap, service::Interceptor, Status};

/// Interface for Server side authentication handlers.
pub trait CallHeaderAuthenticator {
    type AuthResult: AuthResult + Send + Sync;
    /// Implementations of CallHeaderAuthenticator should
    /// take care not to provide leak confidential details
    /// for security reasons when reporting errors back to clients.
    fn authenticate(&self, req_headers: &MetadataMap) -> Result<Self::AuthResult, Status>;
}

pub trait AuthResult {
    fn identity(&self) -> UserInfo;
    fn append_to_outgoing_headers(&self, resp_headers: &mut MetadataMap) -> Result<(), Status>;
}

pub struct CommonAuthResult {
    user_info: UserInfo,
}

impl CommonAuthResult {
    pub fn new(user_info: UserInfo) -> Self {
        Self { user_info }
    }
}

impl AuthResult for CommonAuthResult {
    fn identity(&self) -> UserInfo {
        self.user_info.clone()
    }

    fn append_to_outgoing_headers(&self, resp_headers: &mut MetadataMap) -> Result<(), Status> {
        Ok(())
    }
}
