use std::fmt::format;

use arrow_flight::FlightDescriptor;
use http_protocol::header::{AUTHORIZATION, BASIC_PREFIX};
use models::auth::user::UserInfo;
use tonic::Request;

use crate::http::header::Header;

pub fn parse_user_info(
    request: &Request<FlightDescriptor>,
) -> std::result::Result<UserInfo, String> {
    let authorization = request
        .metadata()
        .get(AUTHORIZATION.to_string())
        .ok_or("authorization field not present".to_string())?
        .to_str()
        .map_err(|_| "authorization not parsable".to_string())?;

    Header::with(None, authorization.to_string())
        .try_get_basic_auth()
        .map_err(|e| format!("authorization not parsable, error: {}", e))
}
