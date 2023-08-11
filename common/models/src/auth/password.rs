use crate::auth::AuthError;

pub fn bcrypt_hash(password: &str) -> Result<String, AuthError> {
    Ok(bcrypt::hash(password, bcrypt::DEFAULT_COST)?)
}

pub fn bcrypt_verify(password: &str, hash_password: &str) -> Result<bool, AuthError> {
    Ok(bcrypt::verify(password, hash_password)?)
}
