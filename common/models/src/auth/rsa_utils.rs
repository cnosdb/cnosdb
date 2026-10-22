use rand::rngs::OsRng;
use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey};
use rsa::{Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey};

use super::{AuthError, AuthResult};

fn rsa_error(error: impl std::fmt::Display) -> AuthError {
    AuthError::Rsa {
        error: error.to_string(),
    }
}

pub fn verify(private_key_pem: &str, passphrase: &str, public_key_pem: &str) -> AuthResult<bool> {
    // 如果用户设置了公钥
    // 查看客户端有没有携带私钥，没有则报错
    let data = passphrase.as_bytes();

    // read pub & private key
    let rsa_pub = RsaPublicKey::from_public_key_pem(public_key_pem).map_err(rsa_error)?;
    let rsa_private =
        RsaPrivateKey::from_pkcs8_encrypted_pem(private_key_pem, passphrase).map_err(rsa_error)?;

    // encrypt with public key
    let mut rng = OsRng;
    let encrypted_data = rsa_pub
        .encrypt(&mut rng, Pkcs1v15Encrypt, data)
        .map_err(rsa_error)?;

    // decrypt with private key
    let decrypted_data = rsa_private
        .decrypt(Pkcs1v15Encrypt, &encrypted_data)
        .map_err(rsa_error)?;

    // verify
    if decrypted_data != data {
        return Ok(false);
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::verify;

    #[test]
    fn verifies_rsa_key_pair() {
        let passphrase = "123456";

        let private_key_pem = fs::read_to_string("tests/resource/rsa/rsa_key.p8").unwrap();
        let public_key_pem = fs::read_to_string("tests/resource/rsa/rsa_key.pub").unwrap();

        let xx = verify(&private_key_pem, passphrase, &public_key_pem).unwrap();
        assert!(xx);
    }

    #[test]
    fn rejects_invalid_private_key_passphrase() {
        let private_key_pem = fs::read_to_string("tests/resource/rsa/rsa_key.p8").unwrap();
        let public_key_pem = fs::read_to_string("tests/resource/rsa/rsa_key.pub").unwrap();

        assert!(verify(&private_key_pem, "wrong passphrase", &public_key_pem).is_err());
    }
}
