use openssl::rsa::{Padding, Rsa};
use snafu::ResultExt;

use super::{Result, RsaSnafu};

pub fn verify(private_key_pem: &[u8], passphrase: &str, public_key_pem: &[u8]) -> Result<bool> {
    // 如果用户设置了公钥
    // 查看客户端有没有携带私钥，没有则报错
    let data = passphrase.as_bytes();

    // read pub & private key
    let rsa_pub = Rsa::public_key_from_pem(public_key_pem).context(RsaSnafu)?;
    let rsa_private = Rsa::private_key_from_pem_passphrase(private_key_pem, passphrase.as_bytes())
        .context(RsaSnafu)?;

    // encrypt with public key
    let mut encrypted_data: Vec<u8> = vec![0; rsa_pub.size() as usize];
    let _ = rsa_pub
        .public_encrypt(data, &mut encrypted_data, Padding::PKCS1)
        .context(RsaSnafu)?;

    // decrypt with private key
    let mut decrypted_data: Vec<u8> = vec![0; rsa_private.size() as usize];
    let len = rsa_private
        .private_decrypt(&encrypted_data, &mut decrypted_data, Padding::PKCS1)
        .context(RsaSnafu)?;

    // verify
    if &decrypted_data[..len] != data {
        return Ok(false);
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use openssl::rsa::{Padding, Rsa};

    use super::verify;

    #[test]
    fn test_openssl() {
        let passphrase = "123456";

        let data = "A quick brown fox jumps over the lazy dog.";

        let private_key_pem = fs::read("tests/resource/rsa/rsa_key.p8").unwrap();
        let public_key_pem = fs::read("tests/resource/rsa/rsa_key.pub").unwrap();

        // Encrypt with public key
        let rsa = Rsa::public_key_from_pem(&public_key_pem).unwrap();
        let mut buf: Vec<u8> = vec![0; rsa.size() as usize];
        let _ = rsa
            .public_encrypt(data.as_bytes(), &mut buf, Padding::PKCS1)
            .unwrap();

        let encrypt_data = buf;

        // Decrypt with private key
        let rsa =
            Rsa::private_key_from_pem_passphrase(&private_key_pem, passphrase.as_bytes()).unwrap();
        let mut buf: Vec<u8> = vec![0; rsa.size() as usize];
        let _ = rsa
            .private_decrypt(&encrypt_data, &mut buf, Padding::PKCS1)
            .unwrap();

        let decrypted_data = String::from_utf8(buf).unwrap();

        assert_eq!(&decrypted_data[0..data.len()], data);
    }

    #[test]
    fn test_verify() {
        let passphrase = "123456";

        let private_key_pem = fs::read("tests/resource/rsa/rsa_key.p8").unwrap();
        let public_key_pem = fs::read("tests/resource/rsa/rsa_key.pub").unwrap();

        let xx = verify(&private_key_pem, passphrase, &public_key_pem).unwrap();
        assert!(xx);
    }
}
