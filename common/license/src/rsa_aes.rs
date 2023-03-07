#![allow(dead_code)]

use std::fs;

use crypto::aes;
use crypto::aes::KeySize::KeySize256;
use crypto::blockmodes::PkcsPadding;
use crypto::buffer::BufferResult::BufferUnderflow;
use crypto::buffer::{ReadBuffer, RefReadBuffer, RefWriteBuffer, WriteBuffer};
use crypto::digest::Digest;
use crypto::md5::Md5;
use rsa::pkcs1::{DecodeRsaPublicKey, EncodeRsaPrivateKey, EncodeRsaPublicKey};
use rsa::pkcs8::LineEnding;
use rsa::{PaddingScheme, PublicKey, RsaPrivateKey, RsaPublicKey};

use crate::LicenseResult;

pub const PUBLIC_RSA_FILENAME: &str = "public.rsa";
pub const PRIVATE_RSA_FILENAME: &str = "private.rsa";

pub struct RsaAes {}

impl RsaAes {
    pub fn hash_md5(str: &str) -> String {
        let mut md5 = Md5::new();
        md5.input_str(str);
        md5.result_str()
    }

    pub fn aes256_encrypt(data: &[u8], key: &[u8; 32], iv: &[u8; 16]) -> LicenseResult<Vec<u8>> {
        let mut encryptor = aes::cbc_encryptor(KeySize256, key, iv, PkcsPadding);

        let mut buffer = [0; 4096];
        let mut write_buffer = RefWriteBuffer::new(&mut buffer);
        let mut read_buffer = RefReadBuffer::new(data);
        let mut final_result = Vec::new();

        loop {
            let result = encryptor.encrypt(&mut read_buffer, &mut write_buffer, true)?;
            final_result.extend(
                write_buffer
                    .take_read_buffer()
                    .take_remaining()
                    .iter()
                    .copied(),
            );

            match result {
                BufferUnderflow => break,
                _ => continue,
            }
        }

        Ok(final_result)
    }

    pub fn aes256_decrypt(data: &[u8], key: &[u8; 32], iv: &[u8; 16]) -> LicenseResult<Vec<u8>> {
        let mut decryptor = aes::cbc_decryptor(KeySize256, key, iv, PkcsPadding);

        let mut buffer = [0; 4096];
        let mut write_buffer = RefWriteBuffer::new(&mut buffer);
        let mut read_buffer = RefReadBuffer::new(data);
        let mut final_result = Vec::new();

        loop {
            let result = decryptor.decrypt(&mut read_buffer, &mut write_buffer, true)?;
            final_result.extend(
                write_buffer
                    .take_read_buffer()
                    .take_remaining()
                    .iter()
                    .copied(),
            );
            match result {
                BufferUnderflow => break,
                _ => continue,
            }
        }

        Ok(final_result)
    }

    pub fn rsa_sign(data: &[u8]) -> LicenseResult<Vec<u8>> {
        let mut rng = rand::thread_rng();

        let private_key = RsaPrivateKey::new(&mut rng, 2048)?;
        let public_key = RsaPublicKey::from(&private_key);

        let private_str = private_key.to_pkcs1_pem(LineEnding::CRLF)?.to_string();

        let public_str = public_key.to_pkcs1_pem(LineEnding::CRLF)?;

        fs::write(PUBLIC_RSA_FILENAME, public_str)?;
        fs::write(PRIVATE_RSA_FILENAME, private_str)?;

        let padding = PaddingScheme::new_pkcs1v15_sign_raw();
        let enc_data = private_key.sign(padding, data)?;

        Ok(enc_data)
    }

    pub fn rsa_verify(data: &[u8], signed: &[u8], pub_key: &str) -> LicenseResult<()> {
        let public_key = RsaPublicKey::from_pkcs1_pem(pub_key)?;

        let padding = PaddingScheme::new_pkcs1v15_sign_raw();
        public_key.verify(padding, data, signed)?;

        Ok(())
    }

    fn test_rsa() {
        let mut rng = rand::thread_rng();

        let bits = 2048;
        let private_key = RsaPrivateKey::new(&mut rng, bits).expect("failed to generate a key");
        let public_key = RsaPublicKey::from(&private_key);

        let private_str = private_key
            .to_pkcs1_pem(LineEnding::CRLF)
            .unwrap()
            .to_string();

        let public_str = public_key.to_pkcs1_pem(LineEnding::CRLF).unwrap();

        println!("PRIVATR:   {}", &private_str);
        println!("PUBLIC:   {}", &public_str);

        /********************* Encrypt Decrypt **********************/
        let data = b"hello world";
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let enc_data = public_key
            .encrypt(&mut rng, padding, &data[..])
            .expect("failed to encrypt");
        assert_ne!(&data[..], &enc_data[..]);

        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let dec_data = private_key
            .decrypt(padding, &enc_data)
            .expect("failed to decrypt");
        assert_eq!(&data[..], &dec_data[..]);

        /********************* Sign Verify **********************/

        let data = b"hello world";
        let padding = PaddingScheme::new_pkcs1v15_sign_raw();
        let enc_data = private_key
            .sign(padding, &data[..])
            .expect("failed to sign");
        assert_ne!(&data[..], &enc_data[..]);

        let base64_str = base64::encode(&enc_data);
        let enc_data = base64::decode(base64_str).unwrap();

        let public_key = RsaPublicKey::from_pkcs1_pem(&public_str).unwrap();

        let padding = PaddingScheme::new_pkcs1v15_sign_raw();
        public_key
            .verify(padding, &data[..], &enc_data)
            .expect("failed to verify");
    }
}

mod test {

    #[test]
    fn test_aes256() {
        use rand::rngs::OsRng;
        use rand::RngCore;

        use crate::rsa_aes::RsaAes;

        let mut rng = OsRng::default();
        let mut key = [0; 32];
        let mut iv = [0; 16];
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut iv);

        let data = "Hello, world!";
        let encrypted_data = RsaAes::aes256_encrypt(data.as_bytes(), &key, &iv).unwrap();
        let decrypted_data = RsaAes::aes256_decrypt(encrypted_data.as_slice(), &key, &iv).unwrap();

        let result = String::from_utf8(decrypted_data.as_slice().to_vec()).unwrap();

        assert_eq!(data, result);
        println!("{}", result);
    }
}
