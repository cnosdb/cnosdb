#![allow(dead_code)]

mod rsa_aes;

use std::{fs, io};

use rsa::pkcs1;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

const AES_IV: &str = "HdeK5LOtDhdjh*9G";
const AES_KEY: &str = "Co7f#ki&Y@xK!OLPOwiaAWP&(Jn9dfje";

#[derive(Snafu, Debug)]
pub enum LicenseError {
    #[snafu(display("Error: {}", msg))]
    CommonError { msg: String },
}

impl From<io::Error> for LicenseError {
    fn from(err: io::Error) -> Self {
        LicenseError::CommonError {
            msg: format!("io error: {}", err),
        }
    }
}

impl From<pkcs1::Error> for LicenseError {
    fn from(err: pkcs1::Error) -> Self {
        LicenseError::CommonError {
            msg: format!("pkcs error: {:?}", err),
        }
    }
}

impl From<rsa::errors::Error> for LicenseError {
    fn from(err: rsa::errors::Error) -> Self {
        LicenseError::CommonError {
            msg: format!("rsa error: {}", err),
        }
    }
}

impl From<crypto::symmetriccipher::SymmetricCipherError> for LicenseError {
    fn from(err: crypto::symmetriccipher::SymmetricCipherError) -> Self {
        LicenseError::CommonError {
            msg: format!("aes error: {:?}", err),
        }
    }
}

pub type LicenseResult<T> = Result<T, LicenseError>;

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct LicenseConfig {
    key: String,
    cores: i32,
    machines: i32,
    product: String,
    expire_time: String,

    signature: String,

    #[serde(skip)]
    filename: String,

    #[serde(skip)]
    expire_stamp: i64,
}

impl LicenseConfig {
    pub fn parse(file: &String) -> LicenseResult<Self> {
        let data = fs::read_to_string(file)?;

        let mut config: LicenseConfig =
            serde_json::from_str(&data).expect("failed to parse config");
        let naive_datetime =
            chrono::NaiveDateTime::parse_from_str(&config.expire_time, "%Y-%m-%d %H:%M:%S")
                .map_err(|_err| LicenseError::CommonError {
                    msg: "parse expire time failed".to_string(),
                })?;

        config.filename = file.clone();
        config.expire_stamp = naive_datetime.timestamp_nanos();

        Ok(config)
    }

    pub fn signature(&mut self) -> LicenseResult<()> {
        let md5 = rsa_aes::RsaAes::hash_md5(&self.text_string());
        let mut rsa_signed = rsa_aes::RsaAes::rsa_sign(md5.as_bytes())?;

        let public_key = fs::read_to_string(rsa_aes::PUBLIC_RSA_FILENAME)?;
        let mut public_key = public_key.as_bytes().to_vec();

        let mut data: Vec<u8> = (public_key.len() as u32).to_be_bytes().try_into().unwrap();
        data.append(&mut public_key);
        data.append(&mut rsa_signed);

        let iv: [u8; 16] = AES_IV.as_bytes().try_into().unwrap();
        let key: [u8; 32] = AES_KEY.as_bytes().try_into().unwrap();

        let enc_data = rsa_aes::RsaAes::aes256_encrypt(&data, &key, &iv)?;
        self.signature = base64::encode(enc_data);

        let data = serde_json::to_string_pretty(self).expect("encode to json failed");

        fs::write(&self.filename, data)?;

        Ok(())
    }

    pub fn verify(&self) -> LicenseResult<()> {
        let enc_data =
            base64::decode(&self.signature).map_err(|err| LicenseError::CommonError {
                msg: format!("base64 decode error: {}", err),
            })?;

        let iv: [u8; 16] = AES_IV.as_bytes().try_into().unwrap();
        let key: [u8; 32] = AES_KEY.as_bytes().try_into().unwrap();
        let enc_data = rsa_aes::RsaAes::aes256_decrypt(&enc_data, &key, &iv)?;

        let ken_len_buf: [u8; 4] = enc_data[0..4].try_into().unwrap();
        let public_key_len = u32::from_be_bytes(ken_len_buf) as usize;

        let md5 = rsa_aes::RsaAes::hash_md5(&self.text_string());
        let signed = &enc_data[public_key_len + 4..];
        let pub_key = String::from_utf8((enc_data[4..public_key_len + 4]).to_vec()).unwrap();
        rsa_aes::RsaAes::rsa_verify(md5.as_bytes(), signed, &pub_key)?;

        Ok(())
    }

    fn text_string(&self) -> String {
        format!(
            "{}.{}.{}.{}.{}",
            self.key, self.cores, self.machines, self.product, self.expire_time
        )
    }
}

mod test {

    use crate::LicenseConfig;

    // cargo test --package license --lib -- test::test_license --exact --nocapture
    fn test_license() {
        let mut license =
            LicenseConfig::parse(&"../../common/license/src/test.json".to_string()).unwrap();

        license.signature().unwrap();

        license.verify().unwrap();
    }
}
