use derive_builder::Builder;
use object_store::aws::AmazonS3Builder;

#[derive(Builder)]
#[builder(setter(into, strip_option))]
pub struct S3StorageConfig {
    // pub allow_http: bool,
    #[builder(default = "None")]
    pub endpoint_url: Option<String>,
    pub region: String,
    pub bucket: String,

    #[builder(default = "None")]
    pub access_key_id: Option<String>,
    #[builder(default = "None")]
    pub secret_access_key: Option<String>,
    /// refer to [documentations](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html) for details
    #[builder(default = "None")]
    pub security_token: Option<String>,
    /// API in virtual host style.
    ///
    /// - Virtual Hosted-Style: `https://<bucket>.s3.<region>.amazonaws.com`
    /// - Path-Style: `https://s3.amazonaws.com/<bucket>`
    #[builder(default = "true")]
    pub virtual_hosted_style_request: bool,
}

impl From<S3StorageConfig> for AmazonS3Builder {
    fn from(config: S3StorageConfig) -> Self {
        let builder = AmazonS3Builder::default()
            .with_region(config.region)
            .with_bucket_name(config.bucket)
            .with_virtual_hosted_style_request(config.virtual_hosted_style_request)
            .with_allow_http(true);

        let builder = if let Some(endpoint_url) = config.endpoint_url {
            builder.with_endpoint(endpoint_url)
        } else {
            builder
        };

        let builder = if let Some(access_key_id) = config.access_key_id {
            builder.with_access_key_id(access_key_id)
        } else {
            builder
        };

        let builder = if let Some(secret_access_key) = config.secret_access_key {
            builder.with_secret_access_key(secret_access_key)
        } else {
            builder
        };

        if let Some(security_token) = config.security_token {
            builder.with_token(security_token)
        } else {
            builder
        }
    }
}
