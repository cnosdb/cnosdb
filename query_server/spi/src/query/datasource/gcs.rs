use derive_builder::Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use serde::Serialize;

/// A deserialized `service-account-********.json`-file.
/// ```json
/// {
///    "gcs_base_url": "https://localhost:4443",
///    "disable_oauth": true,
///    "client_email": "",
///    "private_key": ""
/// }
/// ```
#[derive(Serialize, Debug, Builder)]
#[builder(setter(into, strip_option))]
pub struct ServiceAccountCredentials {
    /// Base URL for GCS
    pub gcs_base_url: String,
    /// Disable oauth and use empty tokens.
    #[builder(default = "false")]
    pub disable_oauth: bool,
    /// The email address associated with the service account.
    #[builder(default = "String::default()")]
    pub client_email: String,
    /// The private key in RSA format.
    #[builder(default = "String::default()")]
    pub private_key: String,
}

pub struct GcsStorageConfig {
    pub bucket: String,
    pub service_account_path: tempfile::TempPath,
}

impl From<&GcsStorageConfig> for GoogleCloudStorageBuilder {
    fn from(config: &GcsStorageConfig) -> Self {
        let builder = GoogleCloudStorageBuilder::default().with_bucket_name(&config.bucket);

        if let Some(path) = config.service_account_path.to_str() {
            builder.with_service_account_path(path)
        } else {
            builder
        }
    }
}
