use derive_builder::Builder;
use object_store::gcp::GoogleCloudStorageBuilder;

/// TODO
#[derive(Builder)]
#[builder(setter(into, strip_option))]
pub struct GcsStorageConfig {
    pub bucket: String,
    pub service_account_path: String,
    // gcs_base_url: String,
    // disable_oauth: bool,
    // client_email: String,
    // private_key: String,
}

impl From<GcsStorageConfig> for GoogleCloudStorageBuilder {
    fn from(config: GcsStorageConfig) -> Self {
        GoogleCloudStorageBuilder::default()
            .with_bucket_name(config.bucket)
            .with_service_account_path(config.service_account_path)
    }
}
