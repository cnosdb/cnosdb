use derive_builder::Builder;
use object_store::azure::MicrosoftAzureBuilder;

#[derive(Builder)]
#[builder(setter(into, strip_option))]
pub struct AzblobStorageConfig {
    // pub allow_http: bool,
    pub account_name: String,
    pub container_name: String,

    #[builder(default = "None")]
    pub access_key: Option<String>,
    #[builder(default = "None")]
    pub bearer_token: Option<String>,
    #[builder(default = "false")]
    pub use_emulator: bool,
}

impl From<AzblobStorageConfig> for MicrosoftAzureBuilder {
    fn from(config: AzblobStorageConfig) -> Self {
        let builder = MicrosoftAzureBuilder::default()
            .with_account(config.account_name)
            .with_container_name(config.container_name)
            .with_use_emulator(config.use_emulator);

        let builder = if let Some(access_key) = config.access_key {
            builder.with_access_key(access_key)
        } else {
            builder
        };

        if let Some(bearer_token) = config.bearer_token {
            builder.with_bearer_token_authorization(bearer_token)
        } else {
            builder
        }
    }
}
