use std::sync::Arc;

use object_store::{
    aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder, ObjectStore,
};

use super::logical_planner::ConnectionOptions;

pub mod azure;
pub mod gcs;
pub mod s3;

pub enum UriSchema {
    Azblob,
    Gcs,
    S3,
    Local,
    Custom(&'static str),
}

impl From<&str> for UriSchema {
    fn from(s: &str) -> Self {
        let s = s.to_lowercase();
        match s.as_str() {
            "azblob" => Self::Azblob,
            "gcs" => Self::Gcs,
            "s3" => Self::S3,
            "" | "file" => Self::Local,
            _ => Self::Custom(Box::leak(s.into_boxed_str())),
        }
    }
}

pub fn build_object_store(
    options: ConnectionOptions,
) -> Result<Option<Arc<dyn ObjectStore>>, object_store::Error> {
    let object_store: Option<Arc<dyn ObjectStore>> = match options {
        ConnectionOptions::S3(config) => Some(Arc::new(AmazonS3Builder::from(config).build()?)),
        ConnectionOptions::Gcs(config) => {
            Some(Arc::new(GoogleCloudStorageBuilder::from(config).build()?))
        }
        ConnectionOptions::Azblob(config) => {
            Some(Arc::new(MicrosoftAzureBuilder::from(config).build()?))
        }
        ConnectionOptions::Local => None,
    };

    Ok(object_store)
}
