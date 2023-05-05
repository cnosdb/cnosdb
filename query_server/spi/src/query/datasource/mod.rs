use std::sync::Arc;

use models::oid;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::ObjectStore;

use super::logical_planner::ConnectionOptions;

pub mod azure;
pub mod gcs;
pub mod s3;
pub mod stream;

pub struct WriteContext {
    location: Path,
    task_id: String,
    partition: usize,
    file_extension: String,
}

impl WriteContext {
    pub fn new(
        location: Path,
        task_id: Option<String>,
        partition: usize,
        file_extension: String,
    ) -> Self {
        // If no task_id is specified, a uuid is used to generate one
        let task_id = task_id.unwrap_or_else(|| oid::uuid_u64().to_string());

        Self {
            location,
            task_id,
            partition,
            file_extension,
        }
    }

    pub fn location(&self) -> &Path {
        &self.location
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn partition(&self) -> usize {
        self.partition
    }

    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }
}

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
        ConnectionOptions::Gcs(ref config) => {
            Some(Arc::new(GoogleCloudStorageBuilder::from(config).build()?))
        }
        ConnectionOptions::Azblob(config) => {
            Some(Arc::new(MicrosoftAzureBuilder::from(config).build()?))
        }
        ConnectionOptions::Local => None,
    };

    Ok(object_store)
}
