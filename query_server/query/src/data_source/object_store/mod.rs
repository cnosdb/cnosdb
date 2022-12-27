#[cfg(test)]
mod tests {
    use std::io::Write;

    use bytes::Bytes;
    use object_store::{
        aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder,
        path::Path, ObjectStore,
    };

    use serde::Serialize;
    use spi::query::datasource::{
        azure::AzblobStorageConfig, gcs::GcsStorageConfig, s3::S3StorageConfig,
    };

    #[tokio::test]
    #[ignore = "no environment"]
    async fn test_s3_connection() {
        let config = S3StorageConfig {
            endpoint_url: Some(
                "http://sample-bucket.s3.us-east-1.localhost.localstack.cloud:4566".into(),
            ),
            region: "us-east-1".into(),
            bucket: "sample-bucket".into(),
            access_key_id: Some("test".into()),
            secret_access_key: Some("test".into()),
            security_token: None,
            virtual_hosted_style_request: true,
        };

        let s3 = AmazonS3Builder::from(config).build().unwrap();

        let path = Path::from("/data/csv/test.csv");
        let body = Bytes::from_static(b"bytes");

        s3.put(&path, body).await.unwrap();

        let _ = s3.head(&path).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "no environment"]
    async fn test_azblob_connection() {
        let config = AzblobStorageConfig {
            account_name: "devstoreaccount1".to_string(),
            container_name: "dev".to_string(),
            access_key: Some("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into()),
            bearer_token: None,
            use_emulator: true,
        };

        let azblob = MicrosoftAzureBuilder::from(config).build().unwrap();

        let path = Path::from("/data/csv/test.csv");
        let body = Bytes::from_static(b"bytes");

        azblob.put(&path, body).await.unwrap();

        let _ = azblob.head(&path).await.unwrap();
    }

    /// A deserialized `service-account-********.json`-file.
    #[derive(Serialize, Debug)]
    struct ServiceAccountCredentials {
        /// The private key in RSA format.
        pub private_key: String,

        /// The email address associated with the service account.
        pub client_email: String,

        /// Base URL for GCS
        pub gcs_base_url: String,

        /// Disable oauth and use empty tokens.
        pub disable_oauth: bool,
    }

    #[tokio::test]
    #[ignore = "no environment"]
    async fn test_gcs_connection() {
        // gcs_base_url: String,
        // disable_oauth: bool,
        // client_email: String,
        // private_key: String,
        let path = ServiceAccountCredentials {
            gcs_base_url: "http://0.0.0.0:4443".into(),
            disable_oauth: true,
            client_email: "".into(),
            private_key: "".into(),
        };

        let body = serde_json::to_vec(&path).unwrap();
        let mut tmp = tempfile::NamedTempFile::new().unwrap();

        let _ = tmp.write(&body).unwrap();

        tmp.flush().unwrap();

        let path = tmp.path().to_str().unwrap().to_string();

        println!("path: {}", path);

        let config = GcsStorageConfig {
            bucket: "csv".into(),
            service_account_path: path,
        };

        let gcs = GoogleCloudStorageBuilder::from(config).build().unwrap();

        let path = Path::from("/data/csv/test.csv");
        let body = Bytes::from_static(b"bytes");

        gcs.put(&path, body).await.unwrap();

        let _ = gcs.head(&path).await.unwrap();
    }
}
