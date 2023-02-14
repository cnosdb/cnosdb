use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingTable;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use object_store::path::Path;
use trace::debug;
use url::Url;

use crate::data_source::sink::obj_store::serializer::csv::CsvRecordBatchSerializer;
use crate::data_source::sink::obj_store::serializer::json::NdJsonRecordBatchSerializer;
use crate::data_source::sink::obj_store::serializer::parquet::ParquetRecordBatchSerializer;
use crate::data_source::sink::obj_store::ObjectStoreSinkProvider;
use crate::data_source::sink::DynRecordBatchSerializer;
use crate::data_source::WriteExecExt;
use crate::extension::physical::plan_node::table_writer::TableWriterExec;

#[async_trait]
impl WriteExecExt for ListingTable {
    async fn write(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<TableWriterExec>, DataFusionError> {
        let table_paths = self.table_paths();
        debug!("Try get ListingTable's ExecutionPlan: {:?}", table_paths);

        let listing_table_url = unsafe {
            debug_assert_eq!(
                1,
                table_paths.len(),
                "ListingTable has multiple ListingTableUrl"
            );
            table_paths.get_unchecked(0)
        };

        let object_store_url = listing_table_url.object_store();
        let url_str = listing_table_url.to_string();
        let url: &Url = listing_table_url.as_ref();

        debug!("Parse external location: {:?}", url.path());
        let location = Path::parse(url.path())?;

        debug!("Get object_store by url: {:?}", object_store_url);
        let object_store = state.runtime_env().object_store(&object_store_url)?;
        let serializer = get_record_batch_serializer(self.options().format.clone())?;
        let file_extension = self.options().file_extension.clone();

        let record_batch_sink_provider = Arc::new(ObjectStoreSinkProvider::new(
            location,
            object_store,
            serializer,
            file_extension,
        ));

        Ok(Arc::new(TableWriterExec::new(
            input,
            url_str,
            record_batch_sink_provider,
        )))
    }
}

fn get_record_batch_serializer(
    format: Arc<dyn FileFormat>,
) -> Result<Arc<DynRecordBatchSerializer>, DataFusionError> {
    let any = format.as_any();
    let result = if any.is::<ParquetFormat>() {
        Arc::new(ParquetRecordBatchSerializer {}) as _
    } else if let Some(format) = any.downcast_ref::<CsvFormat>() {
        Arc::new(CsvRecordBatchSerializer::new(
            format.has_header(),
            format.delimiter(),
        )) as _
    } else if any.is::<JsonFormat>() {
        Arc::new(NdJsonRecordBatchSerializer {}) as _
    } else {
        return Err(DataFusionError::NotImplemented(
            "Only support ParquetFormat | CsvFormat | JsonFormat.".to_string(),
        ));
    };

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use bytes::Bytes;
    use object_store::aws::AmazonS3Builder;
    use object_store::azure::MicrosoftAzureBuilder;
    use object_store::gcp::GoogleCloudStorageBuilder;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use spi::query::datasource::azure::AzblobStorageConfig;
    use spi::query::datasource::gcs::{GcsStorageConfig, ServiceAccountCredentialsBuilder};
    use spi::query::datasource::s3::S3StorageConfig;

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

    #[tokio::test]
    #[ignore = "no environment"]
    async fn test_gcs_connection() {
        // gcs_base_url: String,
        // disable_oauth: bool,
        // client_email: String,
        // private_key: String,
        let sac = ServiceAccountCredentialsBuilder::default()
            .gcs_base_url("http://0.0.0.0:4443".to_string())
            .disable_oauth(true)
            .build()
            .unwrap();

        let body = serde_json::to_vec(&sac).unwrap();
        let mut tmp = tempfile::NamedTempFile::new().unwrap();

        let _ = tmp.write(&body).unwrap();

        tmp.flush().unwrap();

        let path = tmp.into_temp_path();

        println!("path: {:?}", path.to_str());

        let config = GcsStorageConfig {
            bucket: "csv".into(),
            service_account_path: path,
        };

        let gcs = GoogleCloudStorageBuilder::from(&config).build().unwrap();

        let path = Path::from("/data/csv/test.csv");
        let body = Bytes::from_static(b"bytes");

        gcs.put(&path, body).await.unwrap();

        let _ = gcs.head(&path).await.unwrap();
    }
}
