use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema};
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalTableSchema {
    pub tenant: Arc<str>,
    pub db: Arc<str>,
    pub name: Arc<str>,
    pub file_compression_type: String,
    pub file_type: String,
    pub location: String,
    pub target_partitions: usize,
    pub table_partition_cols: Vec<(String, ArrowDataType)>,
    pub has_header: bool,
    pub delimiter: u8,
    pub schema: Schema,
}

impl ExternalTableSchema {
    pub fn table_options(&self) -> DataFusionResult<ListingOptions> {
        let file_compression_type = FileCompressionType::from_str(&self.file_compression_type)?;
        let file_type = self.file_type.to_uppercase();
        let file_format: Arc<dyn FileFormat> = match file_type.as_str() {
            "CSV" => Arc::new(
                CsvFormat::default()
                    .with_has_header(self.has_header)
                    .with_delimiter(self.delimiter)
                    .with_file_compression_type(file_compression_type),
            ),
            "JSON" | "NDJSON" => {
                Arc::new(JsonFormat::default().with_file_compression_type(file_compression_type))
            }
            "PARQUET" => Arc::new(ParquetFormat::default()),
            "AVRO" => Arc::new(AvroFormat),
            "ARROW" => {
                return Err(DataFusionError::NotImplemented(
                    "Not supported external table file_type: 'ARROW'".to_string(),
                ));
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unknown external table file_type: '{other}'",
                )));
            }
        };

        let options =
            ListingOptions::new(file_format).with_target_partitions(self.target_partitions);

        Ok(options)
    }
}
