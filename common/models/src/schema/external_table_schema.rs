use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema};
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalTableSchema {
    pub tenant: String,
    pub db: String,
    pub name: String,
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
        let file_type = FileType::from_str(&self.file_type)?;
        let file_format: Arc<dyn FileFormat> = match file_type {
            FileType::CSV => Arc::new(
                CsvFormat::default()
                    .with_has_header(self.has_header)
                    .with_delimiter(self.delimiter)
                    .with_file_compression_type(file_compression_type),
            ),
            FileType::PARQUET => Arc::new(ParquetFormat::default()),
            FileType::AVRO => Arc::new(AvroFormat),
            FileType::JSON => {
                Arc::new(JsonFormat::default().with_file_compression_type(file_compression_type))
            }
            FileType::ARROW => {
                return Err(DataFusionError::NotImplemented(
                    "Arrow external table.".to_string(),
                ));
            }
        };

        let options =
            ListingOptions::new(file_format).with_target_partitions(self.target_partitions);

        Ok(options)
    }
}
