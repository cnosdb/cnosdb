use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema};
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::{DataFusionError, Result as DFResult};
use serde::{Deserialize, Serialize};

/// For legacy SQL `STORED AS csv WITH HEADER ROW`,
/// now should be `STORED AS csv OPTIONS ('format.has_header' 'true')`
pub const EXTERNAL_TABLE_OPTION_CSV_HAS_HEADER: &str = "format.has_header";
/// For legacy SQL `STORED AS csv WITH DELIMITER ','`,
/// now should be `STORED AS csv OPTIONS ('format.delimiter' ',')`
pub const EXTERNAL_TABLE_OPTION_CSV_DELIMITER: &str = "format.delimiter";
/// For legacy SQL `STORED AS json COMPRESSION TYPE gzip`,
/// now should be `STORED AS json OPTIONS ('format.compression' 'gzip')`
pub const EXTERNAL_TABLE_OPTION_COMPRESSION: &str = "format.compression";

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalTableSchema {
    pub tenant: Arc<str>,
    pub db: Arc<str>,
    pub name: Arc<str>,
    pub file_type: String,
    pub location: String,
    pub options: HashMap<String, String>,
    pub target_partitions: usize,
    pub table_partition_cols: Vec<(String, ArrowDataType)>,
    pub schema: Schema,
}

impl ExternalTableSchema {
    pub fn table_options(&self) -> DFResult<ListingOptions> {
        let file_compression_type = match self.options.get(EXTERNAL_TABLE_OPTION_COMPRESSION) {
            Some(t) => FileCompressionType::from_str(t)?,
            None => CompressionTypeVariant::UNCOMPRESSED.into(),
        };
        let file_type = self.file_type.to_uppercase();
        let file_format: Arc<dyn FileFormat> = match file_type.as_str() {
            "CSV" => {
                let mut csv_format =
                    CsvFormat::default().with_file_compression_type(file_compression_type);
                if let Some(has_header_str) = self.options.get(EXTERNAL_TABLE_OPTION_CSV_HAS_HEADER)
                {
                    if let Ok(has_header) = has_header_str.parse::<bool>() {
                        csv_format = csv_format.with_has_header(has_header);
                    }
                }
                if let Some(delimiter) = self.options.get(EXTERNAL_TABLE_OPTION_CSV_DELIMITER) {
                    if !delimiter.is_empty() {
                        csv_format = csv_format.with_delimiter(delimiter.as_bytes()[0]);
                    }
                }
                Arc::new(csv_format)
            }
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
