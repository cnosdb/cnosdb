use std::str::FromStr;

use datafusion::arrow::csv::writer::WriterBuilder;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::json::{ArrayWriter, LineDelimitedWriter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use http_protocol::encoding::Encoding;
use http_protocol::header::{
    APPLICATION_CSV, APPLICATION_JSON, APPLICATION_NDJSON, APPLICATION_PREFIX, APPLICATION_STAR,
    APPLICATION_TABLE, APPLICATION_TSV, STAR_STAR, TEXT_PREFIX,
};
use metrics::count::U64Counter;
use trace::error;
use warp::http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use warp::http::StatusCode;
use warp::reply::Response;
use warp::{reject, Rejection};

use super::Error as HttpError;
use crate::http::header::Header;
use crate::http::response::ResponseBuilder;

macro_rules! batches_to_json {
    ($WRITER: ident, $batches: expr) => {{
        let mut bytes = vec![];
        {
            let mut writer = $WRITER::new(&mut bytes);
            for batch in $batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        Ok(bytes)
    }};
}

fn batches_with_sep(
    batches: &[RecordBatch],
    delimiter: u8,
    has_headers: bool,
) -> ArrowResult<Vec<u8>> {
    let mut bytes = vec![];
    {
        let builder = WriterBuilder::new()
            .with_header(has_headers)
            .with_delimiter(delimiter);
        let mut writer = builder.build(&mut bytes);
        for batch in batches {
            writer.write(batch)?;
        }
    }
    Ok(bytes)
}

/// Allow records to be printed in different formats
#[derive(Debug, PartialEq, Eq, clap::ValueEnum, Clone)]
pub enum ResultFormat {
    Csv,
    Tsv,
    Json,
    NdJson,
    Table,
}

impl ResultFormat {
    pub fn get_http_content_type(&self) -> &'static str {
        match self {
            Self::Csv => APPLICATION_CSV,
            Self::Tsv => APPLICATION_TSV,
            Self::Json => APPLICATION_JSON,
            Self::NdJson => APPLICATION_NDJSON,
            Self::Table => APPLICATION_TABLE,
        }
    }

    pub fn format_batches(
        &self,
        batches: &[RecordBatch],
        has_headers: bool,
    ) -> ArrowResult<Vec<u8>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        match self {
            Self::Csv => batches_with_sep(batches, b',', has_headers),
            Self::Tsv => batches_with_sep(batches, b'\t', has_headers),
            Self::Json => batches_to_json!(ArrayWriter, batches),
            Self::NdJson => {
                batches_to_json!(LineDelimitedWriter, batches)
            }
            Self::Table => Ok(pretty_format_batches(batches)?.to_string().into_bytes()),
        }
    }

    pub fn wrap_batches_to_response(
        &self,
        batches: &[RecordBatch],
        has_headers: bool,
        http_query_data_out: U64Counter,
        result_encoding: Option<Encoding>,
    ) -> Result<Response, HttpError> {
        let mut result =
            self.format_batches(batches, has_headers)
                .map_err(|e| HttpError::FetchResult {
                    reason: format!("{}", e),
                })?;

        let mut builder = ResponseBuilder::new(StatusCode::OK)
            .insert_header((CONTENT_TYPE, self.get_http_content_type()));
        if let Some(encoding) = result_encoding {
            builder = builder.insert_header((CONTENT_ENCODING, encoding.to_header_value()));
            result = encoding
                .encode(result)
                .map_err(|e| HttpError::EncodeResponse { source: e })?;
        }
        http_query_data_out.inc(result.len() as u64);

        let resp = builder.build(result);

        Ok(resp)
    }
}

impl TryFrom<&str> for ResultFormat {
    type Error = HttpError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.is_empty() || s == APPLICATION_STAR || s == STAR_STAR {
            return Ok(ResultFormat::Csv);
        }

        if let Some(fmt) = s.strip_prefix(APPLICATION_PREFIX) {
            return ResultFormat::from_str(fmt)
                .map_err(|reason| HttpError::InvalidHeader { reason });
        }

        if let Some(fmt) = s.strip_prefix(TEXT_PREFIX) {
            return ResultFormat::from_str(fmt)
                .map_err(|reason| HttpError::InvalidHeader { reason });
        }

        Err(HttpError::InvalidHeader {
            reason: format!("accept type not support: {}", s),
        })
    }
}

impl FromStr for ResultFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

pub fn get_result_format_from_header(header: &Header) -> Result<ResultFormat, Rejection> {
    ResultFormat::try_from(header.get_accept()).map_err(|e| {
        let e = HttpError::InvalidHeader {
            reason: format!("{}", e),
        };
        error!("get_result_format_from_header: {:?}", e);
        reject::custom(e)
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_format_batches_with_sep() {
        let batches = vec![];
        assert_eq!(
            "".as_bytes(),
            batches_with_sep(&batches, b',', true).unwrap()
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap();

        let batches = vec![batch];
        let r = batches_with_sep(&batches, b',', true).unwrap();
        assert_eq!("a,b,c\n1,4,7\n2,5,8\n3,6,9\n".as_bytes(), r);
    }

    #[test]
    fn test_format_batches_to_json_empty() -> ArrowResult<()> {
        let batches = vec![];
        let r: ArrowResult<Vec<u8>> = batches_to_json!(ArrayWriter, &batches);
        assert_eq!("".as_bytes(), r?);

        let r: ArrowResult<Vec<u8>> = batches_to_json!(LineDelimitedWriter, &batches);
        assert_eq!("".as_bytes(), r?);

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap();

        let batches = vec![batch];
        let r: ArrowResult<Vec<u8>> = batches_to_json!(ArrayWriter, &batches);
        assert_eq!(
            "[{\"a\":1,\"b\":4,\"c\":7},{\"a\":2,\"b\":5,\"c\":8},{\"a\":3,\"b\":6,\"c\":9}]"
                .as_bytes(),
            r?
        );

        let r: ArrowResult<Vec<u8>> = batches_to_json!(LineDelimitedWriter, &batches);
        assert_eq!(
            "{\"a\":1,\"b\":4,\"c\":7}\n{\"a\":2,\"b\":5,\"c\":8}\n{\"a\":3,\"b\":6,\"c\":9}\n"
                .as_bytes(),
            r?
        );
        Ok(())
    }
}
