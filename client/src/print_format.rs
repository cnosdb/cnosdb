//! Print format variants
use std::str::FromStr;

use datafusion::arrow::csv::writer::WriterBuilder;
use datafusion::arrow::json::{ArrayWriter, LineDelimitedWriter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty;
use datafusion::error::{DataFusionError, Result};
use http_protocol::header::{
    APPLICATION_CSV, APPLICATION_JSON, APPLICATION_NDJSON, APPLICATION_TABLE, APPLICATION_TSV,
};

/// Allow records to be printed in different formats
#[derive(Debug, PartialEq, Eq, clap::ValueEnum, Clone, Copy)]
pub enum PrintFormat {
    Csv,
    Tsv,
    Table,
    Json,
    NdJson,
}

impl FromStr for PrintFormat {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

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
        String::from_utf8(bytes).map_err(|e| DataFusionError::Execution(e.to_string()))?
    }};
}

fn print_batches_with_sep(batches: &[RecordBatch], delimiter: u8) -> Result<String> {
    let mut bytes = vec![];
    {
        let builder = WriterBuilder::new()
            .with_header(true)
            .with_delimiter(delimiter);
        let mut writer = builder.build(&mut bytes);
        for batch in batches {
            writer.write(batch)?;
        }
    }
    let formatted =
        String::from_utf8(bytes).map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(formatted)
}

impl PrintFormat {
    /// print the batches to stdout using the specified format
    pub fn print_batches(&self, batches: &[RecordBatch]) -> Result<()> {
        match self {
            Self::Csv => println!("{}", print_batches_with_sep(batches, b',')?),
            Self::Tsv => println!("{}", print_batches_with_sep(batches, b'\t')?),
            Self::Table => pretty::print_batches(batches)?,
            Self::Json => {
                println!("{}", batches_to_json!(ArrayWriter, batches))
            }
            Self::NdJson => {
                println!("{}", batches_to_json!(LineDelimitedWriter, batches))
            }
        }
        Ok(())
    }

    pub fn get_http_content_type(&self) -> &'static str {
        match self {
            Self::Csv => APPLICATION_CSV,
            Self::Tsv => APPLICATION_TSV,
            Self::Json => APPLICATION_JSON,
            Self::NdJson => APPLICATION_NDJSON,
            Self::Table => APPLICATION_TABLE,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_print_batches_with_sep() {
        let batches = vec![];
        assert_eq!("", print_batches_with_sep(&batches, b',').unwrap());

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

        let batches = &[batch];
        let r = print_batches_with_sep(batches, b',').unwrap();
        assert_eq!("a,b,c\n1,4,7\n2,5,8\n3,6,9\n", r);
    }

    #[test]
    fn test_print_batches_to_json_empty() -> Result<()> {
        let batches = vec![];
        let r = batches_to_json!(ArrayWriter, &batches);
        assert_eq!("", r);

        let r = batches_to_json!(LineDelimitedWriter, &batches);
        assert_eq!("", r);

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

        let batches = vec![&batch];
        let r = batches_to_json!(ArrayWriter, &batches);
        assert_eq!(
            "[{\"a\":1,\"b\":4,\"c\":7},{\"a\":2,\"b\":5,\"c\":8},{\"a\":3,\"b\":6,\"c\":9}]",
            r
        );

        let r = batches_to_json!(LineDelimitedWriter, &batches);
        assert_eq!(
            "{\"a\":1,\"b\":4,\"c\":7}\n{\"a\":2,\"b\":5,\"c\":8}\n{\"a\":3,\"b\":6,\"c\":9}\n",
            r
        );
        Ok(())
    }
}
