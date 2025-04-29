use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::csv::WriterBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{pin_mut, TryStreamExt};
use snafu::ResultExt;
use spi::query::datasource::WriteContext;
use spi::SerializeCsvSnafu;

use crate::data_source::sink::RecordBatchSerializer;
use crate::data_source::QueryResult;

pub struct CsvRecordBatchSerializer {
    has_header: bool,
    delimiter: u8,
}

impl CsvRecordBatchSerializer {
    pub fn new(has_header: bool, delimiter: u8) -> Self {
        Self {
            has_header,
            delimiter,
        }
    }
}

#[async_trait]
impl RecordBatchSerializer for CsvRecordBatchSerializer {
    async fn stream_to_bytes(
        &self,
        _ctx: &WriteContext,
        stream: SendableRecordBatchStream,
    ) -> QueryResult<(usize, Bytes)> {
        pin_mut!(stream);

        let mut num_rows = 0;
        let mut bytes = vec![];
        {
            let mut writer = WriterBuilder::new()
                .with_header(self.has_header)
                .with_delimiter(self.delimiter)
                .build(&mut bytes);

            while let Some(batch) = stream.try_next().await? {
                num_rows += batch.num_rows();
                writer.write(&batch).context(SerializeCsvSnafu)?;
            }
        }

        Ok((num_rows, Bytes::from(bytes)))
    }

    async fn batches_to_bytes(
        &self,
        _ctx: &WriteContext,
        _schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> QueryResult<(usize, Bytes)> {
        let mut num_rows = 0;
        let mut bytes = vec![];
        {
            let mut writer = WriterBuilder::new()
                .with_header(self.has_header)
                .with_delimiter(self.delimiter)
                .build(&mut bytes);

            for batch in batches {
                num_rows += batch.num_rows();
                writer.write(batch).context(SerializeCsvSnafu)?;
            }
        }

        Ok((num_rows, Bytes::from(bytes)))
    }
}
