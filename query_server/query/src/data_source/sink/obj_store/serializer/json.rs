use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::json::LineDelimitedWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{pin_mut, TryStreamExt};
use snafu::ResultExt;
use spi::query::datasource::WriteContext;
use spi::{Result, SerializeJsonSnafu};

use crate::data_source::sink::RecordBatchSerializer;

pub struct NdJsonRecordBatchSerializer {}

#[async_trait]
impl RecordBatchSerializer for NdJsonRecordBatchSerializer {
    async fn stream_to_bytes(
        &self,
        _ctx: &WriteContext,
        stream: SendableRecordBatchStream,
    ) -> Result<(usize, Bytes)> {
        pin_mut!(stream);

        let mut num_rows = 0;
        let mut bytes = vec![];
        {
            let mut writer = LineDelimitedWriter::new(&mut bytes);

            while let Some(batch) = stream.try_next().await? {
                num_rows += batch.num_rows();
                writer.write(batch).context(SerializeJsonSnafu)?;
            }
        }

        Ok((num_rows, Bytes::from(bytes)))
    }

    async fn batches_to_bytes(
        &self,
        _ctx: &WriteContext,
        _schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<(usize, Bytes)> {
        let mut num_rows = 0;
        let mut bytes = vec![];
        {
            let mut writer = LineDelimitedWriter::new(&mut bytes);

            for batch in batches {
                num_rows += batch.num_rows();
                writer.write(batch.clone()).context(SerializeJsonSnafu)?;
            }
        }

        Ok((num_rows, Bytes::from(bytes)))
    }
}
