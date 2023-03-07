use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::{self};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{pin_mut, TryStreamExt};
use snafu::ResultExt;
use spi::query::datasource::WriteContext;
use spi::{BuildParquetArrowWriterSnafu, CloseParquetWriterSnafu, Result, SerializeParquetSnafu};

use crate::data_source::sink::RecordBatchSerializer;

pub struct ParquetRecordBatchSerializer {}

#[async_trait]
impl RecordBatchSerializer for ParquetRecordBatchSerializer {
    async fn to_bytes(
        &self,
        ctx: &WriteContext,
        stream: SendableRecordBatchStream,
    ) -> Result<(usize, Bytes)> {
        let (data, parquet_file_meta) = to_parquet_bytes(ctx, stream).await?;
        let num_rows = parquet_file_meta.num_rows as usize;
        Ok((num_rows, Bytes::from(data)))
    }
}

pub async fn to_parquet_bytes(
    ctx: &WriteContext,
    batches: SendableRecordBatchStream,
) -> Result<(Vec<u8>, parquet::format::FileMetaData)> {
    let mut bytes = vec![];
    let meta = to_parquet(ctx, batches, &mut bytes).await?;
    bytes.shrink_to_fit();

    Ok((bytes, meta))
}

pub async fn to_parquet<W>(
    _ctx: &WriteContext,
    batches: SendableRecordBatchStream,
    output: W,
) -> Result<parquet::format::FileMetaData>
where
    W: Write + Send,
{
    let schema = batches.schema();

    let stream = batches;
    pin_mut!(stream);

    let mut writer = ArrowWriter::try_new(output, Arc::clone(&schema), None)
        .context(BuildParquetArrowWriterSnafu)?;

    while let Some(batch) = stream.try_next().await? {
        writer.write(&batch).context(SerializeParquetSnafu)?;
    }

    let file_meta_data = writer.close().context(CloseParquetWriterSnafu)?;

    Ok(file_meta_data)
}
