use arrow_schema::ArrowError;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::record_batch::RecordBatch;

use crate::Error;

pub fn record_batch_encode(record: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let buffer: Vec<u8> = Vec::new();
    let write_options = IpcWriteOptions::default();
    //IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD))?;

    let mut stream_writer =
        StreamWriter::try_new_with_options(buffer, &record.schema(), write_options)?;
    stream_writer.write(record)?;
    stream_writer.finish()?;
    let data = stream_writer.into_inner()?;

    Ok(data)
}

pub fn record_batch_decode(buf: &[u8]) -> Result<RecordBatch, ArrowError> {
    let mut stream_reader = StreamReader::try_new(std::io::Cursor::new(buf), None)?;
    let record = stream_reader
        .next()
        .ok_or(ArrowError::ExternalError(Box::new(Error::NoneRecordBatch)))??;
    Ok(record)
}
