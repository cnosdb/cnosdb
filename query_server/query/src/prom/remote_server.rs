use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::ToByteSlice;
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::consistency_level::ConsistencyLevel;
use models::schema::{Precision, TskvTableSchema, TIME_FIELD_NAME};
use protocol_parser::lines_convert::parse_lines_to_points;
use protocol_parser::Line;
use protos::kv_service::WritePointsRequest;
use protos::models_helper::{parse_proto_bytes, to_proto_bytes};
use protos::prompb::remote::{
    Query as PromQuery, QueryResult, ReadRequest, ReadResponse, WriteRequest,
};
use protos::prompb::types::label_matcher::Type;
use protos::prompb::types::TimeSeries;
use protos::FieldValue;
use regex::Regex;
use snap::raw::{decompress_len, max_compress_len, Decoder, Encoder};
use snap::Result as SnapResult;
use spi::server::dbms::DBMSRef;
use spi::server::prom::PromRemoteServer;
use spi::service::protocol::{Context, Query, QueryHandle};
use spi::{QueryError, Result};
use trace::{debug, warn};

use super::time_series::writer::WriterBuilder;
use super::{METRIC_NAME_LABEL, METRIC_SAMPLE_COLUMN_NAME};
use crate::prom::DEFAULT_PROM_TABLE_NAME;

pub struct PromRemoteSqlServer {
    db: DBMSRef,
    codec: SnappyCodec,
    coord: CoordinatorRef,
}

#[async_trait]
impl PromRemoteServer for PromRemoteSqlServer {
    async fn remote_read(&self, ctx: &Context, req: Bytes) -> Result<Vec<u8>> {
        let meta = self
            .coord
            .meta_manager()
            .tenant_manager()
            .tenant_meta(ctx.tenant())
            .await
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: ctx.tenant().to_string(),
            })?;

        let read_request = self.deserialize_read_request(req).await?;

        debug!("Received remote read request: {:?}", read_request);

        let read_response = self.process_read_request(ctx, meta, read_request).await?;

        debug!("Return remote read response: {:?}", read_response);

        self.serialize_read_response(read_response).await
    }

    async fn remote_write(&self, ctx: &Context, req: Bytes) -> Result<()> {
        let prom_write_request = self.deserialize_write_request(req).await?;
        let write_points_request = self
            .prom_write_request_to_write_points_request(ctx, prom_write_request)
            .await?;
        debug!("Received remote write request: {:?}", write_points_request);

        self.coord
            .write_points(
                ctx.tenant().to_string(),
                ConsistencyLevel::Any,
                Precision::NS,
                write_points_request,
            )
            .await?;

        Ok(())
    }
}

impl PromRemoteSqlServer {
    pub fn new(db: DBMSRef, coord: CoordinatorRef) -> Self {
        Self {
            db,
            codec: SnappyCodec::default(),
            coord,
        }
    }

    async fn deserialize_read_request(&self, req: Bytes) -> Result<ReadRequest> {
        let mut decompressed = Vec::new();
        let compressed = req.to_byte_slice();

        self.codec.decompress(compressed, &mut decompressed, None)?;

        parse_proto_bytes::<ReadRequest>(&decompressed).map_err(|source| {
            QueryError::InvalidRemoteReadReq {
                source: Box::new(source),
            }
        })
    }

    async fn deserialize_write_request(&self, req: Bytes) -> Result<WriteRequest> {
        let mut decompressed = Vec::new();
        let compressed = req.to_byte_slice();
        self.codec.decompress(compressed, &mut decompressed, None)?;
        parse_proto_bytes::<WriteRequest>(&decompressed).map_err(|source| {
            QueryError::InvalidRemoteWriteReq {
                source: Box::new(source),
            }
        })
    }

    async fn prom_write_request_to_write_points_request(
        &self,
        ctx: &Context,
        req: WriteRequest,
    ) -> Result<WritePointsRequest> {
        let mut lines = Vec::with_capacity(req.timeseries.len());

        for ts in req.timeseries.iter() {
            let mut table_name = DEFAULT_PROM_TABLE_NAME;
            let tags = ts
                .labels
                .iter()
                .map(|label| {
                    if label.name.eq(METRIC_NAME_LABEL) {
                        table_name = label.value.as_ref()
                    }
                    (label.name.as_ref(), label.value.as_ref())
                })
                .collect::<Vec<(_, _)>>();

            for sample in ts.samples.iter() {
                let fields = vec![(METRIC_SAMPLE_COLUMN_NAME, FieldValue::F64(sample.value))];
                let timestamp = sample.timestamp * 1000000;
                lines.push(Line::new(table_name, tags.clone(), fields, timestamp));
            }
        }

        let points = parse_lines_to_points(ctx.database(), &lines);
        let request = WritePointsRequest {
            version: 0,
            meta: None,
            points,
        };

        Ok(request)
    }

    async fn process_read_request(
        &self,
        ctx: &Context,
        meta: MetaClientRef,
        read_request: ReadRequest,
    ) -> Result<ReadResponse> {
        let mut results = Vec::with_capacity(read_request.queries.len());
        for q in read_request.queries {
            let mut timeseries: Vec<TimeSeries> = Vec::new();
            let sqls = build_sql_with_table(ctx, &meta, q)?;

            debug!("Prepare to execute: {:?}", sqls);

            for sql in sqls {
                timeseries.append(&mut self.process_single_sql(ctx, sql).await?);
            }

            results.push(QueryResult {
                timeseries,
                ..Default::default()
            });
        }

        Ok(ReadResponse {
            results,
            special_fields: Default::default(),
        })
    }

    async fn process_single_sql(
        &self,
        ctx: &Context,
        sql: SqlWithTable,
    ) -> Result<Vec<TimeSeries>> {
        let table_schema = sql.table;
        let tag_name_indices = table_schema.tag_indices();
        let sample_value_idx = table_schema
            .column_index(METRIC_SAMPLE_COLUMN_NAME)
            .ok_or_else(|| QueryError::ColumnNotExists {
                table: table_schema.name.to_string(),
                column: METRIC_SAMPLE_COLUMN_NAME.to_string(),
            })?;
        let sample_time_idx = table_schema.column_index(TIME_FIELD_NAME).ok_or_else(|| {
            QueryError::ColumnNotExists {
                table: table_schema.name.to_string(),
                column: TIME_FIELD_NAME.to_string(),
            }
        })?;

        let inner_query = Query::new(ctx.clone(), sql.sql);
        let result = self.db.execute(&inner_query, None).await?;

        transform_time_series(result, tag_name_indices, sample_value_idx, sample_time_idx).await
    }

    async fn serialize_read_response(&self, read_response: ReadResponse) -> Result<Vec<u8>> {
        let mut compressed = Vec::new();
        let input_buf =
            to_proto_bytes(read_response).map_err(|source| QueryError::CommonError {
                msg: source.to_string(),
            })?;
        self.codec.compress(&input_buf, &mut compressed)?;

        Ok(compressed)
    }
}

fn build_sql_with_table(
    ctx: &Context,
    meta: &MetaClientRef,
    query: PromQuery,
) -> Result<Vec<SqlWithTable>> {
    let PromQuery {
        start_timestamp_ms,
        end_timestamp_ms,
        matchers,
        hints: _,
        special_fields: _,
    } = query;

    let mut tables = Vec::new();
    let mut filters = Vec::with_capacity(matchers.len());

    for m in matchers {
        let type_ = m
            .type_
            .enum_value()
            .map_err(|e| QueryError::InvalidRemoteReadReq {
                source: format!("Unknown label matcher type: {e}").into(),
            })?;

        if METRIC_NAME_LABEL == m.name {
            match type_ {
                Type::EQ => {
                    // Get schema of the specified table
                    let table_name = &m.value;
                    let table = meta
                        .get_tskv_table_schema(ctx.database(), table_name)?
                        .ok_or_else(|| MetaError::TableNotFound {
                            table: table_name.to_string(),
                        })?;
                    tables = vec![table];
                }
                Type::RE => {
                    // Filter table names through regular expressions,
                    // Get the schema of the remaining tables.
                    let pattern =
                        Regex::new(&m.value).map_err(|err| QueryError::InvalidRemoteReadReq {
                            source: Box::new(err),
                        })?;

                    tables = meta
                        .list_tables(ctx.database())?
                        .iter()
                        .filter(|e| pattern.is_match(e))
                        .flat_map(|table_name| {
                            if let Ok(s) = meta.get_tskv_table_schema(ctx.database(), table_name) {
                                s
                            } else {
                                warn!(
                                    "The table {} may have just been dropped, or it may be a bug.",
                                    table_name
                                );
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                }
                _ => {
                    return Err(QueryError::InvalidRemoteReadReq { source: "non-equal or regex-non-equal matchers are not supported on the metric name yet".to_string().into() });
                }
            }

            continue;
        }

        match type_ {
            Type::EQ => {
                filters.push(format!("{} = '{}'", m.name, m.value));
            }
            Type::NEQ => {
                filters.push(format!("{} != '{}'", m.name, m.value));
            }
            Type::RE => {
                filters.push(format!("{} ~ '{}'", m.name, m.value));
            }
            Type::NRE => {
                filters.push(format!("{} !~ '{}'", m.name, m.value));
            }
        }
    }
    // Convert to ns timestamp
    filters.push(format!("time >= {}", start_timestamp_ms * 1_000_000));
    filters.push(format!("time <= {}", end_timestamp_ms * 1_000_000));

    let result = tables
        .into_iter()
        .map(|table| SqlWithTable {
            sql: format!(
                "SELECT * FROM {} WHERE {}",
                table.name,
                filters.join(" AND ")
            ),
            table,
        })
        .collect();

    Ok(result)
}

/// Convert the execution result of query to TimeSeries list of prometheus
async fn transform_time_series(
    query_handle: QueryHandle,
    tag_name_indices: Vec<usize>,
    sample_value_idx: usize,
    sample_time_idx: usize,
) -> Result<Vec<TimeSeries>> {
    let result = query_handle.result();
    let schema = result.schema();
    let batches = result.chunk_result().await?;

    let mut timeseries = HashMap::default();
    {
        let mut writer =
            WriterBuilder::try_new(tag_name_indices, sample_value_idx, sample_time_idx, schema)?
                .build(&mut timeseries);

        for batch in batches {
            writer.write(&batch)?;
        }
    }

    Ok(timeseries.into_values().collect())
}

#[derive(Debug)]
struct SqlWithTable {
    pub sql: String,
    pub table: Arc<TskvTableSchema>,
}

#[derive(Default)]
pub struct SnappyCodec {}

impl SnappyCodec {
    /// Decompresses data stored in slice `input_buf` and appends output to `output_buf`.
    ///
    /// If the uncompress_size is provided it will allocate the exact amount of memory.
    /// Otherwise, it will estimate the uncompressed size, allocating an amount of memory
    /// greater or equal to the real uncompress_size.
    ///
    /// Returns the total number of bytes written.
    fn decompress(
        &self,
        input_buf: &[u8],
        output_buf: &mut Vec<u8>,
        uncompress_size: Option<usize>,
    ) -> SnapResult<usize> {
        let len = match uncompress_size {
            Some(size) => size,
            None => decompress_len(input_buf)?,
        };
        let offset = output_buf.len();
        output_buf.resize(offset + len, 0);
        let mut decoder = Decoder::new();
        decoder.decompress(input_buf, &mut output_buf[offset..])
    }

    /// Compresses data stored in slice `input_buf` and appends the compressed result
    /// to `output_buf`.
    ///
    /// Note that you'll need to call `clear()` before reusing the same `output_buf`
    /// across different `compress` calls.
    fn compress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> SnapResult<()> {
        let output_buf_len = output_buf.len();
        let required_len = max_compress_len(input_buf.len());
        output_buf.resize(output_buf_len + required_len, 0);
        let mut encoder = Encoder::new();
        let n = encoder.compress(input_buf, &mut output_buf[output_buf_len..])?;
        output_buf.truncate(output_buf_len + n);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::vec;

    use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::from_slice::FromSlice;
    use models::auth::user::{User, UserDesc, UserOptions};
    use protos::prompb::types::{Label, Sample, TimeSeries};
    use spi::query::execution::Output;
    use spi::query::recordbatch::RecordBatchStreamWrapper;
    use spi::service::protocol::{ContextBuilder, Query, QueryHandle, QueryId};

    use crate::prom::remote_server::transform_time_series;

    #[tokio::test]
    async fn test_transform_time_series() {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("tag", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from_slice(vec![
                    1673069176267000000,
                ])),
                Arc::new(StringArray::from_slice(vec!["tag1"])),
                Arc::new(Float64Array::from_slice(vec![1.1_f64])),
            ],
        )
        .unwrap();

        let options = UserOptions::default();
        let desc = UserDesc::new(0_u128, "user".to_string(), options, true);
        let query = Query::new(
            ContextBuilder::new(User::new(desc, Default::default())).build(),
            "content".to_string(),
        );

        let query_handle = QueryHandle::new(
            QueryId::next_id(),
            query,
            Output::StreamData(Box::pin(RecordBatchStreamWrapper::new(schema, vec![batch]))),
        );

        let tag_name_indices: Vec<usize> = vec![1];
        let sample_value_idx: usize = 2;
        let sample_time_idx: usize = 0;

        let time_series = transform_time_series(
            query_handle,
            tag_name_indices,
            sample_value_idx,
            sample_time_idx,
        )
        .await
        .unwrap();

        let expect = TimeSeries {
            labels: vec![Label {
                name: "tag".to_string(),
                value: "tag1".to_string(),
                ..Default::default()
            }],
            samples: vec![Sample {
                value: 1.1_f64,
                timestamp: 1673069176267_i64,
                ..Default::default()
            }],
            ..Default::default()
        };

        assert_eq!(vec![expect], time_series);
    }
}
