use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use coordinator::service::CoordinatorRef;
use coordinator::SendableCoordinatorRecordBatchStream;
use datafusion::arrow::array::{Array, Float64Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionConfig;
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt, TryStreamExt};
use models::predicate::domain::Predicate;
use models::schema::table_schema::TableSchema;
use models::schema::tskv_table_schema::TskvTableSchema;
use models::schema::{DEFAULT_CATALOG, DEFAULT_DATABASE};
use prost::Message;
use protos::jaeger_api_v2::{Duration, KeyValue, Log, Process, Span, SpanRef, Timestamp};
use protos::jaeger_storage_v1::span_reader_plugin_server::SpanReaderPlugin;
use protos::jaeger_storage_v1::{
    FindTraceIDsRequest, FindTraceIDsResponse, FindTracesRequest, GetOperationsRequest,
    GetOperationsResponse, GetServicesRequest, GetServicesResponse, GetTraceRequest, Operation,
    SpansResponseChunk, TraceQueryParameters,
};
use query::data_source::split::tskv::TableLayoutHandle;
use query::data_source::split::SplitManager;
use snafu::ResultExt;
use spi::{CoordinatorSnafu, ModelsSnafu, QueryError};
use tonic::{Response, Status};
use tskv::reader::QueryOption;

use super::jaeger_write_server::JAEGER_TRACE_TABLE;

pub struct JaegerReadService {
    coord: CoordinatorRef,
}

impl JaegerReadService {
    pub fn new(coord: CoordinatorRef) -> Self {
        Self { coord }
    }

    async fn get_tskv_iterator(
        &self,
        filter_type: FilterType,
    ) -> Result<Vec<SendableCoordinatorRecordBatchStream>, QueryError> {
        let table_schema = self
            .coord
            .meta_manager()
            .read_tableschema(DEFAULT_CATALOG, DEFAULT_DATABASE, JAEGER_TRACE_TABLE)
            .await
            .map_err(|source| QueryError::Meta { source })?;
        let mut iterators = Vec::new();
        if let TableSchema::TsKvTableSchema(tskv_table_schema) = table_schema {
            let (tskv_table_schema, filter_expr, limit) = match &filter_type {
                FilterType::GetTraceID(trace_id) => (
                    tskv_table_schema,
                    Some(
                        col("jaeger_trace.trace_id").eq(lit(trace_id
                            .iter()
                            .map(|b| b.to_string())
                            .collect::<Vec<_>>()
                            .join("_"))),
                    ),
                    None,
                ),
                FilterType::GetServices => (
                    Arc::new(TskvTableSchema::new(
                        DEFAULT_CATALOG.to_string(),
                        DEFAULT_DATABASE.to_string(),
                        JAEGER_TRACE_TABLE.to_string(),
                        tskv_table_schema
                            .columns()
                            .iter()
                            .filter(|col| col.name == "process.service_name" || col.name == "time")
                            .cloned()
                            .collect(),
                    )),
                    None,
                    None,
                ),
                FilterType::GetOperation(service_name, span_kind) => {
                    let filter_expr = if service_name.is_empty() && span_kind.is_empty() {
                        None
                    } else if service_name.is_empty() && !span_kind.is_empty() {
                        Some(col("jaeger_trace.\"tags.span.kind\"").eq(lit(span_kind)))
                    } else if span_kind.is_empty() && !service_name.is_empty() {
                        Some(col("jaeger_trace.\"process.service_name\"").eq(lit(service_name)))
                    } else {
                        Some(
                            col("jaeger_trace.\"process.service_name\"")
                                .eq(lit(service_name))
                                .and(col("jaeger_trace.\"tags.span.kind\"").eq(lit(span_kind))),
                        )
                    };

                    (
                        Arc::new(TskvTableSchema::new(
                            DEFAULT_CATALOG.to_string(),
                            DEFAULT_DATABASE.to_string(),
                            JAEGER_TRACE_TABLE.to_string(),
                            tskv_table_schema
                                .columns()
                                .iter()
                                .filter(|col| {
                                    col.name == "operation_name"
                                        || col.name == "tags.span.name"
                                        || col.name == "tags.span.kind"
                                        || col.name == "process.service_name"
                                        || col.name == "time"
                                })
                                .cloned()
                                .collect(),
                        )),
                        filter_expr,
                        None,
                    )
                }
                FilterType::FindTraces(opt) | FilterType::FindTraceIDs(opt) => {
                    let (filter_expr, limit) = if let Some(query_paras) = opt {
                        let mut filter_expr = col("jaeger_trace.\"process.service_name\"")
                            .eq(lit(&query_paras.service_name));
                        if !query_paras.operation_name.is_empty() {
                            filter_expr = filter_expr.and(
                                col("jaeger_trace.operation_name")
                                    .eq(lit(&query_paras.operation_name)),
                            );
                        }
                        for (k, v) in query_paras.tags.iter() {
                            filter_expr = filter_expr
                                .and(col(format!("jaeger_trace.\"tags.type0.{}\"", k)).eq(lit(v)));
                        }
                        if let Some(start_time_min) = &query_paras.start_time_min {
                            let nanos: i64 = start_time_min.seconds * 1_000_000_000
                                + start_time_min.nanos as i64;
                            filter_expr =
                                filter_expr.and(col("jaeger_trace.time").gt_eq(lit(
                                    ScalarValue::TimestampNanosecond(Some(nanos), None),
                                )));
                        }
                        if let Some(start_time_max) = &query_paras.start_time_max {
                            let nanos: i64 = start_time_max.seconds * 1_000_000_000
                                + start_time_max.nanos as i64;
                            filter_expr =
                                filter_expr.and(col("jaeger_trace.time").lt_eq(lit(
                                    ScalarValue::TimestampNanosecond(Some(nanos), None),
                                )));
                        }
                        if let Some(duration_min) = &query_paras.duration_min {
                            let second = duration_min.seconds as f64
                                + duration_min.nanos as f64 / 1_000_000_000.0;
                            filter_expr =
                                filter_expr.and(col("jaeger_trace.duration").gt_eq(lit(second)));
                        }
                        if let Some(duration_max) = &query_paras.duration_max {
                            let second = duration_max.seconds as f64
                                + duration_max.nanos as f64 / 1_000_000_000.0;
                            if second > 0.0 {
                                filter_expr = filter_expr
                                    .and(col("jaeger_trace.duration").lt_eq(lit(second)));
                            }
                        }
                        (
                            Some(filter_expr.clone()),
                            Some(query_paras.num_traces as usize),
                        )
                    } else {
                        (None, None)
                    };
                    (tskv_table_schema, filter_expr, limit)
                }
            };

            let schema = tskv_table_schema.to_arrow_schema();
            let table_layout = TableLayoutHandle {
                table: tskv_table_schema.clone(),
                predicate: Arc::new(
                    Predicate::push_down_filter(
                        filter_expr,
                        &*tskv_table_schema.to_df_schema()?,
                        &schema,
                        limit,
                    )
                    .context(ModelsSnafu)?,
                ),
            };
            let split_manager = SplitManager::new(self.coord.clone());
            let splits = split_manager
                .splits(
                    &SessionState::with_config_rt(
                        SessionConfig::default(),
                        Arc::new(RuntimeEnv::default()),
                    ),
                    table_layout,
                )
                .await?;

            for split in splits {
                let query_opt = QueryOption::new(
                    4096,
                    split.clone(),
                    None,
                    schema.clone(),
                    tskv_table_schema.clone(),
                );
                iterators.push(
                    self.coord
                        .table_scan(query_opt, None)
                        .context(CoordinatorSnafu)?,
                );
            }
        }
        Ok(iterators)
    }
}

#[tonic::async_trait]
impl SpanReaderPlugin for JaegerReadService {
    type GetTraceStream = JaegerReadStream;
    type FindTracesStream = JaegerReadStream;

    async fn get_trace(
        &self,
        request: tonic::Request<GetTraceRequest>,
    ) -> std::result::Result<tonic::Response<Self::GetTraceStream>, tonic::Status> {
        let trace_id = &request.get_ref().trace_id;
        let iteartors = self
            .get_tskv_iterator(FilterType::GetTraceID(trace_id.clone()))
            .await
            .map_err(|e| {
                Status::internal(format!("func get_trace failed to get tskv iterator {}", e))
            })?;
        Ok(Response::new(JaegerReadStream::new(
            iteartors,
            FilterType::GetTraceID(trace_id.clone()),
        )))
    }

    async fn get_services(
        &self,
        _request: tonic::Request<GetServicesRequest>,
    ) -> std::result::Result<tonic::Response<GetServicesResponse>, tonic::Status> {
        let iteartors = self
            .get_tskv_iterator(FilterType::GetServices)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "func get_services failed to get tskv iterator {}",
                    e
                ))
            })?;

        let mut record_batch_vec = Vec::new();
        for mut iter in iteartors {
            while let Some(record_batch) = iter
                .try_next()
                .await
                .map_err(|source| tonic::Status::internal(source.to_string()))?
            {
                record_batch_vec.push(record_batch);
            }
        }

        let mut services = HashSet::new();
        for batch in record_batch_vec {
            let column = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal("column 1 is not StringArray".to_string()))?;
            for i in 0..column.len() {
                services.insert(column.value(i).to_string());
            }
        }

        Ok(Response::new(GetServicesResponse {
            services: services.into_iter().collect(),
        }))
    }

    async fn get_operations(
        &self,
        request: tonic::Request<GetOperationsRequest>,
    ) -> std::result::Result<tonic::Response<GetOperationsResponse>, tonic::Status> {
        let service = &request.get_ref().service;
        let span_kind = &request.get_ref().span_kind;

        let iteartors = self
            .get_tskv_iterator(FilterType::GetOperation(service.clone(), span_kind.clone()))
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "func get_operations failed to get tskv iterator {}",
                    e
                ))
            })?;

        let mut record_batch_vec = Vec::new();
        for mut iter in iteartors {
            while let Some(record_batch) = iter.try_next().await.map_err(|e| {
                tonic::Status::internal(format!(
                    "func get_operations failed to iter.try_next() {}",
                    e
                ))
            })? {
                record_batch_vec.push(record_batch);
            }
        }

        let mut operation_names = Vec::new();
        let mut operations = Vec::new();
        for batch in record_batch_vec {
            let col_operation_name = batch
                .column_by_name("operation_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(
                    "column operation_name is not StringArray".to_string(),
                ))?;
            let col_span_name = if let Some(col_span_name) = batch.column_by_name("tags.span.name")
            {
                Some(col_span_name.as_any().downcast_ref::<StringArray>().ok_or(
                    Status::internal("column tags.span.name is not StringArray".to_string()),
                )?)
            } else {
                None
            };
            let col_span_kind = batch
                .column_by_name("tags.span.kind")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(
                    "column tags.span.kind is not StringArray".to_string(),
                ))?;

            for i in 0..col_operation_name.len() {
                let operation_name = col_operation_name.value(i).to_string();
                let span_name = if let Some(col_span_name) = col_span_name {
                    let value = col_span_name.value(i).to_string();
                    if !value.is_empty() {
                        JaegerReadStream::decode_kv(&value).v_str
                    } else {
                        "".to_string()
                    }
                } else {
                    "".to_string()
                };
                let value = col_span_kind.value(i).to_string();
                let span_kind = if !value.is_empty() {
                    JaegerReadStream::decode_kv(&value).v_str
                } else {
                    "".to_string()
                };
                operation_names.push(operation_name);
                operations.push(Operation {
                    name: span_name,
                    span_kind,
                });
            }
        }

        Ok(Response::new(GetOperationsResponse {
            operation_names: operation_names.into_iter().collect(),
            operations,
        }))
    }
    async fn find_traces(
        &self,
        request: tonic::Request<FindTracesRequest>,
    ) -> std::result::Result<tonic::Response<Self::FindTracesStream>, tonic::Status> {
        let iteartors = self
            .get_tskv_iterator(FilterType::FindTraces(request.get_ref().query.clone()))
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "func find_traces failed to get tskv iterator {}",
                    e
                ))
            })?;
        Ok(Response::new(JaegerReadStream::new(
            iteartors,
            FilterType::FindTraces(request.get_ref().query.clone()),
        )))
    }
    async fn find_trace_i_ds(
        &self,
        request: tonic::Request<FindTraceIDsRequest>,
    ) -> std::result::Result<tonic::Response<FindTraceIDsResponse>, tonic::Status> {
        let iteartors = self
            .get_tskv_iterator(FilterType::FindTraceIDs(request.get_ref().query.clone()))
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "func find_trace_i_ds failed to get tskv iterator {}",
                    e
                ))
            })?;

        let mut record_batch_vec = Vec::new();
        for mut iter in iteartors {
            while let Some(record_batch) = iter.try_next().await.map_err(|e| {
                tonic::Status::internal(format!(
                    "func find_trace_i_ds failed to iter.try_next() {}",
                    e
                ))
            })? {
                record_batch_vec.push(record_batch);
            }
        }

        let mut trace_ids: Vec<Vec<u8>> = Vec::new();
        for batch in record_batch_vec {
            let col_trace_id = batch
                .column_by_name("trace_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(
                    "column trace_id is not StringArray".to_string(),
                ))?;
            for i in 0..col_trace_id.len() {
                trace_ids.push(
                    col_trace_id
                        .value(i)
                        .split('_')
                        .map(|s| s.parse().unwrap())
                        .collect(),
                );
            }
        }

        Ok(Response::new(FindTraceIDsResponse { trace_ids }))
    }
}

pub enum FilterType {
    GetTraceID(Vec<u8>),
    GetServices,
    GetOperation(String, String),
    FindTraces(Option<TraceQueryParameters>),
    FindTraceIDs(Option<TraceQueryParameters>),
}

pub struct JaegerReadStream {
    iterators: Vec<SendableCoordinatorRecordBatchStream>,
    filter_type: FilterType,
}

impl JaegerReadStream {
    pub fn new(
        iterators: Vec<SendableCoordinatorRecordBatchStream>,
        filter_type: FilterType,
    ) -> Self {
        Self {
            iterators,
            filter_type,
        }
    }

    fn decode_kv(value: &str) -> KeyValue {
        let value = value.split('_').collect::<Vec<_>>();
        let value: Vec<u8> = value.iter().map(|s| s.parse().unwrap()).collect();
        KeyValue::decode(&value[..]).expect("decode KeyValue failed")
    }

    fn get_span(batch: RecordBatch, row_i: usize) -> Result<Span, tonic::Status> {
        let all_col_names = batch
            .schema()
            .all_fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let mut span = Span::default();

        let start_time = batch
            .column_by_name("time")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or(Status::internal(
                "column time is not TimestampNanosecondArray".to_string(),
            ))?
            .value(row_i);
        span.start_time = Some(Timestamp {
            seconds: start_time / 1_000_000_000,
            nanos: (start_time % 1_000_000_000) as i32,
        });

        if let Some(array) = batch.column_by_name("trace_id") {
            span.trace_id = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(
                    "column trace_id is not StringArray".to_string(),
                ))?
                .value(row_i)
                .split('_')
                .map(|s| s.parse().unwrap())
                .collect();
        }

        if let Some(array) = batch.column_by_name("span_id") {
            span.span_id = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(
                    "column span_id is not StringArray".to_string(),
                ))?
                .value(row_i)
                .split('_')
                .map(|s| s.parse().unwrap())
                .collect();
        }

        if let Some(array) = batch.column_by_name("operation_name") {
            span.operation_name = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(
                    "column operation_name is not StringArray".to_string(),
                ))?
                .value(row_i)
                .to_string();
        }

        if let Some(array) = batch.column_by_name("flags") {
            span.flags = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(
                    "column flags is not StringArray".to_string(),
                ))?
                .value(row_i)
                .parse()
                .unwrap();
        }

        if let Some(array) = batch.column_by_name("duration") {
            let duration = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or(Status::internal(
                    "column duration is not Float64Array".to_string(),
                ))?
                .value(row_i);
            let int_part = duration.trunc();
            let frac_part = duration - int_part;
            span.duration = Some(Duration {
                seconds: int_part as i64,
                nanos: (frac_part * 1_000_000_000.0) as i32,
            });
        }

        if let Some(array) = batch.column_by_name("process_id") {
            let process_id = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(
                    "column process_id is not StringArray".to_string(),
                ))?
                .value(row_i);
            span.process_id = process_id.to_string();
        }

        let mut process = Process::default();
        if let Some(array) = batch.column_by_name("process.service_name") {
            process.service_name = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(
                    "column process.service_name is not StringArray".to_string(),
                ))?
                .value(row_i)
                .to_string();
        }

        let mut ref_i = 0;
        let mut log_i = 0;
        let mut warning_i = 0;
        all_col_names.iter().for_each(|col_name| {
            let ref_prefix = "references.".to_owned() + ref_i.to_string().as_str() + ".";
            let log_prefix = "log.".to_owned() + log_i.to_string().as_str() + ".";
            let warning_prefix = "warnings.".to_owned() + warning_i.to_string().as_str() + ".";
            if col_name.starts_with("tags.") {
                let value = batch
                    .column_by_name(col_name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_i)
                    .to_string();
                if !value.is_empty() {
                    span.tags.push(Self::decode_kv(&value));
                }
            } else if col_name.starts_with("process.tags.") {
                let value = batch
                    .column_by_name(col_name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_i)
                    .to_string();
                if !value.is_empty() {
                    process.tags.push(Self::decode_kv(&value));
                }
            } else if col_name.starts_with(&ref_prefix) {
                let ref_ = SpanRef {
                    trace_id: batch
                        .column_by_name((ref_prefix.clone() + "trace_id").as_str())
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row_i)
                        .split('_')
                        .map(|s| s.parse().unwrap())
                        .collect(),
                    span_id: batch
                        .column_by_name((ref_prefix.clone() + "span_id").as_str())
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row_i)
                        .split('_')
                        .map(|s| s.parse().unwrap())
                        .collect(),
                    ref_type: batch
                        .column_by_name((ref_prefix + "ref_type").as_str())
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row_i)
                        .parse()
                        .unwrap(),
                };
                span.references.push(ref_);
                ref_i += 1;
            } else if col_name.starts_with(&log_prefix) {
                let mut logs = Vec::new();
                let mut log = Log::default();
                let log_timestamp = batch
                    .column_by_name((log_prefix.clone() + "timestamp").as_str())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row_i);
                let int_part = log_timestamp.trunc();
                let frac_part = log_timestamp - int_part;
                log.timestamp = Some(Timestamp {
                    seconds: int_part as i64,
                    nanos: (frac_part * 1_000_000_000.0) as i32,
                });
                let log_fields_prefix = log_prefix.clone() + "fields.";
                all_col_names.iter().for_each(|col_name| {
                    if col_name.starts_with(&log_fields_prefix) {
                        let value = batch
                            .column_by_name(col_name)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row_i)
                            .to_string();
                        if !value.is_empty() {
                            log.fields.push(Self::decode_kv(&value));
                        }
                    }
                });
                logs.push(log);
                log_i += 1;
            } else if col_name.starts_with(&warning_prefix) {
                span.warnings.push(
                    batch
                        .column_by_name(col_name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row_i)
                        .to_string(),
                );
                warning_i += 1;
            }
        });
        span.process = Some(process);

        Ok(span)
    }

    fn poll_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<SpansResponseChunk, tonic::Status>>> {
        let mut record_batch_vec: Vec<RecordBatch> = Vec::new();
        for iter in &mut self.iterators {
            match ready!((*iter).poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    record_batch_vec.push(batch);
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(Status::internal(err.to_string())))),
                None => {}
            }
        }
        if record_batch_vec.is_empty() {
            return Poll::Ready(None);
        }

        match self.filter_type {
            FilterType::GetTraceID(_) | FilterType::FindTraces(_) => {
                let mut spans = Vec::new();
                for batch in record_batch_vec {
                    let column_trace_id = batch
                        .column_by_name("trace_id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or(Status::internal(
                            "column trace_id is not StringArray".to_string(),
                        ))?;
                    if let Some(i) = (0..column_trace_id.len()).next() {
                        spans.push(Self::get_span(batch, i)?);
                    }
                }
                Poll::Ready(Some(Ok(SpansResponseChunk { spans })))
            }
            _ => Poll::Ready(None),
        }
    }
}

impl Stream for JaegerReadStream {
    type Item = std::result::Result<SpansResponseChunk, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}
