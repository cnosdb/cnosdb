use std::collections::HashMap;
use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use coordinator::SendableCoordinatorRecordBatchStream;
use datafusion::arrow::array::{
    Array, Float64Array, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionConfig;
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::parser::ParserError;
use http_protocol::parameter::FindTracesParam;
use models::predicate::domain::Predicate;
use models::schema::table_schema::TableSchema;
use models::schema::tskv_table_schema::TskvTableSchema;
use prost::alloc::vec::Vec;
use protos::common::any_value::Value;
use protos::common::AnyValue;
use protos::trace::status::StatusCode;
use query::data_source::split::tskv::TableLayoutHandle;
use query::data_source::split::SplitManager;
use snafu::ResultExt;
use spi::{CoordinatorSnafu, ModelsSnafu, QueryError};
use tonic::Status;
use tskv::reader::QueryOption;

use super::jaeger_model::{KeyValue, Log, Process, Reference, ReferenceType, Span};

pub const TRACE_ID_COL_NAME: &str = "ResourceSpans/ScopeSpans/Span/trace_id";
pub const SPAN_ID_COL_NAME: &str = "ResourceSpans/ScopeSpans/Span/span_id";
pub const PARENT_SPAN_ID_COL_NAME: &str = "ResourceSpans/ScopeSpans/Span/parent_span_id";
pub const FLAGS_COL_NAME: &str = "ResourceSpans/ScopeSpans/Span/flags";
pub const OPERATION_NAME_COL_NAME: &str = "ResourceSpans/ScopeSpans/Span/name";
pub const SPAN_KIND_COL_NAME: &str = "ResourceSpans/ScopeSpans/Span/kind";
pub const DURATION_COL_NAME: &str = "ResourceSpans/ScopeSpans/Span/duration_nano";
pub const STATUS_CODE_COL_NAME: &str = "ResourceSpans/ScopeSpans/Span/Status/code";
pub const STATUS_MESSAGE_COL_NAME: &str = "ResourceSpans/ScopeSpans/Span/Status/message";
pub const LIBRARY_NAME_COL_NAME: &str = "ResourceSpans/ScopeSpans/InstrumentationScope/name";
pub const LIBRARY_VERSION_COL_NAME: &str = "ResourceSpans/ScopeSpans/InstrumentationScope/version";
pub const SERVICE_NAME_COL_NAME: &str = "ResourceSpans/Resource/attributes/service.name";
pub const RESOURCE_DROPPED_ATTRIBUTES_COUNT_COL_NAME: &str =
    "ResourceSpans/Resource/dropped_attributes_count";
pub const SPAN_DROPPED_ATTRIBUTES_COUNT_COL_NAME: &str =
    "ResourceSpans/ScopeSpans/Span/dropped_attributes_count";
pub const SPAN_DROPPED_EVENTS_COUNT_COL_NAME: &str =
    "ResourceSpans/ScopeSpans/Span/dropped_events_count";
pub const SPAN_DROPPED_LINKS_COUNT_COL_NAME: &str =
    "ResourceSpans/ScopeSpans/Span/dropped_links_count";
pub const TRACE_STATE_COL_NAME: &str = "ResourceSpans/ScopeSpans/Span/trace_state";

pub enum FilterType {
    GetTraceID(String),
    GetServices,
    GetOperation(String, String),
    FindTraces(Option<FindTracesParam>),
    FindTraceIDs(Option<FindTracesParam>),
}

pub struct OtlpToJaeger {}

impl OtlpToJaeger {
    pub async fn get_tskv_iterator(
        coord: CoordinatorRef,
        filter_type: FilterType,
        tenant: String,
        db: String,
        table: String,
    ) -> Result<Vec<SendableCoordinatorRecordBatchStream>, QueryError> {
        let table_schema = coord
            .meta_manager()
            .read_tableschema(&tenant, &db, &table)
            .await
            .map_err(|source| QueryError::Meta { source })?;

        let mut iterators = Vec::new();
        if let TableSchema::TsKvTableSchema(tskv_table_schema) = table_schema {
            let (tskv_table_schema, filter_expr, limit) = match &filter_type {
                FilterType::GetTraceID(trace_id) => (
                    tskv_table_schema,
                    Some(col(format!("{}.\"{}\"", table, TRACE_ID_COL_NAME)).eq(lit(trace_id))),
                    None,
                ),
                FilterType::GetServices => (
                    Arc::new(TskvTableSchema::new(
                        tenant,
                        db,
                        table,
                        tskv_table_schema
                            .columns()
                            .iter()
                            .filter(|col| col.name == SERVICE_NAME_COL_NAME || col.name == "time")
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
                        Some(
                            col(format!("{}.\"{}\"", table, SPAN_KIND_COL_NAME)).eq(lit(span_kind)),
                        )
                    } else if span_kind.is_empty() && !service_name.is_empty() {
                        Some(
                            col(format!("{}.\"{}\"", table, SERVICE_NAME_COL_NAME))
                                .eq(lit(service_name)),
                        )
                    } else {
                        Some(
                            col(format!("{}.\"{}\"", table, SERVICE_NAME_COL_NAME))
                                .eq(lit(service_name))
                                .and(
                                    col(format!("{}.\"{}\"", table, SPAN_KIND_COL_NAME))
                                        .eq(lit(span_kind)),
                                ),
                        )
                    };

                    (
                        Arc::new(TskvTableSchema::new(
                            tenant,
                            db,
                            table,
                            tskv_table_schema
                                .columns()
                                .iter()
                                .filter(|col| {
                                    col.name == SERVICE_NAME_COL_NAME
                                        || col.name == OPERATION_NAME_COL_NAME
                                        || col.name == SPAN_KIND_COL_NAME
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
                        let mut filter_expr_opt = None;

                        if let Some(service) = &query_paras.service {
                            if !service.is_empty() {
                                filter_expr_opt = Some(
                                    col(format!("{}.\"{}\"", table, SERVICE_NAME_COL_NAME))
                                        .eq(lit(service)),
                                );
                            }
                        }

                        if let Some(operation) = &query_paras.operation {
                            if !operation.is_empty() {
                                let expr =
                                    col(format!("{}.\"{}\"", table, OPERATION_NAME_COL_NAME))
                                        .eq(lit(operation));
                                if let Some(filter_expr) = filter_expr_opt {
                                    filter_expr_opt = Some(filter_expr.and(expr));
                                } else {
                                    filter_expr_opt = Some(expr);
                                }
                            }
                        }

                        if let Some(start) = &query_paras.start {
                            let nanos: i64 = start * 1_000;
                            let expr = col(format!("{}.\"time\"", table))
                                .gt_eq(lit(ScalarValue::TimestampNanosecond(Some(nanos), None)));
                            if let Some(filter_expr) = filter_expr_opt {
                                filter_expr_opt = Some(filter_expr.and(expr));
                            } else {
                                filter_expr_opt = Some(expr);
                            }
                        }

                        if let Some(end) = &query_paras.end {
                            let nanos = (end * 1_000) as f64;
                            let expr = col(format!(
                                "{}.\"ResourceSpans/ScopeSpans/Span/end_time_unix_nano\"",
                                table
                            ))
                            .lt_eq(lit(nanos));
                            if let Some(filter_expr) = filter_expr_opt {
                                filter_expr_opt = Some(filter_expr.and(expr));
                            } else {
                                filter_expr_opt = Some(expr);
                            }
                        }
                        let mut tag_map = HashMap::new();
                        if let Some(tag) = &query_paras.tag {
                            let tag = tag.split(',').collect::<Vec<&str>>();
                            for item in tag.iter() {
                                let parts: Vec<&str> = item.split(':').collect();
                                if parts.len() == 2 {
                                    tag_map.insert(parts[0].to_string(), parts[1].to_string());
                                }
                            }
                        }
                        if let Some(tags) = &query_paras.tags {
                            let tags = tags.split(',').collect::<Vec<&str>>();
                            for item in tags.iter() {
                                let parts: HashMap<String, String> =
                                    serde_json::from_str(item).unwrap();
                                tag_map.extend(parts);
                            }
                        }
                        for (k, v) in tag_map.iter() {
                            let expr = col(format!(
                                "{}.\"ResourceSpans/ScopeSpans/Span/attributes/{}\"",
                                table, k
                            ))
                            .eq(lit(v));
                            if let Some(filter_expr) = filter_expr_opt {
                                filter_expr_opt = Some(filter_expr.and(expr));
                            } else {
                                filter_expr_opt = Some(expr);
                            }
                        }

                        if let Some(duration_min) = &query_paras.min_duration {
                            let duration_min = Self::parse_duration(duration_min)? as f64;
                            let expr = col(format!(
                                "{}.\"ResourceSpans/ScopeSpans/Span/duration_nano\"",
                                table
                            ))
                            .gt_eq(lit(duration_min));
                            if let Some(filter_expr) = filter_expr_opt {
                                filter_expr_opt = Some(filter_expr.and(expr));
                            } else {
                                filter_expr_opt = Some(expr);
                            }
                        }

                        if let Some(duration_max) = &query_paras.max_duration {
                            let duration_max = Self::parse_duration(duration_max)? as f64;
                            let expr = col(format!(
                                "{}.\"ResourceSpans/ScopeSpans/Span/duration_nano\"",
                                table
                            ))
                            .lt_eq(lit(duration_max));
                            if let Some(filter_expr) = filter_expr_opt {
                                filter_expr_opt = Some(filter_expr.and(expr));
                            } else {
                                filter_expr_opt = Some(expr);
                            }
                        }

                        let limit = query_paras.limit.as_ref().copied();

                        (filter_expr_opt, limit)
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
            let split_manager = SplitManager::new(coord.clone());
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
                    tskv_table_schema.meta(),
                );
                iterators.push(
                    coord
                        .table_scan(query_opt, None)
                        .context(CoordinatorSnafu)?,
                );
            }
        }
        Ok(iterators)
    }

    fn normal_decode_kv(col_name: &str, value: AnyValue) -> KeyValue {
        if let Some(Value::StringValue(v_str)) = value.value {
            KeyValue {
                key: col_name.to_string(),
                value_type: Some(super::jaeger_model::ValueType::String),
                value: serde_json::Value::String(v_str),
            }
        }else{
            KeyValue {
                key: col_name.to_string(),
                value_type: Some(super::jaeger_model::ValueType::String),
                value: serde_json::Value::String("".to_string()),
            }
        }

    }

    pub fn to_jaeger_span_kind(span_kind: &str) -> &str {
        match span_kind {
            "SPAN_KIND_UNSPECIFIED" => "unspecified",
            "SPAN_KIND_INTERNAL" => "internal",
            "SPAN_KIND_SERVER" => "server",
            "SPAN_KIND_CLIENT" => "client",
            "SPAN_KIND_PRODUCER" => "producer",
            "SPAN_KIND_CONSUMER" => "consumer",
            _ => "unspecified",
        }
    }

    pub fn recordbatch_to_span(batch: RecordBatch, row_i: usize) -> Result<Span, tonic::Status> {
        let mut span = Span::default();

        if let Some(array) = batch.column_by_name(TRACE_ID_COL_NAME) {
            span.trace_id = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(format!(
                    "column {} is not StringArray",
                    TRACE_ID_COL_NAME
                )))?
                .value(row_i)
                .to_string();
        }

        if let Some(array) = batch.column_by_name(SPAN_ID_COL_NAME) {
            span.span_id = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(format!(
                    "column {} is not StringArray",
                    SPAN_ID_COL_NAME
                )))?
                .value(row_i)
                .to_string();
        }

        if let Some(array) = batch.column_by_name(PARENT_SPAN_ID_COL_NAME) {
            span.parent_span_id = Some(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or(Status::internal(format!(
                        "column {} is not StringArray",
                        PARENT_SPAN_ID_COL_NAME
                    )))?
                    .value(row_i)
                    .to_string(),
            );
        }

        if let Some(array) = batch.column_by_name(FLAGS_COL_NAME) {
            span.flags = Some(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or(Status::internal(format!(
                        "column {} is not Float64Array",
                        FLAGS_COL_NAME
                    )))?
                    .value(row_i) as u32,
            );
        }

        if let Some(array) = batch.column_by_name(OPERATION_NAME_COL_NAME) {
            span.operation_name = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(format!(
                    "column {} is not StringArray",
                    OPERATION_NAME_COL_NAME
                )))?
                .value(row_i)
                .to_string();
        }

        if let Some(array) = batch.column_by_name(SPAN_KIND_COL_NAME) {
            let span_kind = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(format!(
                    "column {} is not StringArray",
                    SPAN_KIND_COL_NAME
                )))?
                .value(row_i);
            let span_kind = Self::to_jaeger_span_kind(span_kind).to_string();

            span.tags.push(KeyValue {
                key: "span.kind".to_string(),
                value_type: Some(super::jaeger_model::ValueType::String),
                value: serde_json::Value::String(span_kind),
            });
        }

        span.start_time = (batch
            .column_by_name("time")
            .ok_or(Status::internal("column time is not exist".to_string()))?
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or(Status::internal(
                "column time is not TimestampNanosecondArray".to_string(),
            ))?
            .value(row_i)
            / 1_000) as u64;

        if let Some(array) = batch.column_by_name(DURATION_COL_NAME) {
            span.duration = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or(Status::internal(format!(
                    "column {} is not Float64Array",
                    DURATION_COL_NAME
                )))?
                .value(row_i) as u64
                / 1_000;
        }

        let mut process = Process::default();
        if let Some(array) = batch.column_by_name(STATUS_CODE_COL_NAME) {
            let status_code = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(format!(
                    "column {} is not StringArray",
                    STATUS_CODE_COL_NAME
                )))?
                .value(row_i)
                .to_string();
            process.tags.push(KeyValue {
                key: "otel.status_code".to_string(),
                value_type: Some(super::jaeger_model::ValueType::String),
                value: serde_json::Value::String(status_code.clone()),
            });
            if StatusCode::from_str_name(&status_code).eq(&Some(StatusCode::Error)) {
                process.tags.push(KeyValue {
                    key: "error".to_string(),
                    value_type: Some(super::jaeger_model::ValueType::Bool),
                    value: serde_json::Value::Bool(true),
                });
            }
        }

        if let Some(array) = batch.column_by_name(STATUS_MESSAGE_COL_NAME) {
            process.tags.push(KeyValue {
                key: "otel.status_description".to_string(),
                value_type: Some(super::jaeger_model::ValueType::String),
                value: serde_json::Value::String(
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or(Status::internal(format!(
                            "column {} is not StringArray",
                            STATUS_MESSAGE_COL_NAME
                        )))?
                        .value(row_i)
                        .to_string(),
                ),
            });
        }

        if let Some(array) = batch.column_by_name(LIBRARY_NAME_COL_NAME) {
            process.tags.push(KeyValue {
                key: "otel.library.name".to_string(),
                value_type: Some(super::jaeger_model::ValueType::String),
                value: serde_json::Value::String(
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or(Status::internal(format!(
                            "column {} is not StringArray",
                            LIBRARY_NAME_COL_NAME
                        )))?
                        .value(row_i)
                        .to_string(),
                ),
            });
        }

        if let Some(array) = batch.column_by_name(LIBRARY_VERSION_COL_NAME) {
            process.tags.push(KeyValue {
                key: "otel.library.version".to_string(),
                value_type: Some(super::jaeger_model::ValueType::String),
                value: serde_json::Value::String(
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or(Status::internal(format!(
                            "column {} is not StringArray",
                            LIBRARY_VERSION_COL_NAME
                        )))?
                        .value(row_i)
                        .to_string(),
                ),
            });
        }

        if let Some(array) = batch.column_by_name(SERVICE_NAME_COL_NAME) {
            process.service_name = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(Status::internal(format!(
                    "column {} is not StringArray",
                    SERVICE_NAME_COL_NAME
                )))?
                .value(row_i)
                .to_string();
        }

        let all_col_names = batch
            .schema()
            .all_fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let mut link_i = 0;
        let mut event_i = 0;
        for col_name in &all_col_names {
            let link_prefix = "ResourceSpans/ScopeSpans/Span/Link_".to_owned()
                + link_i.to_string().as_str()
                + "/";
            let event_prefix = "ResourceSpans/ScopeSpans/Span/Event_".to_owned()
                + event_i.to_string().as_str()
                + "/";
            if col_name.starts_with("ResourceSpans/Resource/attributes")
                && !col_name.eq(SERVICE_NAME_COL_NAME)
            {
                let value = convert_column_to_any_value(&batch, col_name,row_i)?;
                process.tags.push(Self::normal_decode_kv(col_name, value));
            } else if col_name.eq(RESOURCE_DROPPED_ATTRIBUTES_COUNT_COL_NAME) {
                let value = batch
                    .column_by_name(col_name)
                    .ok_or(Status::internal(format!(
                        "column {} is not exist",
                        col_name
                    )))?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or(Status::internal(format!(
                        "column {} is not Float64Array",
                        col_name
                    )))?
                    .value(row_i) as i64;
                process.tags.push(KeyValue {
                    key: "dropped_attributes_count".to_string(),
                    value_type: Some(super::jaeger_model::ValueType::Int64),
                    value: serde_json::Value::Number(value.into()),
                });
            } else if col_name.starts_with("ResourceSpans/ScopeSpans/Span/attributes/") {
                let value = convert_column_to_any_value(&batch, col_name,row_i)?;
                process.tags.push(Self::normal_decode_kv(col_name, value));
            } else if col_name.eq(SPAN_DROPPED_ATTRIBUTES_COUNT_COL_NAME)
                || col_name.eq(SPAN_DROPPED_EVENTS_COUNT_COL_NAME)
                || col_name.eq(SPAN_DROPPED_LINKS_COUNT_COL_NAME)
            {
                let value = batch
                    .column_by_name(col_name)
                    .ok_or(Status::internal(format!(
                        "column {} is not exist",
                        col_name
                    )))?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or(Status::internal(format!(
                        "column {} is not Float64Array",
                        col_name
                    )))?
                    .value(row_i) as i64;
                span.tags.push(KeyValue {
                    key: col_name.split('/').last().unwrap_or(col_name).to_string(),
                    value_type: Some(super::jaeger_model::ValueType::Int64),
                    value: serde_json::Value::Number(value.into()),
                });
            } else if col_name.eq(TRACE_STATE_COL_NAME) {
                span.tags.push(KeyValue {
                    key: col_name.split('/').last().unwrap_or(col_name).to_string(),
                    value_type: Some(super::jaeger_model::ValueType::String),
                    value: serde_json::Value::String(
                        batch
                            .column_by_name(col_name)
                            .ok_or(Status::internal(format!(
                                "column {} is not exist",
                                col_name
                            )))?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or(Status::internal(format!(
                                "column {} is not StringArray",
                                col_name
                            )))?
                            .value(row_i)
                            .to_string(),
                    ),
                });
            } else if col_name.starts_with(&event_prefix) {
                let time_unix_nano = batch
                    .column_by_name((event_prefix.clone() + "time_unix_nano").as_str())
                    .ok_or(Status::internal(format!(
                        "column {} is not exist",
                        event_prefix.clone() + "time_unix_nano"
                    )))?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or(Status::internal(format!(
                        "column {} is not Float64Array",
                        col_name
                    )))?
                    .value(row_i) as u64;

                let mut attributes = Vec::new();
                let attributes_prefix = event_prefix.clone() + "attributes/";
                for col_name in &all_col_names {
                    if col_name.starts_with(&attributes_prefix) {
                        let value = convert_column_to_any_value(&batch, col_name,row_i)?;
                        process.tags.push(Self::normal_decode_kv(col_name, value));
                    } else if col_name.eq(&(event_prefix.clone() + "name")) {
                        attributes.push(KeyValue {
                            key: "name".to_string(),
                            value_type: Some(super::jaeger_model::ValueType::String),
                            value: serde_json::Value::String(
                                batch
                                    .column_by_name(col_name)
                                    .ok_or(Status::internal(format!(
                                        "column {} is not exist",
                                        col_name
                                    )))?
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .ok_or(Status::internal(format!(
                                        "column {} is not StringArray",
                                        col_name
                                    )))?
                                    .value(row_i)
                                    .to_string(),
                            ),
                        });
                    } else if col_name.eq(&(event_prefix.clone() + "dropped_attributes_count")) {
                        let value = batch
                            .column_by_name(col_name)
                            .ok_or(Status::internal(format!(
                                "column {} is not exist",
                                col_name
                            )))?
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or(Status::internal(format!(
                                "column {} is not Float64Array",
                                col_name
                            )))?
                            .value(row_i) as i64;
                        attributes.push(KeyValue {
                            key: "dropped_attributes_count".to_string(),
                            value_type: Some(super::jaeger_model::ValueType::Int64),
                            value: serde_json::Value::Number(value.into()),
                        });
                    }
                }
                span.logs.push(Log {
                    timestamp: time_unix_nano,
                    fields: attributes,
                });
                event_i += 1;
            } else if col_name.starts_with(&link_prefix) {
                let trace_id = batch
                    .column_by_name((link_prefix.clone() + "trace_id").as_str())
                    .ok_or(Status::internal(format!(
                        "column {} is not exist",
                        link_prefix.clone() + "trace_id"
                    )))?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or(Status::internal(format!(
                        "column {} is not StringArray",
                        link_prefix.clone() + "trace_id"
                    )))?
                    .value(row_i)
                    .to_string();

                let span_id = batch
                    .column_by_name((link_prefix.clone() + "span_id").as_str())
                    .ok_or(Status::internal(format!(
                        "column {} is not exist",
                        link_prefix.clone() + "span_id"
                    )))?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or(Status::internal(format!(
                        "column {} is not StringArray",
                        link_prefix.clone() + "span_id"
                    )))?
                    .value(row_i)
                    .to_string();

                span.references.push(Reference {
                    trace_id,
                    span_id,
                    ref_type: ReferenceType::ChildOf,
                });
                link_i += 1;
            }
        }
        span.process = Some(process);

        Ok(span)
    }

    fn parse_duration(s: &str) -> Result<u64, QueryError> {
        let orig = s;
        let mut s = s;
        let mut d: u64 = 0;
        let mut neg = false;

        // Consume [-+]?
        if !s.is_empty() {
            let c = s.chars().next().unwrap();
            if c == '-' || c == '+' {
                neg = c == '-';
                s = &s[1..];
            }
        }

        // Special case: if all that is left is "0", this is zero.
        if s == "0" {
            return Ok(0);
        }
        if s.is_empty() {
            return Err(QueryError::Parser {
                source: ParserError::ParserError(format!("time: invalid duration {}", orig)),
            });
        }

        while !s.is_empty() {
            let mut f: u64 = 0;
            let mut scale: f64 = 1.0;
            let mut post = false;

            // The next character must be [0-9.]
            if !(s.starts_with('.') || s.chars().next().unwrap().is_ascii_digit()) {
                return Err(QueryError::Parser {
                    source: ParserError::ParserError(format!("time: invalid duration {}", orig)),
                });
            }

            // Consume [0-9]*
            let pl = s.len();
            let (v_new, s_new) = Self::leading_int(s);
            let mut v = v_new;
            s = s_new;
            let pre = pl != s.len();

            // Consume (\.[0-9]*)?
            if !s.is_empty() && s.starts_with('.') {
                s = &s[1..];
                let pl = s.len();
                let (f_new, scale_new, s_new) = Self::leading_fraction(s);
                f = f_new;
                scale = scale_new;
                s = s_new;
                post = pl != s.len();
            }

            if !pre && !post {
                return Err(QueryError::Parser {
                    source: ParserError::ParserError(format!("time: invalid duration {}", orig)),
                });
            }

            // Consume unit.
            let i = s
                .find(|c: char| c == '.' || c.is_ascii_digit())
                .unwrap_or(s.len());
            if i == 0 {
                return Err(QueryError::Parser {
                    source: ParserError::ParserError(format!("time: invalid duration {}", orig)),
                });
            }
            let u = &s[..i];
            s = &s[i..];

            let unit = match Self::unit_map().get(u) {
                Some(&unit) => unit,
                None => {
                    return Err(QueryError::Parser {
                        source: ParserError::ParserError(format!(
                            "time: unknown unit {} in duration {}",
                            u, orig
                        )),
                    });
                }
            };

            if v > (1 << 63) / unit {
                return Err(QueryError::Parser {
                    source: ParserError::ParserError(format!("time: invalid duration {}", orig)),
                });
            }
            v *= unit;

            if f > 0 {
                v += (f as f64 * (unit as f64 / scale)) as u64;
                if v > 1 << 63 {
                    return Err(QueryError::Parser {
                        source: ParserError::ParserError(format!(
                            "time: invalid duration {}",
                            orig
                        )),
                    });
                }
            }

            d += v;
            if d > 1 << 63 {
                return Err(QueryError::Parser {
                    source: ParserError::ParserError(format!("time: invalid duration {}", orig)),
                });
            }
        }

        if neg {
            return Ok(-(d as i64) as u64);
        }
        if d > 1 << (63 - 1) {
            return Err(QueryError::Parser {
                source: ParserError::ParserError(format!("time: invalid duration {}", orig)),
            });
        }
        Ok(d)
    }

    fn leading_int(s: &str) -> (u64, &str) {
        let mut v: u64 = 0;
        let mut i = 0;
        for c in s.chars() {
            if c.is_ascii_digit() {
                v = v * 10 + c.to_digit(10).unwrap() as u64;
                i += 1;
            } else {
                break;
            }
        }
        (v, &s[i..])
    }

    fn leading_fraction(s: &str) -> (u64, f64, &str) {
        let mut v: u64 = 0;
        let mut scale: f64 = 1.0;
        let mut i = 0;
        for c in s.chars() {
            if c.is_ascii_digit() {
                v = v * 10 + c.to_digit(10).unwrap() as u64;
                scale *= 10.0;
                i += 1;
            } else {
                break;
            }
        }
        (v, scale, &s[i..])
    }

    fn unit_map() -> HashMap<&'static str, u64> {
        let mut m = HashMap::new();
        m.insert("ns", 1);
        m.insert("us", 1_000);
        m.insert("µs", 1_000);
        m.insert("ms", 1_000_000);
        m.insert("s", 1_000_000_000);
        m.insert("m", 60 * 1_000_000_000);
        m.insert("h", 60 * 60 * 1_000_000_000);
        m
    }
}

fn convert_column_to_any_value(
    batch: &RecordBatch,
    col_name: &str,
    row_i: usize,
) -> Result<AnyValue, Status> {
    let array = batch
        .column_by_name(col_name)
        .ok_or_else(|| Status::internal(format!("column {} does not exist", col_name)))?;

    let any_value = if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        if row_i < array.len() {
            AnyValue {
                value: Some(Value::StringValue(array.value(row_i).to_string())),
            }
        } else {
            AnyValue { value: None }
        }
    }else {
        return Err(Status::internal(format!(
            "Unsupported array type for column {}",
            col_name
        )));
    };

    Ok(any_value)
}
