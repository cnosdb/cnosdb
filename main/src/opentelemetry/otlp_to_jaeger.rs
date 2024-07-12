use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use coordinator::SendableCoordinatorRecordBatchStream;
use datafusion::arrow::array::{Array, Float64Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionConfig;
use datafusion::scalar::ScalarValue;
use models::predicate::domain::Predicate;
use models::schema::table_schema::TableSchema;
use models::schema::tskv_table_schema::TskvTableSchema;
use prost::Message;
use protos::common::any_value::Value;
use protos::common::KeyValue as OtlpKeyValue;
use protos::jaeger_storage_v1::TraceQueryParameters;
use protos::trace::status::StatusCode;
use query::data_source::split::tskv::TableLayoutHandle;
use query::data_source::split::SplitManager;
use serde_json::Number;
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
    FindTraces(Option<TraceQueryParameters>),
    FindTraceIDs(Option<TraceQueryParameters>),
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
                    Some(col(format!("jaeger_trace.\"{}\"", TRACE_ID_COL_NAME)).eq(lit(trace_id))),
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
                            tenant,
                            db,
                            table,
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

    fn decode_kv(value: &str) -> KeyValue {
        let value = value.split('_').collect::<Vec<_>>();
        let value: Vec<u8> = value.iter().map(|s| s.parse().unwrap()).collect();
        let kv = OtlpKeyValue::decode(&value[..]).expect("decode KeyValue failed");
        if let Some(value) = kv.value {
            if let Some(value) = value.value {
                match value {
                    Value::StringValue(v_str) => KeyValue {
                        key: kv.key,
                        value_type: Some(super::jaeger_model::ValueType::String),
                        value: serde_json::Value::String(v_str),
                    },
                    Value::BoolValue(v_bool) => KeyValue {
                        key: kv.key,
                        value_type: Some(super::jaeger_model::ValueType::Bool),
                        value: serde_json::Value::Bool(v_bool),
                    },
                    Value::IntValue(v_int64) => KeyValue {
                        key: kv.key,
                        value_type: Some(super::jaeger_model::ValueType::Int64),
                        value: serde_json::Value::Number(v_int64.into()),
                    },
                    Value::DoubleValue(v_float64) => {
                        let value = if let Some(v_float64) = Number::from_f64(v_float64) {
                            serde_json::Value::Number(v_float64)
                        } else {
                            let v_int64 = v_float64 as i64;
                            serde_json::Value::Number(v_int64.into())
                        };
                        KeyValue {
                            key: kv.key,
                            value_type: Some(super::jaeger_model::ValueType::Float64),
                            value,
                        }
                    }
                    Value::ArrayValue(array_value) => KeyValue {
                        key: kv.key,
                        value_type: Some(super::jaeger_model::ValueType::Binary),
                        value: serde_json::Value::String(
                            array_value
                                .encode_to_vec()
                                .iter()
                                .map(|b| b.to_string())
                                .collect::<Vec<_>>()
                                .join("_"),
                        ),
                    },
                    Value::KvlistValue(kvlist_value) => KeyValue {
                        key: kv.key,
                        value_type: Some(super::jaeger_model::ValueType::Binary),
                        value: serde_json::Value::String(
                            kvlist_value
                                .encode_to_vec()
                                .iter()
                                .map(|b| b.to_string())
                                .collect::<Vec<_>>()
                                .join("_"),
                        ),
                    },
                    Value::BytesValue(bytes_value) => KeyValue {
                        key: kv.key,
                        value_type: Some(super::jaeger_model::ValueType::Binary),
                        value: serde_json::Value::String(
                            bytes_value
                                .iter()
                                .map(|b| b.to_string())
                                .collect::<Vec<_>>()
                                .join("_"),
                        ),
                    },
                }
            } else {
                KeyValue {
                    key: kv.key,
                    value_type: Some(super::jaeger_model::ValueType::String),
                    value: serde_json::Value::String("".to_string()),
                }
            }
        } else {
            KeyValue {
                key: kv.key,
                value_type: Some(super::jaeger_model::ValueType::String),
                value: serde_json::Value::String("".to_string()),
            }
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
            span.tags.push(KeyValue {
                key: "span.kind".to_string(),
                value_type: Some(super::jaeger_model::ValueType::String),
                value: serde_json::Value::String(
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or(Status::internal(format!(
                            "column {} is not StringArray",
                            OPERATION_NAME_COL_NAME
                        )))?
                        .value(row_i)
                        .to_string(),
                ),
            });
        }

        span.start_time = batch
            .column_by_name("time")
            .ok_or(Status::internal("column time is not exist".to_string()))?
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or(Status::internal(
                "column time is not TimestampNanosecondArray".to_string(),
            ))?
            .value(row_i) as u64;

        if let Some(array) = batch.column_by_name(DURATION_COL_NAME) {
            span.duration = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or(Status::internal(format!(
                    "column {} is not Float64Array",
                    DURATION_COL_NAME
                )))?
                .value(row_i) as u64;
        }

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
            span.tags.push(KeyValue {
                key: "otel.status_code".to_string(),
                value_type: Some(super::jaeger_model::ValueType::String),
                value: serde_json::Value::String(status_code.clone()),
            });
            if StatusCode::from_str_name(&status_code).eq(&Some(StatusCode::Error)) {
                span.tags.push(KeyValue {
                    key: "error".to_string(),
                    value_type: Some(super::jaeger_model::ValueType::Bool),
                    value: serde_json::Value::Bool(true),
                });
            }
        }

        if let Some(array) = batch.column_by_name(STATUS_MESSAGE_COL_NAME) {
            span.tags.push(KeyValue {
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
            span.tags.push(KeyValue {
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
            span.tags.push(KeyValue {
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

        let mut process = Process::default();
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
                let value = batch
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
                    .to_string();
                if !value.is_empty() {
                    process.tags.push(Self::decode_kv(&value));
                } else {
                    process.tags.push(KeyValue {
                        key: col_name.split('/').last().unwrap_or(col_name).to_string(),
                        value_type: Some(super::jaeger_model::ValueType::String),
                        value: serde_json::Value::String("".to_string()),
                    });
                }
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
                let value = batch
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
                    .to_string();
                if !value.is_empty() {
                    span.tags.push(Self::decode_kv(&value));
                } else {
                    span.tags.push(KeyValue {
                        key: col_name.split('/').last().unwrap_or(col_name).to_string(),
                        value_type: Some(super::jaeger_model::ValueType::String),
                        value: serde_json::Value::String("".to_string()),
                    });
                }
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
                process.tags.push(KeyValue {
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
                        let value = batch
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
                            .to_string();
                        if !value.is_empty() {
                            attributes.push(Self::decode_kv(&value));
                        } else {
                            attributes.push(KeyValue {
                                key: col_name.split('/').last().unwrap_or(col_name).to_string(),
                                value_type: Some(super::jaeger_model::ValueType::String),
                                value: serde_json::Value::String("".to_string()),
                            });
                        }
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
}
