// 引入必要的库
use serde::{Deserialize, Serialize};

// ReferenceType 是一个表示引用类型的字符串类型
#[derive(Debug, Serialize, Deserialize)]
pub enum ReferenceType {
    ChildOf,
    FollowsFrom,
}

// TraceID 是所有 span 在 trace 中共享的 trace ID
pub type TraceID = String;

// SpanID 是 span 的 ID
pub type SpanID = String;

// ProcessID 是 Process 结构体的哈希值，在 trace 内是唯一的
pub type ProcessID = String;

// ValueType 是存储在 KeyValue 结构体中的值的类型
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ValueType {
    String,
    Bool,
    Int64,
    Float64,
    Binary,
}

// Trace 是 span 的列表
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Trace {
    #[serde(rename = "traceID")]
    pub trace_id: TraceID,

    #[serde(rename = "spans")]
    pub spans: Vec<Span>,

    #[serde(rename = "processes")]
    pub processes: std::collections::HashMap<ProcessID, Process>,

    #[serde(rename = "warnings")]
    pub warnings: Vec<String>,
}

// Span 是表示基础设施中一段工作的 span
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Span {
    #[serde(rename = "traceID")]
    pub trace_id: TraceID,

    #[serde(rename = "spanID")]
    pub span_id: SpanID,

    #[serde(rename = "parentSpanID", skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<SpanID>, // deprecated

    #[serde(rename = "flags", skip_serializing_if = "Option::is_none")]
    pub flags: Option<u32>,

    #[serde(rename = "operationName")]
    pub operation_name: String,

    #[serde(rename = "references")]
    pub references: Vec<Reference>,

    #[serde(rename = "startTime")]
    pub start_time: u64, // microseconds since Unix epoch

    #[serde(rename = "duration")]
    pub duration: u64, // microseconds

    #[serde(rename = "tags")]
    pub tags: Vec<KeyValue>,

    #[serde(rename = "logs")]
    pub logs: Vec<Log>,

    #[serde(rename = "processID", skip_serializing_if = "Option::is_none")]
    pub process_id: Option<ProcessID>,

    #[serde(rename = "process", skip_serializing_if = "Option::is_none")]
    pub process: Option<Process>,

    #[serde(rename = "warnings")]
    pub warnings: Vec<String>,
}

// Reference 是一个 span 到另一个 span 的引用
#[derive(Debug, Serialize, Deserialize)]
pub struct Reference {
    #[serde(rename = "refType")]
    pub ref_type: ReferenceType,

    #[serde(rename = "traceID")]
    pub trace_id: TraceID,

    #[serde(rename = "spanID")]
    pub span_id: SpanID,
}

// Process 是发出一组 span 的进程
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Process {
    #[serde(rename = "serviceName")]
    pub service_name: String,

    #[serde(rename = "tags")]
    pub tags: Vec<KeyValue>,
}

// Log 是在 span 中发出的日志
#[derive(Debug, Serialize, Deserialize)]
pub struct Log {
    #[serde(rename = "timestamp")]
    pub timestamp: u64,

    #[serde(rename = "fields")]
    pub fields: Vec<KeyValue>,
}

// KeyValue 是一个带有类型化值的键值对
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KeyValue {
    #[serde(rename = "key")]
    pub key: String,

    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub value_type: Option<ValueType>,

    #[serde(rename = "value")]
    pub value: serde_json::Value,
}

// DependencyLink 显示服务之间的依赖关系
#[derive(Debug, Serialize, Deserialize)]
pub struct DependencyLink {
    #[serde(rename = "parent")]
    pub parent: String,

    #[serde(rename = "child")]
    pub child: String,

    #[serde(rename = "callCount")]
    pub call_count: u64,
}

// Operation 定义了按服务和 span 类型查询操作时的响应数据
#[derive(Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct Operation {
    #[serde(rename = "name")]
    pub name: String,

    #[serde(rename = "spanKind")]
    pub span_kind: String,
}
