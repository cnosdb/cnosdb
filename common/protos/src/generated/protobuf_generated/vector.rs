#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    /// Represents seconds of UTC time since Unix epoch
    /// 1970-01-01T00:00:00Z. Must be from 0001-01-01T00:00:00Z to
    /// 9999-12-31T23:59:59Z inclusive.
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    /// Non-negative fractions of a second at nanosecond resolution. Negative
    /// second values with fractions must still have non-negative nanos values
    /// that count forward in time. Must be from 0 to 999,999,999
    /// inclusive.
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventArray {
    #[prost(oneof = "event_array::Events", tags = "1, 2, 3")]
    pub events: ::core::option::Option<event_array::Events>,
}
/// Nested message and enum types in `EventArray`.
pub mod event_array {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Events {
        #[prost(message, tag = "1")]
        Logs(super::LogArray),
        #[prost(message, tag = "2")]
        Metrics(super::MetricArray),
        #[prost(message, tag = "3")]
        Traces(super::TraceArray),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogArray {
    #[prost(message, repeated, tag = "1")]
    pub logs: ::prost::alloc::vec::Vec<Log>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetricArray {
    #[prost(message, repeated, tag = "1")]
    pub metrics: ::prost::alloc::vec::Vec<Metric>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TraceArray {
    #[prost(message, repeated, tag = "1")]
    pub traces: ::prost::alloc::vec::Vec<Trace>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventWrapper {
    #[prost(oneof = "event_wrapper::Event", tags = "1, 2, 3")]
    pub event: ::core::option::Option<event_wrapper::Event>,
}
/// Nested message and enum types in `EventWrapper`.
pub mod event_wrapper {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Event {
        #[prost(message, tag = "1")]
        Log(super::Log),
        #[prost(message, tag = "2")]
        Metric(super::Metric),
        #[prost(message, tag = "3")]
        Trace(super::Trace),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Log {
    /// Deprecated, use value instead
    #[prost(map = "string, message", tag = "1")]
    pub fields: ::std::collections::HashMap<::prost::alloc::string::String, Value>,
    #[prost(message, optional, tag = "2")]
    pub value: ::core::option::Option<Value>,
    #[prost(message, optional, tag = "3")]
    pub metadata: ::core::option::Option<Value>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Trace {
    #[prost(map = "string, message", tag = "1")]
    pub fields: ::std::collections::HashMap<::prost::alloc::string::String, Value>,
    #[prost(message, optional, tag = "2")]
    pub metadata: ::core::option::Option<Value>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValueMap {
    #[prost(map = "string, message", tag = "1")]
    pub fields: ::std::collections::HashMap<::prost::alloc::string::String, Value>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValueArray {
    #[prost(message, repeated, tag = "1")]
    pub items: ::prost::alloc::vec::Vec<Value>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Value {
    #[prost(oneof = "value::Kind", tags = "1, 2, 4, 5, 6, 7, 8, 9")]
    pub kind: ::core::option::Option<value::Kind>,
}
/// Nested message and enum types in `Value`.
pub mod value {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Kind {
        #[prost(bytes, tag = "1")]
        RawBytes(::prost::alloc::vec::Vec<u8>),
        #[prost(message, tag = "2")]
        Timestamp(super::Timestamp),
        #[prost(int64, tag = "4")]
        Integer(i64),
        #[prost(double, tag = "5")]
        Float(f64),
        #[prost(bool, tag = "6")]
        Boolean(bool),
        #[prost(message, tag = "7")]
        Map(super::ValueMap),
        #[prost(message, tag = "8")]
        Array(super::ValueArray),
        #[prost(enumeration = "super::ValueNull", tag = "9")]
        Null(i32),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Metric {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub timestamp: ::core::option::Option<Timestamp>,
    #[prost(map = "string, string", tag = "3")]
    pub tags_v1: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    #[prost(map = "string, message", tag = "20")]
    pub tags_v2: ::std::collections::HashMap<::prost::alloc::string::String, TagValues>,
    #[prost(enumeration = "metric::Kind", tag = "4")]
    pub kind: i32,
    #[prost(string, tag = "11")]
    pub namespace: ::prost::alloc::string::String,
    #[prost(uint32, tag = "18")]
    pub interval_ms: u32,
    #[prost(message, optional, tag = "19")]
    pub metadata: ::core::option::Option<Value>,
    #[prost(oneof = "metric::Value", tags = "5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17")]
    pub value: ::core::option::Option<metric::Value>,
}
/// Nested message and enum types in `Metric`.
pub mod metric {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Kind {
        Incremental = 0,
        Absolute = 1,
    }
    impl Kind {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Kind::Incremental => "Incremental",
                Kind::Absolute => "Absolute",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "Incremental" => Some(Self::Incremental),
                "Absolute" => Some(Self::Absolute),
                _ => None,
            }
        }
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(message, tag = "5")]
        Counter(super::Counter),
        #[prost(message, tag = "6")]
        Gauge(super::Gauge),
        #[prost(message, tag = "7")]
        Set(super::Set),
        #[prost(message, tag = "8")]
        Distribution1(super::Distribution1),
        #[prost(message, tag = "9")]
        AggregatedHistogram1(super::AggregatedHistogram1),
        #[prost(message, tag = "10")]
        AggregatedSummary1(super::AggregatedSummary1),
        #[prost(message, tag = "12")]
        Distribution2(super::Distribution2),
        #[prost(message, tag = "13")]
        AggregatedHistogram2(super::AggregatedHistogram2),
        #[prost(message, tag = "14")]
        AggregatedSummary2(super::AggregatedSummary2),
        #[prost(message, tag = "15")]
        Sketch(super::Sketch),
        #[prost(message, tag = "16")]
        AggregatedHistogram3(super::AggregatedHistogram3),
        #[prost(message, tag = "17")]
        AggregatedSummary3(super::AggregatedSummary3),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TagValues {
    #[prost(message, repeated, tag = "1")]
    pub values: ::prost::alloc::vec::Vec<TagValue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TagValue {
    #[prost(string, optional, tag = "1")]
    pub value: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Counter {
    #[prost(double, tag = "1")]
    pub value: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Gauge {
    #[prost(double, tag = "1")]
    pub value: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Set {
    #[prost(string, repeated, tag = "1")]
    pub values: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Distribution1 {
    #[prost(double, repeated, tag = "1")]
    pub values: ::prost::alloc::vec::Vec<f64>,
    #[prost(uint32, repeated, tag = "2")]
    pub sample_rates: ::prost::alloc::vec::Vec<u32>,
    #[prost(enumeration = "StatisticKind", tag = "3")]
    pub statistic: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Distribution2 {
    #[prost(message, repeated, tag = "1")]
    pub samples: ::prost::alloc::vec::Vec<DistributionSample>,
    #[prost(enumeration = "StatisticKind", tag = "2")]
    pub statistic: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DistributionSample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(uint32, tag = "2")]
    pub rate: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregatedHistogram1 {
    #[prost(double, repeated, tag = "1")]
    pub buckets: ::prost::alloc::vec::Vec<f64>,
    #[prost(uint32, repeated, tag = "2")]
    pub counts: ::prost::alloc::vec::Vec<u32>,
    #[prost(uint32, tag = "3")]
    pub count: u32,
    #[prost(double, tag = "4")]
    pub sum: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregatedHistogram2 {
    #[prost(message, repeated, tag = "1")]
    pub buckets: ::prost::alloc::vec::Vec<HistogramBucket>,
    #[prost(uint32, tag = "2")]
    pub count: u32,
    #[prost(double, tag = "3")]
    pub sum: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregatedHistogram3 {
    #[prost(message, repeated, tag = "1")]
    pub buckets: ::prost::alloc::vec::Vec<HistogramBucket3>,
    #[prost(uint64, tag = "2")]
    pub count: u64,
    #[prost(double, tag = "3")]
    pub sum: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HistogramBucket {
    #[prost(double, tag = "1")]
    pub upper_limit: f64,
    #[prost(uint32, tag = "2")]
    pub count: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HistogramBucket3 {
    #[prost(double, tag = "1")]
    pub upper_limit: f64,
    #[prost(uint64, tag = "2")]
    pub count: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregatedSummary1 {
    #[prost(double, repeated, tag = "1")]
    pub quantiles: ::prost::alloc::vec::Vec<f64>,
    #[prost(double, repeated, tag = "2")]
    pub values: ::prost::alloc::vec::Vec<f64>,
    #[prost(uint32, tag = "3")]
    pub count: u32,
    #[prost(double, tag = "4")]
    pub sum: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregatedSummary2 {
    #[prost(message, repeated, tag = "1")]
    pub quantiles: ::prost::alloc::vec::Vec<SummaryQuantile>,
    #[prost(uint32, tag = "2")]
    pub count: u32,
    #[prost(double, tag = "3")]
    pub sum: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregatedSummary3 {
    #[prost(message, repeated, tag = "1")]
    pub quantiles: ::prost::alloc::vec::Vec<SummaryQuantile>,
    #[prost(uint64, tag = "2")]
    pub count: u64,
    #[prost(double, tag = "3")]
    pub sum: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SummaryQuantile {
    #[prost(double, tag = "1")]
    pub quantile: f64,
    #[prost(double, tag = "2")]
    pub value: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sketch {
    #[prost(oneof = "sketch::Sketch", tags = "1")]
    pub sketch: ::core::option::Option<sketch::Sketch>,
}
/// Nested message and enum types in `Sketch`.
pub mod sketch {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct AgentDdSketch {
        /// Summary statistics for the samples in this sketch.
        #[prost(uint32, tag = "1")]
        pub count: u32,
        #[prost(double, tag = "2")]
        pub min: f64,
        #[prost(double, tag = "3")]
        pub max: f64,
        #[prost(double, tag = "4")]
        pub sum: f64,
        #[prost(double, tag = "5")]
        pub avg: f64,
        /// The bins (buckets) of this sketch, where `k` and `n` are unzipped pairs.
        /// `k` is the list of bin indexes that are populated, and `n` is the count of samples
        /// within the given bin.
        #[prost(sint32, repeated, tag = "6")]
        pub k: ::prost::alloc::vec::Vec<i32>,
        #[prost(uint32, repeated, tag = "7")]
        pub n: ::prost::alloc::vec::Vec<u32>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Sketch {
        #[prost(message, tag = "1")]
        AgentDdSketch(AgentDdSketch),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushEventsRequest {
    #[prost(message, repeated, tag = "1")]
    pub events: ::prost::alloc::vec::Vec<EventWrapper>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushEventsResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HealthCheckRequest {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HealthCheckResponse {
    #[prost(enumeration = "ServingStatus", tag = "1")]
    pub status: i32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ValueNull {
    NullValue = 0,
}
impl ValueNull {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ValueNull::NullValue => "NULL_VALUE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "NULL_VALUE" => Some(Self::NullValue),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum StatisticKind {
    Histogram = 0,
    Summary = 1,
}
impl StatisticKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            StatisticKind::Histogram => "Histogram",
            StatisticKind::Summary => "Summary",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Histogram" => Some(Self::Histogram),
            "Summary" => Some(Self::Summary),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ServingStatus {
    Serving = 0,
    NotServing = 1,
}
impl ServingStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ServingStatus::Serving => "SERVING",
            ServingStatus::NotServing => "NOT_SERVING",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SERVING" => Some(Self::Serving),
            "NOT_SERVING" => Some(Self::NotServing),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod vector_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct VectorClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl VectorClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> VectorClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> VectorClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            VectorClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn push_events(
            &mut self,
            request: impl tonic::IntoRequest<super::PushEventsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::PushEventsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/vector.Vector/PushEvents");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("vector.Vector", "PushEvents"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn health_check(
            &mut self,
            request: impl tonic::IntoRequest<super::HealthCheckRequest>,
        ) -> std::result::Result<
            tonic::Response<super::HealthCheckResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/vector.Vector/HealthCheck",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("vector.Vector", "HealthCheck"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod vector_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with VectorServer.
    #[async_trait]
    pub trait Vector: Send + Sync + 'static {
        async fn push_events(
            &self,
            request: tonic::Request<super::PushEventsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::PushEventsResponse>,
            tonic::Status,
        >;
        async fn health_check(
            &self,
            request: tonic::Request<super::HealthCheckRequest>,
        ) -> std::result::Result<
            tonic::Response<super::HealthCheckResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct VectorServer<T: Vector> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Vector> VectorServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for VectorServer<T>
    where
        T: Vector,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/vector.Vector/PushEvents" => {
                    #[allow(non_camel_case_types)]
                    struct PushEventsSvc<T: Vector>(pub Arc<T>);
                    impl<T: Vector> tonic::server::UnaryService<super::PushEventsRequest>
                    for PushEventsSvc<T> {
                        type Response = super::PushEventsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PushEventsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).push_events(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PushEventsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/vector.Vector/HealthCheck" => {
                    #[allow(non_camel_case_types)]
                    struct HealthCheckSvc<T: Vector>(pub Arc<T>);
                    impl<
                        T: Vector,
                    > tonic::server::UnaryService<super::HealthCheckRequest>
                    for HealthCheckSvc<T> {
                        type Response = super::HealthCheckResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::HealthCheckRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).health_check(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = HealthCheckSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Vector> Clone for VectorServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: Vector> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Vector> tonic::server::NamedService for VectorServer<T> {
        const NAME: &'static str = "vector.Vector";
    }
}
