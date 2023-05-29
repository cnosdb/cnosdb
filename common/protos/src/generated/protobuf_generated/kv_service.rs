/// --------------------------------------------------------------------
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingRequest {
    #[prost(uint64, tag = "1")]
    pub version: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub body: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingResponse {
    #[prost(uint64, tag = "1")]
    pub version: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub body: ::prost::alloc::vec::Vec<u8>,
}
/// --------------------------------------------------------------------
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Meta {
    #[prost(string, tag = "1")]
    pub tenant: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub user: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "3")]
    pub password: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WritePointsRequest {
    #[prost(uint64, tag = "1")]
    pub version: u64,
    #[prost(message, optional, tag = "2")]
    pub meta: ::core::option::Option<Meta>,
    /// flatbuffers bytes ( models::Points )
    #[prost(bytes = "vec", tag = "3")]
    pub points: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WritePointsResponse {
    #[prost(uint64, tag = "1")]
    pub points_number: u64,
}
/// --------------------------------------------------------------------
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileInfo {
    #[prost(string, tag = "1")]
    pub md5: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub size: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetVnodeFilesMetaRequest {
    #[prost(string, tag = "1")]
    pub tenant: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub db: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub vnode_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetVnodeFilesMetaResponse {
    #[prost(string, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub infos: ::prost::alloc::vec::Vec<FileInfo>,
}
/// --------------------------------------------------------------------
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusResponse {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub data: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteVnodeRequest {
    #[prost(uint32, tag = "1")]
    pub vnode_id: u32,
    #[prost(string, tag = "2")]
    pub tenant: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub precision: u32,
    #[prost(bytes = "vec", tag = "4")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropDbRequest {
    #[prost(string, tag = "1")]
    pub db: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropTableRequest {
    #[prost(string, tag = "1")]
    pub db: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub table: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteVnodeRequest {
    #[prost(string, tag = "1")]
    pub db: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub vnode_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CopyVnodeRequest {
    #[prost(uint32, tag = "1")]
    pub vnode_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MoveVnodeRequest {
    #[prost(uint32, tag = "1")]
    pub vnode_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompactVnodeRequest {
    #[prost(uint32, repeated, tag = "1")]
    pub vnode_ids: ::prost::alloc::vec::Vec<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropColumnRequest {
    #[prost(string, tag = "1")]
    pub db: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub table: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub column: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddColumnRequest {
    #[prost(string, tag = "1")]
    pub db: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub table: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "3")]
    pub column: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterColumnRequest {
    #[prost(string, tag = "1")]
    pub db: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub table: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub name: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "4")]
    pub column: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdminCommandRequest {
    #[prost(string, tag = "1")]
    pub tenant: ::prost::alloc::string::String,
    #[prost(
        oneof = "admin_command_request::Command",
        tags = "2, 3, 4, 5, 6, 7, 8, 9, 10"
    )]
    pub command: ::core::option::Option<admin_command_request::Command>,
}
/// Nested message and enum types in `AdminCommandRequest`.
pub mod admin_command_request {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Command {
        #[prost(message, tag = "2")]
        DropDb(super::DropDbRequest),
        #[prost(message, tag = "3")]
        DropTab(super::DropTableRequest),
        #[prost(message, tag = "4")]
        DelVnode(super::DeleteVnodeRequest),
        #[prost(message, tag = "5")]
        CopyVnode(super::CopyVnodeRequest),
        #[prost(message, tag = "6")]
        MoveVnode(super::MoveVnodeRequest),
        #[prost(message, tag = "7")]
        CompactVnode(super::CompactVnodeRequest),
        #[prost(message, tag = "8")]
        DropColumn(super::DropColumnRequest),
        #[prost(message, tag = "9")]
        AddColumn(super::AddColumnRequest),
        #[prost(message, tag = "10")]
        AlterColumn(super::AlterColumnRequest),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchVnodeChecksumRequest {
    #[prost(uint32, tag = "1")]
    pub vnode_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdminFetchCommandRequest {
    #[prost(string, tag = "1")]
    pub tenant: ::prost::alloc::string::String,
    #[prost(oneof = "admin_fetch_command_request::Command", tags = "8")]
    pub command: ::core::option::Option<admin_fetch_command_request::Command>,
}
/// Nested message and enum types in `AdminFetchCommandRequest`.
pub mod admin_fetch_command_request {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Command {
        #[prost(message, tag = "8")]
        FetchVnodeChecksum(super::FetchVnodeChecksumRequest),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchBytesResponse {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(bytes = "vec", tag = "2")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DownloadFileRequest {
    #[prost(string, tag = "1")]
    pub tenant: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub db: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub vnode_id: u32,
    #[prost(string, tag = "4")]
    pub filename: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchVnodeSummaryRequest {
    #[prost(string, tag = "1")]
    pub tenant: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub database: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub vnode_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryRecordBatchRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub args: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub aggs: ::prost::alloc::vec::Vec<u8>,
}
/// Generated client implementations.
pub mod tskv_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// --------------------------------------------------------------------
    #[derive(Debug, Clone)]
    pub struct TskvServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl TskvServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> TskvServiceClient<T>
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
        ) -> TskvServiceClient<InterceptedService<T, F>>
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
            TskvServiceClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn ping(
            &mut self,
            request: impl tonic::IntoRequest<super::PingRequest>,
        ) -> Result<tonic::Response<super::PingResponse>, tonic::Status> {
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
                "/kv_service.TSKVService/Ping",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn write_points(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::WritePointsRequest,
            >,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::WritePointsResponse>>,
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
                "/kv_service.TSKVService/WritePoints",
            );
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
        pub async fn write_vnode_points(
            &mut self,
            request: impl tonic::IntoRequest<super::WriteVnodeRequest>,
        ) -> Result<tonic::Response<super::StatusResponse>, tonic::Status> {
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
                "/kv_service.TSKVService/WriteVnodePoints",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn query_record_batch(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryRecordBatchRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::BatchBytesResponse>>,
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
                "/kv_service.TSKVService/QueryRecordBatch",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
        pub async fn exec_admin_command(
            &mut self,
            request: impl tonic::IntoRequest<super::AdminCommandRequest>,
        ) -> Result<tonic::Response<super::StatusResponse>, tonic::Status> {
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
                "/kv_service.TSKVService/ExecAdminCommand",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn exec_admin_fetch_command(
            &mut self,
            request: impl tonic::IntoRequest<super::AdminFetchCommandRequest>,
        ) -> Result<tonic::Response<super::BatchBytesResponse>, tonic::Status> {
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
                "/kv_service.TSKVService/ExecAdminFetchCommand",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn download_file(
            &mut self,
            request: impl tonic::IntoRequest<super::DownloadFileRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::BatchBytesResponse>>,
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
                "/kv_service.TSKVService/DownloadFile",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
        pub async fn get_vnode_files_meta(
            &mut self,
            request: impl tonic::IntoRequest<super::GetVnodeFilesMetaRequest>,
        ) -> Result<tonic::Response<super::GetVnodeFilesMetaResponse>, tonic::Status> {
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
                "/kv_service.TSKVService/GetVnodeFilesMeta",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn fetch_vnode_summary(
            &mut self,
            request: impl tonic::IntoRequest<super::FetchVnodeSummaryRequest>,
        ) -> Result<tonic::Response<super::BatchBytesResponse>, tonic::Status> {
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
                "/kv_service.TSKVService/FetchVnodeSummary",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn tag_scan(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryRecordBatchRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::BatchBytesResponse>>,
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
                "/kv_service.TSKVService/TagScan",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod tskv_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with TskvServiceServer.
    #[async_trait]
    pub trait TskvService: Send + Sync + 'static {
        async fn ping(
            &self,
            request: tonic::Request<super::PingRequest>,
        ) -> Result<tonic::Response<super::PingResponse>, tonic::Status>;
        /// Server streaming response type for the WritePoints method.
        type WritePointsStream: futures_core::Stream<
                Item = Result<super::WritePointsResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn write_points(
            &self,
            request: tonic::Request<tonic::Streaming<super::WritePointsRequest>>,
        ) -> Result<tonic::Response<Self::WritePointsStream>, tonic::Status>;
        async fn write_vnode_points(
            &self,
            request: tonic::Request<super::WriteVnodeRequest>,
        ) -> Result<tonic::Response<super::StatusResponse>, tonic::Status>;
        /// Server streaming response type for the QueryRecordBatch method.
        type QueryRecordBatchStream: futures_core::Stream<
                Item = Result<super::BatchBytesResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn query_record_batch(
            &self,
            request: tonic::Request<super::QueryRecordBatchRequest>,
        ) -> Result<tonic::Response<Self::QueryRecordBatchStream>, tonic::Status>;
        async fn exec_admin_command(
            &self,
            request: tonic::Request<super::AdminCommandRequest>,
        ) -> Result<tonic::Response<super::StatusResponse>, tonic::Status>;
        async fn exec_admin_fetch_command(
            &self,
            request: tonic::Request<super::AdminFetchCommandRequest>,
        ) -> Result<tonic::Response<super::BatchBytesResponse>, tonic::Status>;
        /// Server streaming response type for the DownloadFile method.
        type DownloadFileStream: futures_core::Stream<
                Item = Result<super::BatchBytesResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn download_file(
            &self,
            request: tonic::Request<super::DownloadFileRequest>,
        ) -> Result<tonic::Response<Self::DownloadFileStream>, tonic::Status>;
        async fn get_vnode_files_meta(
            &self,
            request: tonic::Request<super::GetVnodeFilesMetaRequest>,
        ) -> Result<tonic::Response<super::GetVnodeFilesMetaResponse>, tonic::Status>;
        async fn fetch_vnode_summary(
            &self,
            request: tonic::Request<super::FetchVnodeSummaryRequest>,
        ) -> Result<tonic::Response<super::BatchBytesResponse>, tonic::Status>;
        /// Server streaming response type for the TagScan method.
        type TagScanStream: futures_core::Stream<
                Item = Result<super::BatchBytesResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn tag_scan(
            &self,
            request: tonic::Request<super::QueryRecordBatchRequest>,
        ) -> Result<tonic::Response<Self::TagScanStream>, tonic::Status>;
    }
    /// --------------------------------------------------------------------
    #[derive(Debug)]
    pub struct TskvServiceServer<T: TskvService> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: TskvService> TskvServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
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
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for TskvServiceServer<T>
    where
        T: TskvService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/kv_service.TSKVService/Ping" => {
                    #[allow(non_camel_case_types)]
                    struct PingSvc<T: TskvService>(pub Arc<T>);
                    impl<T: TskvService> tonic::server::UnaryService<super::PingRequest>
                    for PingSvc<T> {
                        type Response = super::PingResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PingRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).ping(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PingSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/kv_service.TSKVService/WritePoints" => {
                    #[allow(non_camel_case_types)]
                    struct WritePointsSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::StreamingService<super::WritePointsRequest>
                    for WritePointsSvc<T> {
                        type Response = super::WritePointsResponse;
                        type ResponseStream = T::WritePointsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::WritePointsRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).write_points(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = WritePointsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/kv_service.TSKVService/WriteVnodePoints" => {
                    #[allow(non_camel_case_types)]
                    struct WriteVnodePointsSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::UnaryService<super::WriteVnodeRequest>
                    for WriteVnodePointsSvc<T> {
                        type Response = super::StatusResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::WriteVnodeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).write_vnode_points(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = WriteVnodePointsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/kv_service.TSKVService/QueryRecordBatch" => {
                    #[allow(non_camel_case_types)]
                    struct QueryRecordBatchSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::ServerStreamingService<
                        super::QueryRecordBatchRequest,
                    > for QueryRecordBatchSvc<T> {
                        type Response = super::BatchBytesResponse;
                        type ResponseStream = T::QueryRecordBatchStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::QueryRecordBatchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).query_record_batch(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = QueryRecordBatchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/kv_service.TSKVService/ExecAdminCommand" => {
                    #[allow(non_camel_case_types)]
                    struct ExecAdminCommandSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::UnaryService<super::AdminCommandRequest>
                    for ExecAdminCommandSvc<T> {
                        type Response = super::StatusResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AdminCommandRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).exec_admin_command(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ExecAdminCommandSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/kv_service.TSKVService/ExecAdminFetchCommand" => {
                    #[allow(non_camel_case_types)]
                    struct ExecAdminFetchCommandSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::UnaryService<super::AdminFetchCommandRequest>
                    for ExecAdminFetchCommandSvc<T> {
                        type Response = super::BatchBytesResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AdminFetchCommandRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).exec_admin_fetch_command(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ExecAdminFetchCommandSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/kv_service.TSKVService/DownloadFile" => {
                    #[allow(non_camel_case_types)]
                    struct DownloadFileSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::ServerStreamingService<super::DownloadFileRequest>
                    for DownloadFileSvc<T> {
                        type Response = super::BatchBytesResponse;
                        type ResponseStream = T::DownloadFileStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DownloadFileRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).download_file(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DownloadFileSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/kv_service.TSKVService/GetVnodeFilesMeta" => {
                    #[allow(non_camel_case_types)]
                    struct GetVnodeFilesMetaSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::UnaryService<super::GetVnodeFilesMetaRequest>
                    for GetVnodeFilesMetaSvc<T> {
                        type Response = super::GetVnodeFilesMetaResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetVnodeFilesMetaRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).get_vnode_files_meta(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetVnodeFilesMetaSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/kv_service.TSKVService/FetchVnodeSummary" => {
                    #[allow(non_camel_case_types)]
                    struct FetchVnodeSummarySvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::UnaryService<super::FetchVnodeSummaryRequest>
                    for FetchVnodeSummarySvc<T> {
                        type Response = super::BatchBytesResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FetchVnodeSummaryRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).fetch_vnode_summary(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = FetchVnodeSummarySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/kv_service.TSKVService/TagScan" => {
                    #[allow(non_camel_case_types)]
                    struct TagScanSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::ServerStreamingService<
                        super::QueryRecordBatchRequest,
                    > for TagScanSvc<T> {
                        type Response = super::BatchBytesResponse;
                        type ResponseStream = T::TagScanStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::QueryRecordBatchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).tag_scan(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = TagScanSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
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
    impl<T: TskvService> Clone for TskvServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: TskvService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: TskvService> tonic::server::NamedService for TskvServiceServer<T> {
        const NAME: &'static str = "kv_service.TSKVService";
    }
}
