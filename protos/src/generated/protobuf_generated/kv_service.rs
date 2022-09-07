#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingRequest {
    #[prost(uint64, tag="1")]
    pub version: u64,
    #[prost(bytes="vec", tag="2")]
    pub body: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingResponse {
    #[prost(uint64, tag="1")]
    pub version: u64,
    #[prost(bytes="vec", tag="2")]
    pub body: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Tag {
    /// tag key utf-8 bytes
    #[prost(bytes="vec", tag="1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    /// tag value utf-8 bytes
    #[prost(bytes="vec", tag="2")]
    pub value: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldInfo {
    #[prost(enumeration="FieldType", tag="1")]
    pub field_type: i32,
    /// field name utf-8 bytes
    #[prost(bytes="vec", tag="2")]
    pub name: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag="3")]
    pub id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddSeriesRpcRequest {
    #[prost(uint64, tag="1")]
    pub version: u64,
    #[prost(uint64, tag="2")]
    pub series_id: u64,
    #[prost(message, repeated, tag="3")]
    pub tags: ::prost::alloc::vec::Vec<Tag>,
    #[prost(message, repeated, tag="4")]
    pub fields: ::prost::alloc::vec::Vec<FieldInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddSeriesRpcResponse {
    #[prost(uint64, tag="1")]
    pub version: u64,
    #[prost(uint64, tag="2")]
    pub series_id: u64,
    #[prost(message, repeated, tag="3")]
    pub tags: ::prost::alloc::vec::Vec<Tag>,
    #[prost(message, repeated, tag="4")]
    pub fields: ::prost::alloc::vec::Vec<FieldInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSeriesInfoRpcRequest {
    #[prost(uint64, tag="1")]
    pub protocol_version: u64,
    #[prost(uint64, tag="2")]
    pub series_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSeriesInfoRpcResponse {
    #[prost(uint64, tag="1")]
    pub version: u64,
    #[prost(uint64, tag="2")]
    pub series_id: u64,
    #[prost(message, repeated, tag="3")]
    pub tags: ::prost::alloc::vec::Vec<Tag>,
    #[prost(message, repeated, tag="4")]
    pub fields: ::prost::alloc::vec::Vec<FieldInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteRowsRpcRequest {
    #[prost(uint64, tag="1")]
    pub version: u64,
    /// flatbuffers bytes ( models::Rows )
    #[prost(bytes="vec", tag="2")]
    pub rows: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteRowsRpcResponse {
    #[prost(uint64, tag="1")]
    pub version: u64,
    /// flatbuffers bytes ( models::Rows )
    #[prost(bytes="vec", tag="2")]
    pub rows: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WritePointsRpcRequest {
    #[prost(uint64, tag="1")]
    pub version: u64,
    /// flatbuffers bytes ( models::Points )
    #[prost(bytes="vec", tag="2")]
    pub points: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WritePointsRpcResponse {
    #[prost(uint64, tag="1")]
    pub version: u64,
    /// flatbuffers bytes ( models::Points )
    #[prost(bytes="vec", tag="2")]
    pub points: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum FieldType {
    Float = 0,
    Integer = 1,
    Unsigned = 2,
    Boolean = 3,
    String = 5,
}
/// Generated client implementations.
pub mod tskv_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
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
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
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
        pub async fn add_series(
            &mut self,
            request: impl tonic::IntoRequest<super::AddSeriesRpcRequest>,
        ) -> Result<tonic::Response<super::AddSeriesRpcResponse>, tonic::Status> {
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
                "/kv_service.TSKVService/AddSeries",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get_series_info(
            &mut self,
            request: impl tonic::IntoRequest<super::GetSeriesInfoRpcRequest>,
        ) -> Result<tonic::Response<super::GetSeriesInfoRpcResponse>, tonic::Status> {
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
                "/kv_service.TSKVService/GetSeriesInfo",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn write_rows(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::WriteRowsRpcRequest,
            >,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::WriteRowsRpcResponse>>,
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
                "/kv_service.TSKVService/WriteRows",
            );
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
        pub async fn write_points(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::WritePointsRpcRequest,
            >,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::WritePointsRpcResponse>>,
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
    }
}
/// Generated server implementations.
pub mod tskv_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with TskvServiceServer.
    #[async_trait]
    pub trait TskvService: Send + Sync + 'static {
        async fn ping(
            &self,
            request: tonic::Request<super::PingRequest>,
        ) -> Result<tonic::Response<super::PingResponse>, tonic::Status>;
        async fn add_series(
            &self,
            request: tonic::Request<super::AddSeriesRpcRequest>,
        ) -> Result<tonic::Response<super::AddSeriesRpcResponse>, tonic::Status>;
        async fn get_series_info(
            &self,
            request: tonic::Request<super::GetSeriesInfoRpcRequest>,
        ) -> Result<tonic::Response<super::GetSeriesInfoRpcResponse>, tonic::Status>;
        ///Server streaming response type for the WriteRows method.
        type WriteRowsStream: futures_core::Stream<
                Item = Result<super::WriteRowsRpcResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn write_rows(
            &self,
            request: tonic::Request<tonic::Streaming<super::WriteRowsRpcRequest>>,
        ) -> Result<tonic::Response<Self::WriteRowsStream>, tonic::Status>;
        ///Server streaming response type for the WritePoints method.
        type WritePointsStream: futures_core::Stream<
                Item = Result<super::WritePointsRpcResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn write_points(
            &self,
            request: tonic::Request<tonic::Streaming<super::WritePointsRpcRequest>>,
        ) -> Result<tonic::Response<Self::WritePointsStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct TskvServiceServer<T: TskvService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
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
                "/kv_service.TSKVService/AddSeries" => {
                    #[allow(non_camel_case_types)]
                    struct AddSeriesSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::UnaryService<super::AddSeriesRpcRequest>
                    for AddSeriesSvc<T> {
                        type Response = super::AddSeriesRpcResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AddSeriesRpcRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).add_series(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AddSeriesSvc(inner);
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
                "/kv_service.TSKVService/GetSeriesInfo" => {
                    #[allow(non_camel_case_types)]
                    struct GetSeriesInfoSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::UnaryService<super::GetSeriesInfoRpcRequest>
                    for GetSeriesInfoSvc<T> {
                        type Response = super::GetSeriesInfoRpcResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetSeriesInfoRpcRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).get_series_info(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetSeriesInfoSvc(inner);
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
                "/kv_service.TSKVService/WriteRows" => {
                    #[allow(non_camel_case_types)]
                    struct WriteRowsSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::StreamingService<super::WriteRowsRpcRequest>
                    for WriteRowsSvc<T> {
                        type Response = super::WriteRowsRpcResponse;
                        type ResponseStream = T::WriteRowsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::WriteRowsRpcRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).write_rows(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = WriteRowsSvc(inner);
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
                "/kv_service.TSKVService/WritePoints" => {
                    #[allow(non_camel_case_types)]
                    struct WritePointsSvc<T: TskvService>(pub Arc<T>);
                    impl<
                        T: TskvService,
                    > tonic::server::StreamingService<super::WritePointsRpcRequest>
                    for WritePointsSvc<T> {
                        type Response = super::WritePointsRpcResponse;
                        type ResponseStream = T::WritePointsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::WritePointsRpcRequest>,
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
    impl<T: TskvService> tonic::transport::NamedService for TskvServiceServer<T> {
        const NAME: &'static str = "kv_service.TSKVService";
    }
}
