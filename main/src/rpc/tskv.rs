use std::pin::Pin;
use std::sync::Arc;

use coordinator::errors::CoordinatorResult;
use coordinator::file_info::get_files_meta;
use coordinator::reader::table_scan::remote::TonicRecordBatchEncoder;
use coordinator::reader::QueryExecutor;
use coordinator::service::{CoordServiceMetrics, CoordinatorRef};
use coordinator::vnode_mgr::VnodeManager;
use coordinator::{
    SendableCoordinatorRecordBatchStream, FAILED_RESPONSE_CODE, SUCCESS_RESPONSE_CODE,
};
use futures::{Stream, StreamExt, TryStreamExt};
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::meta_data::VnodeInfo;
use models::predicate::domain::{self, QueryArgs, QueryExpr};
use models::record_batch_encode;
use models::schema::{Precision, TableColumn};
use protos::kv_service::tskv_service_server::TskvService;
use protos::kv_service::*;
use protos::models::{PingBody, PingBodyBuilder};
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Extensions, Request, Response, Status};
use trace::{debug, error, info, SpanContext, SpanExt, SpanRecorder};
use tskv::query_iterator::QueryOption;
use tskv::EngineRef;

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send>>;

#[derive(Clone)]
pub struct TskvServiceImpl {
    pub runtime: Arc<Runtime>,
    pub kv_inst: EngineRef,
    pub coord: CoordinatorRef,
    pub metrics_register: Arc<MetricsRegister>,
}

impl TskvServiceImpl {
    fn status_response(
        &self,
        code: i32,
        data: String,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(StatusResponse { code, data }))
    }

    fn bytes_response(
        &self,
        code: i32,
        data: Vec<u8>,
    ) -> Result<tonic::Response<BatchBytesResponse>, tonic::Status> {
        Ok(tonic::Response::new(BatchBytesResponse { code, data }))
    }

    fn tonic_status(&self, msg: String) -> tonic::Status {
        tonic::Status::new(tonic::Code::Internal, msg)
    }

    async fn admin_drop_db(
        &self,
        tenant: &str,
        request: &DropDbRequest,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let _ = self.kv_inst.drop_database(tenant, &request.db).await;

        self.status_response(SUCCESS_RESPONSE_CODE, "".to_string())
    }

    async fn admin_drop_table(
        &self,
        tenant: &str,
        request: &DropTableRequest,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let _ = self
            .kv_inst
            .drop_table(tenant, &request.db, &request.table)
            .await;

        self.status_response(SUCCESS_RESPONSE_CODE, "".to_string())
    }

    async fn admin_drop_column(
        &self,
        tenant: &str,
        request: &DropColumnRequest,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let _ = self
            .kv_inst
            .drop_table_column(tenant, &request.db, &request.table, &request.column)
            .await;

        self.status_response(SUCCESS_RESPONSE_CODE, "".to_string())
    }

    async fn admin_add_column(
        &self,
        tenant: &str,
        request: &AddColumnRequest,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let column = match bincode::deserialize::<TableColumn>(&request.column) {
            Ok(column) => column,
            Err(err) => return self.status_response(SUCCESS_RESPONSE_CODE, err.to_string()),
        };

        let _ = self
            .kv_inst
            .add_table_column(tenant, &request.db, &request.table, column)
            .await;

        self.status_response(SUCCESS_RESPONSE_CODE, "".to_string())
    }

    async fn admin_alter_column(
        &self,
        tenant: &str,
        request: &AlterColumnRequest,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let column = match bincode::deserialize::<TableColumn>(&request.column) {
            Ok(column) => column,
            Err(err) => return self.status_response(SUCCESS_RESPONSE_CODE, err.to_string()),
        };

        let _ = self
            .kv_inst
            .change_table_column(tenant, &request.db, &request.table, &request.name, column)
            .await;

        self.status_response(SUCCESS_RESPONSE_CODE, "".to_string())
    }

    async fn admin_delete_vnode(
        &self,
        tenant: &str,
        request: &DeleteVnodeRequest,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let meta = self.coord.meta_manager();
        let manager = VnodeManager::new(meta, self.kv_inst.clone(), self.coord.node_id());
        if let Err(err) = manager.drop_vnode(tenant, request.vnode_id).await {
            self.status_response(FAILED_RESPONSE_CODE, err.to_string())
        } else {
            self.status_response(SUCCESS_RESPONSE_CODE, "".to_string())
        }
    }

    async fn admin_copy_vnode(
        &self,
        tenant: &str,
        request: &CopyVnodeRequest,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let meta = self.coord.meta_manager();
        let manager = VnodeManager::new(meta, self.kv_inst.clone(), self.coord.node_id());
        if let Err(err) = manager.copy_vnode(tenant, request.vnode_id, true).await {
            self.status_response(FAILED_RESPONSE_CODE, err.to_string())
        } else {
            self.status_response(SUCCESS_RESPONSE_CODE, "".to_string())
        }
    }

    async fn admin_move_vnode(
        &self,
        tenant: &str,
        request: &MoveVnodeRequest,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let meta = self.coord.meta_manager();
        let manager = VnodeManager::new(meta, self.kv_inst.clone(), self.coord.node_id());
        if let Err(err) = manager.move_vnode(tenant, request.vnode_id).await {
            self.status_response(FAILED_RESPONSE_CODE, err.to_string())
        } else {
            self.status_response(SUCCESS_RESPONSE_CODE, "".to_string())
        }
    }

    async fn admin_compact_vnode(
        &self,
        _tenant: &str,
        request: &CompactVnodeRequest,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        if let Err(err) = self.kv_inst.compact(request.vnode_ids.clone()).await {
            self.status_response(FAILED_RESPONSE_CODE, err.to_string())
        } else {
            self.status_response(SUCCESS_RESPONSE_CODE, "".to_string())
        }
    }

    async fn admin_fetch_vnode_checksum(
        &self,
        _tenant: &str,
        request: &FetchVnodeChecksumRequest,
    ) -> Result<tonic::Response<BatchBytesResponse>, tonic::Status> {
        match self.kv_inst.get_vnode_hash_tree(request.vnode_id).await {
            Ok(record) => match record_batch_encode(&record) {
                Ok(bytes) => self.bytes_response(SUCCESS_RESPONSE_CODE, bytes),
                Err(_) => self.bytes_response(FAILED_RESPONSE_CODE, vec![]),
            },
            // TODO(zipper): Add error message in BatchBytesResponse
            Err(_) => self.bytes_response(FAILED_RESPONSE_CODE, vec![]),
        }
    }

    fn query_record_batch_exec(
        self,
        args: QueryArgs,
        expr: QueryExpr,
        aggs: Option<Vec<TableColumn>>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        let option = QueryOption::new(
            args.batch_size,
            expr.split,
            aggs,
            Arc::new(expr.df_schema),
            expr.table_schema,
        );

        let meta = self.coord.meta_manager();
        let node_id = meta.node_id();
        let mut vnodes = Vec::with_capacity(args.vnode_ids.len());
        for id in args.vnode_ids.iter() {
            vnodes.push(VnodeInfo::new(*id, node_id))
        }

        let executor = QueryExecutor::new(
            option,
            self.runtime.clone(),
            meta,
            Some(self.kv_inst.clone()),
            Arc::new(CoordServiceMetrics::new(&self.metrics_register)),
        );
        executor.local_node_executor(node_id, vnodes)
    }

    fn tag_scan_exec(
        args: QueryArgs,
        expr: QueryExpr,
        meta: MetaRef,
        run_time: Arc<Runtime>,
        kv_inst: EngineRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        let option = QueryOption::new(
            args.batch_size,
            expr.split,
            None,
            Arc::new(expr.df_schema),
            expr.table_schema,
        );

        let node_id = meta.node_id();
        let vnodes = args
            .vnode_ids
            .iter()
            .map(|id| VnodeInfo::new(*id, node_id))
            .collect::<Vec<_>>();

        let executor = QueryExecutor::new(
            option,
            run_time,
            meta,
            Some(kv_inst),
            Arc::new(CoordServiceMetrics::new(&metrics_register)),
        );
        executor.local_node_tag_scan(node_id, vnodes)
    }
}

#[tonic::async_trait]
impl TskvService for TskvServiceImpl {
    async fn ping(
        &self,
        _request: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingResponse>, tonic::Status> {
        debug!("PING");

        let ping_req = _request.into_inner();
        let ping_body = flatbuffers::root::<PingBody>(&ping_req.body);
        if let Err(e) = ping_body {
            error!("{}", e);
        } else {
            info!("ping_req:body(flatbuffer): {:?}", ping_body);
        }

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let payload = fbb.create_vector(b"hello, caller");

        let mut builder = PingBodyBuilder::new(&mut fbb);
        builder.add_payload(payload);
        let root = builder.finish();
        fbb.finish(root, None);

        let finished_data = fbb.finished_data();

        Ok(tonic::Response::new(PingResponse {
            version: 1,
            body: finished_data.to_vec(),
        }))
    }

    type WritePointsStream = ResponseStream<WritePointsResponse>;
    async fn write_points(
        &self,
        request: tonic::Request<tonic::Streaming<WritePointsRequest>>,
    ) -> Result<tonic::Response<Self::WritePointsStream>, tonic::Status> {
        let mut stream = request.into_inner();
        let (resp_sender, resp_receiver) = mpsc::channel(128);
        while let Some(result) = stream.next().await {
            match result {
                Ok(req) => {
                    let ret = self
                        .kv_inst
                        .write(0, Precision::NS, req)
                        .await
                        .map_err(|err| tonic::Status::internal(err.to_string()));
                    resp_sender.send(ret).await.expect("successful");
                }
                Err(status) => {
                    match resp_sender.send(Err(status)).await {
                        Ok(_) => (),
                        Err(_err) => break, // response was dropped
                    }
                }
            }
        }

        let out_stream = ReceiverStream::new(resp_receiver);

        Ok(tonic::Response::new(Box::pin(out_stream)))
    }

    async fn write_vnode_points(
        &self,
        request: tonic::Request<WriteVnodeRequest>,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let inner = request.into_inner();
        let request = WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: inner.tenant.clone(),
                user: None,
                password: None,
            }),
            points: inner.data,
        };

        if let Err(err) = self
            .kv_inst
            .write(
                inner.vnode_id,
                Precision::from(inner.precision as u8),
                request,
            )
            .await
        {
            self.status_response(FAILED_RESPONSE_CODE, err.to_string())
        } else {
            info!("success write data to vnode: {}", inner.vnode_id);
            self.status_response(SUCCESS_RESPONSE_CODE, "".to_string())
        }
    }

    async fn exec_admin_command(
        &self,
        request: tonic::Request<AdminCommandRequest>,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let inner = request.into_inner();

        if let Some(command) = inner.command {
            let resp = match &command {
                admin_command_request::Command::DropDb(command) => {
                    self.admin_drop_db(&inner.tenant, command).await
                }
                admin_command_request::Command::DropTab(command) => {
                    self.admin_drop_table(&inner.tenant, command).await
                }
                admin_command_request::Command::DelVnode(command) => {
                    self.admin_delete_vnode(&inner.tenant, command).await
                }
                admin_command_request::Command::CopyVnode(command) => {
                    self.admin_copy_vnode(&inner.tenant, command).await
                }
                admin_command_request::Command::MoveVnode(command) => {
                    self.admin_move_vnode(&inner.tenant, command).await
                }
                admin_command_request::Command::CompactVnode(command) => {
                    self.admin_compact_vnode(&inner.tenant, command).await
                }
                admin_command_request::Command::DropColumn(command) => {
                    self.admin_drop_column(&inner.tenant, command).await
                }
                admin_command_request::Command::AddColumn(command) => {
                    self.admin_add_column(&inner.tenant, command).await
                }
                admin_command_request::Command::AlterColumn(command) => {
                    self.admin_alter_column(&inner.tenant, command).await
                }
            };

            info!("admin command: {:?}, result: {:?}", command, resp);
            resp
        } else {
            self.status_response(FAILED_RESPONSE_CODE, "Command is None".to_string())
        }
    }

    async fn exec_admin_fetch_command(
        &self,
        request: Request<AdminFetchCommandRequest>,
    ) -> Result<Response<BatchBytesResponse>, Status> {
        let inner = request.into_inner();

        if let Some(command) = inner.command {
            match &command {
                admin_fetch_command_request::Command::FetchVnodeChecksum(command) => {
                    self.admin_fetch_vnode_checksum(&inner.tenant, command)
                        .await
                }
            }
        } else {
            self.bytes_response(FAILED_RESPONSE_CODE, vec![])
        }
    }

    async fn fetch_vnode_summary(
        &self,
        request: tonic::Request<FetchVnodeSummaryRequest>,
    ) -> Result<tonic::Response<BatchBytesResponse>, tonic::Status> {
        let inner = request.into_inner();
        match self
            .kv_inst
            .get_vnode_summary(&inner.tenant, &inner.database, inner.vnode_id)
            .await
        {
            Ok(opt_ve) => {
                if let Some(ve) = opt_ve {
                    match ve.encode() {
                        Ok(bytes) => self.bytes_response(SUCCESS_RESPONSE_CODE, bytes),
                        Err(err) => Err(self.tonic_status(err.to_string())),
                    }
                } else {
                    self.bytes_response(SUCCESS_RESPONSE_CODE, vec![])
                }
            }
            Err(err) => Err(self.tonic_status(err.to_string())),
        }
    }

    async fn get_vnode_files_meta(
        &self,
        request: tonic::Request<GetVnodeFilesMetaRequest>,
    ) -> Result<tonic::Response<GetVnodeFilesMetaResponse>, tonic::Status> {
        let inner = request.into_inner();
        let owner = models::schema::make_owner(&inner.tenant, &inner.db);
        let storage_opt = self.kv_inst.get_storage_options();

        if let Err(err) = self
            .kv_inst
            .prepare_copy_vnode(&inner.tenant, &inner.db, inner.vnode_id)
            .await
        {
            return Err(tonic::Status::new(tonic::Code::Internal, err.to_string()));
        }

        let path = storage_opt.ts_family_dir(&owner, inner.vnode_id);
        match get_files_meta(&path.as_path().to_string_lossy()).await {
            Ok(files_meta) => {
                info!("files meta: {:?} {:?}", path, files_meta);
                Ok(tonic::Response::new(files_meta.into()))
            }
            Err(err) => Err(tonic::Status::new(tonic::Code::Internal, err.to_string())),
        }
    }

    type DownloadFileStream = ResponseStream<BatchBytesResponse>;
    async fn download_file(
        &self,
        request: tonic::Request<DownloadFileRequest>,
    ) -> Result<tonic::Response<Self::DownloadFileStream>, tonic::Status> {
        let inner = request.into_inner();
        let owner = models::schema::make_owner(&inner.tenant, &inner.db);
        let storage_opt = self.kv_inst.get_storage_options();
        let data_dir = storage_opt.ts_family_dir(&owner, inner.vnode_id);
        let path = data_dir.join(inner.filename);
        info!("download file: {}", path.display());

        let (send, recv) = mpsc::channel(1024);
        tokio::spawn(async move {
            if let Ok(mut file) = tokio::fs::File::open(path).await {
                let mut buffer = vec![0; 8 * 1024];
                while let Ok(len) = file.read(&mut buffer).await {
                    if len == 0 {
                        break;
                    }

                    let _ = send
                        .send(Ok(BatchBytesResponse {
                            code: SUCCESS_RESPONSE_CODE,
                            data: (buffer[0..len]).to_vec(),
                        }))
                        .await;
                }
            }
        });

        let out_stream = ReceiverStream::new(recv);

        Ok(tonic::Response::new(Box::pin(out_stream)))
    }

    /// Server streaming response type for the QueryRecordBatch method.
    type QueryRecordBatchStream = ResponseStream<BatchBytesResponse>;
    async fn query_record_batch(
        &self,
        request: tonic::Request<QueryRecordBatchRequest>,
    ) -> Result<tonic::Response<Self::QueryRecordBatchStream>, tonic::Status> {
        let span_recorder = get_span_recorder(request.extensions(), "grpc query_record_batch");
        let inner = request.into_inner();

        let args = match QueryArgs::decode(&inner.args) {
            Ok(args) => args,
            Err(err) => return Err(self.tonic_status(err.to_string())),
        };

        let expr = match QueryExpr::decode(&inner.expr) {
            Ok(expr) => expr,
            Err(err) => return Err(self.tonic_status(err.to_string())),
        };

        let aggs = match domain::decode_agg(&inner.aggs) {
            Ok(aggs) => aggs,
            Err(err) => return Err(self.tonic_status(err.to_string())),
        };

        let service = self.clone();
        let stream = TskvServiceImpl::query_record_batch_exec(service, args, expr, aggs)?;

        let encoded_stream =
            TonicRecordBatchEncoder::new(stream, span_recorder.child("encode record batch"))
                .map_err(Into::into);

        Ok(tonic::Response::new(Box::pin(encoded_stream)))
    }

    type TagScanStream = ResponseStream<BatchBytesResponse>;
    async fn tag_scan(
        &self,
        request: Request<QueryRecordBatchRequest>,
    ) -> Result<Response<Self::TagScanStream>, Status> {
        let span_recorder = get_span_recorder(request.extensions(), "grpc query_record_batch");
        let inner = request.into_inner();

        let args = match QueryArgs::decode(&inner.args) {
            Ok(args) => args,
            Err(err) => return Err(self.tonic_status(err.to_string())),
        };

        let expr = match QueryExpr::decode(&inner.expr) {
            Ok(expr) => expr,
            Err(err) => return Err(self.tonic_status(err.to_string())),
        };
        let stream = TskvServiceImpl::tag_scan_exec(
            args,
            expr,
            self.coord.meta_manager(),
            self.runtime.clone(),
            self.kv_inst.clone(),
            self.metrics_register.clone(),
        )?;

        let stream =
            TonicRecordBatchEncoder::new(stream, span_recorder.child("encode record batch"))
                .map_err(Into::into);

        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

fn get_span_recorder(extensions: &Extensions, child_span_name: &'static str) -> SpanRecorder {
    let span_context = extensions.get::<SpanContext>();
    SpanRecorder::new(span_context.child_span(child_span_name))
}
