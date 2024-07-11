use std::pin::Pin;
use std::sync::Arc;

use coordinator::errors::{
    encode_grpc_response, ArrowSnafu, CommonSnafu, CoordinatorResult, TskvSnafu,
};
use coordinator::service::CoordinatorRef;
use futures::{Stream, TryStreamExt};
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::meta_data::VnodeInfo;
use models::predicate::domain::{self, QueryArgs, QueryExpr};
use models::record_batch_encode;
use models::schema::tskv_table_schema::TableColumn;
use protos::kv_service::tskv_service_server::TskvService;
use protos::kv_service::*;
use protos::models::{PingBody, PingBodyBuilder};
use snafu::ResultExt;
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Extensions, Request, Response, Status};
use trace::span_ext::SpanExt;
use trace::{debug, error, info, Span, SpanContext};
use tskv::error::TskvResult;
use tskv::reader::query_executor::QueryExecutor;
use tskv::reader::serialize::TonicRecordBatchEncoder;
use tskv::reader::{QueryOption, SendableTskvRecordBatchStream};
use tskv::EngineRef;

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send>>;

#[derive(Clone)]
pub struct TskvServiceImpl {
    pub runtime: Arc<Runtime>,
    pub kv_inst: EngineRef,
    pub coord: CoordinatorRef,
    pub metrics_register: Arc<MetricsRegister>,
    pub grpc_enable_gzip: bool,
}

impl TskvServiceImpl {
    fn internal_status(&self, msg: String) -> tonic::Status {
        tonic::Status::new(tonic::Code::Internal, msg)
    }

    async fn warp_admin_request(
        &self,
        tenant: &str,
        command: &admin_command::Command,
    ) -> CoordinatorResult<Vec<u8>> {
        match command {
            admin_command::Command::CompactVnode(req) => {
                self.kv_inst
                    .compact(req.vnode_ids.clone())
                    .await
                    .context(TskvSnafu)?;
                Ok(vec![])
            }

            admin_command::Command::OpenRaftNode(req) => {
                self.coord
                    .raft_manager()
                    .exec_open_raft_node(tenant, &req.db_name, req.vnode_id, req.replica_id)
                    .await?;
                Ok(vec![])
            }

            admin_command::Command::DropRaftNode(req) => {
                self.coord
                    .raft_manager()
                    .exec_drop_raft_node(tenant, &req.db_name, req.vnode_id, req.replica_id)
                    .await?;
                Ok(vec![])
            }

            admin_command::Command::FetchChecksum(req) => {
                let record = self
                    .kv_inst
                    .get_vnode_hash_tree(req.vnode_id)
                    .await
                    .context(TskvSnafu)?;
                let data = record_batch_encode(&record).context(ArrowSnafu)?;
                Ok(data)
            }

            admin_command::Command::AddRaftFollower(command) => {
                self.coord
                    .raft_manager()
                    .add_follower_to_group(
                        tenant,
                        &command.db_name,
                        command.follower_nid,
                        command.replica_id,
                    )
                    .await?;
                Ok(vec![])
            }

            admin_command::Command::RemoveRaftNode(command) => {
                self.coord
                    .raft_manager()
                    .remove_node_from_group(
                        tenant,
                        &command.db_name,
                        command.vnode_id,
                        command.replica_id,
                    )
                    .await?;
                Ok(vec![])
            }

            admin_command::Command::DestoryRaftGroup(command) => {
                self.coord
                    .raft_manager()
                    .destory_replica_group(tenant, &command.db_name, command.replica_id)
                    .await?;
                Ok(vec![])
            }
            admin_command::Command::PromoteLeader(command) => {
                self.coord
                    .raft_manager()
                    .promote_follower_to_leader(
                        tenant,
                        &command.db_name,
                        command.new_leader_id,
                        command.replica_id,
                    )
                    .await?;
                Ok(vec![])
            }
            admin_command::Command::LearnerToFollower(command) => {
                self.coord
                    .raft_manager()
                    .learner_to_follower(tenant, &command.db_name, command.replica_id)
                    .await?;
                Ok(vec![])
            }
        }
    }

    fn query_record_batch_exec(
        self,
        args: QueryArgs,
        expr: QueryExpr,
        aggs: Option<Vec<TableColumn>>,
        span_ctx: Option<&SpanContext>,
    ) -> TskvResult<SendableTskvRecordBatchStream> {
        let option = QueryOption::new(
            args.batch_size,
            expr.split,
            aggs,
            Arc::new(expr.df_schema),
            expr.table_schema,
            expr.schema_meta,
        );

        let meta = self.coord.meta_manager();
        let node_id = meta.node_id();
        let mut vnodes = Vec::with_capacity(args.vnode_ids.len());
        for id in args.vnode_ids.iter() {
            vnodes.push(VnodeInfo::new(*id, node_id))
        }

        let executor = QueryExecutor::new(option, self.runtime.clone(), meta, self.kv_inst.clone());
        executor.local_node_executor(vnodes, span_ctx)
    }

    fn tag_scan_exec(
        args: QueryArgs,
        expr: QueryExpr,
        meta: MetaRef,
        run_time: Arc<Runtime>,
        kv_inst: EngineRef,
        span_ctx: Option<&SpanContext>,
    ) -> TskvResult<SendableTskvRecordBatchStream> {
        let option = QueryOption::new(
            args.batch_size,
            expr.split,
            None,
            Arc::new(expr.df_schema),
            expr.table_schema,
            expr.schema_meta,
        );

        let node_id = meta.node_id();
        let vnodes = args
            .vnode_ids
            .iter()
            .map(|id| VnodeInfo::new(*id, node_id))
            .collect::<Vec<_>>();

        let executor = QueryExecutor::new(option, run_time, meta, kv_inst);
        executor.local_node_tag_scan(vnodes, span_ctx)
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

    async fn raft_write(
        &self,
        request: tonic::Request<RaftWriteCommand>,
    ) -> Result<tonic::Response<BatchBytesResponse>, tonic::Status> {
        let inner = request.into_inner();

        let client = self.coord.tenant_meta(&inner.tenant).await.ok_or_else(|| {
            self.internal_status(format!("Not Found tenant({}) meta", inner.tenant))
        })?;

        let replica = client
            .get_replication_set(&inner.db_name, inner.replica_id)
            .await
            .map_err(|err| self.internal_status(format!("Meta for replication set: {:?}", err)))?
            .ok_or_else(|| {
                self.internal_status(format!("Not Found Replica Set({})", inner.replica_id))
            })?;

        let writer = self.coord.tskv_raft_writer(inner);
        let result = writer.write_to_local(&replica).await;

        Ok(encode_grpc_response(result))
    }

    async fn admin_request(
        &self,
        request: tonic::Request<AdminCommand>,
    ) -> std::result::Result<tonic::Response<BatchBytesResponse>, tonic::Status> {
        let inner = request.into_inner();
        if let Some(command) = inner.command {
            info!("Exec admin request: {:?}", command);
            let result = self.warp_admin_request(&inner.tenant, &command).await;
            Ok(encode_grpc_response(result))
        } else {
            Ok(encode_grpc_response(Err(CommonSnafu {
                msg: "Command is None".to_string(),
            }
            .build())))
        }
    }

    type DownloadFileStream = ResponseStream<BatchBytesResponse>;
    async fn download_file(
        &self,
        request: tonic::Request<DownloadFileRequest>,
    ) -> Result<tonic::Response<Self::DownloadFileStream>, tonic::Status> {
        let inner = request.into_inner();
        let opt = self.kv_inst.get_storage_options();
        let filename = opt.path().join(inner.filename);
        info!("request download file name: {:?}", filename);

        let (send, recv) = mpsc::channel(1024);
        tokio::spawn(async move {
            if let Ok(mut file) = tokio::fs::File::open(filename).await {
                let mut buffer = vec![0; 8 * 1024];
                while let Ok(len) = file.read(&mut buffer).await {
                    if len == 0 {
                        break;
                    }

                    let _ = send
                        .send(Ok(BatchBytesResponse {
                            code: coordinator::errors::SUCCESS_RESPONSE_CODE,
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
        let span = get_span(request.extensions(), "grpc query_record_batch");
        let inner = request.into_inner();

        let args = match QueryArgs::decode(&inner.args) {
            Ok(args) => args,
            Err(err) => return Err(self.internal_status(err.to_string())),
        };

        let expr = match QueryExpr::decode(&inner.expr) {
            Ok(expr) => expr,
            Err(err) => return Err(self.internal_status(err.to_string())),
        };

        let aggs = match domain::decode_agg(&inner.aggs) {
            Ok(aggs) => aggs,
            Err(err) => return Err(self.internal_status(err.to_string())),
        };

        let service = self.clone();

        let encoded_stream = {
            let span = Span::enter_with_parent("RecordBatch encorder stream", &span);

            let stream = TskvServiceImpl::query_record_batch_exec(
                service,
                args,
                expr,
                aggs,
                span.context().as_ref(),
            )?;
            TonicRecordBatchEncoder::new(stream, span).map_err(Into::into)
        };

        Ok(tonic::Response::new(Box::pin(encoded_stream)))
    }

    type TagScanStream = ResponseStream<BatchBytesResponse>;
    async fn tag_scan(
        &self,
        request: Request<QueryRecordBatchRequest>,
    ) -> Result<Response<Self::TagScanStream>, Status> {
        let span = get_span(request.extensions(), "grpc tag_scan");
        let inner = request.into_inner();

        let args = match QueryArgs::decode(&inner.args) {
            Ok(args) => args,
            Err(err) => return Err(self.internal_status(err.to_string())),
        };

        let expr = match QueryExpr::decode(&inner.expr) {
            Ok(expr) => expr,
            Err(err) => return Err(self.internal_status(err.to_string())),
        };

        let stream = {
            let span = Span::enter_with_parent("RecordBatch encorder stream", &span);
            let stream = TskvServiceImpl::tag_scan_exec(
                args,
                expr,
                self.coord.meta_manager(),
                self.runtime.clone(),
                self.kv_inst.clone(),
                span.context().as_ref(),
            )?;

            TonicRecordBatchEncoder::new(stream, span).map_err(Into::into)
        };

        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

fn get_span(extensions: &Extensions, child_span_name: &'static str) -> Span {
    let context = extensions.get::<SpanContext>();
    Span::from_context(child_span_name, context)
}
