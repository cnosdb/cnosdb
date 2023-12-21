#![allow(dead_code)]
#![allow(unused)]

use std::sync::Arc;
use std::time::Duration;

use meta::error::MetaError;
use meta::model::MetaRef;
use models::meta_data::{NodeId, ReplicationSet, VnodeId, VnodeInfo};
use models::predicate::domain::ResolvedPredicate;
use models::schema::Precision;
use models::utils::{now_timestamp_millis, now_timestamp_nanos};
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{DeleteFromTableRequest, Meta, WritePointsRequest, WriteVnodeRequest};
use protos::{tskv_service_time_out_client, DEFAULT_GRPC_SERVER_MESSAGE_LEN};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::Code;
use tower::timeout::Timeout;
use trace::{debug, info, SpanContext, SpanRecorder};
use trace_http::ctx::append_trace_context;
use tskv::EngineRef;

use crate::errors::*;
use crate::hh_queue::{HintedOffBlock, HintedOffWriteReq};
use crate::status_response_to_result;

#[derive(Debug)]
pub struct PointWriter {
    node_id: u64,
    timeout: Duration,
    kv_inst: Option<EngineRef>,
    meta_manager: MetaRef,
    hh_sender: Sender<HintedOffWriteReq>,
    grpc_enable_gzip: bool,
}

impl PointWriter {
    pub fn new(
        node_id: u64,
        timeout_ms: u64,
        kv_inst: Option<EngineRef>,
        meta_manager: MetaRef,
        hh_sender: Sender<HintedOffWriteReq>,
        grpc_enable_gzip: bool,
    ) -> Self {
        let timeout = Duration::from_millis(timeout_ms);
        Self {
            node_id,
            kv_inst,
            timeout,
            meta_manager,
            hh_sender,
            grpc_enable_gzip,
        }
    }

    pub(crate) async fn write_to_node(
        &self,
        vnode_id: u32,
        tenant: &str,
        node_id: u64,
        precision: Precision,
        data: Arc<Vec<u8>>,
        span_recorder: SpanRecorder,
    ) -> CoordinatorResult<()> {
        if node_id == self.node_id && self.kv_inst.is_some() {
            let span_recorder = span_recorder.child("write to local node");

            let result = self
                .write_to_local_node(span_recorder.span_ctx(), vnode_id, tenant, precision, data)
                .await;
            debug!("write data to local {}({}) {:?}", node_id, vnode_id, result);

            return result;
        }

        let mut span_recorder = span_recorder.child("write to remote node");

        let result = self
            .write_to_remote_node(
                vnode_id,
                node_id,
                tenant,
                precision,
                data.clone(),
                span_recorder.span_ctx(),
            )
            .await;

        if let Err(ref err) = result {
            let meta_retry = MetaError::Retry {
                msg: "default".to_string(),
            };
            let tskv_memory = tskv::Error::MemoryExhausted;
            if matches!(*err, CoordinatorError::FailoverNode { .. })
                || err.error_code().to_string() == meta_retry.error_code().to_string()
                || err.error_code().to_string() == tskv_memory.error_code().to_string()
            {
                info!(
                    "write data to remote {}({}) failed {}; write to hh!",
                    node_id, vnode_id, err
                );

                span_recorder.error(err.to_string());

                return self
                    .write_to_handoff(
                        vnode_id,
                        node_id,
                        tenant,
                        precision,
                        Arc::unwrap_or_clone(data),
                    )
                    .await;
            }
        }

        debug!(
            "write data to remote {}({}) , inst exist: {}, {:?}!",
            node_id,
            vnode_id,
            self.kv_inst.is_some(),
            result
        );

        result
    }

    async fn write_to_handoff(
        &self,
        vnode_id: u32,
        node_id: u64,
        tenant: &str,
        precision: Precision,
        data: Vec<u8>,
    ) -> CoordinatorResult<()> {
        let (sender, receiver) = oneshot::channel();
        let block = HintedOffBlock::new(
            now_timestamp_nanos(),
            vnode_id,
            tenant.to_string(),
            precision,
            data,
        );
        let request = HintedOffWriteReq {
            node_id,
            sender,
            block,
        };

        self.hh_sender.send(request).await?;

        receiver.await?
    }

    pub async fn write_to_remote_node(
        &self,
        vnode_id: u32,
        node_id: u64,
        tenant: &str,
        precision: Precision,
        data: Arc<Vec<u8>>,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let channel = self
            .meta_manager
            .get_node_conn(node_id)
            .await
            .map_err(|error| CoordinatorError::FailoverNode {
                id: node_id,
                error: error.to_string(),
            })?;
        let mut client = tskv_service_time_out_client(
            channel,
            self.timeout,
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.grpc_enable_gzip,
        );

        let mut cmd = tonic::Request::new(WriteVnodeRequest {
            vnode_id,
            precision: precision as u32,
            tenant: tenant.to_string(),
            data: Arc::unwrap_or_clone(data),
        });

        // 将当前的trace span信息写入到请求的metadata中
        append_trace_context(span_ctx, cmd.metadata_mut()).map_err(|_| {
            CoordinatorError::CommonError {
                msg: "Parse trace_id, this maybe a bug".to_string(),
            }
        })?;

        let begin_time = now_timestamp_millis();
        let response = client
            .write_vnode_points(cmd)
            .await
            .map_err(|err| match err.code() {
                Code::Internal => CoordinatorError::TskvError { source: err.into() },
                _ => CoordinatorError::FailoverNode {
                    id: node_id,
                    error: format!("{err:?}"),
                },
            })?
            .into_inner();

        let use_time = now_timestamp_millis() - begin_time;
        if use_time > 200 {
            debug!(
                "write points to node:{}, use time too long {}",
                node_id, use_time
            )
        }
        status_response_to_result(&response)
    }

    async fn write_to_local_node(
        &self,
        span_ctx: Option<&SpanContext>,
        vnode_id: u32,
        tenant: &str,
        precision: Precision,
        data: Arc<Vec<u8>>,
    ) -> CoordinatorResult<()> {
        let req = WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: tenant.to_string(),
                user: None,
                password: None,
            }),
            points: Arc::unwrap_or_clone(data),
        };

        if let Some(kv_inst) = self.kv_inst.clone() {
            let _ = kv_inst.write(span_ctx, vnode_id, precision, req).await?;
            Ok(())
        } else {
            Err(CoordinatorError::KvInstanceNotFound { node_id: 0 })
        }
    }

    pub(crate) async fn delete_from_table_on_vnode(
        &self,
        vnode: VnodeInfo,
        tenant: &str,
        database: &str,
        table: &str,
        predicate: &ResolvedPredicate,
    ) -> CoordinatorResult<()> {
        let node_id = vnode.node_id;
        let vnode_id = vnode.id;

        if node_id == self.node_id && self.kv_inst.is_some() {
            let result = self
                .delete_from_table_on_local_node(vnode_id, tenant, database, table, predicate)
                .await;

            return result;
        }

        let result = self
            .delete_from_table_on_remote_node(vnode_id, node_id, tenant, database, table, predicate)
            .await;

        if let Err(ref err) = result {
            let meta_retry = MetaError::Retry {
                msg: "default".to_string(),
            };
            let tskv_memory = tskv::Error::MemoryExhausted;
            if matches!(*err, CoordinatorError::FailoverNode { .. })
                || err.error_code().to_string() == meta_retry.error_code().to_string()
                || err.error_code().to_string() == tskv_memory.error_code().to_string()
            {

                // TODO(zipper): handle errors.
            }
        }

        result
    }

    pub async fn delete_from_table_on_remote_node(
        &self,
        vnode_id: VnodeId,
        node_id: NodeId,
        tenant: &str,
        database: &str,
        table: &str,
        predicate: &ResolvedPredicate,
    ) -> CoordinatorResult<()> {
        let predicate_bytes = bincode::serialize(predicate)?;

        let channel = self
            .meta_manager
            .get_node_conn(node_id)
            .await
            .map_err(|error| CoordinatorError::FailoverNode {
                id: node_id,
                error: error.to_string(),
            })?;
        let mut client = tskv_service_time_out_client(
            channel,
            self.timeout,
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.grpc_enable_gzip,
        );

        let cmd = tonic::Request::new(DeleteFromTableRequest {
            vnode_id,
            tenant: tenant.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            predicate: predicate_bytes,
        });

        let begin_time = now_timestamp_millis();
        let _ = client
            .delete_from_table(cmd)
            .await
            .map_err(|err| match err.code() {
                Code::Internal => CoordinatorError::TskvError { source: err.into() },
                _ => CoordinatorError::FailoverNode {
                    id: node_id,
                    error: format!("{err:?}"),
                },
            })?;

        let use_time = now_timestamp_millis() - begin_time;
        if use_time > 200 {
            debug!("delete on node:{}, use time too long {}", node_id, use_time)
        }

        Ok(())
    }

    async fn delete_from_table_on_local_node(
        &self,
        vnode_id: VnodeId,
        tenant: &str,
        database: &str,
        table: &str,
        predicate: &ResolvedPredicate,
    ) -> CoordinatorResult<()> {
        if let Some(kv_inst) = self.kv_inst.clone() {
            if let Err(e) = kv_inst
                .delete_from_table(vnode_id, tenant, database, table, predicate)
                .await
            {
                debug!("Failed to run delete_from_table: {e}");
            }
            Ok(())
        } else {
            Err(CoordinatorError::KvInstanceNotFound { node_id: 0 })
        }
    }
}
