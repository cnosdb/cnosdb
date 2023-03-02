use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use meta::MetaRef;
use metrics::count::U64Counter;
use models::meta_data::VnodeInfo;
use models::predicate::domain::{QueryArgs, QueryExpr};
use models::utils::now_timestamp;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::QueryRecordBatchRequest;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use trace::info;
use tskv::engine::EngineRef;
use tskv::iterator::{QueryOption, RowIterator};

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::service::CoordServiceMetrics;
use crate::SUCCESS_RESPONSE_CODE;

#[derive(Debug)]
pub struct ReaderIterator {
    receiver: Receiver<CoordinatorResult<RecordBatch>>,
}

impl ReaderIterator {
    pub fn new() -> (Self, Sender<CoordinatorResult<RecordBatch>>) {
        let (sender, receiver) = mpsc::channel(1024);

        (Self { receiver }, sender)
    }

    pub async fn next(&mut self) -> Option<CoordinatorResult<RecordBatch>> {
        self.receiver.recv().await
    }

    pub async fn next_and_encdoe(&mut self) -> Option<CoordinatorResult<Vec<u8>>> {
        if let Some(record) = self.receiver.recv().await {
            match record {
                Ok(record) => match record_batch_encode(&record) {
                    Ok(data) => return Some(Ok(data)),
                    Err(err) => return Some(Err(err)),
                },
                Err(err) => return Some(Err(err)),
            }
        }

        None
    }
}

pub struct QueryExecutor {
    option: QueryOption,

    kv_inst: Option<EngineRef>,
    meta_manager: MetaRef,

    sender: Sender<CoordinatorResult<RecordBatch>>,
    data_out: U64Counter,
}

impl QueryExecutor {
    pub fn new(
        option: QueryOption,
        kv_inst: Option<EngineRef>,
        meta_manager: MetaRef,
        sender: Sender<CoordinatorResult<RecordBatch>>,
        metrics: Arc<CoordServiceMetrics>,
    ) -> Self {
        let data_out = metrics.data_out(option.tenant.as_str(), option.table_schema.db.as_str());
        Self {
            option,
            kv_inst,
            meta_manager,
            sender,
            data_out,
        }
    }
    pub async fn execute(&self) -> CoordinatorResult<()> {
        let mut routines = vec![];
        let mapping = self.map_vnode().await?;
        let now = tokio::time::Instant::now();
        for (node_id, vnodes) in mapping.iter() {
            info!(
                "execute select on node {}, vnode list: {:?} now: {:?}",
                node_id, vnodes, now
            );

            let routine = self.node_executor(*node_id, vnodes.clone());
            routines.push(routine);
        }

        let res = futures::future::try_join_all(routines).await.map(|_| ());

        info!(
            "parallel execute select on vnodes over, start at: {:?} elapsed: {:?}, result: {:?}",
            now,
            now.elapsed(),
            res,
        );

        res
    }

    async fn node_executor(&self, node_id: u64, vnodes: Vec<VnodeInfo>) -> CoordinatorResult<()> {
        if node_id == self.meta_manager.node_id() {
            self.local_node_executor(vnodes.clone()).await
        } else {
            let result = self.remote_node_executor(node_id, vnodes.clone()).await;
            if let Err(CoordinatorError::FailoverNode { id: _ }) = result {
                let mut routines = vec![];
                let mapping = self.try_map_vnode(&vnodes).await?;
                for (tmp_id, tmp_vnodes) in mapping.iter() {
                    info!(
                        "try execute select on node {}, vnode list: {:?}",
                        tmp_id, tmp_vnodes
                    );

                    let routine = self.remote_node_executor(*tmp_id, tmp_vnodes.clone());
                    routines.push(routine);
                }

                futures::future::try_join_all(routines).await?;

                Ok(())
            } else {
                result
            }
        }
    }

    async fn remote_node_executor(
        &self,
        node_id: u64,
        vnodes: Vec<VnodeInfo>,
    ) -> CoordinatorResult<()> {
        let now = tokio::time::Instant::now();
        info!(
            "execute select command on remote node: {} vnodes: {:?} now: {:?}",
            node_id, vnodes, now
        );

        let res = self.warp_remote_node_executor(node_id, vnodes).await;

        info!(
            "execute select command on remote node over: {}  start at: {:?}, elapsed: {:?}, result: {:?}",
            node_id,
            now,
            now.elapsed(),
            res
        );

        res
    }

    async fn warp_remote_node_executor(
        &self,
        node_id: u64,
        vnodes: Vec<VnodeInfo>,
    ) -> CoordinatorResult<()> {
        let mut vnode_ids = Vec::with_capacity(vnodes.len());
        for item in vnodes.iter() {
            vnode_ids.push(item.id);
        }
        let args = QueryArgs {
            vnode_ids,
            tenant: self.option.tenant.clone(),
            limit: self.option.filter.limit(),
            batch_size: self.option.batch_size,
        };
        let expr = QueryExpr {
            filters: self.option.filter.exprs().to_vec(),
            df_schema: self.option.df_schema.clone(),
            table_schema: self.option.table_schema.clone(),
        };

        let args_bytes = QueryArgs::encode(&args)?;
        let expr_bytes = QueryExpr::encode(&expr)?;
        let cmd = tonic::Request::new(QueryRecordBatchRequest {
            args: args_bytes,
            expr: expr_bytes,
        });

        // .map_err(|e| CoordinatorError::FailoverNode { id: node_id })?;
        let channel = self
            .meta_manager
            .admin_meta()
            .get_node_conn(node_id)
            .await?;
        let timeout_channel = Timeout::new(channel, Duration::from_secs(60 * 60));
        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
        let mut resp_stream = client
            .query_record_batch(cmd)
            .await
            .map_err(|_| CoordinatorError::FailoverNode { id: node_id })?
            .into_inner();
        while let Some(received) = resp_stream.next().await {
            let received = received?;
            if received.code != SUCCESS_RESPONSE_CODE {
                return Err(CoordinatorError::GRPCRequest {
                    msg: format!(
                        "server status: {}, {:?}",
                        received.code,
                        String::from_utf8(received.data)
                    ),
                });
            }
            let record = record_batch_decode(&received.data)?;

            self.meta_manager
                .tenant_manager()
                .limiter(self.option.tenant.as_str())
                .await
                .check_data_out(record.get_array_memory_size())
                .await?;

            self.sender.send(Ok(record)).await?;
        }

        Ok(())
    }

    pub async fn local_node_executor(&self, vnodes: Vec<VnodeInfo>) -> CoordinatorResult<()> {
        let mut routines = vec![];
        let now = tokio::time::Instant::now();
        for vnode in vnodes.iter() {
            info!("query local vnode: {:?}, now: {:?}", vnode, now);
            let routine = self.local_vnode_executor(vnode.clone());
            routines.push(routine);
        }

        let res = futures::future::try_join_all(routines).await.map(|_| ());
        info!(
            "parallel query local vnode over, start at: {:?} elapsed: {:?}, result: {:?}",
            now,
            now.elapsed(),
            res,
        );

        res
    }

    async fn local_vnode_executor(&self, vnode: VnodeInfo) -> CoordinatorResult<()> {
        let kv_inst = self
            .kv_inst
            .as_ref()
            .ok_or(CoordinatorError::KvInstanceNotFound {
                vnode_id: vnode.id,
                node_id: vnode.node_id,
            })?
            .clone();

        let mut iterator = RowIterator::new(kv_inst, self.option.clone(), vnode.id).await?;

        while let Some(data) = iterator.next().await {
            match data {
                Ok(val) => {
                    self.data_out.inc(val.get_array_memory_size() as u64);
                    self.sender.send(Ok(val)).await?;
                }
                Err(err) => {
                    return Err(CoordinatorError::from(err));
                }
            };
        }

        Ok(())
    }

    async fn map_vnode(&self) -> CoordinatorResult<HashMap<u64, Vec<VnodeInfo>>> {
        let meta = self
            .meta_manager
            .tenant_manager()
            .tenant_meta(&self.option.tenant)
            .await
            .ok_or(CoordinatorError::TenantNotFound {
                name: self.option.tenant.clone(),
            })?;

        let mut vnode_mapping: HashMap<u64, Vec<VnodeInfo>> = HashMap::new();
        for item in QueryOption::parse_time_ranges(
            self.option.filter.clone(),
            self.option.table_schema.clone(),
        )
        .iter()
        {
            let buckets =
                meta.mapping_bucket(&self.option.table_schema.db, item.min_ts, item.max_ts)?;
            for bucket in buckets.iter() {
                for repl in bucket.shard_group.iter() {
                    if repl.vnodes.is_empty() {
                        continue;
                    }

                    let random = now_timestamp() as usize % repl.vnodes.len();
                    let vnode = repl.vnodes[random].clone();

                    let list = vnode_mapping.entry(vnode.node_id).or_default();
                    list.push(vnode);
                }
            }
        }
        for (_, list) in vnode_mapping.iter_mut() {
            list.dedup_by(|a, b| a.id == b.id);
        }

        Ok(vnode_mapping)
    }

    async fn try_map_vnode(
        &self,
        vnodes: &[VnodeInfo],
    ) -> CoordinatorResult<HashMap<u64, Vec<VnodeInfo>>> {
        let meta = self
            .meta_manager
            .tenant_manager()
            .tenant_meta(&self.option.tenant)
            .await
            .ok_or(CoordinatorError::TenantNotFound {
                name: self.option.tenant.clone(),
            })?;

        let mut vnode_mapping: HashMap<u64, Vec<VnodeInfo>> = HashMap::new();
        for item in vnodes.iter() {
            let mut repl = meta
                .get_vnode_repl_set(item.id)
                .ok_or(CoordinatorError::VnodeNotFound { id: item.id })?;

            repl.vnodes.retain(|x| x.node_id != item.node_id);
            if repl.vnodes.is_empty() {
                return Err(CoordinatorError::CommonError {
                    msg: format!("try map vnode:{} failed,not found replication", item.id),
                });
            }

            let random = now_timestamp() as usize % repl.vnodes.len();
            let vnode = repl.vnodes[random].clone();

            let list = vnode_mapping.entry(vnode.node_id).or_default();
            list.push(vnode);
        }
        for (_, list) in vnode_mapping.iter_mut() {
            list.dedup_by(|a, b| a.id == b.id);
        }

        Ok(vnode_mapping)
    }
}

pub fn record_batch_encode(record: &RecordBatch) -> CoordinatorResult<Vec<u8>> {
    let buffer: Vec<u8> = Vec::new();
    let mut stream_writer = StreamWriter::try_new(buffer, &record.schema())?;
    stream_writer.write(record)?;
    stream_writer.finish()?;
    let data = stream_writer.into_inner()?;

    Ok(data)
}

pub fn record_batch_decode(buf: &[u8]) -> CoordinatorResult<RecordBatch> {
    let mut stream_reader = StreamReader::try_new(std::io::Cursor::new(buf), None)?;
    let record = stream_reader.next().ok_or(CoordinatorError::CommonError {
        msg: "record batch is None".to_string(),
    })??;

    Ok(record)
}
