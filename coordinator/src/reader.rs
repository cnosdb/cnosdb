use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use futures::future::ok;
use meta::MetaRef;
use models::meta_data::VnodeInfo;
use models::predicate::domain::{PredicateRef, QueryArgs, QueryExpr};
use models::schema::TskvTableSchema;
use models::utils::now_timestamp;
use tokio::sync::mpsc::{self, Receiver, Sender};
use trace::info;
use tskv::engine::EngineRef;
use tskv::iterator::{filter_to_time_ranges, QueryOption, RowIterator, TableScanMetrics};
use tskv::TimeRange;

use crate::command::{
    recv_command, send_command, CoordinatorTcpCmd, QueryRecordBatchRequest, FAILED_RESPONSE_CODE,
};
use crate::errors::{CoordinatorError, CoordinatorResult};

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
}

pub struct QueryExecutor {
    option: QueryOption,

    kv_inst: Option<EngineRef>,
    meta_manager: MetaRef,

    sender: Sender<CoordinatorResult<RecordBatch>>,
}

impl QueryExecutor {
    pub fn new(
        option: QueryOption,
        kv_inst: Option<EngineRef>,
        meta_manager: MetaRef,
        sender: Sender<CoordinatorResult<RecordBatch>>,
    ) -> Self {
        Self {
            option,
            kv_inst,
            meta_manager,
            sender,
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
            "parallel execute select on vnode over, start at: {:?} elapsed: {:?}, result: {:?}",
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
            if let Err(CoordinatorError::FailoverNode { id }) = result {
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
        let mut conn = self
            .meta_manager
            .admin_meta()
            .get_node_conn(node_id)
            .await
            .map_err(|e| CoordinatorError::FailoverNode { id: node_id })?;

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
        let req_cmd = QueryRecordBatchRequest { args, expr };
        send_command(&mut conn, &CoordinatorTcpCmd::QueryRecordBatchCmd(req_cmd))
            .await
            .map_err(|e| CoordinatorError::FailoverNode { id: node_id })?;

        loop {
            let rsp_cmd = recv_command(&mut conn).await?;
            match rsp_cmd {
                CoordinatorTcpCmd::StatusResponseCmd(rsp) => {
                    info!("remote node execute status: {:?}", rsp);
                    if rsp.code == FAILED_RESPONSE_CODE {
                        return Err(CoordinatorError::CommonError { msg: rsp.data });
                    }

                    break;
                }

                CoordinatorTcpCmd::RecordBatchResponseCmd(rsp) => {
                    let tenant = self.option.tenant.clone();

                    self.meta_manager
                        .tenant_manager()
                        .limiter(self.option.tenant.as_str())
                        .await
                        .check_data_out(rsp.record.get_array_memory_size())
                        .await?;

                    self.sender.send(Ok(rsp.record)).await?;
                }
                _ => {
                    return Err(CoordinatorError::UnExpectResponse);
                }
            }
        }
        self.meta_manager.admin_meta().put_node_conn(node_id, conn);

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
        if let Some(kv_inst) = self.kv_inst.clone() {
            let tenant = self.option.tenant.clone();
            let mut iterator =
                RowIterator::new(kv_inst.clone(), self.option.clone(), vnode.id).await?;

            while let Some(data) = iterator.next().await {
                match data {
                    Ok(val) => {
                        self.sender.send(Ok(val)).await?;
                    }
                    Err(err) => {
                        return Err(CoordinatorError::from(err));
                    }
                };
            }
            return Ok(());
        }
        Err(CoordinatorError::KvInstanceNotFound {
            vnode_id: vnode.id,
            node_id: vnode.node_id,
        })
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
