use std::{collections::HashMap, sync::Arc};

use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::future::ok;
use models::{
    meta_data::VnodeInfo,
    predicate::domain::{PredicateRef, QueryArgs, QueryExpr},
    schema::TskvTableSchema,
    utils::now_timestamp,
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use trace::info;
use tskv::{
    engine::EngineRef,
    iterator::{filter_to_time_ranges, QueryOption, RowIterator, TableScanMetrics},
    TimeRange,
};

use crate::{
    command::{
        recv_command, send_command, CoordinatorTcpCmd, QueryRecordBatchRequest,
        FAILED_RESPONSE_CODE,
    },
    errors::{CoordinatorError, CoordinatorResult},
};

use meta::meta_client::MetaRef;

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

    kv_inst: EngineRef,
    meta_manager: MetaRef,

    sender: Sender<CoordinatorResult<RecordBatch>>,
}

impl QueryExecutor {
    pub fn new(
        option: QueryOption,
        kv_inst: EngineRef,
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
        let mapping = self.map_vnode()?;
        for (node_id, vnodes) in mapping.iter() {
            let routine = self.node_executor(*node_id, vnodes.clone());
            routines.push(routine);
        }

        futures::future::try_join_all(routines).await?;

        Ok(())
    }

    async fn node_executor(&self, node_id: u64, vnodes: Vec<VnodeInfo>) -> CoordinatorResult<()> {
        if node_id == self.meta_manager.node_id() {
            return self.local_node_executor(vnodes).await;
        } else {
            return self.remote_node_executor(node_id, vnodes).await;
        }
    }

    async fn remote_node_executor(
        &self,
        node_id: u64,
        vnodes: Vec<VnodeInfo>,
    ) -> CoordinatorResult<()> {
        let mut conn = self
            .meta_manager
            .admin_meta()
            .get_node_conn(node_id)
            .await?;

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
        send_command(&mut conn, &CoordinatorTcpCmd::QueryRecordBatchCmd(req_cmd)).await?;

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
        for vnode in vnodes.iter() {
            let routine = self.local_vnode_executor(vnode.clone());
            routines.push(routine);
        }

        futures::future::try_join_all(routines).await?;

        Ok(())
    }

    async fn local_vnode_executor(&self, vnode: VnodeInfo) -> CoordinatorResult<()> {
        let mut iterator = RowIterator::new(self.kv_inst.clone(), self.option.clone(), vnode.id)?;
        while let Some(data) = iterator.next() {
            match data {
                Ok(val) => {
                    self.sender.send(Ok(val)).await?;
                }
                Err(err) => {
                    return Err(CoordinatorError::from(err));
                }
            };
        }

        Ok(())
    }

    fn map_vnode(&self) -> CoordinatorResult<HashMap<u64, Vec<VnodeInfo>>> {
        let meta = self
            .meta_manager
            .tenant_manager()
            .tenant_meta(&self.option.tenant)
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
                    if repl.vnodes.len() == 0 {
                        continue;
                    }

                    let random = now_timestamp() as usize % repl.vnodes.len();
                    let vnode = repl.vnodes[random].clone();

                    let list = vnode_mapping.entry(vnode.node_id).or_insert(vec![]);
                    list.push(vnode);
                }
            }
        }
        for (_, list) in vnode_mapping.iter_mut() {
            list.dedup_by(|a, b| a.id == b.id);
        }

        Ok(vnode_mapping)
    }
}
