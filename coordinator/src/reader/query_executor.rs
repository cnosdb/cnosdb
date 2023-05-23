use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use meta::model::MetaRef;
use metrics::count::U64Counter;
use models::arrow_array::build_arrow_array_builders;
use models::meta_data::{VnodeInfo, VnodeStatus};
use models::utils::{now_timestamp_millis, now_timestamp_nanos};
use models::{record_batch_decode, SeriesKey};
use protos::kv_service::tskv_service_client::TskvServiceClient;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use trace::debug;
use tracing::error;
use tskv::query_iterator::{QueryOption, RowIterator};
use tskv::EngineRef;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::service::CoordServiceMetrics;
use crate::SUCCESS_RESPONSE_CODE;

pub struct QueryExecutor {
    option: QueryOption,
    timeout_ms: u64,

    runtime: Arc<Runtime>,
    kv_inst: Option<EngineRef>,
    meta_manager: MetaRef,

    sender: Sender<CoordinatorResult<RecordBatch>>,
    data_out: U64Counter,
}

impl QueryExecutor {
    pub fn new(
        option: QueryOption,
        timeout_ms: u64,
        runtime: Arc<Runtime>,
        kv_inst: Option<EngineRef>,
        meta_manager: MetaRef,
        sender: Sender<CoordinatorResult<RecordBatch>>,
        metrics: Arc<CoordServiceMetrics>,
    ) -> Self {
        let data_out = metrics.data_out(
            option.table_schema.tenant.as_str(),
            option.table_schema.db.as_str(),
        );
        Self {
            option,
            runtime,
            kv_inst,
            timeout_ms,
            meta_manager,
            sender,
            data_out,
        }
    }

    pub async fn tag_scan(&self) -> CoordinatorResult<()> {
        let mut routines = vec![];
        let mapping = self.map_vnode().await?;
        let now = tokio::time::Instant::now();
        for (node_id, vnodes) in mapping.iter() {
            debug!(
                "execute tag scan on node {}, vnode list: {:?} now: {:?}",
                node_id, vnodes, now
            );

            let routine = self.node_tag_scan_executor(*node_id, vnodes.clone());
            routines.push(routine);
        }

        let res = futures::future::try_join_all(routines).await.map(|_| ());

        debug!(
            "parallel execute tag scan on vnodes over, start at: {:?} elapsed: {:?}, result: {:?}",
            now,
            now.elapsed(),
            res,
        );

        res
    }

    pub async fn execute(&self) -> CoordinatorResult<()> {
        let mut routines = vec![];
        let mapping = self.map_vnode().await?;
        let now = tokio::time::Instant::now();
        for (node_id, vnodes) in mapping.iter() {
            debug!(
                "execute select on node {}, vnode list: {:?} now: {:?}",
                node_id, vnodes, now
            );

            let routine = self.node_executor(*node_id, vnodes.clone());
            routines.push(routine);
        }

        let res = futures::future::try_join_all(routines).await.map(|_| ());

        debug!(
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
                    debug!(
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
    async fn node_tag_scan_executor(
        &self,
        node_id: u64,
        vnodes: Vec<VnodeInfo>,
    ) -> CoordinatorResult<()> {
        if node_id == self.meta_manager.node_id() {
            self.local_node_tag_scan(vnodes.clone()).await
        } else {
            let result = self
                .remote_node_tag_scan_executor(node_id, vnodes.clone())
                .await;
            if let Err(CoordinatorError::FailoverNode { id: _ }) = result {
                let mut routines = vec![];
                let mapping = self.try_map_vnode(&vnodes).await?;
                for (tmp_id, tmp_vnodes) in mapping.iter() {
                    debug!(
                        "try execute tag scan on node {}, vnode list: {:?}",
                        tmp_id, tmp_vnodes
                    );

                    let routine = self.remote_node_tag_scan_executor(*tmp_id, tmp_vnodes.clone());
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
        debug!(
            "execute select command on remote node: {} vnodes: {:?} now: {:?}",
            node_id, vnodes, now
        );

        let res = self.warp_remote_node_executor(node_id, vnodes).await;

        debug!(
            "execute select command on remote node over: {}  start at: {:?}, elapsed: {:?}, result: {:?}",
            node_id,
            now,
            now.elapsed(),
            res
        );

        res
    }

    async fn remote_node_tag_scan_executor(
        &self,
        node_id: u64,
        vnodes: Vec<VnodeInfo>,
    ) -> CoordinatorResult<()> {
        let now = tokio::time::Instant::now();
        debug!(
            "execute tag scan command on remote node: {} vnodes: {:?} now: {:?}",
            node_id, vnodes, now
        );

        let res = self
            .warp_remote_node_tag_scan_executor(node_id, vnodes)
            .await;

        debug!(
            "execute tag scan command on remote node over: {}  start at: {:?}, elapsed: {:?}, result: {:?}",
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
        let cmd = {
            let vnode_ids = vnodes.iter().map(|v| v.id).collect::<Vec<_>>();
            let req = self.option.to_query_record_batch_request(vnode_ids)?;
            tonic::Request::new(req)
        };

        let channel = self
            .meta_manager
            .admin_meta()
            .get_node_conn(node_id)
            .await
            .map_err(|_| CoordinatorError::FailoverNode { id: node_id })?;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(self.timeout_ms));
        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
        let begin_time = now_timestamp_millis();
        let mut resp_stream = client
            .query_record_batch(cmd)
            .await
            .map_err(|_| CoordinatorError::FailoverNode { id: node_id })?
            .into_inner();
        let use_time = now_timestamp_millis() - begin_time;
        if use_time > 200 {
            debug!(
                "query data to node:{}, use time too long {}",
                node_id, use_time
            )
        }
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
                .limiter(self.option.table_schema.tenant.as_str())
                .await
                .check_data_out(record.get_array_memory_size())
                .await?;

            self.sender.send(Ok(record)).await?;
        }

        Ok(())
    }
    async fn warp_remote_node_tag_scan_executor(
        &self,
        node_id: u64,
        vnodes: Vec<VnodeInfo>,
    ) -> CoordinatorResult<()> {
        let cmd = {
            let vnode_ids = vnodes.iter().map(|v| v.id).collect::<Vec<_>>();
            let req = self.option.to_query_record_batch_request(vnode_ids)?;
            tonic::Request::new(req)
        };

        let channel = self
            .meta_manager
            .admin_meta()
            .get_node_conn(node_id)
            .await?;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(self.timeout_ms));
        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
        let mut resp_stream = client
            .tag_scan(cmd)
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
                .limiter(self.option.table_schema.tenant.as_str())
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
            debug!("query local vnode: {:?}, now: {:?}", vnode, now);
            let routine = self.local_vnode_executor(vnode.clone());
            routines.push(routine);
        }

        let res = futures::future::try_join_all(routines).await.map(|_| ());
        debug!(
            "parallel query local vnode over, start at: {:?} elapsed: {:?}, result: {:?}",
            now,
            now.elapsed(),
            res,
        );

        res
    }

    pub async fn local_node_tag_scan(&self, vnode: Vec<VnodeInfo>) -> CoordinatorResult<()> {
        let mut routines = vec![];
        let now = tokio::time::Instant::now();
        for vnode in vnode.into_iter() {
            debug!("tag scan local vnode: {:?}, now: {:?}", vnode, now);
            let routine = self.local_vnode_tag_scan(vnode);
            routines.push(routine);
        }

        let res = futures::future::try_join_all(routines).await.map(|_| ());
        debug!(
            "parallel tag scan local vnode over, start at: {:?} elapsed: {:?}, result: {:?}",
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

        let mut iterator =
            RowIterator::new(self.runtime.clone(), kv_inst, self.option.clone(), vnode.id).await?;
        while let Some(data) = iterator.next().await {
            match data {
                Ok(val) => {
                    self.data_out.inc(val.get_array_memory_size() as u64);
                    self.sender.send(Ok(val)).await?;
                }
                Err(err) => {
                    if let tskv::Error::ReadTsm { source } = &err {
                        match source {
                            tskv::tsm::ReadTsmError::CrcCheck
                            | tskv::tsm::ReadTsmError::FileNotFound { .. }
                            | tskv::tsm::ReadTsmError::Invalid { .. } => {
                                error!("vnode {} data broken: {:?}", vnode.id, source);
                                let _ = self
                                    .change_vnode_to_broken(&self.option.table_schema.tenant, vnode)
                                    .await;
                            }

                            _ => {}
                        }
                    }
                    return Err(CoordinatorError::from(err));
                }
            };
        }

        Ok(())
    }

    pub async fn change_vnode_to_broken(
        &self,
        tenant: &str,
        vnode: VnodeInfo,
    ) -> CoordinatorResult<()> {
        let mut all_info =
            crate::service::get_vnode_all_info(self.meta_manager.clone(), tenant, vnode.id).await?;

        let meta_client = self
            .meta_manager
            .tenant_manager()
            .tenant_meta(tenant)
            .await
            .ok_or(CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            })?;

        all_info.set_status(VnodeStatus::Broken);
        meta_client.update_vnode(&all_info).await?;

        Ok(())
    }

    pub async fn local_vnode_tag_scan(&self, vnode: VnodeInfo) -> CoordinatorResult<()> {
        let kv = self
            .kv_inst
            .as_ref()
            .ok_or(CoordinatorError::KvInstanceNotFound {
                vnode_id: vnode.id,
                node_id: vnode.node_id,
            })?
            .clone();

        let (tenant, db, table) = (
            self.option.table_schema.tenant.as_str(),
            self.option.table_schema.db.as_str(),
            self.option.table_schema.name.as_str(),
        );

        let mut keys = Vec::new();

        for series_id in kv
            .get_series_id_by_filter(tenant, db, table, vnode.id, self.option.split.tags_filter())
            .await?
            .into_iter()
        {
            if let Some(key) = kv.get_series_key(tenant, db, vnode.id, series_id).await? {
                keys.push(key)
            }
        }

        for chunk in keys.chunks(self.option.batch_size) {
            let record_batch = series_keys_to_record_batch(self.option.df_schema.clone(), chunk)?;
            self.sender.send(Ok(record_batch)).await?;
        }

        Ok(())
    }

    async fn map_vnode(&self) -> CoordinatorResult<HashMap<u64, Vec<VnodeInfo>>> {
        let meta = self
            .meta_manager
            .tenant_manager()
            .tenant_meta(&self.option.table_schema.tenant)
            .await
            .ok_or(CoordinatorError::TenantNotFound {
                name: self.option.table_schema.tenant.clone(),
            })?;

        let mut vnode_mapping: HashMap<u64, Vec<VnodeInfo>> = HashMap::new();
        let time_ranges = self.option.split.time_ranges();
        let buckets = meta.mapping_bucket(
            &self.option.table_schema.db,
            time_ranges.min_ts(),
            time_ranges.max_ts(),
        )?;
        for bucket in buckets.iter() {
            for repl in bucket.shard_group.iter() {
                if repl.vnodes.is_empty() {
                    continue;
                }

                let random = now_timestamp_nanos() as usize % repl.vnodes.len();
                let vnode = {
                    let mut tmp = repl.vnodes[random].clone();
                    for index in random..(random + repl.vnodes.len()) {
                        let status = repl.vnodes[index % repl.vnodes.len()].status;
                        if status == VnodeStatus::Running || status == VnodeStatus::Copying {
                            tmp = repl.vnodes[index % repl.vnodes.len()].clone();
                            break;
                        }
                    }
                    tmp
                };

                let list = vnode_mapping.entry(vnode.node_id).or_default();
                list.push(vnode);
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
            .tenant_meta(&self.option.table_schema.tenant)
            .await
            .ok_or(CoordinatorError::TenantNotFound {
                name: self.option.table_schema.tenant.clone(),
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

            let random = now_timestamp_nanos() as usize % repl.vnodes.len();
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

fn series_keys_to_record_batch(
    schema: SchemaRef,
    series_keys: &[SeriesKey],
) -> Result<RecordBatch, ArrowError> {
    let tag_key_array = schema.fields.iter().map(|f| f.name()).collect::<Vec<_>>();
    let mut array_builders = build_arrow_array_builders(&schema, series_keys.len())?;
    for key in series_keys {
        for (k, array_builder) in tag_key_array.iter().zip(&mut array_builders) {
            let tag_value = key
                .tag_string_val(k)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            let builder = array_builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("Cast failed for List Builder<StringBuilder> during nested data parsing");
            builder.append_option(tag_value)
        }
    }
    let columns = array_builders
        .into_iter()
        .map(|mut b| b.finish())
        .collect::<Vec<_>>();
    let record_batch = RecordBatch::try_new(schema.clone(), columns)?;
    Ok(record_batch)
}
