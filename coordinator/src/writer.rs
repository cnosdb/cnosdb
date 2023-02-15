use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_channel as channel;
use flatbuffers::FlatBufferBuilder;
use futures::future::ok;
//use std::net::{TcpListener, TcpStream};
use meta::meta_manager::RemoteMetaManager;
use meta::{MetaClientRef, MetaRef};
use models::auth::user::{ROOT, ROOT_PWD};
use models::meta_data::*;
use models::utils::now_timestamp;
use models::RwLockRef;
use parking_lot::{RwLock, RwLockReadGuard};
use protos::kv_service::{Meta, WritePointsRequest, WritePointsResponse};
use protos::models as fb_models;
use protos::models::{FieldBuilder, PointArgs, Points, PointsArgs, TagBuilder};
use snafu::ResultExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use trace::{debug, info};
use tskv::engine::EngineRef;

use crate::command::*;
use crate::errors::*;
use crate::hh_queue::{HintedOffBlock, HintedOffManager, HintedOffWriteReq};

pub struct VnodePoints<'a> {
    db: String,
    fbb: FlatBufferBuilder<'a>,
    offset: Vec<flatbuffers::WIPOffset<fb_models::Point<'a>>>,

    pub data: Vec<u8>,
    pub repl_set: ReplicationSet,
}

impl VnodePoints<'_> {
    pub fn new(db: String, repl_set: ReplicationSet) -> Self {
        Self {
            db,
            repl_set,
            fbb: FlatBufferBuilder::new(),
            offset: Vec::new(),
            data: vec![],
        }
    }

    pub fn add_point(&mut self, point: models::Point) {
        let mut tags = Vec::with_capacity(point.tags.len());
        for item in point.tags.iter() {
            let fbk = self.fbb.create_vector(&item.key);
            let fbv = self.fbb.create_vector(&item.value);
            let mut tag_builder = TagBuilder::new(&mut self.fbb);
            tag_builder.add_key(fbk);
            tag_builder.add_value(fbv);
            tags.push(tag_builder.finish());
        }

        let mut fields = Vec::with_capacity(point.fields.len());
        for item in point.fields.iter() {
            let fbk = self.fbb.create_vector(&item.name);
            let fbv = self.fbb.create_vector(&item.value);

            //fb_models::FieldType::Boolean,

            let vtype = item.value_type.to_fb_type();
            let mut field_builder = FieldBuilder::new(&mut self.fbb);
            field_builder.add_name(fbk);
            field_builder.add_type_(vtype);
            field_builder.add_value(fbv);
            fields.push(field_builder.finish());
        }

        let point_args = PointArgs {
            db: Some(self.fbb.create_vector(point.db.as_bytes())),
            tab: Some(self.fbb.create_vector(point.table.as_bytes())),
            tags: Some(self.fbb.create_vector(&tags)),
            fields: Some(self.fbb.create_vector(&fields)),
            timestamp: point.timestamp,
        };

        self.offset
            .push(fb_models::Point::create(&mut self.fbb, &point_args));
    }

    pub fn finish(&mut self) {
        let fbb_db = self.fbb.create_vector(self.db.as_bytes());
        let points_raw = self.fbb.create_vector(&self.offset);

        let points = Points::create(
            &mut self.fbb,
            &PointsArgs {
                db: Some(fbb_db),
                points: Some(points_raw),
            },
        );
        self.fbb.finish(points, None);
        self.data = self.fbb.finished_data().to_vec();
    }
}

pub struct VnodeMapping<'a> {
    pub points: HashMap<u32, VnodePoints<'a>>,
    pub sets: HashMap<u32, ReplicationSet>,
}

impl<'a> VnodeMapping<'a> {
    pub fn new() -> Self {
        Self {
            points: HashMap::new(),
            sets: HashMap::new(),
        }
    }

    pub async fn map_point(
        &mut self,
        meta_client: MetaClientRef,
        point: models::Point,
    ) -> CoordinatorResult<()> {
        if let Some(val) = meta_client.database_min_ts(&point.db) {
            if point.timestamp < val {
                return Err(CoordinatorError::CommonError {
                    msg: "write expired time data not permit".to_string(),
                });
            }
        }

        //let full_name = format!("{}.{}", meta_client.tenant_name(), db);
        let info = meta_client
            .locate_replcation_set_for_write(&point.db, point.hash_id, point.timestamp)
            .await?;
        self.sets.entry(info.id).or_insert_with(|| info.clone());
        let entry = self
            .points
            .entry(info.id)
            .or_insert_with(|| VnodePoints::new(point.db.clone(), info));

        entry.add_point(point);

        Ok(())
    }
}

impl<'a> Default for VnodeMapping<'a> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct PointWriter {
    node_id: u64,
    kv_inst: Option<EngineRef>,
    meta_manager: MetaRef,
    hh_sender: Sender<HintedOffWriteReq>,
}

impl PointWriter {
    pub fn new(
        node_id: u64,
        kv_inst: Option<EngineRef>,
        meta_manager: MetaRef,
        hh_sender: Sender<HintedOffWriteReq>,
    ) -> Self {
        Self {
            node_id,
            kv_inst,
            meta_manager,
            hh_sender,
        }
    }

    pub async fn write_points(&self, req: &WriteRequest) -> CoordinatorResult<()> {
        let meta_client = self
            .meta_manager
            .tenant_manager()
            .tenant_meta(&req.tenant)
            .await
            .ok_or(CoordinatorError::TenantNotFound {
                name: req.tenant.clone(),
            })?;

        let mut mapping = VnodeMapping::new();
        let fb_points = flatbuffers::root::<fb_models::Points>(&req.request.points)
            .context(InvalidFlatbufferSnafu)?;
        let fb_points = fb_points.points().unwrap();
        for item in fb_points {
            let point = models::Point::from_flatbuffers(&item).map_err(|err| {
                CoordinatorError::CommonError {
                    msg: err.to_string(),
                }
            })?;

            mapping.map_point(meta_client.clone(), point).await?;
        }

        let mut requests = vec![];
        let now = tokio::time::Instant::now();
        for (id, points) in mapping.points.iter_mut() {
            points.finish();

            for vnode in points.repl_set.vnodes.iter() {
                info!("write points on vnode {:?},  now: {:?}", vnode, now);

                let request =
                    self.write_to_node(vnode.id, &req.tenant, vnode.node_id, points.data.clone());
                requests.push(request);
            }
        }

        let res = futures::future::try_join_all(requests).await.map(|_| ());

        info!(
            "parallel write points on vnode over, start at: {:?} elapsed: {:?}, result: {:?}",
            now,
            now.elapsed(),
            res,
        );

        res
    }

    async fn write_to_node(
        &self,
        vnode_id: u32,
        tenant: &str,
        node_id: u64,
        data: Vec<u8>,
    ) -> CoordinatorResult<()> {
        if node_id == self.node_id && self.kv_inst.is_some() {
            let result = self.write_to_local_node(vnode_id, tenant, data).await;
            debug!("write data to local {}({}) {:?}", node_id, vnode_id, result);

            return result;
        }

        if let Err(err) = self
            .write_to_remote_node(vnode_id, node_id, tenant, data.clone())
            .await
        {
            info!(
                "write data to remote {}({}) failed; {}!",
                node_id,
                vnode_id,
                err.to_string()
            );

            return self.write_to_handoff(vnode_id, node_id, tenant, data).await;
        }

        debug!(
            "write data to remote {}({}) , inst exist: {}, success!",
            node_id,
            vnode_id,
            self.kv_inst.is_some()
        );
        Ok(())
    }

    async fn write_to_handoff(
        &self,
        vnode_id: u32,
        node_id: u64,
        tenant: &str,
        data: Vec<u8>,
    ) -> CoordinatorResult<()> {
        let (sender, receiver) = oneshot::channel();
        let block = HintedOffBlock::new(now_timestamp(), vnode_id, tenant.to_string(), data);
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
        data: Vec<u8>,
    ) -> CoordinatorResult<()> {
        let mut conn = self
            .meta_manager
            .admin_meta()
            .get_node_conn(node_id)
            .await?;

        let req_cmd = WriteVnodeRequest {
            vnode_id,
            tenant: tenant.to_string(),
            data,
        };
        send_command(&mut conn, &CoordinatorTcpCmd::WriteVnodePointCmd(req_cmd)).await?;

        let rsp_cmd = recv_command(&mut conn).await?;
        if let CoordinatorTcpCmd::StatusResponseCmd(msg) = rsp_cmd {
            self.meta_manager.admin_meta().put_node_conn(node_id, conn);
            if msg.code == SUCCESS_RESPONSE_CODE {
                Ok(())
            } else {
                Err(CoordinatorError::WriteVnode {
                    msg: format!("code: {}, msg: {}", msg.code, msg.data),
                })
            }
        } else {
            Err(CoordinatorError::UnExpectResponse)
        }
    }

    async fn write_to_local_node(
        &self,
        vnode_id: u32,
        tenant: &str,
        data: Vec<u8>,
    ) -> CoordinatorResult<()> {
        let req = WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: tenant.to_string(),
                user: None,
                password: None,
            }),
            points: data.clone(),
        };

        if let Some(kv_inst) = self.kv_inst.clone() {
            let _ = kv_inst.write(vnode_id, req).await?;
            Ok(())
        } else {
            Err(CoordinatorError::KvInstanceNotFound {
                vnode_id,
                node_id: 0,
            })
        }
    }
}
