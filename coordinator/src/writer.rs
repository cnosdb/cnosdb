use std::collections::HashMap;
use std::time::Duration;

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use meta::error::MetaError;
use meta::model::{MetaClientRef, MetaRef};
use models::meta_data::*;
use models::schema::{timestamp_convert, Precision};
use models::utils::{now_timestamp_millis, now_timestamp_nanos};
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{Meta, WritePointsRequest, WriteVnodeRequest};
use protos::models::{
    FieldBuilder, Point, PointBuilder, Points, PointsArgs, Schema, SchemaBuilder, TableBuilder,
    TagBuilder,
};
use protos::{fb_table_name, get_db_from_fb_points, models as fb_models};
use snafu::ResultExt;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use trace::{debug, SpanContext, SpanExt, SpanRecorder};
use trace_http::ctx::append_trace_context;
use tskv::EngineRef;
use utils::bitset::BitSet;
use utils::BkdrHasher;

use crate::errors::*;
use crate::hh_queue::{HintedOffBlock, HintedOffWriteReq};
use crate::{status_response_to_result, WriteRequest};

pub struct VnodePoints<'a> {
    db: String,
    fbb: FlatBufferBuilder<'a>,
    offset: HashMap<String, Vec<WIPOffset<Point<'a>>>>,
    schema: HashMap<String, WIPOffset<Schema<'a>>>,

    pub data: Vec<u8>,
    pub repl_set: ReplicationSet,
}

impl VnodePoints<'_> {
    pub fn new(db: String, repl_set: ReplicationSet) -> Self {
        Self {
            db,
            repl_set,
            fbb: FlatBufferBuilder::new(),
            offset: HashMap::new(),
            schema: HashMap::new(),
            data: vec![],
        }
    }

    pub fn add_schema(&mut self, table_name: &str, schema: Schema) {
        if self.schema.get(table_name).is_none() {
            let tag_names_off = schema
                .tag_name()
                .unwrap_or_default()
                .iter()
                .map(|item| self.fbb.create_string(item))
                .collect::<Vec<_>>();
            let tag_names = self.fbb.create_vector(&tag_names_off);

            let field_name_off = schema
                .field_name()
                .unwrap_or_default()
                .iter()
                .map(|item| self.fbb.create_string(item))
                .collect::<Vec<_>>();
            let field_names = self.fbb.create_vector(&field_name_off);
            let field_type = self.fbb.create_vector(
                &schema
                    .field_type()
                    .unwrap_or_default()
                    .iter()
                    .collect::<Vec<_>>(),
            );

            let mut schema_builder = SchemaBuilder::new(&mut self.fbb);
            schema_builder.add_tag_name(tag_names);
            schema_builder.add_field_name(field_names);
            schema_builder.add_field_type(field_type);

            let schema = schema_builder.finish();
            self.schema.insert(table_name.to_string(), schema);
        }
    }

    pub fn add_point(&mut self, table_name: &str, point: Point) {
        let mut tags = Vec::with_capacity(point.tags().unwrap_or_default().len());
        for tag in point.tags().unwrap_or_default().iter() {
            let tags_value = self
                .fbb
                .create_vector(tag.value().unwrap_or_default().bytes());

            let mut tag_builder = TagBuilder::new(&mut self.fbb);
            tag_builder.add_value(tags_value);
            tags.push(tag_builder.finish());
        }

        let tags = self.fbb.create_vector(&tags);
        let tags_nullbit = self
            .fbb
            .create_vector(point.tags_nullbit().unwrap_or_default().bytes());

        let mut fields = Vec::with_capacity(point.fields().unwrap_or_default().len());
        for field in point.fields().unwrap_or_default().iter() {
            let field_value = self
                .fbb
                .create_vector(field.value().unwrap_or_default().bytes());

            let mut field_builder = FieldBuilder::new(&mut self.fbb);
            field_builder.add_value(field_value);
            fields.push(field_builder.finish());
        }

        let fields = self.fbb.create_vector(&fields);
        let fields_nullbit = self
            .fbb
            .create_vector(point.fields_nullbit().unwrap_or_default().bytes());

        let mut point_builder = PointBuilder::new(&mut self.fbb);
        point_builder.add_tags(tags);
        point_builder.add_tags_nullbit(tags_nullbit);
        point_builder.add_fields(fields);
        point_builder.add_fields_nullbit(fields_nullbit);
        point_builder.add_timestamp(point.timestamp());

        let point = point_builder.finish();

        match self.offset.get_mut(table_name) {
            None => {
                self.offset.insert(table_name.to_string(), vec![point]);
            }
            Some(points) => {
                points.push(point);
            }
        }
    }

    pub fn finish(&mut self) -> CoordinatorResult<()> {
        let fbb_db = self.fbb.create_vector(self.db.as_bytes());
        let mut fbb_tables = Vec::with_capacity(self.offset.len());
        let table_names = self.offset.iter().map(|item| item.0.as_str());

        for table_name in table_names {
            let table_points = self
                .offset
                .get(table_name)
                .ok_or(CoordinatorError::Points {
                    msg: format!("can not found points for {}", table_name),
                })?;
            let schema = self
                .schema
                .get(table_name)
                .ok_or(CoordinatorError::Points {
                    msg: format!("can not found schema for {}", table_name),
                })?;
            let num_rows = table_points.len();
            let table_points = self.fbb.create_vector(table_points);
            let table_name = self.fbb.create_vector(table_name.as_bytes());

            let mut table_builder = TableBuilder::new(&mut self.fbb);
            table_builder.add_tab(table_name);
            table_builder.add_schema(*schema);
            table_builder.add_points(table_points);
            table_builder.add_num_rows(num_rows as u64);
            fbb_tables.push(table_builder.finish())
        }

        let tables = self.fbb.create_vector(&fbb_tables);

        let points = Points::create(
            &mut self.fbb,
            &PointsArgs {
                db: Some(fbb_db),
                tables: Some(tables),
            },
        );
        self.fbb.finish(points, None);
        self.data = self.fbb.finished_data().to_vec();
        Ok(())
    }
}

pub struct VnodeMapping<'a> {
    // replication id -> VnodePoints
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
        db_name: &str,
        tab_name: &str,
        precision: Precision,
        schema: Schema<'_>,
        point: Point<'_>,
    ) -> CoordinatorResult<()> {
        let db_schema = meta_client
            .get_db_schema(db_name)?
            .ok_or(MetaError::DatabaseNotFound {
                database: db_name.to_string(),
            })?;
        let db_precision = db_schema.config.precision_or_default();
        let ts = timestamp_convert(precision, *db_precision, point.timestamp()).ok_or(
            CoordinatorError::CommonError {
                msg: "timestamp overflow".to_string(),
            },
        )?;

        if let Some(val) = meta_client.database_min_ts(db_name) {
            if ts < val {
                return Err(CoordinatorError::CommonError {
                    msg: "write expired time data not permit".to_string(),
                });
            }
        }

        let hash_id = {
            let mut hasher = BkdrHasher::new();
            hasher.hash_with(tab_name.as_bytes());
            if let Some(tags_key) = schema.tag_name() {
                let tag_nullbit = point.tags_nullbit().ok_or(CoordinatorError::Points {
                    msg: "point missing tag null bit".to_string(),
                })?;
                let len = tags_key.len();
                let tag_nullbit = BitSet::new_without_check(len, tag_nullbit.bytes());
                for (idx, (tag_key, tag_value)) in tags_key
                    .iter()
                    .zip(point.tags().ok_or(CoordinatorError::Points {
                        msg: "point missing tag value".to_string(),
                    })?)
                    .enumerate()
                {
                    if !tag_nullbit.get(idx) {
                        continue;
                    }
                    hasher.hash_with(tag_key.as_bytes());
                    hasher.hash_with(
                        tag_value
                            .value()
                            .ok_or(CoordinatorError::Points {
                                msg: "point missing tag value".to_string(),
                            })?
                            .bytes(),
                    );
                }
            }

            hasher.number()
        };

        //let full_name = format!("{}.{}", meta_client.tenant_name(), db);
        let info = meta_client
            .locate_replication_set_for_write(db_name, hash_id, ts)
            .await?;
        self.sets.entry(info.id).or_insert_with(|| info.clone());
        let entry = self
            .points
            .entry(info.id)
            .or_insert_with(|| VnodePoints::new(db_name.to_string(), info));

        entry.add_point(tab_name, point);
        entry.add_schema(tab_name, schema);

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
    timeout_ms: u64,
    kv_inst: Option<EngineRef>,
    meta_manager: MetaRef,
    hh_sender: Sender<HintedOffWriteReq>,
}

impl PointWriter {
    pub fn new(
        node_id: u64,
        timeout_ms: u64,
        kv_inst: Option<EngineRef>,
        meta_manager: MetaRef,
        hh_sender: Sender<HintedOffWriteReq>,
    ) -> Self {
        Self {
            node_id,
            kv_inst,
            timeout_ms,
            meta_manager,
            hh_sender,
        }
    }

    pub async fn write_points(
        &self,
        req: &WriteRequest,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let meta_client = self.meta_manager.tenant_meta(&req.tenant).await.ok_or(
            CoordinatorError::TenantNotFound {
                name: req.tenant.clone(),
            },
        )?;

        let mut mapping = VnodeMapping::new();
        {
            let _span_recorder = SpanRecorder::new(span_ctx.child_span("map point"));
            let fb_points = flatbuffers::root::<fb_models::Points>(&req.request.points)
                .context(InvalidFlatbufferSnafu)?;
            let database_name = get_db_from_fb_points(&fb_points)?;
            for table in fb_points.tables().ok_or(CoordinatorError::Points {
                msg: "point missing tables".to_string(),
            })? {
                let table_name = fb_table_name(&table)?;

                let schema = table.schema().ok_or(CoordinatorError::Points {
                    msg: "points missing table schema".to_string(),
                })?;

                for item in table.points().ok_or(CoordinatorError::Points {
                    msg: "table missing table points".to_string(),
                })? {
                    mapping
                        .map_point(
                            meta_client.clone(),
                            &database_name,
                            &table_name,
                            req.precision,
                            schema,
                            item,
                        )
                        .await?;
                }
            }
        }

        let now = tokio::time::Instant::now();
        let mut requests = vec![];
        {
            let _span_recorder = SpanRecorder::new(span_ctx.child_span("build requests"));
            for (_id, points) in mapping.points.iter_mut() {
                points.finish()?;
                if points.repl_set.vnodes.is_empty() {
                    return Err(CoordinatorError::CommonError {
                        msg: "no available vnode in replication set".to_string(),
                    });
                }
                for vnode in points.repl_set.vnodes.iter() {
                    debug!("write points on vnode {:?},  now: {:?}", vnode, now);
                    if vnode.status == VnodeStatus::Copying {
                        return Err(CoordinatorError::CommonError {
                            msg: "vnode is moving write forbidden ".to_string(),
                        });
                    }
                    let request = self.write_to_node(
                        vnode.id,
                        &req.tenant,
                        vnode.node_id,
                        req.precision,
                        points.data.clone(),
                        SpanRecorder::new(span_ctx.child_span(format!(
                            "write to vnode {} on node {}",
                            vnode.id, vnode.node_id
                        ))),
                    );
                    requests.push(request);
                }
            }
        }

        let res = futures::future::try_join_all(requests).await.map(|_| ());

        debug!(
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
        precision: Precision,
        data: Vec<u8>,
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
        if let Err(err @ CoordinatorError::FailoverNode { id: _ }) = result {
            debug!(
                "write data to remote {}({}) failed; write to hinted handoff!",
                node_id, vnode_id
            );

            span_recorder.error(err.to_string());

            return self
                .write_to_handoff(vnode_id, node_id, tenant, precision, data)
                .await;
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
        data: Vec<u8>,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let channel = self
            .meta_manager
            .get_node_conn(node_id)
            .await
            .map_err(|_| CoordinatorError::FailoverNode { id: node_id })?;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(self.timeout_ms));
        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);

        let mut cmd = tonic::Request::new(WriteVnodeRequest {
            vnode_id,
            precision: precision as u32,
            tenant: tenant.to_string(),
            data,
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
            .map_err(|_| CoordinatorError::FailoverNode { id: node_id })?
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
            let _ = kv_inst.write(span_ctx, vnode_id, precision, req).await?;
            Ok(())
        } else {
            Err(CoordinatorError::KvInstanceNotFound { node_id: 0 })
        }
    }
}
