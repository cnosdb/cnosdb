use std::cmp;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use blake3::Hasher;
use datafusion::arrow::array::{StringBuilder, TimestampNanosecondArray, UInt32Array, UInt64Array};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema, SchemaRef,
};
use datafusion::arrow::record_batch::RecordBatch;
use models::predicate::domain::TimeRange;
use models::{utils as model_utils, ColumnId, FieldId, SeriesId, Timestamp};
use tokio::sync::RwLock;

use super::compact::{CompactIterator, CompactingBlockMeta};
use crate::error::{Error, Result};
use crate::tseries_family::TseriesFamily;
use crate::tsm::{DataBlock, TsmReader};
use crate::TseriesFamilyId;

/// Duration of each TimeRangeHashTreeNode, 24 hour.
const DEFAULT_DURATION: i64 = 24 * 60 * 60 * 1_000_000_000;

pub type Hash = [u8; 32];

pub fn hash_to_string(hash: Hash) -> String {
    let mut s = String::with_capacity(32);
    for v in hash {
        s.push_str(format!("{:x}", v).as_str());
    }
    s
}

#[derive(Default, Debug)]
pub struct VnodeHashTreeNode {
    pub vnode_id: TseriesFamilyId,
    pub fields: Vec<FieldHashTreeNode>,
    min_ts: Timestamp,
    max_ts: Timestamp,
}

impl VnodeHashTreeNode {
    pub fn with_capacity(vnode_id: TseriesFamilyId, capacity: usize) -> Self {
        Self {
            vnode_id,
            fields: Vec::with_capacity(capacity),
            min_ts: Timestamp::MAX,
            max_ts: Timestamp::MIN,
        }
    }

    pub fn push(&mut self, value: FieldHashTreeNode) {
        self.min_ts = self.min_ts.min(value.min_ts);
        self.max_ts = self.max_ts.max(value.max_ts);
        self.fields.push(value);
    }

    /// Calculate checksum with checksums of all field hash trees.
    pub fn checksum(&self) -> Hash {
        let mut hasher = Hasher::new();
        for f in self.fields.iter() {
            hasher.update(&f.checksum());
        }
        hasher.finalize().into()
    }

    /// Calculate checksum with calculated hash values of all time range hash trees.
    ///
    /// TODO(zipper): This method has double memory cost.
    pub fn into_checksum(self) -> Hash {
        let mut hashes = Vec::with_capacity(self.len());
        for f in self.fields {
            for t in f.time_ranges {
                hashes.push(t.hash);
            }
        }
        hashes.sort();

        let mut hasher = Hasher::new();
        for h in hashes {
            hasher.update(&h);
        }
        hasher.finalize().into()
    }

    pub fn len(&self) -> usize {
        self.fields.iter().map(|f| f.len()).sum()
    }
}

impl std::fmt::Display for VnodeHashTreeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ \"vnode_id\": {}, \"fields\": [ ", self.vnode_id)?;
        let last_field_i = self.fields.len() - 1;
        for (i, node) in self.fields.iter().enumerate() {
            write!(f, "{node}")?;
            if i < last_field_i {
                write!(f, ", ")?;
            }
        }
        write!(f, "] }}")
    }
}

#[derive(Default, Debug)]
pub struct FieldHashTreeNode {
    pub field_id: FieldId,
    pub time_ranges: Vec<TimeRangeHashTreeNode>,
    min_ts: Timestamp,
    max_ts: Timestamp,
}

impl FieldHashTreeNode {
    pub fn with_capacity(field_id: FieldId, capacity: usize) -> Self {
        Self {
            field_id,
            time_ranges: Vec::with_capacity(capacity),
            min_ts: Timestamp::MAX,
            max_ts: Timestamp::MIN,
        }
    }

    pub fn push(&mut self, value: TimeRangeHashTreeNode) {
        self.min_ts = self.min_ts.min(value.min_ts);
        self.max_ts = self.max_ts.max(value.max_ts);
        self.time_ranges.push(value);
    }

    pub fn checksum(&self) -> Hash {
        let mut hasher = Hasher::new();
        for v in self.time_ranges.iter() {
            hasher.update(&v.hash);
        }
        hasher.finalize().into()
    }

    pub fn column_series(&self) -> (ColumnId, SeriesId) {
        model_utils::split_id(self.field_id)
    }

    pub fn len(&self) -> usize {
        self.time_ranges.len()
    }
}

impl std::fmt::Display for FieldHashTreeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (cid, sid) = self.column_series();
        write!(
            f,
            "{{ \"series_id\": {sid}, \"column_id\": {cid}, \"values\": [ "
        )?;
        let last_tr_i = self.time_ranges.len() - 1;
        for (i, node) in self.time_ranges.iter().enumerate() {
            write!(f, "{node}")?;
            if i < last_tr_i {
                write!(f, ", ")?;
            }
        }
        write!(f, "] }}")
    }
}

#[derive(Default, Debug)]
pub struct TimeRangeHashTreeNode {
    pub min_ts: Timestamp,
    pub max_ts: Timestamp,
    pub hash: Hash,
}

impl TimeRangeHashTreeNode {
    pub fn new(time_range: TimeRange, hash: Hash) -> Self {
        Self {
            min_ts: time_range.min_ts,
            max_ts: time_range.max_ts,
            hash,
        }
    }

    pub fn checksum(&self) -> Hash {
        self.hash
    }
}

impl std::fmt::Display for TimeRangeHashTreeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ \"time_range\": [{}, {}], \"hash\": \"",
            self.min_ts, self.max_ts
        )?;
        for v in self.hash {
            f.write_fmt(format_args!("{:x}", v))?;
        }
        write!(f, "\" }}")
    }
}

pub fn vnode_table_checksum_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ArrowField::new("vnode_id", ArrowDataType::UInt32, false),
        ArrowField::new("checksum", ArrowDataType::Utf8, false),
    ]))
}

pub fn vnode_field_time_range_table_checksum_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ArrowField::new("vnode_id", ArrowDataType::UInt32, false),
        ArrowField::new("field_Id", ArrowDataType::UInt32, false),
        ArrowField::new("min_ts", ArrowDataType::UInt32, false),
        ArrowField::new("max_ts", ArrowDataType::UInt32, false),
        ArrowField::new("checksum", ArrowDataType::Utf8, false),
    ]))
}

/// Get checksum of all data of a vnode, returns RecordBatch with columns of vnode_id and it's checksum, for example:
///
/// | vnode_id | checksum |
/// | -------- | -------- |
/// | 1        | a1a2a3a4 |
pub(crate) async fn vnode_checksum(vnode: Arc<RwLock<TseriesFamily>>) -> Result<RecordBatch> {
    let vnode_id = vnode.read().await.tf_id();
    let root_node = vnode_hash_tree(vnode).await?;

    let capacity = root_node.len();
    let mut vnode_id_array = UInt32Array::builder(capacity);
    let mut check_sum_array = StringBuilder::with_capacity(capacity, 32 * capacity);
    vnode_id_array.append_value(vnode_id);
    check_sum_array.append_value(hash_to_string(root_node.into_checksum()));
    RecordBatch::try_new(
        vnode_table_checksum_schema(),
        vec![
            Arc::new(vnode_id_array.finish()),
            Arc::new(check_sum_array.finish()),
        ],
    )
    .map_err(|err| Error::CommonError {
        reason: format!("get checksum fail, {}", err),
    })
}

/// Get checksum of all data of a vnode, returns RecordBatch with more columns, for example:
///
/// | vnode_id | field_id | min_time | max_time | checksum |
/// | -------- | -------- | -------- | -------- | -------- |
/// | 1        | 1        | 10000100 | 10000200 | a1a2a3a4 |
pub(crate) async fn vnode_field_time_range_checksum(
    vnode: Arc<RwLock<TseriesFamily>>,
) -> Result<RecordBatch> {
    let vnode_id = vnode.read().await.tf_id();
    let root_node = vnode_hash_tree(vnode).await?;

    let capacity = root_node.len();
    let mut vnode_id_array = UInt32Array::builder(capacity);
    let mut field_id_array = UInt64Array::builder(capacity);
    let mut min_time_array = TimestampNanosecondArray::builder(capacity);
    let mut max_time_array = TimestampNanosecondArray::builder(capacity);
    let mut check_sum_array = StringBuilder::with_capacity(capacity, 32 * capacity);

    for field in root_node.fields {
        for time_range in field.time_ranges {
            vnode_id_array.append_value(vnode_id);
            field_id_array.append_value(field.field_id);
            min_time_array.append_value(time_range.min_ts);
            max_time_array.append_value(time_range.max_ts);
            check_sum_array.append_value(hash_to_string(time_range.checksum()));
        }
    }

    RecordBatch::try_new(
        vnode_field_time_range_table_checksum_schema(),
        vec![
            Arc::new(vnode_id_array.finish()),
            Arc::new(field_id_array.finish()),
            Arc::new(min_time_array.finish()),
            Arc::new(max_time_array.finish()),
            Arc::new(check_sum_array.finish()),
        ],
    )
    .map_err(|err| Error::CommonError {
        reason: format!("get checksum fail, {}", err),
    })
}

pub(crate) async fn vnode_hash_tree(
    vnode: Arc<RwLock<TseriesFamily>>,
) -> Result<VnodeHashTreeNode> {
    const MAX_DATA_BLOCK_SIZE: u32 = 1000;

    let (version, vnode_id) = {
        let vnode_rlock = vnode.read().await;
        (vnode_rlock.version(), vnode_rlock.tf_id())
    };
    let mut readers: Vec<Arc<TsmReader>> = Vec::new();
    let tsm_paths: Vec<&PathBuf> = version
        .levels_info()
        .iter()
        .flat_map(|l| l.files.iter().map(|f| f.file_path()))
        .collect();
    for p in tsm_paths {
        let r = version.get_tsm_reader(p).await?;
        readers.push(r);
    }

    // Build a compact iterator, read data, split by time range and then calculate hash.
    let iter = CompactIterator::new(readers, MAX_DATA_BLOCK_SIZE as usize, true);
    let mut fid_tr_hash_val_map: HashMap<FieldId, Vec<(TimeRange, Hash)>> =
        read_from_compact_iterator(iter, DEFAULT_DURATION).await?;

    let mut field_ids: Vec<FieldId> = fid_tr_hash_val_map.keys().cloned().collect();
    field_ids.sort();
    let mut vnode_hash_tree_node = VnodeHashTreeNode::with_capacity(vnode_id, field_ids.len());
    for fid in field_ids {
        let tr_hashes = fid_tr_hash_val_map.remove(&fid).unwrap();
        let mut filed_hash_tree_node = FieldHashTreeNode::with_capacity(fid, tr_hashes.len());
        for (tr, hash) in tr_hashes {
            filed_hash_tree_node.push(TimeRangeHashTreeNode::new(tr, hash));
        }
        vnode_hash_tree_node.push(filed_hash_tree_node);
    }
    trace::trace!("VnodeHashTree({vnode_id}): {}", vnode_hash_tree_node);

    Ok(vnode_hash_tree_node)
}

/// Returns a time range calculated by the given `ts_nanoseconds`
/// and `duration_nanoseconds`.
///
/// **NOTE**: `duration_nanoseconds` must not greater than `ts_nanoseconds`.
fn calc_time_range(
    ts_nanoseconds: Timestamp,
    duration_nanoseconds: i64,
) -> Result<(Timestamp, Timestamp)> {
    if duration_nanoseconds < 0 {
        return Err(Error::Transform {
            reason: format!("duration({duration_nanoseconds}) < 0 while doing duration_trunc",),
        });
    }
    if duration_nanoseconds > ts_nanoseconds.abs() {
        return Err(Error::Transform {
            reason: format!(
                "duration({duration_nanoseconds}) > timestamp({ts_nanoseconds}) while doing duration_trunc",
            ),
        });
    }

    let delta_down = ts_nanoseconds % duration_nanoseconds;
    // Copy from: chrono::round.rs duration_trunc()
    let min_ts = match delta_down.cmp(&0) {
        cmp::Ordering::Equal => ts_nanoseconds,
        cmp::Ordering::Greater => ts_nanoseconds - delta_down,
        cmp::Ordering::Less => ts_nanoseconds - (duration_nanoseconds - delta_down.abs()),
    };
    Ok((min_ts, min_ts + duration_nanoseconds))
}

/// Find the index of the given value in a list using binary search,
/// then return the index and the next value of the index.
///
/// If the value is i64::MIN, then return (list.len(), None).
fn find_value_index_and_next(list: &[Timestamp], value: Timestamp) -> (usize, Option<Timestamp>) {
    if value != Timestamp::MIN {
        match list.binary_search(&value) {
            Ok(i) => {
                if i + 1 == list.len() {
                    (i, None)
                } else {
                    (i, Some(list[i + 1]))
                }
            }
            Err(i) => {
                if i == list.len() {
                    (i, None)
                } else {
                    (i, Some(list[i]))
                }
            }
        }
    } else {
        (list.len(), None)
    }
}

/// Calculate a hash from a range of a data block,
/// return the index of the range end, and the next value of the index.
fn hash_partial_datablock(
    hasher: &mut Hasher,
    data_block: &DataBlock,
    min_idx: usize,
    max_timestamp: Timestamp,
) -> Option<(usize, Option<Timestamp>)> {
    match data_block {
        DataBlock::U64 { ts, val, .. } => {
            let (max_ts_i, after_max_ts) = find_value_index_and_next(&ts[min_idx..], max_timestamp);
            if max_ts_i == 0 {
                return None;
            }
            let limit = min_idx + max_ts_i;
            let ts_iter = ts[min_idx..limit].iter().map(|v| v.to_be_bytes());
            let val_iter = val[min_idx..limit].iter().map(|v| v.to_be_bytes());
            for (t, v) in ts_iter.zip(val_iter) {
                hasher.update(&t);
                hasher.update(&v);
            }
            Some((limit, after_max_ts))
        }
        DataBlock::I64 { ts, val, .. } => {
            let (max_ts_i, after_max_ts) = find_value_index_and_next(&ts[min_idx..], max_timestamp);
            if max_ts_i == 0 {
                return None;
            }
            let limit = min_idx + max_ts_i;
            let ts_iter = ts[min_idx..limit].iter().map(|v| v.to_be_bytes());
            let val_iter = val[min_idx..limit].iter().map(|v| v.to_be_bytes());
            for (t, v) in ts_iter.zip(val_iter) {
                hasher.update(&t);
                hasher.update(&v);
            }
            Some((limit, after_max_ts))
        }
        DataBlock::F64 { ts, val, .. } => {
            let (max_ts_i, after_max_ts) = find_value_index_and_next(&ts[min_idx..], max_timestamp);
            if max_ts_i == 0 {
                return None;
            }
            let limit = min_idx + max_ts_i;
            let ts_iter = ts[min_idx..limit].iter().map(|v| v.to_be_bytes());
            let val_iter = val[min_idx..limit].iter().map(|v| v.to_be_bytes());
            for (t, v) in ts_iter.zip(val_iter) {
                hasher.update(&t);
                hasher.update(&v);
            }
            Some((limit, after_max_ts))
        }
        DataBlock::Str { ts, val, .. } => {
            let (max_ts_i, after_max_ts) = find_value_index_and_next(&ts[min_idx..], max_timestamp);
            if max_ts_i == 0 {
                return None;
            }
            let limit = min_idx + max_ts_i;
            let ts_iter = ts[min_idx..limit].iter().map(|v| v.to_be_bytes());
            let val_iter = val[min_idx..limit].iter();
            for (t, v) in ts_iter.zip(val_iter) {
                hasher.update(&t);
                hasher.update(v);
            }
            Some((limit, after_max_ts))
        }
        DataBlock::Bool { ts, val, .. } => {
            let (max_ts_i, after_max_ts) = find_value_index_and_next(&ts[min_idx..], max_timestamp);
            if max_ts_i == 0 {
                return None;
            }
            let limit = min_idx + max_ts_i;
            let ts_iter = ts[min_idx..limit].iter().map(|v| v.to_be_bytes());
            let val_iter = val[min_idx..limit].iter();
            for (t, v) in ts_iter.zip(val_iter) {
                hasher.update(&t);
                hasher.update(if *v { &[1_u8] } else { &[0_u8] });
            }
            Some((limit, after_max_ts))
        }
    }
}

fn hash_data_block(
    fid_tr_hash_val_map: &mut HashMap<FieldId, Vec<(TimeRange, Hash)>>,
    time_range_nanosec: i64,
    field_id: FieldId,
    data_block: &DataBlock,
) -> Result<()> {
    if let Some(blk_time_range) = data_block.time_range() {
        // Get truncated time range by DataBlock.time[0]
        // TODO: Support data block to be split into multi time ranges.
        let (mut min_ts, mut max_ts) = calc_time_range(blk_time_range.0, time_range_nanosec)?;

        // Calculate and store the hash value of data in time range
        let hash_vec = fid_tr_hash_val_map.entry(field_id).or_default();
        let mut min_idx = 0;
        let mut ts_after_max_ts = None;
        let mut hasher = Hasher::new();
        while blk_time_range.1 > min_ts {
            if let Some((min_i, max_ts_next)) =
                hash_partial_datablock(&mut hasher, data_block, min_idx, max_ts)
            {
                min_idx = min_i;
                ts_after_max_ts = max_ts_next;
                hash_vec.push((TimeRange::new(min_ts, max_ts), hasher.finalize().into()));
                hasher.reset();
            }
            if let Some(ts) = ts_after_max_ts {
                max_ts += time_range_nanosec;
                if max_ts < ts {
                    // Sometimes the next time_range is long after.
                    (min_ts, max_ts) = calc_time_range(ts, time_range_nanosec)?;
                }
            } else {
                break;
            }
        }
    } else {
        // Ignore: Because argument decode_non_overlap_blocks in CompactIterator::new()
        // is set to true, we may ignore this case.
    }

    Ok(())
}

async fn read_from_compact_iterator(
    mut iter: CompactIterator,
    time_range_nanosec: i64,
) -> Result<HashMap<FieldId, Vec<(TimeRange, Hash)>>> {
    let mut compacting_block_metas: Vec<CompactingBlockMeta> = Vec::new();
    let mut fid = iter.curr_fid();
    let mut fid_tr_hash_val_map: HashMap<FieldId, Vec<(TimeRange, Hash)>> = HashMap::new();
    while let Some(blk_meta_group) = iter.next().await {
        if fid.is_some() && fid != iter.curr_fid() {
            if let Some(data_block) = load_and_merge_data_block(&mut compacting_block_metas).await?
            {
                hash_data_block(
                    &mut fid_tr_hash_val_map,
                    time_range_nanosec,
                    fid.unwrap(),
                    &data_block,
                )?;
            }
            compacting_block_metas.clear();
        }
        fid = iter.curr_fid();
        compacting_block_metas.append(&mut blk_meta_group.into_compacting_block_metas());
    }
    if let Some(field_id) = fid {
        if let Some(data_block) = load_and_merge_data_block(&mut compacting_block_metas).await? {
            hash_data_block(
                &mut fid_tr_hash_val_map,
                time_range_nanosec,
                field_id,
                &data_block,
            )?;
        }
    }

    Ok(fid_tr_hash_val_map)
}

async fn load_and_merge_data_block(
    compacting_block_metas: &mut [CompactingBlockMeta],
) -> Result<Option<DataBlock>> {
    if compacting_block_metas.is_empty() {
        return Ok(None);
    }
    let head = &mut compacting_block_metas[0];
    let mut head_block = head.get_data_block().await?;
    if compacting_block_metas.len() > 1 {
        for blk_meta in compacting_block_metas[1..].iter_mut() {
            let blk_block = blk_meta.get_data_block().await?;
            head_block = head_block.merge(blk_block);
        }
    }
    Ok(Some(head_block))
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use blake3::Hasher;
    use chrono::{Duration, NaiveDateTime};
    use config::Config;
    use datafusion::arrow::datatypes::TimeUnit;
    use memory_pool::GreedyMemoryPool;
    use meta::model::meta_admin::AdminMeta;
    use meta::model::{MetaClientRef, MetaRef};
    use metrics::metric_register::MetricsRegister;
    use minivec::MiniVec;
    use models::predicate::domain::TimeRange;
    use models::schema::{
        ColumnType, DatabaseOptions, DatabaseSchema, Precision, TableColumn, TableSchema,
        TenantOptions, TskvTableSchema,
    };
    use models::{FieldId, Timestamp, ValueType};
    use protos::kv_service::{Meta, WritePointsRequest};
    use protos::models::{self as fb_models, FieldType};
    use protos::models_helper;
    use protos::models_helper::create_points;
    use tokio::runtime::{self, Runtime};
    use tokio::sync::RwLock;

    use super::{
        calc_time_range, find_value_index_and_next, hash_data_block, hash_partial_datablock, Hash,
    };
    use crate::compaction::check::{vnode_hash_tree, TimeRangeHashTreeNode, DEFAULT_DURATION};
    use crate::compaction::flush;
    use crate::tseries_family::TseriesFamily;
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::DataBlock;
    use crate::{Engine, Options, TsKv, TseriesFamilyId};

    fn parse_nanos(datetime: &str) -> Timestamp {
        NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .timestamp_nanos()
    }

    #[test]
    fn test_calc_time_range() {
        /// Returns timestamp in nanoseconds of a date-time, and duration in nanoseconds of 30 minutes.
        fn get_args(datetime: &str) -> (Timestamp, i64) {
            let datetime = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").unwrap();
            let timestamp = datetime.timestamp_nanos();
            let duration = Duration::minutes(30);
            let duration_nanos = duration.num_nanoseconds().unwrap();
            (timestamp, duration_nanos)
        }

        // 2023-01-01 00:29:01 is in between 2023-01-01 00:00:00 and 2023-01-01 00:30:00
        let (stamp, span) = get_args("2023-01-01 00:29:01");
        assert_eq!(
            (
                parse_nanos("2023-01-01 00:00:00"),
                parse_nanos("2023-01-01 00:30:00"),
            ),
            calc_time_range(stamp, span).unwrap(),
        );
        // 2023-01-01 00:30:00 is in between 2023-01-01 00:30:00 and 2023-01-01 01:00:00
        let (stamp, span) = get_args("2023-01-01 00:30:00");
        assert_eq!(
            (
                parse_nanos("2023-01-01 00:30:00"),
                parse_nanos("2023-01-01 01:00:00"),
            ),
            calc_time_range(stamp, span).unwrap(),
        );
        // 2023-01-01 00:30:01 is between 2023-01-01 00:30:00 and 2023-01-01 01:00:00
        let (stamp, span) = get_args("2023-01-01 00:30:01");
        assert_eq!(
            (
                parse_nanos("2023-01-01 00:30:00"),
                parse_nanos("2023-01-01 01:00:00")
            ),
            calc_time_range(stamp, span).unwrap(),
        );
    }

    #[test]
    fn test_find_value_index_and_next() {
        let timestamps = vec![
            parse_nanos("2023-01-01 00:01:00"),
            parse_nanos("2023-01-01 00:02:00"),
            parse_nanos("2023-01-01 00:03:00"),
            parse_nanos("2023-01-01 00:04:00"),
            parse_nanos("2023-01-01 00:05:00"),
        ];

        assert_eq!(
            (0, Some(parse_nanos("2023-01-01 00:01:00"))),
            find_value_index_and_next(&timestamps, parse_nanos("2023-01-01 00:00:00")),
        );
        assert_eq!(
            (2, Some(parse_nanos("2023-01-01 00:04:00"))),
            find_value_index_and_next(&timestamps, parse_nanos("2023-01-01 00:03:00"))
        );
        assert_eq!(
            (3, Some(parse_nanos("2023-01-01 00:04:00"))),
            find_value_index_and_next(&timestamps, parse_nanos("2023-01-01 00:03:30"))
        );
        assert_eq!(
            (3, Some(parse_nanos("2023-01-01 00:05:00"))),
            find_value_index_and_next(&timestamps, parse_nanos("2023-01-01 00:04:00"))
        );
        assert_eq!(
            (5, None),
            find_value_index_and_next(&timestamps, parse_nanos("2023-01-01 00:30:00"))
        );
        assert_eq!(
            (5, None),
            find_value_index_and_next(&timestamps, Timestamp::MIN),
        );
    }

    #[test]
    fn test_hash_partial_datablock() {
        fn data_block_partial_to_bytes(data_block: &DataBlock, from: usize, to: usize) -> Vec<u8> {
            let mut ret: Vec<u8> = vec![];
            for i in from..to {
                let v = data_block
                    .get(i)
                    .unwrap_or_else(|| panic!("data block has at least {} items", i));
                ret.extend_from_slice(&v.to_bytes());
            }
            ret
        }

        let timestamps = vec![
            parse_nanos("2023-01-01 00:01:00"),
            parse_nanos("2023-01-01 00:02:00"),
            parse_nanos("2023-01-01 00:03:00"),
            parse_nanos("2023-01-01 00:04:00"),
            parse_nanos("2023-01-01 00:05:00"),
            parse_nanos("2023-01-01 00:06:00"),
        ];
        #[rustfmt::skip]
        let data_blocks = vec![
            DataBlock::U64 { ts: timestamps.clone(), val: vec![1, 2, 3, 4, 5, 6], enc: DataBlockEncoding::default() },
            DataBlock::I64 { ts: timestamps.clone(), val: vec![1, 2, 3, 4, 5, 6], enc: DataBlockEncoding::default() },
            DataBlock::Str {
                ts: timestamps.clone(),
                val: vec![
                    MiniVec::from("1"), MiniVec::from("2"), MiniVec::from("3"), MiniVec::from("4"), MiniVec::from("5"), MiniVec::from("6"),
                ],
                enc: DataBlockEncoding::default(),
            },
            DataBlock::F64 { ts: timestamps.clone(), val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], enc: DataBlockEncoding::default() },
            DataBlock::Bool { ts: timestamps, val: vec![true, false, true, false, true, false], enc: DataBlockEncoding::default() },
        ];

        for data_block in data_blocks {
            let mut hasher_blk = Hasher::new();
            let max_ts_i_and_next_ts = hash_partial_datablock(
                &mut hasher_blk,
                &data_block,
                0,
                parse_nanos("2023-01-01 00:04:00"),
            );
            assert_eq!(
                Some((3, Some(parse_nanos("2023-01-01 00:05:00")))),
                max_ts_i_and_next_ts
            );
            let mut hasher_expected = Hasher::new();
            assert_eq!(
                hasher_expected
                    .update(data_block_partial_to_bytes(&data_block, 0, 3).as_slice())
                    .finalize(),
                hasher_blk.finalize(),
                "checking {}",
                data_block
            );

            let mut hasher_blk = Hasher::new();
            let min_idx = hash_partial_datablock(
                &mut hasher_blk,
                &data_block,
                max_ts_i_and_next_ts.unwrap().0,
                Timestamp::MIN,
            );
            assert_eq!(Some((6, None)), min_idx);
            let mut hasher_expected = Hasher::new();
            assert_eq!(
                hasher_expected
                    .update(data_block_partial_to_bytes(&data_block, 3, 6).as_slice())
                    .finalize(),
                hasher_blk.finalize()
            );
        }
    }

    const U64_COL_NAME: &str = "col_u64";
    const I64_COL_NAME: &str = "col_i64";
    const F64_COL_NAME: &str = "col_f64";
    const STR_COL_NAME: &str = "col_str";
    const BOOL_COL_NAME: &str = "col_bool";
    const TIME_COL_NAME: &str = "time";

    fn data_blocks_to_write_batch_args(
        data_blocks: &[DataBlock],
        _timestamps: &mut [Timestamp],
        fields: &mut HashMap<&str, Vec<Vec<u8>>>,
        time: &mut Vec<i64>,
    ) {
        let mut u64_vec = vec![];
        let mut i64_vec = vec![];
        let mut f64_vec = vec![];
        let mut str_vec = vec![];
        let mut bool_vec = vec![];
        for data_block in data_blocks {
            match data_block {
                DataBlock::U64 { ts, val, .. } => {
                    for (t, v) in ts.iter().zip(val) {
                        u64_vec.push((*t, v.to_be_bytes().to_vec()));
                    }
                }
                DataBlock::I64 { ts, val, .. } => {
                    for (t, v) in ts.iter().zip(val) {
                        i64_vec.push((*t, v.to_be_bytes().to_vec()));
                    }
                }
                DataBlock::F64 { ts, val, .. } => {
                    for (t, v) in ts.iter().zip(val) {
                        f64_vec.push((*t, v.to_be_bytes().to_vec()));
                    }
                }
                DataBlock::Str { ts, val, .. } => {
                    for (t, v) in ts.iter().zip(val) {
                        str_vec.push((*t, v.to_vec()));
                    }
                }
                DataBlock::Bool { ts, val, .. } => {
                    for (t, v) in ts.iter().zip(val) {
                        bool_vec.push((*t, if *v { vec![1_u8] } else { vec![0_u8] }));
                    }
                }
            }
        }

        u64_vec.sort_by_key(|v| v.0);
        i64_vec.sort_by_key(|v| v.0);
        f64_vec.sort_by_key(|v| v.0);
        str_vec.sort_by_key(|v| v.0);
        bool_vec.sort_by_key(|v| v.0);

        #[allow(clippy::type_complexity)]
        fn write_vec_into_map(
            vec: Vec<(i64, Vec<u8>)>,
            col_name: &'static str,
            map: &mut BTreeMap<Timestamp, Vec<(&str, Vec<u8>)>>,
        ) {
            vec.into_iter().for_each(|(t, v)| {
                let entry = map.entry(t).or_default();
                entry.push((col_name, v));
            });
        }

        let entry = fields.entry(U64_COL_NAME).or_default();
        u64_vec.into_iter().for_each(|(t, v)| {
            time.push(t);
            entry.push(v);
        });
        let entry = fields.entry(I64_COL_NAME).or_default();
        i64_vec.into_iter().for_each(|(_, v)| {
            entry.push(v);
        });
        let entry = fields.entry(F64_COL_NAME).or_default();
        f64_vec.into_iter().for_each(|(_, v)| {
            entry.push(v);
        });
        let entry = fields.entry(STR_COL_NAME).or_default();
        str_vec.into_iter().for_each(|(_, v)| {
            entry.push(v);
        });
        let entry = fields.entry(BOOL_COL_NAME).or_default();
        bool_vec.into_iter().for_each(|(_, v)| {
            entry.push(v);
        });
    }

    async fn do_write_batch(
        engine: &TsKv,
        vnode_id: TseriesFamilyId,
        _timestamps: Vec<i64>,
        tenant: &str,
        database: &str,
        table: &str,
        columns: HashMap<&str, Vec<Vec<u8>>>,
        time: Vec<i64>,
    ) {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();

        let mut tags = HashMap::new();
        tags.insert("ta", vec![]);
        tags.insert("tb", vec![]);
        let len = time.len();
        for _ in 0..len {
            tags.get_mut("ta").unwrap().push("a1".to_string());
            tags.get_mut("tb").unwrap().push("b1".to_string());
        }
        let fields_type = HashMap::from([
            (U64_COL_NAME, FieldType::Unsigned),
            (I64_COL_NAME, FieldType::Integer),
            (F64_COL_NAME, FieldType::Float),
            (STR_COL_NAME, FieldType::String),
            (BOOL_COL_NAME, FieldType::Boolean),
        ]);

        let points = create_points(
            &mut fbb,
            database,
            table,
            tags.iter()
                .map(|(k, v)| (*k, v.iter().map(|v| v.as_str()).collect()))
                .collect(),
            columns
                .iter()
                .map(|(k, v)| (*k, v.iter().map(|v| v.as_slice()).collect()))
                .collect(),
            fields_type,
            &time,
            time.len(),
        );
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let write_batch = WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: tenant.to_string(),
                user: None,
                password: None,
            }),
            points,
        };
        let points = flatbuffers::root::<fb_models::Points>(&write_batch.points).unwrap();
        models_helper::print_points(points);
        let _ = engine
            .write(None, vnode_id, Precision::NS, write_batch)
            .await;
    }

    fn data_block_to_hash_tree(
        data_block: &DataBlock,
        duration_nanosecs: i64,
    ) -> Vec<(TimeRange, Hash)> {
        if !data_block.is_empty() {
            const FIELD_ID: FieldId = 0;

            let mut fid_tr_hash_val_map: HashMap<FieldId, Vec<(TimeRange, Hash)>> = HashMap::new();
            hash_data_block(
                &mut fid_tr_hash_val_map,
                duration_nanosecs,
                FIELD_ID,
                data_block,
            )
            .unwrap();
            fid_tr_hash_val_map.remove(&FIELD_ID).unwrap()
        } else {
            Vec::new()
        }
    }

    fn check_hash_tree_node(
        col_values: &[TimeRangeHashTreeNode],
        cmp_values: &[(TimeRange, Hash)],
        column_name: &str,
    ) {
        assert_eq!(col_values.len(), cmp_values.len());
        for (a, b) in col_values.iter().zip(cmp_values.iter()) {
            assert_eq!(a.min_ts, b.0.min_ts, "col '{}' min_ts compare", column_name);
            assert_eq!(a.max_ts, b.0.max_ts, "col '{}' max_ts compare", column_name);
            assert_eq!(a.hash, b.1, "col '{}' hash compare", column_name);
        }
    }

    async fn init_meta(config: &Config, tenant: &str) -> (MetaRef, MetaClientRef) {
        let meta = AdminMeta::new(config.clone()).await;

        meta.add_data_node().await.unwrap();
        // Create tenant may get 'TenantAlreadyExists.
        let _ = meta
            .create_tenant(tenant.to_string(), TenantOptions::default())
            .await;
        let meta_client = meta.tenant_meta(tenant).await.unwrap();

        (meta, meta_client)
    }

    #[allow(clippy::too_many_arguments)]
    async fn init_tskv(
        options: &Options,
        runtime: Arc<Runtime>,
        meta_manager: MetaRef,
        meta_client: MetaClientRef,
        vnode_id: TseriesFamilyId,
        tenant: &str,
        database: &str,
        table: &str,
        columns: Vec<TableColumn>,
    ) -> TsKv {
        let engine = TsKv::open(
            meta_manager,
            options.clone(),
            runtime.clone(),
            Arc::new(GreedyMemoryPool::default()),
            Arc::new(MetricsRegister::default()),
        )
        .await
        .unwrap();
        let _ = engine.drop_database(tenant, database).await;
        let _ = meta_client.drop_db(database).await;

        // Create database and vnode
        let mut database_schema = DatabaseSchema::new(tenant, database);
        database_schema
            .config
            .with_ttl(DatabaseOptions::DEFAULT_TTL);
        meta_client
            .create_db(database_schema.clone())
            .await
            .unwrap();
        meta_client
            .create_table(&TableSchema::TsKvTableSchema(
                TskvTableSchema::new(
                    tenant.to_string(),
                    database.to_string(),
                    table.to_string(),
                    columns,
                )
                .into(),
            ))
            .await
            .unwrap();
        let db = engine
            .get_db_or_else_create(tenant, database)
            .await
            .unwrap();
        let mut db = db.write().await;
        let tsf = db
            .add_tsfamily(vnode_id, None, engine.context())
            .await
            .unwrap();
        let tsf = tsf.read().await;
        assert_eq!(vnode_id, tsf.tf_id());

        engine
    }

    async fn write_tskv(
        engine: &TsKv,
        vnode: Arc<RwLock<TseriesFamily>>,
        tenant: &str,
        database: &str,
        table: &str,
        data_blocks: &[DataBlock],
    ) {
        let vnode_id = vnode.read().await.tf_id();
        // Write data to database and vnode
        let mut timestamps: Vec<Timestamp> = Vec::new();
        let _fields: Vec<Vec<(&str, Vec<u8>)>> = Vec::new();
        let mut fields: HashMap<&str, Vec<Vec<u8>>> = HashMap::new();
        let mut time = Vec::new();
        data_blocks_to_write_batch_args(data_blocks, &mut timestamps, &mut fields, &mut time);
        do_write_batch(
            engine, vnode_id, timestamps, tenant, database, table, fields, time,
        )
        .await;

        let flush_req = {
            let mut vnode = vnode.write().await;
            vnode.switch_to_immutable();
            vnode.build_flush_req(true).unwrap()
        };
        flush::run_flush_memtable_job(flush_req, engine.context(), false)
            .await
            .unwrap();

        let sec_1 = Duration::seconds(1).to_std().unwrap();
        let mut check_num = 0;
        loop {
            tokio::time::sleep(sec_1).await;
            // If flushing is finished, the newest super_version contains a level 1 file.
            let vnode = vnode.read().await;
            let super_version = vnode.super_version();
            if !super_version.version.levels_info()[1].files.is_empty() {
                break;
            }
            check_num += 1;
            if check_num >= 10 {
                println!("Checksum: warn: flushing takes more than {check_num} seconds.",);
            }
        }
    }

    #[test]
    fn test_get_vnode_hash_tree() {
        let base_dir = "/tmp/test/repair/1".to_string();
        let wal_dir = "/tmp/test/repair/1/wal".to_string();
        let log_dir = "/tmp/test/repair/1/log".to_string();
        // trace::init_default_global_tracing(&log_dir, "test.log", "debug");
        let _ = std::fs::remove_dir_all(&base_dir);
        std::fs::create_dir_all(&base_dir).unwrap();
        let tenant_name = "cnosdb".to_string();
        let database_name = "test_get_vnode_hash_tree".to_string();
        let vnode_id: TseriesFamilyId = 1;
        let table_name = "test_table".to_string();

        let timestamps = vec![
            parse_nanos("2023-01-01 00:01:00"),
            parse_nanos("2023-01-01 00:02:00"),
            parse_nanos("2023-01-01 00:03:00"),
            parse_nanos("2023-02-01 00:01:00"),
            parse_nanos("2023-02-01 00:02:00"),
            parse_nanos("2023-02-01 00:03:00"),
            parse_nanos("2023-03-01 00:01:00"),
            parse_nanos("2023-03-01 00:02:00"),
            parse_nanos("2023-03-01 00:03:00"),
        ];
        #[rustfmt::skip]
        let data_blocks = vec![
            DataBlock::U64 { ts: timestamps.clone(), val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() },
            DataBlock::I64 { ts: timestamps.clone(), val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() },
            DataBlock::F64 { ts: timestamps.clone(), val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], enc: DataBlockEncoding::default() },
            DataBlock::Str {
                ts: timestamps.clone(),
                val: vec![
                    MiniVec::from("1"), MiniVec::from("2"), MiniVec::from("3"),
                    MiniVec::from("4"), MiniVec::from("5"), MiniVec::from("6"),
                    MiniVec::from("7"), MiniVec::from("8"), MiniVec::from("9"),
                ],
                enc: DataBlockEncoding::default(),
            },
            DataBlock::Bool { ts: timestamps, val: vec![true, false, true, false, true, false, true, false, true], enc: DataBlockEncoding::default() },
        ];
        let data_block_tr_hashes = vec![
            data_block_to_hash_tree(&data_blocks[0], DEFAULT_DURATION),
            data_block_to_hash_tree(&data_blocks[1], DEFAULT_DURATION),
            data_block_to_hash_tree(&data_blocks[2], DEFAULT_DURATION),
            data_block_to_hash_tree(&data_blocks[3], DEFAULT_DURATION),
            data_block_to_hash_tree(&data_blocks[4], DEFAULT_DURATION),
        ];
        #[rustfmt::skip]
        let columns = vec![
            TableColumn::new(0, U64_COL_NAME.to_string(), ColumnType::Field(ValueType::Unsigned), Default::default()),
            TableColumn::new(1, I64_COL_NAME.to_string(), ColumnType::Field(ValueType::Integer), Default::default()),
            TableColumn::new(2, F64_COL_NAME.to_string(), ColumnType::Field(ValueType::Float), Default::default()),
            TableColumn::new(3, STR_COL_NAME.to_string(), ColumnType::Field(ValueType::String), Default::default()),
            TableColumn::new(4, BOOL_COL_NAME.to_string(), ColumnType::Field(ValueType::Boolean), Default::default()),
            TableColumn::new(5, TIME_COL_NAME.to_string(), ColumnType::Time(TimeUnit::Nanosecond), Default::default()),
        ];

        let mut config = config::get_config_for_test();
        config.storage.path = base_dir;
        config.wal.path = wal_dir;
        config.wal.sync = true;
        config.log.path = log_dir;
        let options = Options::from(&config);

        let rt = Arc::new(
            runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let (meta_manager, meta_client) = rt.block_on(init_meta(&config, &tenant_name));
        let engine = rt.block_on(init_tskv(
            &options,
            rt.clone(),
            meta_manager,
            meta_client,
            vnode_id,
            &tenant_name,
            &database_name,
            &table_name,
            columns.clone(),
        ));

        rt.block_on(async {
            // Get created database and vnode
            let database_ref = engine.get_db(&tenant_name, &database_name).await.unwrap();
            let vnode_ref = database_ref
                .read()
                .await
                .get_tsfamily(vnode_id)
                .unwrap_or_else(|| {
                    panic!("created vnode '{}' not exist", vnode_id);
                });
            write_tskv(
                &engine,
                vnode_ref.clone(),
                &tenant_name,
                &database_name,
                &table_name,
                &data_blocks,
            )
            .await;

            // Get hash values and check them.
            let tree_root = vnode_hash_tree(vnode_ref).await.unwrap();
            println!("{tree_root}");
            assert_eq!(tree_root.len(), 15);
            assert_eq!(tree_root.vnode_id, vnode_id);
            assert_eq!(tree_root.fields.len(), 5);
            for field in tree_root.fields.iter() {
                let (cid, _) = field.column_series();
                let col_tr_hashes = &data_block_tr_hashes[cid as usize];
                let col_name = &columns[cid as usize].name;
                check_hash_tree_node(&field.time_ranges, col_tr_hashes, col_name);
            }

            engine.close().await;
        });
    }
}
