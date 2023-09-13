use std::cmp;
use std::collections::HashMap;
use std::fmt::{Display, Write};
use std::path::PathBuf;
use std::sync::Arc;

use blake3::Hasher;
use datafusion::arrow::array::{StringBuilder, UInt32Array};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema, SchemaRef,
};
use datafusion::arrow::record_batch::RecordBatch;
use models::predicate::domain::TimeRange;
use models::{utils as model_utils, ColumnId, FieldId, SeriesId, Timestamp};
use tokio::sync::RwLock;

use crate::compaction::CompactIterator;
use crate::error::{Error, Result};
use crate::tseries_family::TseriesFamily;
use crate::tsm::{DataBlock, TsmReader};
use crate::TseriesFamilyId;

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

    pub fn checksum(&self) -> Hash {
        let mut hasher = Hasher::new();
        for f in self.fields.iter() {
            hasher.update(&f.checksum());
        }
        hasher.finalize().into()
    }

    pub fn len(&self) -> usize {
        self.fields.iter().map(|f| f.len()).sum()
    }
}

impl Display for VnodeHashTreeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{{ VnodeId: {}, fields: [ ", self.vnode_id))?;
        for v in self.fields.iter() {
            v.fmt(f)?;
            f.write_str(", ")?;
        }
        f.write_str("] }")?;
        Ok(())
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

impl Display for FieldHashTreeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (cid, sid) = self.column_series();
        f.write_fmt(format_args!(
            "{{ SeriesId: {}, ColumnId: {}, values: [ ",
            sid, cid
        ))?;
        for v in self.time_ranges.iter() {
            v.fmt(f)?;
            f.write_str(", ")?;
        }
        f.write_str("] }")?;
        Ok(())
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

impl Display for TimeRangeHashTreeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{{ time_range: ({}, {}), hash: ",
            self.min_ts, self.max_ts
        ))?;
        for v in self.hash {
            f.write_fmt(format_args!("{:x}", v))?;
        }
        f.write_char('}')?;
        Ok(())
    }
}

pub fn vnode_table_checksum_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ArrowField::new("VNODE_ID", ArrowDataType::UInt32, false),
        ArrowField::new("CHECK_SUM", ArrowDataType::Utf8, false),
    ]))
}

pub(crate) async fn vnode_checksum(vnode: Arc<RwLock<TseriesFamily>>) -> Result<RecordBatch> {
    let vnode_id = vnode.read().await.tf_id();
    let root_node = vnode_hash_tree(vnode).await?;

    let capacity = root_node.len();
    let mut vnode_id_array = UInt32Array::builder(capacity);
    // let mut field_id_array = UInt64Array::builder(capacity);
    // let mut min_time_array = TimestampNanosecondArray::builder(capacity);
    // let mut max_time_array = TimestampNanosecondArray::builder(capacity);
    let mut check_sum_array = StringBuilder::with_capacity(capacity, 32 * capacity);

    // for field in root_node.fields {
    //     for time_range in field.time_ranges {
    //         vnode_id_array.append_value(vnode_id);
    //         field_id_array.append_value(field.field_id);
    //         min_time_array.append_value(time_range.min_ts);
    //         max_time_array.append_value(time_range.max_ts);
    //         check_sum_array.append_value(hash_to_string(time_range.checksum()));
    //     }
    // }
    vnode_id_array.append_value(vnode_id);
    check_sum_array.append_value(hash_to_string(root_node.checksum()));

    RecordBatch::try_new(
        vnode_table_checksum_schema(),
        vec![
            Arc::new(vnode_id_array.finish()),
            // Arc::new(field_id_array.finish()),
            // Arc::new(min_time_array.finish()),
            // Arc::new(max_time_array.finish()),
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
        read_from_compact_iterator(iter, vnode_id, DEFAULT_DURATION).await?;

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

    Ok(vnode_hash_tree_node)
}

/// Returns a time range calculated by the given `blk_min_ts1`
/// and `split_time_range_nanosecs`.
///
/// **NOTE**: `split_time_range_nanosecs` must not greater than `blk_min_ts_nanosecs`.
fn calc_block_partial_time_range(
    blk_min_ts_nanosecs: Timestamp,
    split_time_range_nanosecs: i64,
) -> Result<(Timestamp, Timestamp)> {
    if split_time_range_nanosecs > blk_min_ts_nanosecs.abs() {
        return Err(Error::Transform {
            reason: format!(
                "duration({}) > timestamp({}) in duration_trunc",
                split_time_range_nanosecs, blk_min_ts_nanosecs,
            ),
        });
    }

    let delta_down = blk_min_ts_nanosecs % split_time_range_nanosecs;
    // Copy from: chrono::round.rs duration_trunc()
    let min_ts = match delta_down.cmp(&0) {
        cmp::Ordering::Equal => blk_min_ts_nanosecs,
        cmp::Ordering::Greater => blk_min_ts_nanosecs - delta_down,
        cmp::Ordering::Less => blk_min_ts_nanosecs - (split_time_range_nanosecs - delta_down.abs()),
    };
    Ok((min_ts, min_ts + split_time_range_nanosecs))
}

fn find_timestamp(timestamps: &[Timestamp], max_timestamp: Timestamp) -> usize {
    if max_timestamp != Timestamp::MIN {
        match timestamps.binary_search(&max_timestamp) {
            Ok(i) => i,
            Err(i) => i,
        }
    } else {
        timestamps.len()
    }
}

fn hash_partial_datablock(
    hasher: &mut Hasher,
    data_block: &DataBlock,
    min_idx: usize,
    max_timestamp: Timestamp,
) -> usize {
    match data_block {
        DataBlock::U64 { ts, val, .. } => {
            let limit = min_idx + find_timestamp(&ts[min_idx..], max_timestamp);
            let ts_iter = ts[min_idx..limit].iter().map(|v| v.to_be_bytes());
            let val_iter = val[min_idx..limit].iter().map(|v| v.to_be_bytes());
            for (t, v) in ts_iter.zip(val_iter) {
                hasher.update(&t);
                hasher.update(&v);
            }
            limit
        }
        DataBlock::I64 { ts, val, .. } => {
            let limit = min_idx + find_timestamp(&ts[min_idx..], max_timestamp);
            let ts_iter = ts[min_idx..limit].iter().map(|v| v.to_be_bytes());
            let val_iter = val[min_idx..limit].iter().map(|v| v.to_be_bytes());
            for (t, v) in ts_iter.zip(val_iter) {
                hasher.update(&t);
                hasher.update(&v);
            }
            limit
        }
        DataBlock::F64 { ts, val, .. } => {
            let limit = min_idx + find_timestamp(&ts[min_idx..], max_timestamp);
            let ts_iter = ts[min_idx..limit].iter().map(|v| v.to_be_bytes());
            let val_iter = val[min_idx..limit].iter().map(|v| v.to_be_bytes());
            for (t, v) in ts_iter.zip(val_iter) {
                hasher.update(&t);
                hasher.update(&v);
            }
            limit
        }
        DataBlock::Str { ts, val, .. } => {
            let limit = min_idx + find_timestamp(&ts[min_idx..], max_timestamp);
            let ts_iter = ts[min_idx..limit].iter().map(|v| v.to_be_bytes());
            let val_iter = val[min_idx..limit].iter();
            for (t, v) in ts_iter.zip(val_iter) {
                hasher.update(&t);
                hasher.update(v);
            }
            limit
        }
        DataBlock::Bool { ts, val, .. } => {
            let limit = min_idx + find_timestamp(&ts[min_idx..], max_timestamp);
            let ts_iter = ts[min_idx..limit].iter().map(|v| v.to_be_bytes());
            let val_iter = val[min_idx..limit].iter();
            for (t, v) in ts_iter.zip(val_iter) {
                hasher.update(&t);
                hasher.update(if *v { &[1_u8] } else { &[0_u8] });
            }
            limit
        }
    }
}

async fn read_from_compact_iterator(
    mut _iter: CompactIterator,
    _vnode_id: TseriesFamilyId,
    _time_range_nanosec: i64,
) -> Result<HashMap<FieldId, Vec<(TimeRange, Hash)>>> {
    // let mut fid_tr_hash_val_map: HashMap<FieldId, Vec<(TimeRange, Hash)>> = HashMap::new();
    // let mut last_hashed_tr_fid: Option<(TimeRange, FieldId)> = None;
    // let mut hasher = Hasher::new();
    // loop {
    //     match iter.next().await {
    //         None => break,
    //         Some(Ok(blk)) => {
    //             if let CompactingBlock::Decoded {
    //                 field_id,
    //                 data_block,
    //                 ..
    //             } = blk
    //             {
    //                 // Check if there is last hash value that not stored.
    //                 if let Some((time_range, last_fid)) = last_hashed_tr_fid {
    //                     if last_fid != field_id {
    //                         fid_tr_hash_val_map
    //                             .entry(last_fid)
    //                             .or_default()
    //                             .push((time_range, hasher.finalize().into()));
    //                         hasher.reset();
    //                     }
    //                 }
    //                 if let Some(blk_time_range) = data_block.time_range() {
    //                     // Get trunced time range by DataBlock.time[0]
    //                     // TODO: Support data block to be split into multi time ranges.
    //                     let (min_ts, max_ts) = match calc_block_partial_time_range(
    //                         blk_time_range.0,
    //                         time_range_nanosec,
    //                     ) {
    //                         Ok(tr) => tr,
    //                         Err(e) => return Err(e),
    //                     };

    //                     // Calculate and store the hash value of data in time range
    //                     let hash_vec = fid_tr_hash_val_map.entry(field_id).or_default();
    //                     if blk_time_range.1 > max_ts {
    //                         // Time range of data block need split.
    //                         let min_idx =
    //                             hash_partial_datablock(&mut hasher, &data_block, 0, max_ts);
    //                         hash_vec
    //                             .push((TimeRange::new(min_ts, max_ts), hasher.finalize().into()));
    //                         hasher.reset();
    //                         hash_partial_datablock(
    //                             &mut hasher,
    //                             &data_block,
    //                             min_idx,
    //                             Timestamp::MIN,
    //                         );
    //                         last_hashed_tr_fid =
    //                             Some((TimeRange::new(max_ts, blk_time_range.1), field_id));
    //                     } else {
    //                         hash_partial_datablock(&mut hasher, &data_block, 0, Timestamp::MIN);
    //                         last_hashed_tr_fid = Some((TimeRange::new(min_ts, max_ts), field_id));
    //                     }
    //                 } else {
    //                     // Ignore: Case argument decode_non_overlap_blocks in CompactIterator::new()
    //                     // is set to true, we may ignore it.
    //                 }
    //             };
    //         }
    //         Some(Err(e)) => {
    //             return Err(Error::CommonError {
    //                 reason: format!(
    //                     "error getting hashes for vnode {} when compacting: {:?}",
    //                     vnode_id, e
    //                 ),
    //             });
    //         }
    //     }
    // }
    // if let Some((tr, last_fid)) = last_hashed_tr_fid {
    //     fid_tr_hash_val_map
    //         .entry(last_fid)
    //         .or_default()
    //         .push((tr, hasher.finalize().into()));
    // }

    // Ok(fid_tr_hash_val_map)

    Ok(HashMap::new())
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
    use models::{Timestamp, ValueType};
    use protos::kv_service::{Meta, WritePointsRequest};
    use protos::models::{self as fb_models, FieldType};
    use protos::models_helper;
    use protos::models_helper::create_points;
    use tokio::runtime::{self, Runtime};
    use tokio::sync::RwLock;

    use super::{calc_block_partial_time_range, find_timestamp, hash_partial_datablock, Hash};
    use crate::compaction::check::{vnode_hash_tree, TimeRangeHashTreeNode, DEFAULT_DURATION};
    use crate::compaction::flush;
    use crate::context::GlobalContext;
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
    fn test_calc_blcok_time_range() {
        fn get_args(datetime: &str) -> (Timestamp, i64) {
            let datetime = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").unwrap();
            let timestamp = datetime.timestamp_nanos();
            let duration = Duration::minutes(30);
            let duration_nanos = duration.num_nanoseconds().unwrap();
            (timestamp, duration_nanos)
        }

        let (stamp, span) = get_args("2023-01-01 00:29:01");
        assert_eq!(
            (
                parse_nanos("2023-01-01 00:00:00"),
                parse_nanos("2023-01-01 00:30:00")
            ),
            calc_block_partial_time_range(stamp, span).unwrap(),
        );

        let (stamp, span) = get_args("2023-01-01 00:30:01");
        assert_eq!(
            (
                parse_nanos("2023-01-01 00:30:00"),
                parse_nanos("2023-01-01 01:00:00")
            ),
            calc_block_partial_time_range(stamp, span).unwrap(),
        );
    }

    #[test]
    fn test_find_timestamp() {
        let timestamps = vec![
            parse_nanos("2023-01-01 00:01:00"),
            parse_nanos("2023-01-01 00:02:00"),
            parse_nanos("2023-01-01 00:03:00"),
            parse_nanos("2023-01-01 00:04:00"),
            parse_nanos("2023-01-01 00:05:00"),
        ];

        assert_eq!(
            0,
            find_timestamp(&timestamps, parse_nanos("2023-01-01 00:00:00")),
        );
        assert_eq!(
            3,
            find_timestamp(&timestamps, parse_nanos("2023-01-01 00:03:30"))
        );
        assert_eq!(
            3,
            find_timestamp(&timestamps, parse_nanos("2023-01-01 00:04:00"))
        );
        assert_eq!(
            5,
            find_timestamp(&timestamps, parse_nanos("2023-01-01 00:30:00"))
        );
        assert_eq!(5, find_timestamp(&timestamps, Timestamp::MIN),);
    }

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

    #[test]
    fn test_hash_partial_datablock() {
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
            let min_idx = hash_partial_datablock(
                &mut hasher_blk,
                &data_block,
                0,
                parse_nanos("2023-01-01 00:04:00"),
            );
            assert_eq!(3, min_idx);
            let mut hasher_cmp = Hasher::new();
            assert_eq!(
                hasher_cmp
                    .update(data_block_partial_to_bytes(&data_block, 0, 3).as_slice())
                    .finalize(),
                hasher_blk.finalize(),
                "checking {}",
                data_block
            );

            let mut hasher_blk = Hasher::new();
            let min_idx =
                hash_partial_datablock(&mut hasher_blk, &data_block, min_idx, Timestamp::MIN);
            assert_eq!(6, min_idx);
            let mut hasher_cmp = Hasher::new();
            assert_eq!(
                hasher_cmp
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
        let mut tr_hashes: Vec<(TimeRange, Hash)> = Vec::new();
        let mut hasher = Hasher::new();

        if let Some(blk_time_range) = data_block.time_range() {
            // Get trunced time range by DataBlock.time[0]
            let (min_ts, max_ts) =
                calc_block_partial_time_range(blk_time_range.0, duration_nanosecs).unwrap();

            // Calculate and store the hash value of data in time range
            if blk_time_range.1 > max_ts {
                // Time range of data block need to split.
                let min_idx = hash_partial_datablock(&mut hasher, data_block, 0, max_ts);
                tr_hashes.push((TimeRange::new(min_ts, max_ts), hasher.finalize().into()));
                hasher.reset();
                hash_partial_datablock(&mut hasher, data_block, min_idx, Timestamp::MIN);
                tr_hashes.push((
                    TimeRange::new(max_ts, blk_time_range.1),
                    hasher.finalize().into(),
                ));
            } else {
                hash_partial_datablock(&mut hasher, data_block, 0, Timestamp::MIN);
                tr_hashes.push((TimeRange::new(min_ts, max_ts), hasher.finalize().into()));
            }
        }

        tr_hashes
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
            .add_tsfamily(
                vnode_id,
                None,
                engine.summary_task_sender(),
                engine.flush_task_sender(),
                engine.compact_task_sender(),
                Arc::new(GlobalContext::new()),
            )
            .await
            .unwrap();
        let tsf = tsf.read().await;
        assert_eq!(vnode_id, tsf.tf_id());

        engine
    }

    #[rustfmt::skip]
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
        let mut fields : HashMap<&str, Vec<Vec<u8>>> = HashMap::new();
        let mut time = Vec::new();
        data_blocks_to_write_batch_args(data_blocks, &mut timestamps, &mut fields, &mut time);
        do_write_batch(engine, vnode_id, timestamps, tenant, database, table, fields, time).await;

        let flush_req = {
            let mut vnode = vnode.write().await;
            vnode.switch_to_immutable();
            vnode.build_flush_req(true).unwrap()
        };
        flush::run_flush_memtable_job(
            flush_req, engine.global_ctx(),
            engine.version_set(), engine.summary_task_sender(), None,
        ).await.unwrap();

        let sec_1 = Duration::seconds(1).to_std().unwrap();
        let mut check_num = 0;
        loop {
            tokio::time::sleep(sec_1).await;
            // If flushing is finished, the newest super_version contains a level 1 file.
            let vnode = vnode.read().await;
            let super_version = vnode.super_version();
            if !super_version.version.levels_info[1].files.is_empty() {
                break;
            }
            check_num += 1;
            if check_num >= 10 {
                println!("Checksum: warn: flushing takes more than {} seconds.", check_num);
            }
        }
    }

    #[test]
    #[ignore]
    fn test_get_vnode_hash_tree() {
        let base_dir = "/tmp/test/repair/1".to_string();
        let wal_dir = "/tmp/test/repair/1/wal".to_string();
        let log_dir = "/tmp/test/repair/1/log".to_string();
        // trace::init_default_global_tracing(&log_dir, "test.log", "debug");
        let _ = std::fs::remove_dir_all(&base_dir);
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
        ];
        #[rustfmt::skip]
        let data_blocks = vec![
            DataBlock::U64 { ts: timestamps.clone(), val: vec![1, 2, 3, 4, 5, 6], enc: DataBlockEncoding::default() },
            DataBlock::I64 { ts: timestamps.clone(), val: vec![1, 2, 3, 4, 5, 6], enc: DataBlockEncoding::default() },
            DataBlock::F64 { ts: timestamps.clone(), val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], enc: DataBlockEncoding::default() },
            DataBlock::Str {
                ts: timestamps.clone(),
                val: vec![
                    MiniVec::from("1"), MiniVec::from("2"), MiniVec::from("3"), MiniVec::from("4"), MiniVec::from("5"), MiniVec::from("6"),
                ],
                enc: DataBlockEncoding::default(),
            },
            DataBlock::Bool { ts: timestamps, val: vec![true, false, true, false, true, false], enc: DataBlockEncoding::default() },
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
            let database_ref = engine
                .get_db(&tenant_name, &database_name)
                .await
                .unwrap_or_else(|e| {
                    panic!("created database '{}' exists: {:?}", &database_name, e)
                });
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
            assert_eq!(tree_root.len(), 10);
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
