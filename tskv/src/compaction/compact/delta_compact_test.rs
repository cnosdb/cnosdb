#![cfg(test)]

use std::path::Path;

use cache::ShardedAsyncCache;
use models::predicate::domain::TimeRanges;
use models::{FieldId, Timestamp, ValueType};

use super::test::*;
use super::*;
use crate::file_system::file_manager;
use crate::tseries_family::{ColumnFile, LevelInfo, Version};
use crate::tsm::test::write_to_tsm_tombstone_v2;
use crate::tsm::{DataBlock, TsmTombstoneCache, TsmVersion};
use crate::{file_utils, record_file, Options};

#[tokio::test]
#[ignore = "Manually test"]
async fn test_big_delta_compaction() {
    let dir = "/tmp/test/big_delta_compaction/1";
    let tenant_database = Arc::new("cnosdb.benchmark".to_string());
    let opt = create_options(dir.to_string(), 1);

    let delta_dir = opt.storage.delta_dir(&tenant_database, 1);
    #[rustfmt::skip]
    let (compact_req, kernel) = prepare_delta_compaction(
        tenant_database,
        opt,
        5,
        vec![
            Arc::new(ColumnFile::new(1187, 0, (1626052320072000000, 1626057359280000000).into(), 0, delta_dir.join("_001187.delta"))),
            Arc::new(ColumnFile::new(1199, 0, (1626057360072000000, 1626074639280000000).into(), 0, delta_dir.join("_001199.delta"))),
            Arc::new(ColumnFile::new(1210, 0, (1626069600072000000, 1626073919280000000).into(), 0, delta_dir.join("_001210.delta"))),
            Arc::new(ColumnFile::new(1225, 0, (1626074640072000000, 1626086159280000000).into(), 0, delta_dir.join("_001225.delta"))),
        ],
        vec![],
        (1626053759280000001, 1626086159280000000).into(),
        1,
        0,
    );
    let (version_edit, _) = run_delta_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();
    println!("{version_edit}");
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_delta_compaction(
    tenant_database: Arc<String>,
    opt: Arc<Options>,
    next_file_id: ColumnFileId,
    delta_files: Vec<Arc<ColumnFile>>,
    tsm_files: Vec<Arc<ColumnFile>>,
    out_time_range: TimeRange,
    out_level: LevelId,
    out_level_max_ts: Timestamp,
) -> (CompactReq, Arc<GlobalContext>) {
    let vnode_id = 1;
    let version = Arc::new(Version::new(
        vnode_id,
        tenant_database.clone(),
        opt.storage.clone(),
        1,
        LevelInfo::init_levels(tenant_database, 0, opt.storage.clone()),
        out_level_max_ts,
        Arc::new(ShardedAsyncCache::create_lru_sharded_cache(1)),
    ));
    let mut files = delta_files;
    files.extend_from_slice(&tsm_files);
    let compact_req = CompactReq {
        compact_task: CompactTask::Delta(vnode_id),
        version,
        files,
        in_level: 0,
        out_level,
        out_time_range,
    };
    let context = Arc::new(GlobalContext::new());
    context.set_file_id(next_file_id);

    (compact_req, context)
}

#[tokio::test]
async fn test_delta_compaction_1() {
    #[rustfmt::skip]
    let data = vec![
        vec![
            (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![114, 115, 116], enc: INT_BLOCK_ENCODING }]),
            (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![124, 125, 126], enc: INT_BLOCK_ENCODING }]),
            (3, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![134, 135, 136], enc: INT_BLOCK_ENCODING }]),
        ],
        vec![
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![211, 212, 213], enc: INT_BLOCK_ENCODING }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![221, 222, 223], enc: INT_BLOCK_ENCODING }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![231, 232, 233], enc: INT_BLOCK_ENCODING }]),
        ],
        vec![
            (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![317, 318, 319], enc: INT_BLOCK_ENCODING }]),
            (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![327, 328, 329], enc: INT_BLOCK_ENCODING }]),
            (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![337, 338, 339], enc: INT_BLOCK_ENCODING }]),
        ],
    ];
    #[rustfmt::skip]
    let expected_data = HashMap::from([
        (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![211, 212, 213, 114, 115, 116, 317, 318, 319], enc: INT_BLOCK_ENCODING }]),
        (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![221, 222, 223, 124, 125, 126, 327, 328, 329], enc: INT_BLOCK_ENCODING }]),
        (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![231, 232, 233, 134, 135, 136, 337, 338, 339], enc: INT_BLOCK_ENCODING }]),
    ]);

    let dir = "/tmp/test/delta_compaction/1";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) =
        write_data_blocks_to_column_file(&dir, data, 0, TsmVersion::V2).await;
    let (compact_req, kernel) = prepare_delta_compaction(
        tenant_database,
        opt,
        next_file_id,
        files,
        vec![],
        (1, 9).into(),
        1,
        max_level_ts,
    );
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_delta_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compact with duplicate timestamp.
#[tokio::test]
async fn test_delta_compaction_2() {
    #[rustfmt::skip]
    let data = vec![
        vec![
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![111, 112, 113, 114], enc: INT_BLOCK_ENCODING }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![131, 132, 133, 134], enc: INT_BLOCK_ENCODING }]),
            (4, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![141, 142, 143], enc: INT_BLOCK_ENCODING }]),
        ],
        vec![
            (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![214, 215, 216], enc: INT_BLOCK_ENCODING }]),
            (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![224, 225, 226], enc: INT_BLOCK_ENCODING }]),
            (3, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7], val: vec![234, 235, 236, 237], enc: INT_BLOCK_ENCODING }]),
        ],
        vec![
            (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![317, 318, 319], enc: INT_BLOCK_ENCODING }]),
            (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![327, 328, 329], enc: INT_BLOCK_ENCODING }]),
            (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![337, 338, 339], enc: INT_BLOCK_ENCODING }]),
        ],
    ];
    #[rustfmt::skip]
    let expected_data = HashMap::from([
        (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![111, 112, 113, 214, 215, 216, 317, 318, 319], enc: INT_BLOCK_ENCODING }]),
        (2, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7, 8, 9], val: vec![224, 225, 226, 327, 328, 329], enc: INT_BLOCK_ENCODING }]),
        (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![131, 132, 133, 234, 235, 236, 337, 338, 339], enc: INT_BLOCK_ENCODING }]),
        (4, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![141, 142, 143], enc: INT_BLOCK_ENCODING }]),
    ]);

    let dir = "/tmp/test/delta_compaction/2";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) =
        write_data_blocks_to_column_file(&dir, data, 0, TsmVersion::V2).await;
    let (compact_req, kernel) = prepare_delta_compaction(
        tenant_database,
        opt,
        next_file_id,
        files,
        vec![],
        (1, 9).into(),
        1,
        max_level_ts,
    );
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_delta_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compact with tombstones.
#[tokio::test]
async fn test_delta_compaction_3() {
    #[rustfmt::skip]
    let data = vec![
        vec![
            (1, vec![DataBlock::I64 { ts: vec![1], val: vec![111], enc: INT_BLOCK_ENCODING }]),
        ],
        vec![
            (1, vec![DataBlock::I64 { ts: vec![2, 3, 4], val: vec![212, 213, 214], enc: INT_BLOCK_ENCODING }]),
        ],
        vec![
            (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![314, 315, 316], enc: INT_BLOCK_ENCODING }]),
        ],
        vec![
            (1, vec![DataBlock::I64 { ts: vec![8, 9], val: vec![418, 419], enc: INT_BLOCK_ENCODING }]),
        ],
    ];
    #[rustfmt::skip]
    let expected_data = HashMap::from([
        (1, vec![DataBlock::I64 { ts: vec![1, 8, 9], val: vec![111, 418, 419], enc: INT_BLOCK_ENCODING }]),
    ]);

    let dir = "/tmp/test/delta_compaction/3";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) =
        write_data_blocks_to_column_file(&dir, data, 0, TsmVersion::V2).await;
    for f in files.iter().take(2 + 1).skip(1) {
        let mut path = f.file_path().clone();
        path.set_extension("tombstone");
        write_to_tsm_tombstone_v2(path, &TsmTombstoneCache::with_all_excluded((2, 6).into())).await;
    }
    let (compact_req, kernel) = prepare_delta_compaction(
        tenant_database,
        opt,
        next_file_id,
        files,
        vec![],
        (1, 9).into(),
        1,
        max_level_ts,
    );
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_delta_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

#[allow(clippy::too_many_arguments)]
async fn test_delta_compaction(
    dir: &str,
    delta_files_desc: &[TsmSchema],
    tsm_files_desc: &[TsmSchema],
    out_time_range: TimeRange,
    max_ts: Timestamp,
    expected_data_desc: HashMap<FieldId, Vec<DataBlock>>,
    expected_data_level: LevelId,
    expected_delta_tombstone_all_excluded: HashMap<ColumnFileId, TimeRanges>,
) {
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let tsm_dir = opt.storage.tsm_dir(&tenant_database, 1);
    if !file_manager::try_exists(&tsm_dir) {
        std::fs::create_dir_all(&tsm_dir).unwrap();
    }
    let delta_dir = opt.storage.delta_dir(&tenant_database, 1);
    if !file_manager::try_exists(&delta_dir) {
        std::fs::create_dir_all(&delta_dir).unwrap();
    }

    let delta_files = write_data_block_desc(&delta_dir, delta_files_desc, 0).await;
    let tsm_files = write_data_block_desc(&tsm_dir, tsm_files_desc, 2).await;
    let next_file_id = delta_files_desc
        .iter()
        .map(|(_file_id, blk_desc, _tomb_desc)| {
            blk_desc
                .iter()
                .map(|(_vtype, field_id, _min_ts, _max_ts)| *field_id)
                .max()
                .unwrap_or(1)
        })
        .max()
        .unwrap_or(1)
        + 1;
    let (mut compact_req, kernel) = prepare_delta_compaction(
        tenant_database,
        opt,
        next_file_id,
        delta_files,
        tsm_files,
        out_time_range,
        expected_data_level,
        max_ts,
    );
    compact_req.in_level = 0;
    compact_req.out_level = expected_data_level;

    let (version_edit, _) = run_delta_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .expect("Delta compaction sucessfully generated some new files");

    check_column_file(
        tsm_dir,
        version_edit,
        expected_data_desc,
        expected_data_level,
    )
    .await;
    check_delta_file_tombstone(delta_dir, expected_delta_tombstone_all_excluded).await;
}

async fn check_delta_file_tombstone(
    dir: impl AsRef<Path>,
    all_excludes: HashMap<ColumnFileId, TimeRanges>,
) {
    for (file_id, time_ranges) in all_excludes {
        let tombstone_path = file_utils::make_tsm_tombstone_file_name(&dir, file_id);
        let tombstone_path = tsm::tombstone_compact_tmp_path(&tombstone_path).unwrap();
        let mut record_reader = record_file::Reader::open(tombstone_path).await.unwrap();
        let tombstone = TsmTombstoneCache::load_from(&mut record_reader, false)
            .await
            .unwrap();
        assert_eq!(tombstone.all_excluded(), &time_ranges);
    }
}

/// Test compaction on level-0 (delta compaction) with multi-field.
#[tokio::test]
async fn test_big_delta_compaction_1() {
    #[rustfmt::skip]
    let delta_files_desc: [TsmSchema; 3] = [
        // [( tsm_data:  tsm_sequence, vec![(ValueType, FieldId, Timestamp_Begin, Timestamp_end)],
        //    tombstone: vec![(FieldId, MinTimestamp, MaxTimestamp)]
        // )]
        (2, vec![
            // 1, 1~2500
            (ValueType::Unsigned, 1, 1, 1000), (ValueType::Unsigned, 1, 1001, 2000),  (ValueType::Unsigned, 1, 2001, 2500),
            // 2, 1~1500
            (ValueType::Integer, 2, 1, 1000), (ValueType::Integer, 2, 1001, 1500),
            // 3, 1~1500
            (ValueType::Boolean, 3, 1, 1000), (ValueType::Boolean, 3, 1001, 1500),
        ], vec![]),
        (3, vec![
            // 1, 2001~4500
            (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 3001, 4000), (ValueType::Unsigned, 1, 4001, 4500),
            // 2, 1001~3000
            (ValueType::Integer, 2, 1001, 2000), (ValueType::Integer, 2, 2001, 3000),
            // 3, 1001~2500
            (ValueType::Boolean, 3, 1001, 2000), (ValueType::Boolean, 3, 2001, 2500),
            // 4, 1~1500
            (ValueType::Float, 4, 1, 1000), (ValueType::Float, 4, 1001, 1500),
        ], vec![]),
        (4, vec![
            // 1, 4001~6500
            (ValueType::Unsigned, 1, 4001, 5000), (ValueType::Unsigned, 1, 5001, 6000), (ValueType::Unsigned, 1, 6001, 6500),
            // 2, 3001~5000
            (ValueType::Integer, 2, 3001, 4000), (ValueType::Integer, 2, 4001, 5000),
            // 3, 2001~3500
            (ValueType::Boolean, 3, 2001, 3000), (ValueType::Boolean, 3, 3001, 3500),
            // 4. 1001~2500
            (ValueType::Float, 4, 1001, 2000), (ValueType::Float, 4, 2001, 2500),
        ], vec![]),
    ];
    // The target tsm file: [2001~5050]
    let max_level_ts = 5050;
    #[rustfmt::skip]
    let tsm_file_desc: TsmSchema = (1, vec![
        // 1, 2001~5050
        (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 4001, 5000),  (ValueType::Unsigned, 1, 5001, 5050),
        // 2, 2001~5000
        (ValueType::Integer, 2, 2001, 3000), (ValueType::Integer, 2, 4001, 5000),
        // 3, 3001~5000
        (ValueType::Boolean, 3, 3001, 4000), (ValueType::Boolean, 3, 4001, 5000),
        // 3, 2001~2500
        (ValueType::Float, 4, 2001, 2500),
    ], vec![]);

    let expected_data_target_level: HashMap<FieldId, Vec<DataBlock>> = HashMap::from([
        (
            // 1, 2001~5050
            1,
            vec![
                generate_data_block(ValueType::Unsigned, vec![(2001, 3000)]),
                generate_data_block(ValueType::Unsigned, vec![(3001, 4000)]),
                generate_data_block(ValueType::Unsigned, vec![(4001, 5000)]),
                generate_data_block(ValueType::Unsigned, vec![(5001, 5050)]),
            ],
        ),
        (
            // 2, 2001~5000
            2,
            vec![
                generate_data_block(ValueType::Integer, vec![(2001, 3000)]),
                generate_data_block(ValueType::Integer, vec![(3001, 4000)]),
                generate_data_block(ValueType::Integer, vec![(4001, 5000)]),
            ],
        ),
        (
            // 3, 2001~3500
            3,
            vec![
                generate_data_block(ValueType::Boolean, vec![(2001, 3000)]),
                generate_data_block(ValueType::Boolean, vec![(3001, 4000)]),
                generate_data_block(ValueType::Boolean, vec![(4001, 5000)]),
            ],
        ),
        (
            // 4, 2001~3500
            4,
            vec![generate_data_block(ValueType::Float, vec![(2001, 2500)])],
        ),
    ]);

    test_delta_compaction(
        "/tmp/test/delta_compaction/big_1",
        &delta_files_desc, // (1,2500), (1,4500), (1001,6500)
        &[tsm_file_desc],  // (2005,5050)
        (2001, 5050).into(),
        max_level_ts,
        expected_data_target_level,
        1,
        HashMap::from([
            (2, TimeRanges::new(vec![(2001, 5050).into()])),
            (3, TimeRanges::new(vec![(2001, 5050).into()])),
            (4, TimeRanges::new(vec![(2001, 5050).into()])),
        ]),
    )
    .await;
}
