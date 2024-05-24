#![cfg(test)]

use std::collections::HashMap;
use std::sync::Arc;

use cache::ShardedAsyncCache;
use models::predicate::domain::TimeRange;
use models::{FieldId, Timestamp, ValueType};

use super::test::*;
use super::*;
use crate::compaction::CompactTask;
use crate::context::GlobalContext;
use crate::file_system::file_manager;
use crate::kv_option::Options;
use crate::tseries_family::{ColumnFile, LevelInfo, Version};
use crate::tsm::codec::DataBlockEncoding;
use crate::tsm::test::write_to_tsm_tombstone_v2;
use crate::tsm::{DataBlock, TsmTombstoneCache, TsmVersion};
use crate::ColumnFileId;

#[tokio::test]
#[ignore = "Manually test"]
async fn test_big_compaction() {
    let dir = "/tmp/test/big_compaction/1";
    let tenant_database = Arc::new("cnosdb.public".to_string());
    let opt = create_options(dir.to_string(), 1);

    let tsm_dir = opt.storage.tsm_dir(&tenant_database, 1);
    #[rustfmt::skip]
    let (compact_req, kernel) = prepare_compaction(
        tenant_database,
        opt,
        5,
        vec![
            Arc::new(ColumnFile::new(393, 0, (1704067260000000000, 1705263600000000000).into(), 0, tsm_dir.join("_000393.tsm"))),
            Arc::new(ColumnFile::new(394, 0, (1705263660000000000, 1705466820000000000).into(), 0, tsm_dir.join("_000394.tsm"))),
        ],
        1,
    );
    let (version_edit, _) = run_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();
    println!("{version_edit}");
}

pub fn prepare_compaction(
    tenant_database: Arc<String>,
    opt: Arc<Options>,
    next_file_id: ColumnFileId,
    files: Vec<Arc<ColumnFile>>,
    max_level_ts: Timestamp,
) -> (CompactReq, Arc<GlobalContext>) {
    let vnode_id = 1;
    let version = Arc::new(Version::new(
        vnode_id,
        tenant_database.clone(),
        opt.storage.clone(),
        1,
        LevelInfo::init_levels(tenant_database, 0, opt.storage.clone()),
        max_level_ts,
        Arc::new(ShardedAsyncCache::create_lru_sharded_cache(1)),
    ));
    let compact_req = CompactReq {
        compact_task: CompactTask::Normal(vnode_id),
        version,
        files,
        in_level: 1,
        out_level: 2,
        out_time_range: TimeRange::all(),
    };
    let context = Arc::new(GlobalContext::new(opt.node_id));
    context.set_file_id(next_file_id);

    (compact_req, context)
}

/// Test compaction with ordered data.
#[tokio::test]
async fn test_compaction_fast() {
    #[rustfmt::skip]
    let data = vec![
        vec![
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
        ],
        vec![
            (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
        ],
        vec![
            (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
        ],
    ];
    #[rustfmt::skip]
    let expected_data = HashMap::from([
        (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
        (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
        (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
    ]);

    let dir = "/tmp/test/compaction/0";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) =
        write_data_blocks_to_column_file(&dir, data, 2, TsmVersion::V2).await;
    let (compact_req, kernel) =
        prepare_compaction(tenant_database, opt, next_file_id, files, max_level_ts);
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

#[tokio::test]
async fn test_compaction_1() {
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

    let dir = "/tmp/test/compaction/1";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) =
        write_data_blocks_to_column_file(&dir, data, 2, TsmVersion::V2).await;
    let (compact_req, kernel) =
        prepare_compaction(tenant_database, opt, next_file_id, files, max_level_ts);
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compact with duplicate timestamp.
#[tokio::test]
async fn test_compaction_2() {
    #[rustfmt::skip]
    let data = vec![
        vec![
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![111, 112, 113, 114], enc: INT_BLOCK_ENCODING }]),
            (4, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![141, 142, 143], enc: INT_BLOCK_ENCODING }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![131, 132, 133, 134], enc: INT_BLOCK_ENCODING }]),
        ],
        vec![
            (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![224, 225, 226], enc: INT_BLOCK_ENCODING }]),
            (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![214, 215, 216], enc: INT_BLOCK_ENCODING }]),
            (3, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7], val: vec![234, 235, 236, 237], enc: INT_BLOCK_ENCODING }]),
        ],
        vec![
            (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![337, 338, 339], enc: INT_BLOCK_ENCODING }]),
            (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![317, 318, 319], enc: INT_BLOCK_ENCODING }]),
            (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![327, 328, 329], enc: INT_BLOCK_ENCODING }]),
        ],
    ];
    #[rustfmt::skip]
    let expected_data = HashMap::from([
        (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![111, 112, 113, 214, 215, 216, 317, 318, 319], enc: INT_BLOCK_ENCODING }]),
        (2, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7, 8, 9], val: vec![224, 225, 226, 327, 328, 329], enc: INT_BLOCK_ENCODING }]),
        (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![131, 132, 133, 234, 235, 236, 337, 338, 339], enc: INT_BLOCK_ENCODING }]),
        (4, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![141, 142, 143], enc: INT_BLOCK_ENCODING }]),
    ]);

    let dir = "/tmp/test/compaction/2";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) =
        write_data_blocks_to_column_file(&dir, data, 2, TsmVersion::V1).await;
    let (compact_req, kernel) =
        prepare_compaction(tenant_database, opt, next_file_id, files, max_level_ts);
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compact with tombstones.
#[tokio::test]
async fn test_compaction_3() {
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

    let dir = "/tmp/test/compaction/3";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    let max_level_ts = 9;

    let (next_file_id, files) =
        write_data_blocks_to_column_file(&dir, data, 2, TsmVersion::V2).await;
    for f in files.iter().take(2 + 1).skip(1) {
        let mut path = f.file_path().clone();
        path.set_extension("tombstone");
        write_to_tsm_tombstone_v2(path, &TsmTombstoneCache::with_all_excluded((2, 6).into())).await;
    }
    let (compact_req, kernel) =
        prepare_compaction(tenant_database, opt, next_file_id, files, max_level_ts);
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();
    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compaction without tombstones.
#[tokio::test]
async fn test_big_compaction_1() {
    #[rustfmt::skip]
    let data_desc: [TsmSchema; 3] = [
        // [( tsm_sequence, vec![ (ValueType, FieldId, Timestamp_Begin, Timestamp_end) ] )]
        (1_u64, vec![
            // 1, 1~2500
            (ValueType::Unsigned, 1_u64, 1_i64, 1000_i64),
            (ValueType::Unsigned, 1, 1001, 2000),
            (ValueType::Unsigned, 1, 2001, 2500),
            // 2, 1~1500
            (ValueType::Integer, 2, 1, 1000),
            (ValueType::Integer, 2, 1001, 1500),
            // 3, 1~1500
            (ValueType::Boolean, 3, 1, 1000),
            (ValueType::Boolean, 3, 1001, 1500),
        ], vec![]),
        (2, vec![
            // 1, 2001~4500
            (ValueType::Unsigned, 1, 2001, 3000),
            (ValueType::Unsigned, 1, 3001, 4000),
            (ValueType::Unsigned, 1, 4001, 4500),
            // 2, 1001~3000
            (ValueType::Integer, 2, 1001, 2000),
            (ValueType::Integer, 2, 2001, 3000),
            // 3, 1001~2500
            (ValueType::Boolean, 3, 1001, 2000),
            (ValueType::Boolean, 3, 2001, 2500),
            // 4, 1~1500
            (ValueType::Float, 4, 1, 1000),
            (ValueType::Float, 4, 1001, 1500),
        ], vec![]),
        (3, vec![
            // 1, 4001~6500
            (ValueType::Unsigned, 1, 4001, 5000),
            (ValueType::Unsigned, 1, 5001, 6000),
            (ValueType::Unsigned, 1, 6001, 6500),
            // 2, 3001~5000
            (ValueType::Integer, 2, 3001, 4000),
            (ValueType::Integer, 2, 4001, 5000),
            // 3, 2001~3500
            (ValueType::Boolean, 3, 2001, 3000),
            (ValueType::Boolean, 3, 3001, 3500),
            // 4. 1001~2500
            (ValueType::Float, 4, 1001, 2000),
            (ValueType::Float, 4, 2001, 2500),
        ], vec![]),
    ];
    #[rustfmt::skip]
    let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
        [
            // 1, 1~6500
            (1, vec![
                generate_data_block(ValueType::Unsigned, vec![(1, 1000)]),
                generate_data_block(ValueType::Unsigned, vec![(1001, 2000)]),
                generate_data_block(ValueType::Unsigned, vec![(2001, 3000)]),
                generate_data_block(ValueType::Unsigned, vec![(3001, 4000)]),
                generate_data_block(ValueType::Unsigned, vec![(4001, 5000)]),
                generate_data_block(ValueType::Unsigned, vec![(5001, 6000)]),
                generate_data_block(ValueType::Unsigned, vec![(6001, 6500)]),
            ]),
            // 2, 1~5000
            (2, vec![
                generate_data_block(ValueType::Integer, vec![(1, 1000)]),
                generate_data_block(ValueType::Integer, vec![(1001, 2000)]),
                generate_data_block(ValueType::Integer, vec![(2001, 3000)]),
                generate_data_block(ValueType::Integer, vec![(3001, 4000)]),
                generate_data_block(ValueType::Integer, vec![(4001, 5000)]),
            ]),
            // 3, 1~3500
            (3, vec![
                generate_data_block(ValueType::Boolean, vec![(1, 1000)]),
                generate_data_block(ValueType::Boolean, vec![(1001, 2000)]),
                generate_data_block(ValueType::Boolean, vec![(2001, 3000)]),
                generate_data_block(ValueType::Boolean, vec![(3001, 3500)]),
            ]),
            // 4, 1~2500
            (4, vec![
                generate_data_block(ValueType::Float, vec![(1, 1000)]),
                generate_data_block(ValueType::Float, vec![(1001, 2000)]),
                generate_data_block(ValueType::Float, vec![(2001, 2500)]),
            ]),
        ]
    );

    let dir = "/tmp/test/compaction/big_1";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    if !file_manager::try_exists(&dir) {
        std::fs::create_dir_all(&dir).unwrap();
    }
    let max_level_ts = 6500;

    let column_files = write_data_block_desc(&dir, &data_desc, 2).await;
    let next_file_id = 4_u64;

    let (compact_req, kernel) = prepare_compaction(
        tenant_database,
        opt,
        next_file_id,
        column_files,
        max_level_ts,
    );
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();

    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compaction with tombstones
#[tokio::test]
async fn test_big_compaction_2() {
    #[rustfmt::skip]
    let data_desc: [TsmSchema; 3] = [
        // [( tsm_data:  tsm_sequence, vec![(ValueType, FieldId, Timestamp_Begin, Timestamp_end)],
        //    tombstone: vec![(FieldId, MinTimestamp, MaxTimestamp)]
        // )]
        (1, vec![
            // 1, 1~2500
            (ValueType::Unsigned, 1, 1, 1000), (ValueType::Unsigned, 1, 1001, 2000), (ValueType::Unsigned, 1, 2001, 2500),
        ], vec![(1, 1, 2), (1, 2001, 2100)]),
        (2, vec![
            // 1, 2001~4500
            // 2101~3100, 3101~4100, 4101~4499
            (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 3001, 4000), (ValueType::Unsigned, 1, 4001, 4500),
        ], vec![(1, 2001, 2100), (1, 4500, 4501)]),
        (3, vec![
            // 1, 4001~6500
            // 4001~4499, 4502~5501, 5502~6500
            (ValueType::Unsigned, 1, 4001, 5000), (ValueType::Unsigned, 1, 5001, 6000), (ValueType::Unsigned, 1, 6001, 6500),
        ], vec![(1, 4500, 4501)]),
    ];
    #[rustfmt::skip]
    let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
        [
            // 1, 1~6500
            (1, vec![
                generate_data_block(ValueType::Unsigned, vec![(3, 1002)]),
                generate_data_block(ValueType::Unsigned, vec![(1003, 2000), (2101, 2102)]),
                generate_data_block(ValueType::Unsigned, vec![(2103, 3102)]),
                generate_data_block(ValueType::Unsigned, vec![(3103, 4102)]),
                generate_data_block(ValueType::Unsigned, vec![(4103, 4499), (4502, 5104)]),
                generate_data_block(ValueType::Unsigned, vec![(5105, 6104)]),
                generate_data_block(ValueType::Unsigned, vec![(6105, 6500)]),
            ]),
        ]
    );

    let dir = "/tmp/test/compaction/big_2";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    if !file_manager::try_exists(&dir) {
        std::fs::create_dir_all(&dir).unwrap();
    }
    let max_level_ts = 6500;

    let column_files = write_data_block_desc(&dir, &data_desc, 2).await;
    let next_file_id = 4_u64;
    let (compact_req, kernel) = prepare_compaction(
        tenant_database,
        opt,
        next_file_id,
        column_files,
        max_level_ts,
    );
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();

    check_column_file(dir, version_edit, expected_data, out_level).await;
}

/// Test compaction with multi-field and tombstones.
#[tokio::test]
async fn test_big_compaction_3() {
    #[rustfmt::skip]
    let data_desc: [TsmSchema; 3] = [
        // [( tsm_data:  tsm_sequence, vec![(ValueType, FieldId, Timestamp_Begin, Timestamp_end)],
        //    tombstone: vec![(FieldId, MinTimestamp, MaxTimestamp)]
        // )]
        (1, vec![
            // 1, 1~2500
            (ValueType::Unsigned, 1, 1, 1000), (ValueType::Unsigned, 1, 1001, 2000),  (ValueType::Unsigned, 1, 2001, 2500),
            // 2, 1~1500
            (ValueType::Integer, 2, 1, 1000), (ValueType::Integer, 2, 1001, 1500),
            // 3, 1~1500
            (ValueType::Boolean, 3, 1, 1000), (ValueType::Boolean, 3, 1001, 1500),
        ], vec![
            (1, 1, 2), (1, 2001, 2100),
            (2, 1001, 1002),
            (3, 1499, 1500),
        ]),
        (2, vec![
            // 1, 2001~4500
            (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 3001, 4000), (ValueType::Unsigned, 1, 4001, 4500),
            // 2, 1001~3000
            (ValueType::Integer, 2, 1001, 2000), (ValueType::Integer, 2, 2001, 3000),
            // 3, 1001~2500
            (ValueType::Boolean, 3, 1001, 2000), (ValueType::Boolean, 3, 2001, 2500),
            // 4, 1~1500
            (ValueType::Float, 4, 1, 1000), (ValueType::Float, 4, 1001, 1500),
        ], vec![
            (1, 2001, 2100), (1, 4500, 4501),
            (2, 1001, 1002), (2, 2501, 2502),
            (3, 1499, 1500),
        ]),
        (3, vec![
            // 1, 4001~6500
            (ValueType::Unsigned, 1, 4001, 5000), (ValueType::Unsigned, 1, 5001, 6000), (ValueType::Unsigned, 1, 6001, 6500),
            // 2, 3001~5000
            (ValueType::Integer, 2, 3001, 4000), (ValueType::Integer, 2, 4001, 5000),
            // 3, 2001~3500
            (ValueType::Boolean, 3, 2001, 3000), (ValueType::Boolean, 3, 3001, 3500),
            // 4. 1001~2500
            (ValueType::Float, 4, 1001, 2000), (ValueType::Float, 4, 2001, 2500),
        ], vec![
            (1, 4500, 4501),
            (2, 4001, 4002),
        ]),
    ];
    #[rustfmt::skip]
    let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
        [
            // 1, 1~6500
            (1, vec![
                generate_data_block(ValueType::Unsigned, vec![(3, 1002)]),
                generate_data_block(ValueType::Unsigned, vec![(1003, 2000), (2101, 2102)]),
                generate_data_block(ValueType::Unsigned, vec![(2103, 3102)]),
                generate_data_block(ValueType::Unsigned, vec![(3103, 4102)]),
                generate_data_block(ValueType::Unsigned, vec![(4103, 4499), (4502, 5104)]),
                generate_data_block(ValueType::Unsigned, vec![(5105, 6104)]),
                generate_data_block(ValueType::Unsigned, vec![(6105, 6500)]),
            ]),
            // 2, 1~5000
            (2, vec![
                generate_data_block(ValueType::Integer, vec![(1, 1000)]),
                generate_data_block(ValueType::Integer, vec![(1003, 2002)]),
                generate_data_block(ValueType::Integer, vec![(2003, 2500), (2503, 3004)]),
                generate_data_block(ValueType::Integer, vec![(3005, 4000), (4003, 4006)]),
                generate_data_block(ValueType::Integer, vec![(4007, 5000)]),
            ]),
            // 3, 1~3500
            (3, vec![
                generate_data_block(ValueType::Boolean, vec![(1, 1000)]),
                generate_data_block(ValueType::Boolean, vec![(1001, 1498), (1501, 2002)]),
                generate_data_block(ValueType::Boolean, vec![(2003, 3002)]),
                generate_data_block(ValueType::Boolean, vec![(3003, 3500)]),
            ]),
            // 4, 1~2500
            (4, vec![
                generate_data_block(ValueType::Float, vec![(1, 1000)]),
                generate_data_block(ValueType::Float, vec![(1001, 2000)]),
                generate_data_block(ValueType::Float, vec![(2001, 2500)]),
            ]),
        ]
    );

    let dir = "/tmp/test/compaction/big_3";
    let _ = std::fs::remove_dir_all(dir);
    let tenant_database = Arc::new("cnosdb.dba".to_string());
    let opt = create_options(dir.to_string(), 1);
    let dir = opt.storage.tsm_dir(&tenant_database, 1);
    if !file_manager::try_exists(&dir) {
        std::fs::create_dir_all(&dir).unwrap();
    }
    let max_level_ts = 6500;

    let column_files = write_data_block_desc(&dir, &data_desc, 2).await;
    let next_file_id = 4_u64;
    let (compact_req, kernel) = prepare_compaction(
        tenant_database,
        opt,
        next_file_id,
        column_files,
        max_level_ts,
    );
    let out_level = compact_req.out_level;
    let (version_edit, _) = run_compaction_job(compact_req, kernel)
        .await
        .unwrap()
        .unwrap();

    check_column_file(dir, version_edit, expected_data, out_level).await;
}
