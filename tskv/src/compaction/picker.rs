use std::fmt::Debug;
use std::{
    cmp::Ordering,
    collections::HashMap,
    ops::{Add, Div},
    sync::{
        atomic::{self, AtomicBool},
        Arc,
    },
};

use chrono::{
    DateTime, Datelike, Duration, DurationRound, Local, NaiveDate, NaiveDateTime, NaiveTime, Utc,
};
use lazy_static::lazy_static;
use models::Timestamp;
use parking_lot::RwLock;
use trace::error;
use tracing::info;

use crate::{
    compaction::CompactReq,
    direct_io::File,
    error::Result,
    kv_option::{Options, StorageOptions},
    tseries_family::{ColumnFile, LevelInfo, TseriesFamily, Version},
    LevelId, TimeRange, TseriesFamilyId,
};

pub trait Picker: Send + Sync + Debug {
    fn pick_compaction(&self, version: Arc<Version>) -> Option<CompactReq>;
}

/// Compaction picker for picking files in level
#[derive(Debug)]
pub struct LevelCompactionPicker {
    picking: AtomicBool,
}

impl Picker for LevelCompactionPicker {
    fn pick_compaction(&self, version: Arc<Version>) -> Option<CompactReq> {
        //! 1. Get TseriesFamily's newest **version**(`Arc<Version>`)
        //! 2. Get all level's score, pick LevelInfo with the max score.
        //! 3. Get files(`Vec<Arc<ColumnFile>>`) from the picked level, sorted by min_ts(ascending)
        //!    and size(ascending), pick ColumnFile until picking_files_size reaches
        //!    max_compact_size, remove away the last picked files with overlapped time_range.
        //! 4. Get files(`Vec<Arc<ColumnFile>>`) from level-0, sorted by min_ts(ascending)
        //!    and size(ascending), pick ColumnFile until picking_files_size reaches
        //!    max_compact_size.
        //! 5. Build CompactReq using **version**, picked level and picked files.

        info!(
            "Picker: Version info: [ {} ]",
            version
                .levels_info()
                .iter()
                .enumerate()
                .map(|(i, lvl)| {
                    format!(
                        "Level-{}: files: [ {} ]",
                        i,
                        lvl.files
                            .iter()
                            .map(|f| format!(
                                "{}(C:{}, {}-{}, {} B)",
                                f.file_id(),
                                if f.is_compacting() { "Y" } else { "N" },
                                f.time_range().min_ts,
                                f.time_range().max_ts,
                                f.size()
                            ))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )
                })
                .collect::<Vec<String>>()
                .join(", ")
        );

        let level_infos = version.levels_info();

        // Pick a level to compact with level 0
        let level_start: &LevelInfo;
        let out_level;

        if let Some((start_lvl, out_lvl)) = self.pick_level(level_infos) {
            info!("Picker: picked level: {} to {}", start_lvl, out_lvl);
            level_start = &level_infos[start_lvl as usize];
            out_level = out_lvl;
        } else {
            info!("Picker: picked level: None");
            return None;
        }
        let max_compact_size = version.storage_opt.level_file_size(out_level);

        // Pick selected level files.
        let mut picking_files: Vec<Arc<ColumnFile>> = Vec::new();
        let (mut picking_files_size, picking_time_range) = if level_start.files.is_empty() {
            info!("Picker: picked files: None");
            return None;
        } else {
            let mut files = level_start.files.clone();
            files.sort_by(Self::compare_column_file);
            Self::pick_files(files, max_compact_size, &mut picking_files)
        };

        // Pick level 0 files.
        let mut files = level_infos[0].files.clone();
        files.sort_by(Self::compare_column_file);
        for file in files.iter() {
            if file.time_range().min_ts > picking_time_range.max_ts {
                break;
            }
            if file.is_compacting() || !file.time_range().overlaps(&picking_time_range) {
                continue;
            }
            picking_files_size += file.size();
            if picking_files_size > max_compact_size {
                break;
            }
            picking_files.push(file.clone());
            file.mark_compacting();
        }

        if picking_files.len() <= 1 {
            info!("Picker: picked files: None");
            return None;
        }

        info!(
            "Picker: Picked files: [ {} ]",
            picking_files
                .iter()
                .map(|f| {
                    format!(
                        "{{ Level-{}, file_id: {}, time_range: {}-{} }}",
                        f.level(),
                        f.file_id(),
                        f.time_range().min_ts,
                        f.time_range().max_ts
                    )
                })
                .collect::<Vec<String>>()
                .join(", ")
        );

        Some(CompactReq {
            ts_family_id: version.ts_family_id,
            database: version.database.clone(),
            storage_opt: version.storage_opt.clone(),
            files: picking_files,
            version: version.clone(),
            out_level,
        })
    }
}

impl LevelCompactionPicker {
    pub fn new() -> LevelCompactionPicker {
        Self {
            picking: AtomicBool::new(false),
        }
    }

    #[inline(always)]
    fn level_weight(level: LevelId) -> f64 {
        // TODO level_weight is a temporary method
        match level {
            0 => 0.9,
            1 => 0.8,
            2 => 0.5,
            3 => 0.2,
            4 => 0.1,
            _ => 0.0,
        }
    }

    fn compare_column_file(a: &Arc<ColumnFile>, b: &Arc<ColumnFile>) -> std::cmp::Ordering {
        match a.time_range().min_ts.cmp(&b.time_range().min_ts) {
            std::cmp::Ordering::Equal => a.size().cmp(&b.size()),
            ord => ord,
        }
    }

    fn pick_level_1(
        &self,
        storage_opt: &StorageOptions,
        levels: &[LevelInfo],
    ) -> Option<(LevelId, LevelId)> {
        let mut ctx = LevelCompatContext::default();
        ctx.cal_score(levels, storage_opt);
        ctx.pick_level()
    }

    fn pick_level(&self, levels: &[LevelInfo]) -> Option<(LevelId, LevelId)> {
        //! - Level max_size (level closer to max_size
        //!     has more possibility to run compact)
        //! - Level running compactions (level running compaction
        //!     has less possibility to run compact)
        //! - Level weight (higher level that file is too large
        //!     has less possibility to run compact)
        //! - Level file_count (after all, level has more files
        //!     has more possibility to run compact)

        if levels.is_empty() {
            return None;
        }

        // Level score context: Vec<(level, level_size, compacting_files in level, level_weight, level_score)>
        let mut level_scores: Vec<(LevelId, u64, usize, f64, f64)> =
            Vec::with_capacity(levels.len());
        for lvl in levels.iter() {
            if lvl.level == 0 || lvl.cur_size == 0 || lvl.files.len() <= 1 {
                continue;
            }
            let mut compacting_files = 0_usize;
            for file in lvl.files.iter() {
                if file.is_compacting() {
                    compacting_files += 1;
                }
            }
            let level_weight = Self::level_weight(lvl.level);

            let level_score = (lvl.files.len() as f64) * level_weight * lvl.cur_size as f64
                / (lvl.max_size as f64 + 10000.0 * level_weight * compacting_files as f64);

            level_scores.push((
                lvl.level,
                lvl.cur_size,
                compacting_files,
                level_weight,
                level_score,
            ));
        }

        if level_scores.is_empty() {
            return None;
        }
        level_scores.sort_by(|a, b| a.4.partial_cmp(&b.4).expect("a NaN score").reverse());

        info!(
            "Picker: Calculate level scores: [ {} ]",
            level_scores
                .iter()
                .map(|lc| format!("{{ Level-{}: {} }}", lc.0, lc.4))
                .collect::<Vec<String>>()
                .join(", ")
        );

        level_scores.first().map(|lvl_score| {
            if lvl_score.0 == 4 {
                (lvl_score.0, lvl_score.0)
            } else {
                (lvl_score.0, lvl_score.0 + 1)
            }
        })
    }

    fn pick_files(
        src_files: Vec<Arc<ColumnFile>>,
        max_compact_size: u64,
        dst_files: &mut Vec<Arc<ColumnFile>>,
    ) -> (u64, TimeRange) {
        let mut picking_file_size = 0_u64;
        let mut picking_time_range = TimeRange::from((Timestamp::MAX, Timestamp::MIN));
        let mut prev_non_overlaped_idx = 0_usize;
        for (i, file) in src_files.iter().enumerate() {
            // The first serial files may be in compaction
            if file.is_compacting() {
                continue;
            }
            if file.time_range().overlaps(&picking_time_range) {
                picking_time_range.merge(file.time_range());
            } else {
                picking_time_range = *file.time_range();
                prev_non_overlaped_idx = i;
            }
            picking_file_size += file.size() as u64;
            if picking_file_size > max_compact_size && prev_non_overlaped_idx > 0 {
                break;
            }
            dst_files.push(file.clone());
            file.mark_compacting();
        }

        let mut picked_time_range = *src_files[0].time_range();
        picked_time_range.merge(src_files[prev_non_overlaped_idx].time_range());
        (picking_file_size, picked_time_range)
    }
}

#[derive(Default)]
struct LevelCompatContext {
    level_scores: Vec<(u32, f64)>,
    base_level: u32,
    max_level: u32,
}

impl LevelCompatContext {
    fn cal_score(&mut self, level_infos: &[LevelInfo], storage_opt: &StorageOptions) {
        let mut level0_being_compact = false;
        for t in &level_infos[0].files {
            if t.is_compacting() {
                level0_being_compact = true;
                break;
            }
        }
        let l0_size = storage_opt.base_file_size;
        let base_level = 0;

        if !level0_being_compact {
            let score = level_infos[0].files.len() as f64 / storage_opt.compact_trigger as f64;
            self.level_scores.push((
                0,
                f64::max(
                    score,
                    level_infos[0].cur_size as f64 / level_infos[base_level].max_size as f64,
                ),
            ));
        }
        for (l, item) in level_infos.iter().enumerate() {
            let score = match item.cur_size.checked_div(item.max_size) {
                None => {
                    error!("failed to get score by max size");
                    continue;
                }
                Some(v) => v,
            };
            self.level_scores.push((l as u32, score as f64));
        }
        self.base_level = 0;
        self.max_level = level_infos.len() as u32 - 1;
    }

    fn pick_level(&mut self) -> Option<(u32, u32)> {
        self.level_scores.sort_by(|a, b| {
            if a.1 > b.1 {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        });

        println!("==========Debug(pick_level)1==========");
        println!("Calculate level scores:");
        for lvl_score in self.level_scores.iter() {
            println!("Level-{} | {}", lvl_score.0, lvl_score.1);
        }
        println!("==========Debug(pick_level)2==========");

        let base_level = self.base_level;
        if let Some((level, score)) = self.level_scores.first() {
            return if *score < 1.0 {
                None
            } else if *level == 0 {
                Some((*level, base_level))
            } else if *level + 1 == self.max_level {
                Some((*level, *level))
            } else {
                Some((*level, *level + 1))
            };
        }
        None
    }

    fn pick_files(
        &self,
        level_infos: &[LevelInfo; 5],
        storage_opt: &StorageOptions,
        level: u32,
        output_level: u32,
    ) -> Option<(u32, Vec<Arc<ColumnFile>>)> {
        if level > (level_infos.len() - 1) as u32 {
            return None;
        }
        let mut inputs = vec![];
        let mut ts_min = i64::MAX;
        let mut ts_max = i64::MIN;
        let mut file_size = 0;
        let max_size = storage_opt.level_file_size(output_level);
        let lvl_info = &level_infos[level as usize];
        for file in &lvl_info.files {
            file_size += file.size();
            if ts_min > file.time_range().min_ts {
                ts_min = file.time_range().min_ts;
            }

            if ts_max < file.time_range().max_ts {
                ts_max = file.time_range().max_ts;
            }
            inputs.push(file.clone());
            if file_size >= max_size {
                break;
            }
        }
        Some((level, inputs))
    }
}

mod test {
    use std::sync::Arc;
    use tokio::sync::mpsc;

    use crate::{
        file_utils::make_tsm_file_name,
        kv_option::{Options, StorageOptions},
        memcache::MemCache,
        tseries_family::{ColumnFile, LevelInfo, TseriesFamily, Version},
        TimeRange,
    };

    fn create_options(base_dir: String) -> Arc<Options> {
        let dir = "../config/config.toml";
        let mut config = config::get_config(dir);
        config.storage.path = base_dir;
        let opt = Options::from(&config);
        Arc::new(opt)
    }

    type ColumnFilesSketch = (u64, i64, i64, u64, bool);
    type LevelsSketch = Vec<(u32, i64, i64, Vec<ColumnFilesSketch>)>;
    /// Returns a TseriesFamily by TseriesFamOpt and levels_sketch.
    ///
    /// All elements in levels_sketch is :
    ///
    /// - level
    /// - Timestamp_Begin
    /// - Timestamp_end
    /// - Vec<(column_files_sketch)>, all elements in column_files_sketch is:
    ///   - file_id
    ///   - Timestamp_Begin
    ///   - Timestamp_end
    ///   - size
    ///   - being_compact
    fn create_tseries_family(
        database: String,
        opt: Arc<Options>,
        levels_sketch: LevelsSketch,
    ) -> TseriesFamily {
        let mut level_infos = LevelInfo::init_levels(database.clone(), opt.storage.clone());
        let mut max_level_ts = 0_i64;
        let ts_family_id = 0;
        let tsm_dir = opt.storage.tsm_dir(&database, ts_family_id);
        for lvl_desc in levels_sketch.iter() {
            max_level_ts = max_level_ts.max(lvl_desc.2);
            let mut col_files = Vec::new();
            let mut cur_size = 0_u64;
            for file_desc in lvl_desc.3.iter() {
                cur_size += file_desc.3;
                let col = ColumnFile::new(
                    file_desc.0,
                    lvl_desc.0,
                    TimeRange::new(file_desc.1, file_desc.2),
                    file_desc.3,
                    lvl_desc.0 == 0,
                    make_tsm_file_name(&tsm_dir, file_desc.0),
                );
                if file_desc.4 {
                    col.mark_compacting();
                }
                col_files.push(Arc::new(col));
            }
            level_infos[lvl_desc.0 as usize] = LevelInfo {
                files: col_files,
                database: database.clone(),
                tsf_id: 0,
                storage_opt: opt.storage.clone(),
                level: lvl_desc.0,
                cur_size,
                max_size: opt.storage.level_file_size(lvl_desc.0),
                time_range: TimeRange::new(lvl_desc.1, lvl_desc.2),
            };
        }

        let version = Arc::new(Version::new(
            1,
            "version_1".to_string(),
            opt.storage.clone(),
            1,
            level_infos,
            1000,
        ));
        let (flush_task_sender, flush_task_receiver) = mpsc::unbounded_channel();
        TseriesFamily::new(
            1,
            "ts_family_1".to_string(),
            MemCache::new(1, 1000, 1),
            version,
            opt.cache.clone(),
            opt.storage.clone(),
            flush_task_sender,
        )
    }

    #[test]
    fn test_pick_1() {
        //! There are Level 0-4, and Level 1 is now in compaction.
        //! In this case, Level 2, and serial files in Level 0 will be picked,
        //! and compact to Level 3.
        let dir = "/tmp/test/pick/1";
        let opt = create_options(dir.to_string());

        #[rustfmt::skip]
        let levels_sketch: LevelsSketch = vec![
            // vec![( level, Timestamp_Begin, Timestamp_end, vec![(file_id, Timestamp_Begin, Timestamp_end, size, being_compact)] )]
            (0_u32, 1_i64, 1000_i64, vec![
                (11_u64, 1_i64, 1000_i64, 1000_u64, false),
                (12, 33010, 34000, 1000, false),
            ]),
            (1, 1, 1000, vec![
                (7, 34001, 35000, 1000, false),
                (8, 35001, 36000, 1000, false),
                (9, 34501, 35500, 1000, true),
                (10, 35001, 36000, 1000, true),
            ]), // 0.00019
            (2, 30001, 34000, vec![
                (5, 30001, 32000, 2000, false),
                (6, 32001, 34000, 2000, false),
            ]), // 0.00002
            (3, 20001, 30000, vec![
                (3, 20001, 25000, 5000, false),
                (4, 25001, 30000, 5000, false),
            ]), // 0.00002
            (4, 1, 20000, vec![
                (1, 1, 10000, 10000, false),
                (2, 10001, 20000, 10000, false),
            ]), // 0.00001
        ];

        let tsf = create_tseries_family("dba".to_string(), opt, levels_sketch);
        let compact_req = tsf.pick_compaction().unwrap();
        assert_eq!(compact_req.out_level, 2);
        assert_eq!(compact_req.files.len(), 2);
    }
}
