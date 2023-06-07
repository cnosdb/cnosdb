use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use models::predicate::domain::TimeRange;
use models::Timestamp;
use trace::{debug, error, info};

use crate::compaction::CompactReq;
use crate::kv_option::StorageOptions;
use crate::tseries_family::{ColumnFile, LevelInfo, Version};
use crate::LevelId;

pub trait Picker: Send + Sync + Debug {
    fn pick_compaction(&self, version: Arc<Version>) -> Option<CompactReq>;
}

/// Compaction picker for picking files in level
#[derive(Debug)]
pub struct LevelCompactionPicker {
    picking: AtomicBool,
    storage: Arc<StorageOptions>,
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

        debug!(
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

        let storage_opt = version.storage_opt();
        let level_infos = version.levels_info();

        // Pick a level to compact with level 0
        let level_start: &LevelInfo;
        let out_level;

        if let Some((start_lvl, out_lvl)) = self.pick_level(level_infos) {
            info!("Picker: picked level: {} to {}", start_lvl, out_lvl);
            level_start = &level_infos[start_lvl as usize];
            out_level = out_lvl;

            // If start_lvl is L1, compare the number of L1 files
            // with compact_trigger_file_num.
            if storage_opt.compact_trigger_file_num != 0
                && start_lvl == 1
                && (level_infos[1].files.len() as u32) < storage_opt.compact_trigger_file_num
            {
                info!(
                    "Picker: picked L1 files({}) does not reach trigger({}), return None",
                    level_infos[1].files.len(),
                    storage_opt.compact_trigger_file_num
                );
                return None;
            }
        } else {
            info!("Picker: picked level: None");
            return None;
        }

        // Pick selected level files.
        let mut picking_files: Vec<Arc<ColumnFile>> = Vec::new();
        let (mut picking_files_size, picking_time_range) = if level_start.files.is_empty() {
            info!("Picker: picked files: None");
            return None;
        } else {
            let mut files = level_start.files.clone();
            files.sort_by(Self::compare_column_file);
            Self::pick_files(files, storage_opt.max_compact_size, &mut picking_files)
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
            if !file.mark_compacting() {
                continue;
            }
            picking_files_size += file.size();
            if picking_files_size > storage_opt.max_compact_size {
                break;
            }
            picking_files.push(file.clone());
        }
        if picking_files.len() <= 1 {
            info!("Picker: picked files: None");
            if let Some(f) = picking_files.first() {
                f.unmark_compacting();
            }
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
    pub fn new(storage_opt: Arc<StorageOptions>) -> LevelCompactionPicker {
        Self {
            picking: AtomicBool::new(false),
            storage: storage_opt,
        }
    }

    /// Weight of file number of a level to be picked.
    #[inline(always)]
    fn level_weight_file_num(level: LevelId) -> f64 {
        match level {
            0 => 1.0,
            1 => 0.8,
            2 => 0.4,
            3 => 0.2,
            4 => 0.0,
            _ => 0.0,
        }
    }

    /// Weight of the ramaining size of a level to be picked.
    #[inline(always)]
    fn level_weight_remaining_size(level: LevelId) -> f64 {
        match level {
            0 => 1.0,
            1 => 1.0,
            2 => 0.1,
            3 => 0.01,
            4 => 0.0001,
            _ => 0.0,
        }
    }

    fn compare_column_file(a: &Arc<ColumnFile>, b: &Arc<ColumnFile>) -> std::cmp::Ordering {
        match a.time_range().min_ts.cmp(&b.time_range().min_ts) {
            std::cmp::Ordering::Equal => a.size().cmp(&b.size()),
            ord => ord,
        }
    }

    fn pick_level_legacy(
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
        //!   - (level.max_size - level.cur_size) as numerator
        //! - Level running compactions (level running compaction
        //!     has less possibility to run compact)
        //!   - compacting_files as deniminator
        //! - Level weight (higher level that file is too large
        //!     has less possibility to run compact)
        //!   - level_weight as numerator
        //! - Level file_count (after all, level has more files
        //!     has more possibility to run compact)
        //!   - level.files.len() as numerator

        if levels.is_empty() {
            return None;
        }

        // Level score context: Vec<(level, level_size, compacting_files in level, level_weight, level_score)>
        let mut level_scores: Vec<(LevelId, u64, usize, f64, f64)> =
            Vec::with_capacity(levels.len());
        for lvl in levels.iter() {
            // Ignore level 0 (delta files)
            if lvl.level == 0 || lvl.cur_size == 0 || lvl.files.len() <= 1 {
                continue;
            }
            let mut compacting_files = 0_usize;
            for file in lvl.files.iter() {
                if file.is_compacting() {
                    compacting_files += 1;
                }
            }
            let level_file_num_weight = Self::level_weight_file_num(lvl.level);

            // let level_score = (lvl.files.len() as f64) * level_weight * lvl.cur_size as f64
            //     / (lvl.max_size as f64 + 10000.0 * level_weight * compacting_files as f64);
            let level_score = (lvl.files.len() - compacting_files) as f64 * level_file_num_weight;

            level_scores.push((
                lvl.level,
                lvl.cur_size,
                compacting_files,
                level_file_num_weight,
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
        let mut prev_non_overlapped_idx = 0_usize;
        for (i, file) in src_files.iter().enumerate() {
            // The first serial files may be in compaction
            if file.is_compacting() {
                continue;
            }
            if file.time_range().overlaps(&picking_time_range) {
                picking_time_range.merge(file.time_range());
            } else {
                picking_time_range = *file.time_range();
                prev_non_overlapped_idx = i;
            }
            picking_file_size += file.size();
            if picking_file_size > max_compact_size && prev_non_overlapped_idx > 0 {
                break;
            }
            dst_files.push(file.clone());
            file.mark_compacting();
        }

        let mut picked_time_range = *src_files[0].time_range();
        picked_time_range.merge(src_files[prev_non_overlapped_idx].time_range());
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
        let base_level = 0;

        if !level0_being_compact {
            let score =
                level_infos[0].files.len() as f64 / storage_opt.compact_trigger_file_num as f64;
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
        let max_size = storage_opt.level_max_file_size(output_level);
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use lru_cache::asynchronous::ShardedCache;
    use memory_pool::{GreedyMemoryPool, MemoryPoolRef};
    use metrics::metric_register::MetricsRegister;
    use models::predicate::domain::TimeRange;
    use tokio::sync::mpsc;

    use crate::compaction::test::create_options;
    use crate::compaction::{LevelCompactionPicker, Picker};
    use crate::file_utils::make_tsm_file_name;
    use crate::kv_option::Options;
    use crate::kvcore::COMPACT_REQ_CHANNEL_CAP;
    use crate::memcache::MemCache;
    use crate::tseries_family::{ColumnFile, LevelInfo, TseriesFamily, Version};

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
        database: Arc<String>,
        opt: Arc<Options>,
        levels_sketch: LevelsSketch,
    ) -> TseriesFamily {
        let ts_family_id = 0;
        let mut level_infos =
            LevelInfo::init_levels(database.clone(), ts_family_id, opt.storage.clone());
        let mut max_level_ts = 0_i64;
        let tsm_dir = &opt.storage.tsm_dir(&database, ts_family_id);
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
                    make_tsm_file_name(tsm_dir, file_desc.0),
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
                max_size: opt.storage.level_max_file_size(lvl_desc.0),
                time_range: TimeRange::new(lvl_desc.1, lvl_desc.2),
            };
        }
        let memory_pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let version = Arc::new(Version::new(
            1,
            Arc::new("version_1".to_string()),
            opt.storage.clone(),
            1,
            level_infos,
            1000,
            Arc::new(ShardedCache::with_capacity(1)),
        ));
        let (flush_task_sender, _) = mpsc::channel(opt.storage.flush_req_channel_cap);
        let (compactt_task_sender, _) = mpsc::channel(COMPACT_REQ_CHANNEL_CAP);
        TseriesFamily::new(
            1,
            Arc::new("ts_family_1".to_string()),
            MemCache::new(1, 1000, 1, &memory_pool),
            version,
            opt.cache.clone(),
            opt.storage.clone(),
            flush_task_sender,
            compactt_task_sender,
            memory_pool,
            &Arc::new(MetricsRegister::default()),
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

        let storage_opt = opt.storage.clone();
        let tsf = create_tseries_family(Arc::new("dba".to_string()), opt, levels_sketch);
        let picker = LevelCompactionPicker::new(storage_opt);
        let compact_req = picker.pick_compaction(tsf.version()).unwrap();
        assert_eq!(compact_req.out_level, 2);
        assert_eq!(compact_req.files.len(), 2);
    }
}
