use std::fmt::Debug;
use std::sync::Arc;

use trace::{debug, info};

use crate::compaction::{context_datetime, format_column_files, format_level_infos, CompactReq};
use crate::tseries_family::{ColumnFile, LevelInfo, Version};
use crate::LevelId;

pub trait Picker: Send + Sync + Debug {
    fn pick_compaction(&self, version: Arc<Version>) -> Option<CompactReq>;
}

const LEVEL_WEIGHT_FILE_NUM: [f64; 5] = [1.0, 1.0, 1.0, 1.0, 1.0];
const LEVEL_WEIGHT_REMAINING_SIZE: [f64; 5] = [1.0, 1.0, 1.0, 1.0, 1.0];

/// Compaction picker for picking a level from level-1 to level-4, and then
/// pick inner files of the level.
#[derive(Debug)]
pub struct LevelCompactionPicker {
    /// Datetime when this picker created.
    strategy: String,
    datetime: String,
}

impl Picker for LevelCompactionPicker {
    fn pick_compaction(&self, version: Arc<Version>) -> Option<CompactReq> {
        //! 1. Get TseriesFamily's newest **version**(`Arc<Version>`)
        //! 2. Get all level's score, pick LevelInfo with the max score.
        //! 3. Get files(`Vec<Arc<ColumnFile>>`) from the picked level, sorted by min_ts(ascending)
        //!    and size(ascending), pick ColumnFile until picking_files_size reaches
        //!    max_compact_size, remove away the last picked files with overlapped time_range.
        //! 4. (Deprecated and skipped): Pick files from level-0.
        //! 5. Build CompactReq using **version**, picked level and picked files.

        debug!(
            "Picker: strategy: {}, version: [ {} ]",
            self.strategy,
            format_level_infos(&version.levels_info)
        );

        let storage_opt = version.storage_opt();
        let level_infos = version.levels_info();

        // Pick a level to compact
        let level_start: &LevelInfo;
        let in_level;
        let out_level;
        if let Some((start_lvl, out_lvl)) = self.pick_level(level_infos) {
            level_start = &level_infos[start_lvl as usize];
            in_level = start_lvl;
            out_level = out_lvl;

            // If start_lvl is L1, compare the number of L1 files
            // with compact_trigger_file_num.
            if storage_opt.compact_trigger_file_num != 0
                && start_lvl == 1
                && (level_infos[1].files.len() as u32) < storage_opt.compact_trigger_file_num
            {
                info!(
                    "Picker: strategy: {}, picked L1 files({}) does not reach trigger({}), return None",
                    self.strategy,
                    level_infos[1].files.len(),
                    storage_opt.compact_trigger_file_num
                );
                return None;
            }
        } else {
            info!("Picker: strategy: {}, picked no level", self.strategy);
            return None;
        }

        // Pick selected level files.
        if level_start.files.is_empty() {
            return None;
        }

        let mut files = level_start.files.clone();
        files.sort_by(Self::compare_column_file);
        let picking_files: Vec<Arc<ColumnFile>> =
            Self::pick_files(files, storage_opt.max_compact_size);
        info!(
            "Picker: strategy: {}, Picked files: [ {} ]",
            self.strategy,
            format_column_files(&picking_files)
        );
        if picking_files.is_empty() {
            return None;
        }

        // Run compaction and send them to the next level, even if picked only 1 file,.
        Some(CompactReq {
            ts_family_id: version.ts_family_id,
            database: version.database.clone(),
            storage_opt: version.storage_opt.clone(),
            files: picking_files,
            version: version.clone(),
            in_level,
            out_level,
            time_range: level_infos[out_level as usize].time_range,
        })
    }
}

impl LevelCompactionPicker {
    pub fn new() -> LevelCompactionPicker {
        Self {
            strategy: "normal".to_string(),
            datetime: context_datetime(),
        }
    }

    /// Weight of file number of a level to be picked.
    fn level_weight_file_num(level: LevelId) -> f64 {
        match level {
            0 => 1.0,
            1 => 0.8,
            2 => 0.4,
            3 => 0.2,
            4 => 0.1,
            _ => 0.0,
        }
    }

    /// Weight of the ramaining size of a level to be picked.
    fn level_weight_remaining_size(level: LevelId) -> f64 {
        match level {
            0 => 1.0,
            1 => 1.0,
            2 => 1.0,
            3 => 1.0,
            4 => 1.0,
            _ => 0.0,
        }
    }

    fn compare_column_file(a: &Arc<ColumnFile>, b: &Arc<ColumnFile>) -> std::cmp::Ordering {
        match a.time_range().min_ts.cmp(&b.time_range().min_ts) {
            std::cmp::Ordering::Equal => a.size().cmp(&b.size()),
            ord => ord,
        }
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

            // let level_file_num_weight = (lvl.files.len() - compacting_files) as f64
            //     * Self::level_weight_file_num(lvl.level);
            // let level_remaining_size_weight = lvl.max_size.checked_sub(lvl.cur_size).unwrap_or(1)
            //     as f64
            //     * Self::level_weight_remaining_size(lvl.level);
            // let level_score = 10e6 * (level_file_num_weight / level_remaining_size_weight);

            let level_score: f64 = (lvl.files.len() - compacting_files) as f64
                * Self::level_weight_file_num(lvl.level);

            level_scores.push((lvl.level, lvl.cur_size, compacting_files, 0.0, level_score));
        }

        if level_scores.is_empty() {
            return None;
        }
        level_scores.sort_by(|(_, _, _, _, score_a), (_, _, _, _, score_b)| {
            score_a.partial_cmp(score_b).expect("a NaN score").reverse()
        });

        debug!(
            "Picker: strategy: {}, level scores: [ {} ]",
            self.strategy,
            level_scores
                .iter()
                .map(|lc| format!("{{ Level-{}: {} }}", lc.0, lc.4))
                .collect::<Vec<String>>()
                .join(", ")
        );

        level_scores.first().cloned().map(|(level, _, _, _, _)| {
            if level == 4 {
                (level, level)
            } else {
                (level, level + 1)
            }
        })
    }

    fn pick_files(src_files: Vec<Arc<ColumnFile>>, max_compact_size: u64) -> Vec<Arc<ColumnFile>> {
        let mut dst_files = Vec::with_capacity(src_files.len());

        let mut picking_file_size = 0_u64;
        for file in src_files.iter() {
            if file.is_compacting() || !file.mark_compacting() {
                // If file already compacting, continue to next file.
                continue;
            }
            picking_file_size += file.size();
            dst_files.push(file.clone());

            if picking_file_size >= max_compact_size {
                // Picked file size >= max_compact_size, try break picking files.
                break;
            }
        }

        dst_files
    }
}

/// Compaction picker for picking files in level-0 (delta files)
#[derive(Debug)]
pub struct DeltaCompactionPicker {
    strategy: String,
    /// Datetime when this picker created.
    datetime: String,
}

impl Picker for DeltaCompactionPicker {
    fn pick_compaction(&self, version: Arc<Version>) -> Option<CompactReq> {
        debug!(
            "Picker: strategy: {}, version: [ {} ]",
            self.strategy,
            format_level_infos(&version.levels_info)
        );

        let levels = version.levels_info();
        if levels[0].files.is_empty() {
            return None;
        }

        let mut level_overlaped_files: [Vec<Arc<ColumnFile>>; 5] =
            [vec![], Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        let mut file_picked: bool;
        let mut level_picking: usize;
        for file in levels[0].files.iter() {
            if file.is_compacting() {
                continue;
            }
            file_picked = false;
            level_picking = 4;
            // Form level-4 to level-1, put the overlapped files.
            for level in levels.iter().skip(1).rev() {
                if file.time_range().min_ts < level.time_range.max_ts {
                    level_overlaped_files[level_picking].push(file.clone());
                    file_picked = true;
                    break;
                }
                level_picking -= 1;
            }
            // If time_range of a file is too old than level-4, put to level-4 files.
            if !file_picked {
                level_overlaped_files[4].push(file.clone());
            }
        }
        debug!(
            "Picker: strategy: {}, level overlaped files: [ {} ]",
            self.strategy,
            level_overlaped_files
                .iter()
                .enumerate()
                .map(|(i, files)| format!("{{ Level-{}: {} }}", i, files.len()))
                .collect::<Vec<String>>()
                .join(", ")
        );

        // Find the level with maximum overlaped level-0 files.
        let mut out_level = 0;
        let mut max_files = 0_usize;
        for (i, files) in level_overlaped_files.iter().enumerate() {
            if files.len() > max_files {
                out_level = i;
                max_files = files.len();
            }
        }
        if out_level == 0 || max_files == 0 {
            info!(
                "Picker: strategy: {}, picked out-level is 0 or picked no files",
                self.strategy
            );
            return None;
        }

        // Pick files from level-0 files overlapped with that level.
        let max_compact_size = version.storage_opt.max_compact_size;
        let mut picking_files = Vec::with_capacity(level_overlaped_files.len());
        let mut picking_file_size = 0_u64;
        for file in level_overlaped_files[out_level].iter() {
            if file.is_compacting() || !file.mark_compacting() {
                // If file already compacting, continue to next file.
                continue;
            }
            picking_file_size += file.size();
            picking_files.push(file.clone());

            if picking_file_size >= max_compact_size {
                // Picked file size >= max_compact_size
                break;
            }
        }
        info!(
            "Picker: strategy: {}, Picked files: [ {} ]",
            self.strategy,
            format_column_files(&picking_files)
        );
        if picking_files.is_empty() {
            return None;
        }

        // Run compaction and send them to the target level, even if picked only 1 file,.
        Some(CompactReq {
            ts_family_id: version.ts_family_id,
            database: version.database.clone(),
            storage_opt: version.storage_opt.clone(),
            files: picking_files,
            version: version.clone(),
            in_level: 0,
            out_level: out_level as u32,
            time_range: levels[out_level].time_range,
        })
    }
}

impl DeltaCompactionPicker {
    pub fn new() -> Self {
        Self {
            strategy: "delta".to_string(),
            datetime: context_datetime(),
        }
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
    use crate::compaction::{DeltaCompactionPicker, LevelCompactionPicker, Picker};
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
        for (level, lts_min, lts_max, column_files_sketch) in levels_sketch {
            max_level_ts = max_level_ts.max(lts_max);
            let mut col_files = Vec::new();
            let mut cur_size = 0_u64;
            for (file_id, fts_min, fts_max, file_size, compacting) in column_files_sketch {
                cur_size += file_size;
                let col = ColumnFile::new(
                    file_id,
                    level,
                    TimeRange::new(fts_min, fts_max),
                    file_size,
                    level == 0,
                    make_tsm_file_name(tsm_dir, file_id),
                );
                if compacting {
                    col.mark_compacting();
                }
                col_files.push(Arc::new(col));
            }
            level_infos[level as usize] = LevelInfo {
                files: col_files,
                database: database.clone(),
                tsf_id: 0,
                storage_opt: opt.storage.clone(),
                level,
                cur_size,
                max_size: opt.storage.level_max_file_size(level),
                time_range: TimeRange::new(lts_min, lts_max),
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
            MemCache::new(1, 1000, 2, 1, &memory_pool),
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
    fn test_pick_level_files_1() {
        //! There are Level 0-4, and Level 1 is now in compaction.
        //! In this case, Level 2, and serial files in Level 0 will be picked,
        //! and compact to Level 3.
        let dir = "/tmp/test/pick/level_files_1";
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

        let tsf = create_tseries_family(Arc::new("dba".to_string()), opt, levels_sketch);
        let picker = LevelCompactionPicker::new();
        let compact_req = picker.pick_compaction(tsf.version()).unwrap();
        assert_eq!(compact_req.out_level, 2);
        assert_eq!(compact_req.files.len(), 2);
    }

    #[test]
    fn test_pick_delta_files_1() {
        //! There are Level 0-4, and Level 0 is now in compaction.
        //! In this case, Level 2, and serial files in Level 0 will be picked,
        //! and compact to Level 3.
        let dir = "/tmp/test/pick/delta_files_1";
        let opt = create_options(dir.to_string());

        #[rustfmt::skip]
        let levels_sketch: LevelsSketch = vec![
            // vec![( level, Timestamp_Begin, Timestamp_end, vec![(file_id, Timestamp_Begin, Timestamp_end, size, being_compact)] )]
            (0_u32, 1_i64, 370_i64, vec![
                (11_u64, 1_i64, 5_i64, 10_u64, false), // 4
                (12, 10, 20, 10, true),                // 4
                (12, 30, 40, 10, false),               // 4
                (12, 110, 120, 10, false),             // 4
                (12, 230, 240, 10, false),             // 3
                (12, 300, 350, 10, false),             // 3
                (12, 340, 350, 10, false),             // 2
                (12, 360, 370, 10, false),             // 1
            ]),
            (1, 341, 380, vec![
                (7, 341, 350, 1000, false),
                (8, 351, 360, 1000, false),
                (9, 361, 370, 1000, true),
                (10, 371, 380, 1000, true),
            ]), // 1
            (2, 301, 340, vec![
                (5, 301, 320, 2000, false),
                (6, 321, 340, 2000, false),
            ]), // 1
            (3, 201, 300, vec![
                (3, 201, 250, 5000, false),
                (4, 251, 300, 5000, false),
            ]), // 2
            (4, 1, 200, vec![
                (1, 10, 100, 10000, false),
                (2, 101, 200, 10000, false),
            ]), // 3
        ];

        let tsf = create_tseries_family(Arc::new("dba".to_string()), opt, levels_sketch);
        let picker = DeltaCompactionPicker::new();
        let compact_req = picker.pick_compaction(tsf.version()).unwrap();
        assert_eq!(compact_req.out_level, 4);
        assert_eq!(compact_req.files.len(), 3);
    }
}
