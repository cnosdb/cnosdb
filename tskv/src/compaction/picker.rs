use std::{cmp::Ordering, collections::HashMap, ops::Div, sync::Arc};

use trace::error;

use crate::{
    compaction::CompactReq,
    direct_io::File,
    error::Result,
    kv_option::TseriesFamOpt,
    tseries_family::{ColumnFile, LevelInfo, Version},
    TseriesFamilyId,
};

pub trait Picker: Send + Sync {
    fn pick_compaction(&self, tsf_id: TseriesFamilyId, version: Arc<Version>)
        -> Option<CompactReq>;
}

pub struct LevelCompactionPicker {
    ts_family_opt: Arc<TseriesFamOpt>,
}

impl Picker for LevelCompactionPicker {
    fn pick_compaction(
        &self,
        tsf_id: TseriesFamilyId,
        version: Arc<Version>,
    ) -> Option<CompactReq> {
        let level_infos = version.levels_info();

        let mut ctx = LevelCompatContext::default();
        ctx.cal_score(level_infos, self.ts_family_opt.as_ref());
        if let Some((start_level, out_lvl)) = ctx.pick_level() {
            let input = ctx.pick_files(
                level_infos,
                self.ts_family_opt.as_ref(),
                start_level,
                out_lvl,
            );
            if let Some((level, mut files)) = input {
                for file in files.iter_mut() {
                    file.mark_compaction();
                }
                let request = CompactReq {
                    ts_family_id: tsf_id,
                    ts_family_opt: self.ts_family_opt.clone(),
                    files,
                    version,
                    out_level: out_lvl,
                };
                return Some(request);
            }
        }
        None
    }
}

impl LevelCompactionPicker {
    pub fn new(ts_family_opt: Arc<TseriesFamOpt>) -> Self {
        Self { ts_family_opt }
    }
}

#[derive(Default)]
struct LevelCompatContext {
    level_scores: Vec<(u32, f64)>,
    base_level: u32,
    max_level: u32,
}

impl LevelCompatContext {
    fn cal_score(&mut self, level_infos: &[LevelInfo; 5], opts: &TseriesFamOpt) {
        let mut level0_being_compact = false;
        for t in &level_infos[0].files {
            if t.is_compacting() {
                level0_being_compact = true;
                break;
            }
        }
        let l0_size = opts.base_file_size;
        let base_level = 0;

        if !level0_being_compact {
            let score = level_infos[0].files.len() as f64 / opts.compact_trigger as f64;
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
        opts: &TseriesFamOpt,
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
        let max_size = opts.level_file_size(output_level);
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
