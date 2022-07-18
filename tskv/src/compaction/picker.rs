use std::{cmp::Ordering, collections::HashMap, ops::Div, sync::Arc};

use crate::{
    compaction::CompactReq,
    direct_io::File,
    error::Result,
    kv_option::TseriesFamOpt,
    tseries_family::{ColumnFile, Version},
    TseriesFamilyId,
};

pub struct LevelCompactionPicker {
    tseries_fam_opts: HashMap<u32, Arc<TseriesFamOpt>>,
}

impl LevelCompactionPicker {
    pub fn new(tsries_fam_opts: HashMap<u32, Arc<TseriesFamOpt>>) -> Self {
        Self { tseries_fam_opts: tsries_fam_opts }
    }

    pub fn pick_compaction(&self,
                           tsf_id: TseriesFamilyId,
                           version: Arc<Version>)
                           -> Option<CompactReq> {
        let opts = self.tseries_fam_opts.get(&tsf_id).cloned().unwrap();
        let mut ctx = LevelCompatContext::default();
        ctx.cal_score(version.as_ref(), opts.as_ref());
        if let Some((start_level, out_lvl)) = ctx.pick_level() {
            let input = ctx.pick_files(version.as_ref(), opts.as_ref(), start_level, out_lvl);
            if let Some((lvl, mut files)) = input {
                for file in files.iter_mut() {
                    file.mark_compaction();
                }
                let request = CompactReq { files: (lvl, files),
                                           version,
                                           tsf_id,
                                           out_level: out_lvl /* target_file_size_base:
                                                               * opts.target_file_size_base,
                                                               * cf_options: opts,
                                                               * options: self.db_opts.
                                                               * clone(), */ };
                return Some(request);
            }
        }
        None
    }
}
#[derive(Default)]
struct LevelCompatContext {
    level_scores: Vec<(u32, f64)>,
    base_level: u32,
    max_level: u32,
}

impl LevelCompatContext {
    fn cal_score(&mut self, version: &Version, opts: &TseriesFamOpt) {
        let info = version.levels_info();
        let mut level0_being_compact = false;
        for t in &info[0].files {
            if t.is_pending_compaction() {
                level0_being_compact = true;
                break;
            }
        }
        let l0_size = opts.base_file_size;
        let base_level = 0;

        if !level0_being_compact {
            let score = info[0].files.len() as f64 / opts.compact_trigger as f64;
            self.level_scores
                .push((0,
                       f64::max(score,
                                info[0].cur_size as f64 / info[base_level].max_size as f64)));
        }
        for (l, item) in info.iter().enumerate() {
            let score = item.cur_size.checked_div(item.max_size).unwrap();
            self.level_scores.push((l as u32, score as f64));
        }
        self.base_level = 0;
        self.max_level = info.len() as u32 - 1;
    }

    fn pick_level(&mut self) -> Option<(u32, u32)> {
        self.level_scores
            .sort_by(|a, b| if a.1 > b.1 { Ordering::Less } else { Ordering::Greater });
        let base_level = self.base_level;
        if let Some((level, score)) = self.level_scores.first() {
            if *score < 1.0 {
                return None;
            } else if *level == 0 {
                return Some((*level, base_level));
            } else if *level + 1 == self.max_level {
                return Some((*level, *level));
            } else {
                return Some((*level, *level + 1));
            }
        }
        None
    }

    fn pick_files(&self,
                  version: &Version,
                  opts: &TseriesFamOpt,
                  level: u32,
                  output_level: u32)
                  -> Option<(u32, Vec<Arc<ColumnFile>>)> {
        let infos = version.levels_info();
        if level > (infos.len() - 1) as u32 {
            return None;
        }
        let mut inputs = vec![];
        let mut ts_min = i64::MAX;
        let mut ts_max = i64::MIN;
        let mut file_size = 0;
        let max_size = opts.level_file_size(output_level);
        let lvl_info = &infos[level as usize];
        for file in &lvl_info.files {
            file_size += file.size();
            if ts_min > file.range().min_ts {
                ts_min = file.range().min_ts;
            }

            if ts_max < file.range().max_ts {
                ts_max = file.range().max_ts;
            }
            inputs.push(file.clone());
            if file_size >= max_size {
                break;
            }
        }
        Some((level, inputs))
    }
}
