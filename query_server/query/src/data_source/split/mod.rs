use std::cmp;
use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::execution::context::SessionState;
use models::predicate::domain::TimeRange;
use models::predicate::utils::filter_to_time_ranges;
use models::predicate::Split;
use trace::debug;

use self::tskv::TableLayoutHandle;

pub mod tskv;

pub type SplitManagerRef = Arc<SplitManager>;

#[cfg(test)]
pub fn default_split_manager_ref_only_for_test() -> SplitManagerRef {
    use coordinator::service_mock::MockCoordinator;
    Arc::new(SplitManager::new(Arc::new(MockCoordinator::default())))
}

#[non_exhaustive]
pub struct SplitManager {}

impl SplitManager {
    pub fn new(_coord: CoordinatorRef) -> Self {
        // TODO: use coordinator to get splits
        Self {}
    }

    pub fn splits(&self, ctx: &SessionState, table_layout: TableLayoutHandle) -> Vec<Split> {
        let TableLayoutHandle {
            db,
            table,
            predicate,
        } = table_layout;

        let time_filter = predicate
            .filter()
            .translate_column(|e| Some(e.name.clone()));

        let time_ranges = filter_to_time_ranges(&time_filter);
        let db_time_range = db.time_range();
        let target_partitions = ctx.config().target_partitions();

        debug!("Get table {}'s splits, filter time ranges: {:?}, db time range: {}, target_partitions: {}", table.name, time_ranges, db_time_range, target_partitions);

        let splited_time_ranges = split_time_range(&time_ranges, &db_time_range, target_partitions);

        debug!(
            "Table {}'s {} splits: {:?}",
            table.name,
            splited_time_ranges.len(),
            splited_time_ranges
        );

        splited_time_ranges
            .into_iter()
            .enumerate()
            .map(|(id, time_range)| Split::new(id, table.clone(), time_range, predicate.clone()))
            .collect()
    }
}

pub fn split_time_range(
    ori_time_range: &[TimeRange],
    db_time_range: &TimeRange,
    target_partitions: usize,
) -> Vec<TimeRange> {
    let time_ranges = ori_time_range
        .iter()
        .flat_map(|e| e.intersect(db_time_range))
        .collect::<Vec<_>>();

    let total_time = time_ranges
        .iter()
        .map(|e| e.total_time())
        .reduce(|l, r| l + r)
        .unwrap_or(0);

    debug!(
        "time_ranges: {:?}\ntotal_time: {:?}",
        time_ranges, total_time
    );

    if total_time == 0 {
        return vec![];
    }

    let actual_partitions = cmp::min(total_time, target_partitions as u64);
    let range_per_partition = (total_time / actual_partitions) as i64;

    debug!("range_per_partition: {}", range_per_partition);

    let final_time_ranges = time_ranges
        .iter()
        .flat_map(|tr| {
            let mut time_ranges = vec![];
            let mut current_ts = tr.min_ts;
            while current_ts < tr.max_ts - range_per_partition {
                let time_range = TimeRange::new(current_ts, current_ts + (range_per_partition - 1));
                time_ranges.push(time_range);
                current_ts += range_per_partition;
            }

            let time_range = TimeRange::new(current_ts, tr.max_ts);
            time_ranges.push(time_range);
            time_ranges
        })
        .collect::<Vec<_>>();

    final_time_ranges
}
