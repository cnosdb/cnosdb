/// Returns if r1 (min_ts, max_ts) overlaps r2 (min_ts, max_ts)
pub fn overlaps_tuples(r1: (i64, i64), r2: (i64, i64)) -> bool {
    r1.0 <= r2.1 && r1.1 >= r2.0
}
