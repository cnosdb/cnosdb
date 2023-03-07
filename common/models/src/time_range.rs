use std::cmp;
use std::fmt::Display;
use std::ops::Bound;

use crate::Timestamp;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeRange {
    pub min_ts: i64,
    pub max_ts: i64,
}

impl From<(Bound<i64>, Bound<i64>)> for TimeRange {
    /// TODO 目前TimeRange只支持闭区间
    fn from(range: (Bound<i64>, Bound<i64>)) -> Self {
        let min_ts = match range.0 {
            Bound::Excluded(v) | Bound::Included(v) => v,
            _ => Timestamp::MIN,
        };
        let max_ts = match range.1 {
            Bound::Excluded(v) | Bound::Included(v) => v,
            _ => Timestamp::MAX,
        };

        TimeRange { min_ts, max_ts }
    }
}

impl TimeRange {
    pub fn new(min_ts: i64, max_ts: i64) -> Self {
        Self { min_ts, max_ts }
    }

    pub fn all() -> Self {
        Self {
            min_ts: Timestamp::MIN,
            max_ts: Timestamp::MAX,
        }
    }

    #[inline(always)]
    pub fn overlaps(&self, range: &TimeRange) -> bool {
        !(self.min_ts > range.max_ts || self.max_ts < range.min_ts)
    }

    #[inline(always)]
    pub fn includes(&self, other: &TimeRange) -> bool {
        self.min_ts <= other.min_ts && self.max_ts >= other.max_ts
    }

    #[inline(always)]
    pub fn merge(&mut self, other: &TimeRange) {
        self.min_ts = self.min_ts.min(other.min_ts);
        self.max_ts = self.max_ts.max(other.max_ts);
    }

    #[inline(always)]
    pub fn is_boundless(&self) -> bool {
        self.min_ts == Timestamp::MIN && self.max_ts == Timestamp::MAX
    }

    #[inline(always)]
    pub fn contains(&self, time_stamp: Timestamp) -> bool {
        time_stamp >= self.min_ts && time_stamp <= self.max_ts
    }
}

impl From<(Timestamp, Timestamp)> for TimeRange {
    fn from(time_range: (Timestamp, Timestamp)) -> Self {
        Self {
            min_ts: time_range.0,
            max_ts: time_range.1,
        }
    }
}

impl From<TimeRange> for (Timestamp, Timestamp) {
    fn from(t: TimeRange) -> Self {
        (t.min_ts, t.max_ts)
    }
}

impl PartialOrd for TimeRange {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimeRange {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match self.min_ts.cmp(&other.min_ts) {
            cmp::Ordering::Equal => self.max_ts.cmp(&other.max_ts),
            other => other,
        }
    }
}

impl Display for TimeRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.min_ts, self.max_ts)
    }
}

#[derive(PartialEq, Eq)]
pub enum TimeRangeCmp {
    /// A has no intersection with B
    Exclude,
    /// A includes B
    Include,
    /// A overlaps with B
    Intersect,
}
