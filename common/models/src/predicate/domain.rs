use std::cmp::{self, max, min, Ordering};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::hash::Hash;
use std::ops::{Bound as StdBound, RangeBounds};
use std::sync::Arc;

use arrow_schema::Schema;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::Expr;
use datafusion::optimizer::utils::conjunction;
use datafusion::prelude::Column;
use datafusion::scalar::ScalarValue;
use datafusion_proto::protobuf;
use protos::models_helper::to_prost_bytes;
use serde::de::Visitor;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};

use super::transformation::RowExpressionToDomainsVisitor;
use super::utils::filter_to_time_ranges;
use super::PlacedSplit;
use crate::schema::{
    ColumnType, ScalarValueForkDF, TableColumn, TskvTableSchema, TskvTableSchemaRef,
};
use crate::{Error, Result, Timestamp};

pub type PredicateRef = Arc<Predicate>;
pub type ResolvedPredicateRef = Arc<ResolvedPredicate>;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimeRange {
    pub min_ts: i64,
    pub max_ts: i64,
}

impl From<(StdBound<i64>, StdBound<i64>)> for TimeRange {
    // TODO: TimeRange is a closed interval now.
    // TODO: Using TimeRange { min_ts: Bound, max_ts: Bound }
    fn from(range: (StdBound<i64>, StdBound<i64>)) -> Self {
        let min_ts = match range.0 {
            StdBound::Excluded(v) => v + 1,
            StdBound::Included(v) => v,
            _ => Timestamp::MIN,
        };
        let max_ts = match range.1 {
            StdBound::Excluded(v) => v - 1,
            StdBound::Included(v) => v,
            _ => Timestamp::MAX,
        };

        TimeRange { min_ts, max_ts }
    }
}

impl TimeRange {
    /// notice: when min_ts > max_ts, time_range is considered [`Self::none()`]
    pub fn new(min_ts: i64, max_ts: i64) -> Self {
        Self { min_ts, max_ts }
    }

    pub fn all() -> Self {
        Self {
            min_ts: Timestamp::MIN,
            max_ts: Timestamp::MAX,
        }
    }

    pub fn none() -> Self {
        Self {
            min_ts: Timestamp::MAX,
            max_ts: Timestamp::MIN,
        }
    }

    #[inline(always)]
    pub fn is_boundless(&self) -> bool {
        self.min_ts == Timestamp::MIN && self.max_ts == Timestamp::MAX
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
    pub fn contains(&self, time_stamp: Timestamp) -> bool {
        time_stamp >= self.min_ts && time_stamp <= self.max_ts
    }

    pub fn intersect(&self, other: &Self) -> Option<Self> {
        if self.overlaps(other) {
            // There is overlap, calculate the intersection
            let min_ts = cmp::max(self.min_ts, other.min_ts);
            let max_ts = cmp::min(self.max_ts, other.max_ts);

            return Some(Self { min_ts, max_ts });
        }

        None
    }

    pub fn total_time(&self) -> u64 {
        if self.max_ts < self.min_ts {
            return 0;
        }
        (self.max_ts as i128 - self.min_ts as i128) as u64 + 1_u64
    }

    #[inline(always)]
    pub fn merge(&mut self, other: &TimeRange) {
        self.min_ts = self.min_ts.min(other.min_ts);
        self.max_ts = self.max_ts.max(other.max_ts);
    }

    pub fn exclude(&self, other: &TimeRange) -> (Option<TimeRange>, Option<TimeRange>) {
        //              other
        //           |__________|
        // self
        // |____________________|
        let left = if self.min_ts < other.min_ts {
            Some(TimeRange::new(
                self.min_ts,
                min(self.max_ts, other.min_ts - 1),
            ))
        } else {
            None
        };
        // other
        // |___________|
        //               self
        // |________________|
        let right = if self.max_ts > other.max_ts {
            Some(TimeRange::new(
                max(self.min_ts, other.max_ts + 1),
                self.max_ts,
            ))
        } else {
            None
        };
        (left, right)
    }

    pub fn compact(time_ranges: &mut Vec<TimeRange>) {
        if time_ranges.is_empty() {
            return;
        }

        // Start compact with sorted ascending time ranges
        time_ranges.sort();

        fn can_compact(a: &TimeRange, b: &TimeRange) -> bool {
            (a.min_ts <= b.max_ts && a.max_ts >= b.min_ts)
                || (a.max_ts.checked_add(1).is_some_and(|t| t == b.min_ts))
                || (b.max_ts.checked_add(1).is_some_and(|t| t == a.min_ts))
        }

        let mut i = 0;
        while i < time_ranges.len() - 1 {
            if can_compact(&time_ranges[i], &time_ranges[i + 1]) {
                let mut j = i + 1;
                while j < time_ranges.len() && can_compact(&time_ranges[i], &time_ranges[j]) {
                    time_ranges[i].max_ts = time_ranges[i].max_ts.max(time_ranges[j].max_ts);
                    j += 1;
                }
                time_ranges.drain(i + 1..j);
            } else {
                i += 1;
            }
        }
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
        write!(f, "[{}, {}]", self.min_ts, self.max_ts)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRanges {
    // Sorted time ranges.
    inner: Vec<TimeRange>,
    min_ts: Timestamp,
    max_ts: Timestamp,
    is_boundless: bool,
}

impl TimeRanges {
    pub fn new(time_ranges: Vec<TimeRange>) -> Self {
        let TimeRange { min_ts, max_ts } = Self::time_range_of_time_ranges(&time_ranges);
        if min_ts == Timestamp::MIN && max_ts == Timestamp::MAX {
            Self::all()
        } else {
            Self {
                inner: time_ranges,
                min_ts,
                max_ts,
                is_boundless: false,
            }
        }
    }

    pub fn with_inclusive_bounds(min_ts: i64, max_ts: i64) -> Self {
        Self {
            inner: vec![TimeRange::new(min_ts, max_ts)],
            min_ts,
            max_ts,
            is_boundless: min_ts == Timestamp::MIN && max_ts == Timestamp::MAX,
        }
    }

    pub fn all() -> Self {
        Self {
            inner: vec![TimeRange::all()],
            min_ts: Timestamp::MIN,
            max_ts: Timestamp::MAX,
            is_boundless: true,
        }
    }

    pub fn empty() -> Self {
        Self {
            inner: vec![],
            min_ts: Timestamp::MAX,
            max_ts: Timestamp::MIN,
            is_boundless: false,
        }
    }

    fn time_range_of_time_ranges(time_ranges: &[TimeRange]) -> TimeRange {
        let mut min_ts = Timestamp::MAX;
        let mut max_ts = Timestamp::MIN;
        for tr in time_ranges {
            min_ts = min_ts.min(tr.min_ts);
            max_ts = max_ts.max(tr.max_ts);
        }
        TimeRange { min_ts, max_ts }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn time_ranges(&self) -> &[TimeRange] {
        &self.inner
    }

    pub fn min_ts(&self) -> Timestamp {
        self.min_ts
    }

    pub fn max_ts(&self) -> Timestamp {
        self.max_ts
    }

    pub fn is_boundless(&self) -> bool {
        self.is_boundless
    }

    pub fn overlaps(&self, time_range: &TimeRange) -> bool {
        self.max_time_range().overlaps(time_range)
            && self.inner.iter().any(|tr| tr.overlaps(time_range))
    }

    pub fn includes(&self, time_range: &TimeRange) -> bool {
        if self.inner.is_empty()
            || time_range.max_ts < self.min_ts
            || time_range.min_ts > self.max_ts
        {
            return false;
        }
        match self
            .inner
            .binary_search_by_key(&time_range.min_ts, |tr| tr.min_ts)
        {
            Ok(i) => {
                for tr in self.inner[i..].iter() {
                    if time_range.max_ts <= tr.max_ts {
                        return true;
                    }
                    if time_range.max_ts > tr.max_ts {
                        return false;
                    }
                }
                false
            }
            Err(i) => {
                if i == 0 {
                    false
                } else {
                    for tr in self.inner[..i].iter().rev() {
                        if tr.max_ts >= time_range.max_ts {
                            return true;
                        }
                    }
                    false
                }
            }
        }
    }

    pub fn contains(&self, timestamp: Timestamp) -> bool {
        if self.inner.is_empty() || timestamp < self.min_ts || timestamp > self.max_ts {
            return false;
        }
        match self.inner.binary_search_by_key(&timestamp, |tr| tr.min_ts) {
            Ok(_) => true,
            Err(i) => {
                if i == 0 {
                    false
                } else if i == self.inner.len() {
                    let tr = unsafe { self.inner.get_unchecked(i - 1) };
                    tr.max_ts >= timestamp
                } else {
                    for tr in self.inner[..i].iter().rev() {
                        if tr.max_ts >= timestamp {
                            return true;
                        }
                    }
                    false
                }
            }
        }
    }

    pub fn intersect(&self, time_range: &TimeRange) -> Option<Self> {
        if self.overlaps(time_range) {
            let mut new_time_ranges = vec![];
            for tr in self.inner.iter() {
                if let Some(intersect_tr) = tr.intersect(time_range) {
                    new_time_ranges.push(intersect_tr);
                }
            }
            return Some(Self::new(new_time_ranges));
        }

        None
    }

    pub fn max_time_range(&self) -> TimeRange {
        TimeRange::new(self.min_ts, self.max_ts)
    }

    pub fn exclude(&self, time_range: &TimeRange) -> TimeRanges {
        let trs = self
            .inner
            .iter()
            .flat_map(|tr| {
                let tr_tuple = tr.exclude(time_range);
                [tr_tuple.0, tr_tuple.1]
            })
            .collect::<Vec<Option<TimeRange>>>()
            .into_iter()
            .flatten()
            .collect::<Vec<TimeRange>>();
        TimeRanges::new(trs)
    }

    pub fn exclude_time_ranges(&self, other: &[&TimeRange]) -> TimeRanges {
        let mut res = self.clone();
        other.iter().for_each(|o| res = res.exclude(o));
        res
    }
}

impl AsRef<[TimeRange]> for TimeRanges {
    fn as_ref(&self) -> &[TimeRange] {
        &self.inner
    }
}

impl From<(Timestamp, Timestamp)> for TimeRanges {
    fn from(time_range: (Timestamp, Timestamp)) -> Self {
        Self::with_inclusive_bounds(time_range.0, time_range.1)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Bound {
    /// lower than the value, but infinitesimally close to the value.
    Below,
    /// exactly the value.
    Exactly,
    /// higher than the value, but infinitesimally close to the value.
    Above,
}

impl Display for Bound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Bound::Below => write!(f, "<"),
            Bound::Exactly => write!(f, "="),
            Bound::Above => write!(f, ">"),
        }
    }
}

/// A point on the continuous space defined by the specified type.
///
/// Each point may be just below, exact, or just above the specified value according to the Bound.
#[derive(Debug, Clone)]
pub struct Marker {
    data_type: DataType,
    value: Option<ScalarValue>,
    bound: Bound,
}

impl Serialize for Marker {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value: Option<ScalarValueForkDF> = self.value.clone().map(|e| e.into());

        let mut ve = serializer.serialize_struct("Marker", 3)?;
        ve.serialize_field("data_type", &self.data_type)?;
        ve.serialize_field("value", &value)?;
        ve.serialize_field("bound", &self.bound)?;
        ve.end()
    }
}

impl<'a> Deserialize<'a> for Marker {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let mark = MarkerSerialize::deserialize(deserializer)?;

        Ok(mark.into())
    }
}

#[derive(Serialize, Deserialize)]
struct MarkerSerialize {
    data_type: DataType,
    value: Option<ScalarValueForkDF>,
    bound: Bound,
}

impl From<MarkerSerialize> for Marker {
    fn from(mark: MarkerSerialize) -> Self {
        let MarkerSerialize {
            data_type,
            value,
            bound,
        } = mark;
        Self {
            data_type,
            value: value.map(|e| e.into()),
            bound,
        }
    }
}

struct MarkerVisitor;

impl<'de> Visitor<'de> for MarkerVisitor {
    type Value = MarkerSerialize;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("This Visitor expects to receive Marker")
    }
}

impl<'a> From<&'a Marker> for std::ops::Bound<&'a ScalarValue> {
    fn from(marker: &'a Marker) -> Self {
        if marker.is_lower_unbound() || marker.is_upper_unbound() {
            return std::ops::Bound::Unbounded;
        }

        match marker.bound {
            Bound::Below => unsafe {
                std::ops::Bound::Excluded(marker.value.as_ref().unwrap_unchecked())
            },
            Bound::Exactly => unsafe {
                std::ops::Bound::Included(marker.value.as_ref().unwrap_unchecked())
            },
            Bound::Above => unsafe {
                std::ops::Bound::Excluded(marker.value.as_ref().unwrap_unchecked())
            },
        }
    }
}

impl Marker {
    /// Infinitely (greater than a nonexistent number).
    fn lower_unbound(data_type: DataType) -> Marker {
        Self {
            data_type,
            value: None,
            bound: Bound::Above,
        }
    }

    /// Greater than the value.
    fn lower_bounded(data_type: DataType, value: ScalarValue) -> Marker {
        Self {
            data_type,
            value: Some(value),
            bound: Bound::Above,
        }
    }

    /// Equal to the value.
    fn exactly(data_type: DataType, value: ScalarValue) -> Marker {
        Self {
            data_type,
            value: Some(value),
            bound: Bound::Exactly,
        }
    }

    /// Infinitely small (less than a nonexistent number).
    fn upper_unbound(data_type: DataType) -> Marker {
        Self {
            data_type,
            value: None,
            bound: Bound::Below,
        }
    }

    /// Less than the value.
    fn upper_bounded(data_type: DataType, value: ScalarValue) -> Marker {
        Self {
            data_type,
            value: Some(value),
            bound: Bound::Below,
        }
    }

    fn is_lower_unbound(&self) -> bool {
        match (&self.value, &self.bound) {
            (None, Bound::Above) => true,
            (_, _) => false,
        }
    }

    fn is_upper_unbound(&self) -> bool {
        match (&self.value, &self.bound) {
            (None, Bound::Below) => true,
            (_, _) => false,
        }
    }
    /// Check if data types are compatible.
    fn check_type_compatibility(&self, other: &Self) -> bool {
        self.data_type == other.data_type
    }
}

impl PartialEq for Marker {
    fn eq(&self, other: &Self) -> bool {
        if !self.check_type_compatibility(other) {
            return false;
        }

        if self.is_upper_unbound() {
            return other.is_upper_unbound();
        }

        if self.is_lower_unbound() {
            return other.is_lower_unbound();
        }
        // self not unbound
        if other.is_upper_unbound() {
            return false;
        }
        // self not unbound
        if other.is_lower_unbound() {
            return false;
        }
        // value and other.value are present
        self.value
            .as_ref()
            .unwrap()
            .eq(other.value.as_ref().unwrap())
    }
}
impl Ord for Marker {
    fn cmp(&self, other: &Self) -> Ordering {
        debug_assert!(self.check_type_compatibility(other));
        // (∞, ∞)
        // => Equal
        // (∞, _)
        // => Greater
        if self.is_upper_unbound() {
            return if other.is_upper_unbound() {
                std::cmp::Ordering::Equal
            } else {
                std::cmp::Ordering::Greater
            };
        }
        // (-∞, -∞)
        // => Equal
        // (-∞, _)
        // => Less
        if self.is_lower_unbound() {
            return if other.is_lower_unbound() {
                std::cmp::Ordering::Equal
            } else {
                std::cmp::Ordering::Less
            };
        }
        // self not unbound
        // (_, ∞)
        // => Less
        if other.is_upper_unbound() {
            return std::cmp::Ordering::Less;
        }
        // self not unbound
        // (_, -∞)
        // => Greater
        if other.is_lower_unbound() {
            return std::cmp::Ordering::Greater;
        }
        // value and other.value are present
        let self_data = self.value.as_ref().unwrap();
        let other_data = other.value.as_ref().unwrap();

        self_data.partial_cmp(other_data).unwrap()
    }
}

impl PartialOrd for Marker {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Marker {}

impl Display for Marker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.bound, self.data_type)?;
        if let Some(ref v) = self.value {
            return write!(f, "{}", v);
        }
        Ok(())
    }
}

/// A Range of values across the continuous space defined by the types of the Markers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Range {
    low: Marker,
    high: Marker,
}

impl RangeBounds<ScalarValue> for Range {
    fn start_bound(&self) -> std::ops::Bound<&ScalarValue> {
        self.low_ref().into()
    }

    fn end_bound(&self) -> std::ops::Bound<&ScalarValue> {
        self.high_ref().into()
    }
}

impl Display for Range {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn write_scalar_value_opt_for_bound_display(
            f: &mut std::fmt::Formatter<'_>,
            marker: &Marker,
            is_left_bound: bool,
        ) -> std::fmt::Result {
            write!(f, "({})", marker.data_type)?;
            if let Some(ref v) = &marker.value {
                write!(f, "{}", v)
            } else if is_left_bound {
                write!(f, "-∞")
            } else {
                write!(f, "∞")
            }
        }
        fn write_marker_for_bound_display(
            f: &mut std::fmt::Formatter<'_>,
            marker: &Marker,
            is_left_bound: bool,
        ) -> std::fmt::Result {
            match (is_left_bound, &marker.bound) {
                (true, Bound::Above) => {
                    write!(f, "(",)?;
                    write_scalar_value_opt_for_bound_display(f, marker, is_left_bound)
                }
                (false, Bound::Below) => {
                    write_scalar_value_opt_for_bound_display(f, marker, is_left_bound)?;
                    write!(f, ")")
                }
                (true, Bound::Exactly) => {
                    write!(f, "[",)?;
                    write_scalar_value_opt_for_bound_display(f, marker, is_left_bound)
                }
                (false, Bound::Exactly) => {
                    write_scalar_value_opt_for_bound_display(f, marker, is_left_bound)?;
                    write!(f, "]")
                }
                _ => {
                    // Unexpected patterns.
                    write!(f, "#")?;
                    write_scalar_value_opt_for_bound_display(f, marker, is_left_bound)?;
                    write!(f, "#")
                }
            }
        }

        if self.low.is_lower_unbound() {
            write!(f, "(-∞")?;
        } else {
            write_marker_for_bound_display(f, &self.low, true)?;
        }
        write!(f, ", ")?;
        if self.high.is_upper_unbound() {
            write!(f, "∞)")
        } else {
            write_marker_for_bound_display(f, &self.high, false)
        }
    }
}

impl Range {
    fn get_type(&self) -> &DataType {
        &self.low.data_type
    }
    /// Check if data types are compatible.
    ///
    /// Returns an exception if the data types do not match.
    ///
    /// Returns the data type of self if the data types match.
    fn check_type_compatibility(&self, other: &Range) -> Result<DataType> {
        if !self.low.check_type_compatibility(&other.low) {
            return Err(Error::Internal {
                err: format!(
                    "mismatched types: excepted {}, found {}",
                    self.get_type(),
                    other.get_type()
                ),
            });
        }

        Ok(self.low.data_type.clone())
    }
    /// Constructs a range of values match all records (-∞, +∞).
    pub fn all(data_type: &DataType) -> Range {
        let low = Marker {
            data_type: data_type.clone(),
            value: None,
            bound: Bound::Above,
        };
        let high = Marker {
            data_type: data_type.clone(),
            value: None,
            bound: Bound::Below,
        };

        Self { low, high }
    }
    /// Constructs a range of values equal to scalar_value [scalar_value, scalar_value].
    pub fn eq(data_type: &DataType, scalar_value: &ScalarValue) -> Range {
        let low = Marker::exactly(data_type.clone(), scalar_value.clone());
        let high = Marker::exactly(data_type.clone(), scalar_value.clone());

        Self { low, high }
    }
    /// TODO Constructs a range of values not equal to scalar_value [scalar_value, scalar_value].
    pub fn ne(data_type: &DataType, scalar_value: &ScalarValue) -> Vec<Range> {
        vec![
            Self {
                low: Marker::lower_unbound(data_type.clone()),
                high: Marker::upper_bounded(data_type.clone(), scalar_value.clone()),
            },
            Self {
                low: Marker::lower_bounded(data_type.clone(), scalar_value.clone()),
                high: Marker::upper_unbound(data_type.clone()),
            },
        ]
    }
    /// Construct a range of values greater than scalar_value (scalar_value, +∞).
    pub fn gt(data_type: &DataType, scalar_value: &ScalarValue) -> Range {
        let low = Marker::lower_bounded(data_type.clone(), scalar_value.clone());
        let high = Marker::upper_unbound(data_type.clone());

        Self { low, high }
    }
    /// Construct a range of values greater than or equal to scalar_value [scalar_value, +∞).
    pub fn ge(data_type: &DataType, scalar_value: &ScalarValue) -> Range {
        let low = Marker::exactly(data_type.clone(), scalar_value.clone());
        let high = Marker::upper_unbound(data_type.clone());

        Self { low, high }
    }
    /// Construct a range of values smaller than scalar_value (-∞, scalar_value).
    pub fn lt(data_type: &DataType, scalar_value: &ScalarValue) -> Range {
        let low = Marker::lower_unbound(data_type.clone());
        let high = Marker::upper_bounded(data_type.clone(), scalar_value.clone());

        Self { low, high }
    }
    /// Construct a range of values less than or equal to scalar_value (-∞, scalar_value].
    pub fn le(data_type: &DataType, scalar_value: &ScalarValue) -> Range {
        let low = Marker::lower_unbound(data_type.clone());
        let high = Marker::exactly(data_type.clone(), scalar_value.clone());

        Self { low, high }
    }
    /// Construct a range of values in a left-open and right-open interval (low, high).
    pub fn gtlt(data_type: &DataType, low: &ScalarValue, high: &ScalarValue) -> Range {
        Self {
            low: Marker::lower_bounded(data_type.clone(), low.clone()),
            high: Marker::upper_bounded(data_type.clone(), high.clone()),
        }
    }
    /// Construct a range of values in a left-closed and right-open interval [low, high).
    pub fn gelt(data_type: &DataType, low: &ScalarValue, high: &ScalarValue) -> Range {
        Self {
            low: Marker::exactly(data_type.clone(), low.clone()),
            high: Marker::upper_bounded(data_type.clone(), high.clone()),
        }
    }
    /// Construct a range of values in a left-open and right-closed interval (low, high].
    pub fn gtle(data_type: &DataType, low: &ScalarValue, high: &ScalarValue) -> Range {
        Self {
            low: Marker::lower_bounded(data_type.clone(), low.clone()),
            high: Marker::exactly(data_type.clone(), high.clone()),
        }
    }
    /// Construct a range of values in a left-closed and right-closed interval [low, high].
    pub fn gele(data_type: &DataType, low: &ScalarValue, high: &ScalarValue) -> Range {
        Self {
            low: Marker::exactly(data_type.clone(), low.clone()),
            high: Marker::exactly(data_type.clone(), high.clone()),
        }
    }

    /// Determine if two ranges of values overlap.
    ///
    /// If there is overlap, return Ok(true).
    ///
    /// If there no overlap, return Ok(false).
    ///
    /// Returns an exception if the data types are incompatible.
    fn overlaps(&self, other: &Self) -> Result<bool> {
        self.check_type_compatibility(other)?;
        if self.low <= other.high && other.low <= self.high {
            // like (-∞, 2) and (2, +∞) is not overlap
            if self.high == other.low
                && self.high.bound == Bound::Below
                && other.low.bound == Bound::Above
            {
                return Ok(false);
            }
            return Ok(true);
        }
        Ok(false)
    }
    /// Calculates the intersection of two ranges.
    ///
    /// return an exception, if there is no intersection.
    fn intersect(&self, other: &Self) -> Result<Self> {
        // Type mismatch directly returns an exception
        if self.overlaps(other)? {
            // There is overlap, calculate the intersection
            let low = cmp::max(&self.low, &other.low);
            let high = cmp::min(&self.high, &other.high);

            return Ok(Self {
                low: low.clone(),
                high: high.clone(),
            });
        }
        // If the type matches and there is no intersection, an exception is returned.
        // If it is executed here, it means that there is a bug in the upper-level system logic.
        Err(Error::Internal {
            err: "cannot intersect non-overlapping ranges".to_string(),
        })
    }
    /// Calculates the span of two ranges
    ///
    /// return new Range
    ///
    /// Returns an exception if the data types are incompatible.
    fn span(&self, other: &Self) -> Result<Self> {
        //Type mismatch directly returns an exception
        if self.overlaps(other)? {
            // There is overlap, calculate the intersection
            let low = cmp::min(&self.low, &other.low);
            let high = cmp::max(&self.high, &other.high);

            return Ok(Self {
                low: low.clone(),
                high: high.clone(),
            });
        }
        // If the type matches and there is no intersection, an exception is returned.
        // If it is executed here, it means that there is a bug in the upper-level system logic.
        Err(Error::Internal {
            err: "cannot span non-overlapping ranges".to_string(),
        })
    }
    /// whether to match all lines
    pub fn is_all(&self) -> bool {
        self.low.is_lower_unbound() && self.high.is_upper_unbound()
    }

    pub fn low_ref(&self) -> &Marker {
        &self.low
    }

    pub fn high_ref(&self) -> &Marker {
        &self.high
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValueEntry {
    data_type: DataType,
    value: ScalarValue,
}

impl Serialize for ValueEntry {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value: protobuf::ScalarValue = (&self.value)
            .try_into()
            .map_err(serde::ser::Error::custom)?;

        let value_buf = to_prost_bytes(value);

        let mut ve = serializer.serialize_struct("ValueEntry", 2)?;
        ve.serialize_field("data_type", &self.data_type)?;
        ve.serialize_field("value", &value_buf)?;
        ve.end()
    }
}

impl<'a> Deserialize<'a> for ValueEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        deserializer.deserialize_struct("ValueEntry", &["data_type", "value"], ValueEntryVisitor)
    }
}

impl Display for ValueEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}) {}", self.data_type, self.value)
    }
}

struct ValueEntryVisitor;

impl<'de> Visitor<'de> for ValueEntryVisitor {
    type Value = ValueEntry;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("This Visitor expects to receive ValueEntry")
    }
}

impl ValueEntry {
    pub fn value(&self) -> &ScalarValue {
        &self.value
    }
}

/// 获取utf8类型的值，如果不是utf8类型或值为null，则返回None
pub fn utf8_from(val: &ScalarValue) -> Option<&str> {
    match &val {
        ScalarValue::Utf8(v) => v.as_deref(),
        _ => None,
    }
}

/// A set containing zero or more Ranges of the same type over a continuous space of possible values.
///
/// Ranges are coalesced into the most compact representation of non-overlapping Ranges.
///
/// This structure allows iteration across these compacted Ranges in increasing order, as well as other common set-related operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeValueSet {
    // data_type: DataType,
    low_indexed_ranges: BTreeMap<Marker, Range>,
}

impl RangeValueSet {
    pub fn low_indexed_ranges(&self) -> impl IntoIterator<Item = (&Marker, &Range)> {
        &self.low_indexed_ranges
    }
}

impl Display for RangeValueSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn write_marker_for_range_value_set_display(
            f: &mut std::fmt::Formatter<'_>,
            marker: &Marker,
        ) -> std::fmt::Result {
            // write!(f, "{} ({})", self.bound, self.data_type)?;
            // if let Some(ref v) = self.value {
            //     return write!(f, "{}", v);
            // }
            if marker.is_lower_unbound() {
                write!(f, "(-∞, )")
            } else {
                match marker.bound {
                    Bound::Below => write!(f, "#")?,
                    Bound::Exactly => write!(f, "[")?,
                    Bound::Above => write!(f, "(")?,
                }
                write!(f, "({})", &marker.data_type)?;
                if let Some(ref v) = marker.value {
                    write!(f, "{v}")?;
                }
                write!(f, ",..")
            }
        }

        write!(f, "{{ ")?;
        if !self.low_indexed_ranges.is_empty() {
            let max_i = self.low_indexed_ranges.len() - 1;
            for (i, (marker, range)) in self.low_indexed_ranges.iter().enumerate() {
                write!(f, "(")?;
                write_marker_for_range_value_set_display(f, marker)?;
                write!(f, ": {range}")?;
                if i < max_i {
                    write!(f, ", ")?;
                }
            }
        }
        write!(f, " }}")
    }
}

/// A set containing values that are uniquely identifiable.
///
/// Assumes an infinite number of possible values.
/// The values may be collectively included (aka whitelist) or collectively excluded (aka !whitelist).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EqutableValueSet {
    data_type: DataType,
    white_list: bool,
    entries: HashSet<ValueEntry>,
}

impl EqutableValueSet {
    pub fn is_white_list(&self) -> bool {
        self.white_list
    }

    pub fn entries(&self) -> impl IntoIterator<Item = &ValueEntry> {
        &self.entries
    }
}

impl Display for EqutableValueSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} ", self.data_type)?;
        if self.white_list {
            write!(f, "white_list")?;
        }
        write!(f, ") [")?;
        if !self.entries.is_empty() {
            let max_i = self.entries.len() - 1;
            for (i, value_entry) in self.entries.iter().enumerate() {
                write!(f, "{value_entry}")?;
                if i < max_i {
                    write!(f, ", ")?;
                }
            }
        }
        write!(f, " ]")?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Domain {
    Range(RangeValueSet),
    Equtable(EqutableValueSet),
    None,
    All,
}

impl Domain {
    /// Ranges are coalesced into the most compact representation of non-overlapping Ranges.
    ///
    /// Return new ValueSet(RangeValueSet)
    ///
    /// Returns an exception if the data types are incompatible.
    pub fn of_ranges(res: &[Range]) -> Result<Domain> {
        if res.is_empty() {
            return Ok(Domain::None);
        }

        let mut ranges = res.to_vec();
        ranges.sort_by(|l, r| l.low.cmp(&r.low));

        // at least one element
        let mut current = ranges.first().unwrap().to_owned();

        let mut low_indexed_ranges: BTreeMap<Marker, Range> = BTreeMap::default();

        for next in &ranges[1..] {
            // Check if types match
            current.check_type_compatibility(next)?;

            match current.overlaps(next) {
                Ok(true) => {
                    // If the two ranges have intersection, then merge
                    let span_range = current.span(next).unwrap();
                    current = span_range
                }
                Ok(false) => {
                    // If there is no intersection between the two ranges,
                    // save the current range interpolation,
                    // and switch the current range to the next range
                    low_indexed_ranges.insert(current.low.clone(), current.clone());
                    current = next.clone();
                }
                Err(_) => {}
            }
        }
        low_indexed_ranges.insert(current.low.clone(), current);

        Ok(Domain::Range(RangeValueSet { low_indexed_ranges }))
    }
    /// Construct a set of values.
    ///
    /// white_list = true means equal to values
    ///
    /// white_list = false means not equal to values
    pub fn of_values(data_type: &DataType, white_list: bool, values: &[&ScalarValue]) -> Domain {
        let entries: HashSet<ValueEntry> = values
            .iter()
            .map(|e| ValueEntry {
                data_type: data_type.clone(),
                value: (*e).clone(),
            })
            .collect();

        Domain::Equtable(EqutableValueSet {
            data_type: data_type.clone(),
            white_list,
            entries,
        })
    }
    /// Calculates the intersection of two ranges, and returns None if the intersection does not exist
    ///
    /// This method returns the new value without changing the old value
    fn intersect(&self, other: &Domain) -> Result<Domain> {
        match (self, other) {
            (Self::Range(ref self_val_set), Self::Range(ref other_val_set)) => {
                Domain::range_intersect(self_val_set, other_val_set)
            }
            (Self::Equtable(ref self_val_set), Self::Equtable(ref other_val_set)) => {
                Domain::value_intersect(self_val_set, other_val_set)
            }
            (Self::None, _) | (_, Self::None) => Ok(Self::None),
            (Self::All, _) => Ok(other.clone()),
            (_, Self::All) => Ok(self.clone()),
            _ => Err(Error::Internal {
                err: "mismatched ValueSet type".to_string(),
            }),
        }
    }
    /// Calculates the union of two ranges
    ///
    /// This method returns the new value without changing the old value
    ///
    /// Returns an exception if the ValueSet types are incompatible.
    ///
    /// This method returns the new ValueSet without changing the old value
    fn union(&self, other: &Domain) -> Result<Domain> {
        match (self, other) {
            (Self::Range(ref self_val_set), Self::Range(ref other_val_set)) => {
                Domain::range_union(self_val_set, other_val_set)
            }
            (Self::Equtable(ref self_val_set), Self::Equtable(ref other_val_set)) => {
                Domain::value_union(self_val_set, other_val_set)
            }
            (Self::None, _) => Ok(other.clone()),
            (_, Self::None) => Ok(self.clone()),
            (Self::All, _) | (_, Self::All) => Ok(Self::All),
            _ => Err(Error::Internal {
                err: "mismatched ValueSet type".to_string(),
            }),
        }
    }
    /// Merge and intersect two ordered range sets
    ///
    /// This method returns the new ValueSet without changing the old value
    fn range_intersect(first: &RangeValueSet, other: &RangeValueSet) -> Result<Domain> {
        let mut result: Vec<Range> = Vec::default();

        let mut first_values = first.low_indexed_ranges.values();
        let mut other_values = other.low_indexed_ranges.values();

        let mut first_current_range: Range;
        let mut other_current_range: Range;

        match (first_values.next(), other_values.next()) {
            (Some(first_range), Some(other_range)) => {
                first_current_range = first_range.to_owned();
                other_current_range = other_range.to_owned();
                loop {
                    if first_current_range.overlaps(&other_current_range)? {
                        let range = first_current_range.intersect(&other_current_range)?;
                        result.push(range);
                    }

                    match first_current_range.high.cmp(&other_current_range.high) {
                        Ordering::Less | Ordering::Equal => {
                            if let Some(range) = first_values.next() {
                                first_current_range = range.to_owned();
                            } else {
                                break;
                            }
                        }
                        _ => {
                            if let Some(range) = other_values.next() {
                                other_current_range = range.to_owned();
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
            (_, _) => {
                return Err(Error::Internal {
                    err: "RangeValueSet not contains value".to_string(),
                });
            }
        }

        Domain::of_ranges(result.as_ref())
    }
    /// Merge and intersect two equable value sets
    ///
    /// This method returns the new ValueSet without changing the old value
    fn value_intersect(first: &EqutableValueSet, other: &EqutableValueSet) -> Result<Domain> {
        let result_data_type = &first.data_type;
        let result_white_list: bool;
        let result_entries: Vec<&ScalarValue>;

        let first_entries = &first.entries;
        let other_entries = &other.entries;

        match (first.white_list, other.white_list) {
            // A = {first_entries}
            // B = {other_entries}
            // first = A
            // other = B
            // so.
            //   A ∩ B
            (true, true) => {
                result_white_list = true;
                result_entries = first_entries
                    .intersection(other_entries)
                    .map(|e| &e.value)
                    .collect();
            }
            // A = {first_entries}
            // B = {other_entries}
            // first = A
            // other = !B
            // so.
            //   first ∩ other
            //   => A ∩ !B
            //   => A - B
            (true, false) => {
                result_white_list = true;
                result_entries = first_entries
                    .difference(other_entries)
                    .map(|e| &e.value)
                    .collect();
            }
            // A = {first_entries}
            // B = {other_entries}
            // first = !A
            // other = B
            // so.
            //   first ∩ other
            //   => !A ∩ B
            //   => B - A
            (false, true) => {
                result_white_list = true;
                result_entries = other_entries
                    .difference(first_entries)
                    .map(|e| &e.value)
                    .collect();
            }
            // A = {first_entries}
            // B = {other_entries}
            // first = !A
            // other = !B
            // so.
            //   first ∩ other
            //   => !A ∩ !B
            //   => !(A ∪ B)
            (false, false) => {
                result_white_list = false;
                result_entries = other_entries
                    .union(first_entries)
                    .map(|e| &e.value)
                    .collect();
            }
        }

        Ok(Domain::of_values(
            result_data_type,
            result_white_list,
            result_entries.as_ref(),
        ))
    }
    /// Merge and intersect two equable value sets
    ///
    /// This method returns the new ValueSet without changing the old value
    fn range_union(first: &RangeValueSet, other: &RangeValueSet) -> Result<Domain> {
        let mut result: Vec<_> = first.low_indexed_ranges.values().cloned().collect();
        let mut other_ranges: Vec<_> = other.low_indexed_ranges.values().cloned().collect();

        result.append(&mut other_ranges);

        Domain::of_ranges(&result)
    }

    fn value_union(first: &EqutableValueSet, other: &EqutableValueSet) -> Result<Domain> {
        let result_data_type = &first.data_type;
        let result_white_list: bool;
        let result_entries: Vec<&ScalarValue>;

        let first_entries = &first.entries;
        let other_entries = &other.entries;

        match (first.white_list, other.white_list) {
            (true, true) => {
                result_white_list = true;
                result_entries = first_entries
                    .union(other_entries)
                    .map(|e| &e.value)
                    .collect();
            }
            (true, false) => {
                result_white_list = false;
                result_entries = other_entries
                    .difference(first_entries)
                    .map(|e| &e.value)
                    .collect();
            }
            (false, true) => {
                result_white_list = false;
                result_entries = first_entries
                    .difference(other_entries)
                    .map(|e| &e.value)
                    .collect();
            }
            (false, false) => {
                result_white_list = false;
                result_entries = other_entries
                    .intersection(first_entries)
                    .map(|e| &e.value)
                    .collect();
            }
        }

        Ok(Domain::of_values(
            result_data_type,
            result_white_list,
            result_entries.as_ref(),
        ))
    }
}

impl Display for Domain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Domain::Range(s) => write!(f, "range({s})"),
            Domain::Equtable(s) => write!(f, "equtable({s})"),
            Domain::None => write!(f, "none"),
            Domain::All => write!(f, "all"),
        }
    }
}

/// ColumnDomains is internally represented as a normalized map of each column to its
///
/// respective allowable value domain(ValueSet). Conceptually, these ValueSet can be thought of
///
/// as being AND'ed together to form the representative predicate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDomains<T>
where
    T: Eq + Hash + Clone,
{
    // None means no matching record.
    // Empty map means match all records.
    column_to_domain: Option<HashMap<T, Domain>>,
}

impl<T: Eq + Hash + Clone> Default for ColumnDomains<T> {
    fn default() -> Self {
        ColumnDomains::all()
    }
}

impl<T: Eq + Hash + Clone> ColumnDomains<T> {
    pub fn all() -> Self {
        Self {
            column_to_domain: Some(HashMap::default()),
        }
    }
    pub fn none() -> Self {
        Self {
            column_to_domain: None,
        }
    }
    pub fn of(col: T, domain: &Domain) -> Self {
        let mut result = ColumnDomains::all();
        result
            .column_to_domain
            .as_mut()
            .and_then(|map| map.insert(col, domain.clone()));
        result
    }

    pub fn is_all(&self) -> bool {
        if let Some(map) = &self.column_to_domain {
            return map.is_empty();
        }
        false
    }

    pub fn is_none(&self) -> bool {
        self.column_to_domain.is_none()
    }

    pub fn translate_column<U, F>(&self, f: F) -> ColumnDomains<U>
    where
        U: Eq + Hash + Clone,
        F: Fn(&T) -> Option<U>,
    {
        if self.is_all() {
            return ColumnDomains::all();
        }

        if self.is_none() {
            return ColumnDomains::none();
        }

        let new_column_to_domain: Option<HashMap<U, Domain>> =
            self.column_to_domain.as_ref().map(|e| {
                e.iter()
                    .map(|(k, v)| (f(k), v.clone()))
                    .filter(|(k, _)| k.is_some())
                    .map(|(k, v)| (k.unwrap(), v))
                    .collect()
            });

        ColumnDomains {
            column_to_domain: new_column_to_domain,
        }
    }

    /// Intersection with other will change the current value
    pub fn intersect(&mut self, other: &ColumnDomains<T>) {
        match (
            self.column_to_domain.as_mut(),
            other.column_to_domain.as_ref(),
        ) {
            (Some(map1), Some(map2)) => {
                // Calculate the intersection for each column
                map2.iter().for_each(|(k2, v2)| {
                    map1.entry(k2.clone())
                        .and_modify(|v1| {
                            *v1 = v1.intersect(v2).unwrap_or(Domain::None);
                        })
                        .or_insert_with(|| v2.clone());
                });
            }
            (_, None) => {
                self.column_to_domain = None;
            }
            (None, _) => {}
        }
    }

    /// The union with other will change the current value
    ///
    /// Note:
    ///
    ///   The resulting ColumnDomains only retain all columns that appear in self and other, not a standard union
    pub fn column_wise_union(&mut self, other: &ColumnDomains<T>) {
        match (
            self.column_to_domain.as_ref(),
            other.column_to_domain.as_ref(),
        ) {
            (Some(map1), Some(map2)) => {
                let mut result: HashMap<T, Domain> = HashMap::default();
                // Take the union of the values corresponding to the keys contained in both maps and save them
                map2.iter().for_each(|(k2, v2)| {
                    let unioned_value = map1.get(k2).map(|v1| v1.union(v2).unwrap_or(Domain::All));

                    if let Some(e) = unioned_value {
                        result.insert(k2.clone(), e);
                    };
                });
                self.column_to_domain = Some(result);
            }
            (_, None) => {}
            (None, _) => {
                self.column_to_domain = other.column_to_domain.clone();
            }
        }
    }

    pub fn insert_or_union(&mut self, col: T, domain: &Domain) {
        self.column_to_domain
            .get_or_insert(HashMap::default())
            .entry(col)
            .and_modify(|e| {
                if let Ok(v) = e.union(domain) {
                    *e = v;
                }
            })
            .or_insert_with(|| domain.clone());
    }

    pub fn insert_or_intersect(&mut self, col: T, domain: &Domain) {
        if let Some(map) = self.column_to_domain.as_mut() {
            map.entry(col)
                .and_modify(|e| {
                    *e = e.intersect(domain).unwrap_or(Domain::None);
                })
                .or_insert_with(|| domain.clone());
        }
    }

    /// Returns the contained column_to_domain value,
    /// without checking that the value is not None
    ///
    /// # Safety
    ///
    /// Calling this method on self.is_none() == true is *[undefined behavior]*.
    pub unsafe fn domains_unsafe(&self) -> &HashMap<T, Domain> {
        self.column_to_domain.as_ref().unwrap_unchecked()
    }

    /// Returns the contained column_to_domain value
    ///
    /// None means no matching record.
    /// Empty map means match all records.
    pub fn domains(&self) -> Option<&HashMap<T, Domain>> {
        self.column_to_domain.as_ref()
    }
}

impl<T> Display for ColumnDomains<T>
where
    T: Eq + Hash + Clone + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ ")?;
        if let Some(ref column_to_domain) = self.column_to_domain {
            let max_i = column_to_domain.len() - 1;
            for (i, (column, domain)) in column_to_domain.iter().enumerate() {
                write!(f, "{column}: {domain}")?;
                if i < max_i {
                    write!(f, ", ")?;
                }
            }
        }
        write!(f, " }}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedPredicate {
    time_ranges: Arc<TimeRanges>,
    tags_filter: ColumnDomains<String>,
    fields_filter: ColumnDomains<String>,
}

impl ResolvedPredicate {
    pub fn new(
        time_ranges: Arc<TimeRanges>,
        tags_filter: ColumnDomains<String>,
        fields_filter: ColumnDomains<String>,
    ) -> Self {
        Self {
            time_ranges,
            tags_filter,
            fields_filter,
        }
    }

    pub fn time_ranges(&self) -> Arc<TimeRanges> {
        self.time_ranges.clone()
    }

    pub fn tags_filter(&self) -> &ColumnDomains<String> {
        &self.tags_filter
    }

    pub fn fields_filter(&self) -> &ColumnDomains<String> {
        &self.fields_filter
    }
}

#[derive(Debug, Default)]
pub struct Predicate {
    pushed_down_domains: ColumnDomains<Column>,
    limit: Option<usize>,
}

impl Predicate {
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn filter(&self) -> &ColumnDomains<Column> {
        &self.pushed_down_domains
    }

    pub fn set_limit(mut self, limit: Option<usize>) -> Predicate {
        self.limit = limit;
        self
    }

    /// resolve and extract supported filter
    /// convert filter to ColumnDomains and set self
    pub fn push_down_filter(
        mut self,
        filters: &[Expr],
        _table_schema: &TskvTableSchema,
    ) -> Predicate {
        if let Some(ref expr) = conjunction(filters.to_vec()) {
            if let Ok(domains) = RowExpressionToDomainsVisitor::expr_to_column_domains(expr) {
                self.pushed_down_domains = domains;
            }
        }
        self
    }

    pub fn resolve(&self, table: &TskvTableSchemaRef) -> Result<ResolvedPredicateRef, String> {
        let domains_filter = self
            .filter()
            .translate_column(|c| table.column(&c.name).cloned());

        let tags_filter = domains_filter.translate_column(|e| match e.column_type {
            ColumnType::Tag => Some(e.name.clone()),
            _ => None,
        });

        let fields_filter = domains_filter.translate_column(|e| match e.column_type {
            ColumnType::Field(_) => Some(e.name.clone()),
            _ => None,
        });
        let time_filter = domains_filter.translate_column(|e| match e.column_type {
            ColumnType::Time(_) => Some(e.name.clone()),
            _ => None,
        });
        let time_ranges = filter_to_time_ranges(&time_filter);

        Ok(Arc::new(ResolvedPredicate::new(
            Arc::new(TimeRanges::new(time_ranges)),
            tags_filter,
            fields_filter,
        )))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryArgs {
    pub vnode_ids: Vec<u32>,

    pub limit: Option<usize>,
    pub batch_size: usize,
}

impl QueryArgs {
    pub fn encode(args: &QueryArgs) -> Result<Vec<u8>> {
        let d = bincode::serialize(args).map_err(|err| Error::InvalidSerdeMessage {
            err: err.to_string(),
        })?;

        Ok(d)
    }

    pub fn decode(buf: &[u8]) -> Result<QueryArgs> {
        let args =
            bincode::deserialize::<QueryArgs>(buf).map_err(|err| Error::InvalidSerdeMessage {
                err: err.to_string(),
            })?;

        Ok(args)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExpr {
    pub split: PlacedSplit,
    pub df_schema: Schema,
    pub table_schema: TskvTableSchemaRef,
}

impl QueryExpr {
    pub fn encode(option: &QueryExpr) -> Result<Vec<u8>> {
        let bytes = bincode::serialize(option).map_err(|e| Error::InvalidQueryExprMsg {
            err: format!("encode error {}", e),
        })?;

        Ok(bytes)
    }

    pub fn decode(buf: &[u8]) -> Result<QueryExpr> {
        bincode::deserialize::<QueryExpr>(buf).map_err(|e| Error::InvalidQueryExprMsg {
            err: format!("decode error {}", e),
        })
    }
}

pub fn encode_agg(agg: &Option<Vec<TableColumn>>) -> Result<Vec<u8>> {
    let d = bincode::serialize(agg).map_err(|err| Error::InvalidSerdeMessage {
        err: err.to_string(),
    })?;

    Ok(d)
}

pub fn decode_agg(buf: &[u8]) -> Result<Option<Vec<TableColumn>>> {
    let args = bincode::deserialize::<Option<Vec<TableColumn>>>(buf).map_err(|err| {
        Error::InvalidSerdeMessage {
            err: err.to_string(),
        }
    })?;

    Ok(args)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PushedAggregateFunction {
    Count(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shchema_encode_decode() {
        use std::collections::HashMap;

        use arrow_schema::*;

        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Boolean, false);

        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert("row_count".to_string(), "100".to_string());

        let schema = Arc::new(Schema::new_with_metadata(vec![field_a, field_b], metadata));

        let buffer = serde_json::to_string(&schema.as_ref()).unwrap();
        let df_schema = serde_json::from_str::<Schema>(&buffer).unwrap();

        assert_eq!(schema.as_ref(), &df_schema);
    }

    #[test]
    fn test_time_range() {
        let tr_all = TimeRange::all();
        assert_eq!(tr_all.min_ts, i64::MIN);
        assert_eq!(tr_all.max_ts, i64::MAX);
        assert!(tr_all.is_boundless());
        let tr = TimeRange::new(1, 5);
        assert_eq!(tr.min_ts, 1);
        assert_eq!(tr.max_ts, 5);
        assert!(!tr.is_boundless());
        assert!(tr_all.overlaps(&tr));
        assert!(tr.overlaps(&tr_all));
        assert!(tr.overlaps(&TimeRange::new(1, 5)));
        assert!(tr.overlaps(&TimeRange::new(0, 6)));
        assert!(tr.overlaps(&TimeRange::new(2, 4)));
        assert!(tr.overlaps(&TimeRange::new(0, 1)));
        assert!(tr.overlaps(&TimeRange::new(-1, 2)));
        assert!(tr.overlaps(&TimeRange::new(4, 7)));
        assert!(tr.overlaps(&TimeRange::new(1, 5)));
        assert!(tr.overlaps(&TimeRange::new(2, 4)));
        assert!(tr.overlaps(&TimeRange::new(5, 6)));
        assert!(!tr.overlaps(&TimeRange::new(-3, -1)));
        assert!(!tr.overlaps(&TimeRange::new(-1, 0)));
        assert!(!tr.overlaps(&TimeRange::new(6, 7)));

        // let tr_0 = TimeRange::new(1, 0);
        // assert_eq!(tr_0.min_ts, 1);
        // assert_eq!(tr_0.max_ts, 0);
        // assert!(tr_0.overlaps(&TimeRange::new(0, 1)));
        // assert!(tr_0.overlaps(&TimeRange::new(-1, 2)));
        // assert!(!tr_0.overlaps(&TimeRange::new(0, 0)));
        // assert!(!tr_0.overlaps(&TimeRange::new(1, 1)));
        // assert!(!tr_0.overlaps(&TimeRange::new(1, 0)));
        // assert!(!tr_0.overlaps(&TimeRange::new(2, -1)));

        assert!(tr_all.includes(&tr));
        assert!(tr.includes(&TimeRange::new(1, 5)));
        assert!(tr.includes(&TimeRange::new(1, 2)));
        assert!(tr.includes(&TimeRange::new(3, 4)));
        assert!(tr.includes(&TimeRange::new(4, 5)));
        assert!(!tr.includes(&TimeRange::new(0, 6)));
        assert!(!tr.includes(&TimeRange::new(0, 1)));
        assert!(!tr.includes(&TimeRange::new(5, 6)));

        assert!(tr_all.contains(1));
        assert!(tr.contains(1));
        assert!(tr.contains(3));
        assert!(tr.contains(5));
        assert!(!tr.contains(-1));
        assert!(!tr.contains(0));
        assert!(!tr.contains(6));
        assert!(!tr.contains(7));
        assert!(tr_all.contains(7));

        assert_eq!(tr_all.intersect(&tr), Some(tr));
        assert_eq!(tr_all.intersect(&TimeRange::all()), Some(TimeRange::all()));
        assert_eq!(
            tr.intersect(&TimeRange::new(1, 5)),
            Some(TimeRange::new(1, 5))
        );
        assert_eq!(
            tr.intersect(&TimeRange::new(1, 2)),
            Some(TimeRange::new(1, 2))
        );
        assert_eq!(
            tr.intersect(&TimeRange::new(4, 5)),
            Some(TimeRange::new(4, 5))
        );
        assert_eq!(
            tr.intersect(&TimeRange::new(0, 6)),
            Some(TimeRange::new(1, 5))
        );
        assert_eq!(
            tr.intersect(&TimeRange::new(0, 1)),
            Some(TimeRange::new(1, 1))
        );
        assert_eq!(
            tr.intersect(&TimeRange::new(5, 6)),
            Some(TimeRange::new(5, 5))
        );
        assert_eq!(tr.intersect(&TimeRange::new(-1, 0)), None);
        assert_eq!(tr.intersect(&TimeRange::new(6, 7)), None);

        assert_eq!(tr.total_time(), 5);
        assert_eq!(TimeRange::new(1, 1).total_time(), 1);

        fn check_merge(mut tr: TimeRange, merge_tr: TimeRange, expected_tr: TimeRange) {
            tr.merge(&merge_tr);
            assert_eq!(tr, expected_tr);
        }
        check_merge(tr, TimeRange::new(0, 1), TimeRange::new(0, 5));
        check_merge(tr, TimeRange::new(5, 6), TimeRange::new(1, 6));
        check_merge(tr, TimeRange::new(-1, 0), TimeRange::new(-1, 5));
        check_merge(tr, TimeRange::new(6, 7), TimeRange::new(1, 7));
        check_merge(tr, TimeRange::new(1, 5), TimeRange::new(1, 5));
        check_merge(tr, TimeRange::new(0, 6), TimeRange::new(0, 6));
        check_merge(tr, TimeRange::all(), TimeRange::all());
    }

    #[test]
    fn test_time_ranges() {
        let trs_all = TimeRanges::all();
        assert_eq!(trs_all.time_ranges(), &[TimeRange::all()]);
        assert_eq!(trs_all.min_ts(), i64::MIN);
        assert_eq!(trs_all.max_ts(), i64::MAX);
        assert!(trs_all.is_boundless());

        let trs = TimeRanges::new(vec![
            TimeRange::new(2, 3),
            TimeRange::new(22, 33),
            TimeRange::new(222, 333),
        ]);
        assert_eq!(trs.time_ranges(), &trs.inner);
        assert_eq!(trs.min_ts(), 2);
        assert_eq!(trs.max_ts(), 333);
        assert!(!trs.is_boundless());

        assert!(trs_all.overlaps(&TimeRange::new(2, 3)));
        assert!(trs.overlaps(&TimeRange::new(1, 2)));
        assert!(trs.overlaps(&TimeRange::new(2, 2)));
        assert!(trs.overlaps(&TimeRange::new(2, 3)));
        assert!(trs.overlaps(&TimeRange::new(3, 4)));
        assert!(trs.overlaps(&TimeRange::new(20, 35)));
        assert!(trs.overlaps(&TimeRange::new(20, 25)));
        assert!(trs.overlaps(&TimeRange::new(22, 33)));
        assert!(trs.overlaps(&TimeRange::new(30, 35)));
        assert!(trs.overlaps(&TimeRange::new(222, 333)));
        assert!(trs.overlaps(&TimeRange::new(233, 322)));
        assert!(!trs.overlaps(&TimeRange::new(0, 1)));
        assert!(!trs.overlaps(&TimeRange::new(4, 4)));
        assert!(!trs.overlaps(&TimeRange::new(4, 5)));

        assert!(trs_all.includes(&TimeRange::new(2, 3)));
        assert!(trs.includes(&TimeRange::new(2, 3)));
        assert!(trs.includes(&TimeRange::new(22, 33)));
        assert!(!trs.includes(&TimeRange::new(20, 30)));
        assert!(!trs.includes(&TimeRange::new(1, 2)));
        assert!(!trs.includes(&TimeRange::new(3, 4)));
        assert!(!trs.includes(&TimeRange::new(10, 30)));
        assert!(!trs.includes(&TimeRange::new(30, 40)));

        assert!(trs_all.contains(2));
        assert!(trs.contains(2));
        assert!(trs.contains(3));
        assert!(trs.contains(30));
        assert!(!trs.contains(0));
        assert!(!trs.contains(4));
        assert!(!trs.contains(10));

        assert_eq!(
            trs_all.intersect(&TimeRange::new(1, 5)),
            Some(TimeRanges::new(vec![TimeRange::new(1, 5)]))
        );
        assert_eq!(trs.intersect(&TimeRange::new(0, 1)), None);
        assert_eq!(trs.intersect(&TimeRange::new(4, 5)), None);
        assert_eq!(
            trs.intersect(&TimeRange::new(1, 5)),
            Some(TimeRanges::new(vec![TimeRange::new(2, 3)]))
        );
        assert_eq!(
            trs.intersect(&TimeRange::new(1, 2)),
            Some(TimeRanges::new(vec![TimeRange::new(2, 2)]))
        );
        assert_eq!(
            trs.intersect(&TimeRange::new(3, 4)),
            Some(TimeRanges::new(vec![TimeRange::new(3, 3)]))
        );
        assert_eq!(
            trs.intersect(&TimeRange::new(20, 50)),
            Some(TimeRanges::new(vec![TimeRange::new(22, 33)]))
        );
        assert_eq!(
            trs.intersect(&TimeRange::new(1, 50)),
            Some(TimeRanges::new(vec![
                TimeRange::new(2, 3),
                TimeRange::new(22, 33)
            ]))
        );
    }

    #[test]
    fn test_time_range_exclude() {
        let tr = TimeRange::new(1, 5);
        let exclude = tr.exclude(&TimeRange::new(2, 6));
        assert_eq!(exclude.0.unwrap(), TimeRange::new(1, 1));

        let tr = TimeRange::new(5, 10);
        let exclude = tr.exclude(&TimeRange::new(1, 7));
        assert_eq!(exclude.1.unwrap(), TimeRange::new(8, 10));

        let tr = TimeRange::new(3, 4);
        let exclude = tr.exclude(&TimeRange::new(1, 5));
        assert!(exclude.0.is_none());
        assert!(exclude.1.is_none());

        let tr = TimeRange::new(1, 5);
        let exclude = tr.exclude(&TimeRange::new(3, 4));
        assert_eq!(exclude.0.unwrap(), TimeRange::new(1, 2));
        assert_eq!(exclude.1.unwrap(), TimeRange::new(5, 5));
    }

    #[test]
    fn test_time_ranges_exclude() {
        let trs = vec![
            TimeRange::new(1, 2),
            TimeRange::new(4, 5),
            TimeRange::new(7, 9),
        ];
        let trs = TimeRanges::new(trs);
        let exclude_ranges = trs.exclude(&TimeRange::new(3, 8));
        let expected = TimeRanges::new(vec![(1, 2).into(), (9, 9).into()]);
        assert_eq!(expected, exclude_ranges);

        let exclude_ranges =
            trs.exclude_time_ranges(&[&TimeRange::new(3, 4), &TimeRange::new(5, 8)]);
        assert_eq!(expected, exclude_ranges);
    }
    #[test]
    fn test_time_range_sort() {
        let mut time_range_vec: Vec<TimeRange> = vec![
            (1, 1).into(),
            (1, 2).into(),
            (1, 0).into(),
            (3, 4).into(),
            (2, 3).into(),
            (2, 1).into(),
        ];
        time_range_vec.sort();
        assert_eq!(
            time_range_vec,
            vec![
                (1, 0).into(),
                (1, 1).into(),
                (1, 2).into(),
                (2, 1).into(),
                (2, 3).into(),
                (3, 4).into(),
            ]
        );
    }

    #[test]
    fn test_time_range_compact() {
        {
            let mut time_range_vec: Vec<TimeRange> = vec![];
            TimeRange::compact(&mut time_range_vec);
            assert_eq!(time_range_vec, vec![]);
        }
        {
            let mut time_range_vec: Vec<TimeRange> = vec![(1, 2).into()];
            TimeRange::compact(&mut time_range_vec);
            assert_eq!(time_range_vec, vec![(1, 2).into()]);
        }
        {
            let mut time_range_vec: Vec<TimeRange> =
                vec![(Timestamp::MIN, 999).into(), (1_000, Timestamp::MAX).into()];
            TimeRange::compact(&mut time_range_vec);
            assert_eq!(
                time_range_vec,
                vec![(Timestamp::MIN, Timestamp::MAX).into()]
            );
        }
        {
            let mut time_range_vec: Vec<TimeRange> =
                vec![(Timestamp::MIN, Timestamp::MAX).into(), (999, 1_000).into()];
            TimeRange::compact(&mut time_range_vec);
            assert_eq!(
                time_range_vec,
                vec![(Timestamp::MIN, Timestamp::MAX).into()]
            );
        }
        {
            let mut time_range_vec: Vec<TimeRange> =
                vec![(Timestamp::MIN, 998).into(), (1_000, Timestamp::MAX).into()];
            TimeRange::compact(&mut time_range_vec);
            assert_eq!(
                time_range_vec,
                vec![(Timestamp::MIN, 998).into(), (1_000, Timestamp::MAX).into()]
            );
        }
        {
            let mut time_range_vec: Vec<TimeRange> =
                vec![(1, 2).into(), (3, 4).into(), (9, 10).into()];
            TimeRange::compact(&mut time_range_vec);
            assert_eq!(time_range_vec, vec![(1, 4).into(), (9, 10).into()]);
        }
        {
            let mut time_range_vec: Vec<TimeRange> = vec![
                (5, 5).into(),
                (1, 10).into(),
                (0, 5).into(),
                (3, 6).into(),
                (5, 9).into(),
                (20, 30).into(),
                (21, 29).into(),
            ];
            TimeRange::compact(&mut time_range_vec);
            assert_eq!(time_range_vec, vec![(0, 10).into(), (20, 30).into()]);
        }
    }

    #[test]
    fn test_of_ranges() {
        let f1 = Range::lt(&DataType::Float64, &ScalarValue::Float64(Some(-1000000.1)));
        let f2 = Range::gt(&DataType::Float64, &ScalarValue::Float64(Some(2.2)));
        let f3 = Range::eq(
            &DataType::Float64,
            &ScalarValue::Float64(Some(3333333333333333.3)),
        );
        let f4 = Range::ne(&DataType::Float64, &ScalarValue::Float64(Some(4.4)));

        let domain_1 = Domain::of_ranges(&[f1]).unwrap();
        let domain_2 = Domain::of_ranges(&[f2, f3]).unwrap();
        let domain_3 = Domain::of_ranges(&f4).unwrap();

        assert!(matches!(domain_1, Domain::Range(_)));

        assert!(match domain_2 {
            Domain::Range(val_set) => {
                // f2 and f3 will span to 1 range
                val_set.low_indexed_ranges.len() == 1
            }
            _ => false,
        });

        assert!(match domain_3 {
            Domain::Range(val_set) => {
                val_set.low_indexed_ranges.len() == 2
            }
            _ => false,
        })
    }

    #[test]
    fn test_of_values() {
        let val = ScalarValue::Int32(Some(10_i32));

        let elementss = &[&val, &val];
        let domain = Domain::of_values(&DataType::Int32, true, elementss);

        let except_result = ValueEntry {
            data_type: DataType::Int32,
            value: val,
        };

        // println!("{:#?}", domain);

        match domain {
            Domain::Equtable(val_set) => {
                assert!(
                    val_set.entries.len() == 1,
                    "except val_set contain one value"
                );
                let ele = val_set.entries.iter().next().unwrap();
                assert_eq!(ele.to_owned(), except_result);
            }
            _ => {
                panic!("excepted Domain::Equtable")
            }
        };
    }
}
