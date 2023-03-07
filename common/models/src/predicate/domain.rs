use std::cmp::{self, Ordering};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::io::{BufReader, Read};
use std::ops::RangeBounds;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::Expr;
use datafusion::optimizer::utils::conjunction;
use datafusion::prelude::Column;
use datafusion::scalar::ScalarValue;
use datafusion_proto::bytes::Serializeable;
use serde::{Deserialize, Serialize};

use super::transformation::RowExpressionToDomainsVisitor;
use crate::schema::TskvTableSchema;
use crate::{Error, Result};

pub type PredicateRef = Arc<Predicate>;

#[derive(Default, Debug)]
pub struct TimeRange {
    pub max_ts: i64,
    pub min_ts: i64,
}

impl TimeRange {
    pub fn new(max_ts: i64, min_ts: i64) -> Self {
        Self { max_ts, min_ts }
    }

    pub fn overlaps(&self, range: &TimeRange) -> bool {
        !(self.min_ts > range.max_ts || self.max_ts < range.min_ts)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Bound {
    /// lower than the value, but infinitesimally close to the value.
    Below,
    // exactly the value.
    Exactly,
    // higher than the value, but infinitesimally close to the value.
    Above,
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
    /// Infinitely small (less than a nonexistent number).
    fn upper_unbound(data_type: DataType) -> Marker {
        Self {
            data_type,
            value: None,
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

impl PartialOrd for Marker {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if !self.check_type_compatibility(other) {
            return None;
        }
        // (∞, ∞)
        // => Equal
        // (∞, _)
        // => Greater
        if self.is_upper_unbound() {
            return if other.is_upper_unbound() {
                Some(std::cmp::Ordering::Equal)
            } else {
                Some(std::cmp::Ordering::Greater)
            };
        }
        // (-∞, -∞)
        // => Equal
        // (-∞, _)
        // => Less
        if self.is_lower_unbound() {
            return if other.is_lower_unbound() {
                Some(std::cmp::Ordering::Equal)
            } else {
                Some(std::cmp::Ordering::Less)
            };
        }
        // self not unbound
        // (_, ∞)
        // => Less
        if other.is_upper_unbound() {
            return Some(std::cmp::Ordering::Less);
        }
        // self not unbound
        // (_, -∞)
        // => Greater
        if other.is_lower_unbound() {
            return Some(std::cmp::Ordering::Greater);
        }
        // value and other.value are present
        let self_data = self.value.as_ref().unwrap();
        let other_data = other.value.as_ref().unwrap();

        self_data.partial_cmp(other_data)
    }
}

impl Eq for Marker {}

impl Ord for Marker {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// A Range of values across the continuous space defined by the types of the Markers.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        let low = Marker {
            data_type: data_type.clone(),
            value: Some(scalar_value.clone()),
            bound: Bound::Exactly,
        };
        let high = Marker {
            data_type: data_type.clone(),
            value: Some(scalar_value.clone()),
            bound: Bound::Exactly,
        };

        Self { low, high }
    }
    /// TODO Constructs a range of values not equal to scalar_value [scalar_value, scalar_value].
    pub fn ne(data_type: &DataType, _scalar_value: &ScalarValue) -> Range {
        let low = Marker {
            data_type: data_type.clone(),
            value: None,
            bound: Bound::Below,
        };
        let high = Marker {
            data_type: data_type.clone(),
            value: None,
            bound: Bound::Above,
        };

        Self { low, high }
    }
    /// Construct a range of values greater than scalar_value (scalar_value, +∞).
    pub fn gt(data_type: &DataType, scalar_value: &ScalarValue) -> Range {
        let low = Marker {
            data_type: data_type.clone(),
            value: Some(scalar_value.clone()),
            bound: Bound::Above,
        };
        let high = Marker::upper_unbound(data_type.clone());

        Self { low, high }
    }
    /// Construct a range of values greater than or equal to scalar_value [scalar_value, +∞).
    pub fn ge(data_type: &DataType, scalar_value: &ScalarValue) -> Range {
        let low = Marker {
            data_type: data_type.clone(),
            value: Some(scalar_value.clone()),
            bound: Bound::Exactly,
        };
        let high = Marker::upper_unbound(data_type.clone());

        Self { low, high }
    }
    /// Construct a range of values smaller than scalar_value (-∞, scalar_value).
    pub fn lt(data_type: &DataType, scalar_value: &ScalarValue) -> Range {
        let low = Marker::lower_unbound(data_type.clone());
        let high = Marker {
            data_type: data_type.clone(),
            value: Some(scalar_value.clone()),
            bound: Bound::Below,
        };

        Self { low, high }
    }
    /// Construct a range of values less than or equal to scalar_value (-∞, scalar_value].
    pub fn le(data_type: &DataType, scalar_value: &ScalarValue) -> Range {
        let low = Marker::lower_unbound(data_type.clone());
        let high = Marker {
            data_type: data_type.clone(),
            value: Some(scalar_value.clone()),
            bound: Bound::Exactly,
        };

        Self { low, high }
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeValueSet {
    // data_type: DataType,
    low_indexed_ranges: BTreeMap<Marker, Range>,
}

impl RangeValueSet {
    pub fn low_indexed_ranges(&self) -> impl IntoIterator<Item = (&Marker, &Range)> {
        &self.low_indexed_ranges
    }
}

/// A set containing values that are uniquely identifiable.
///
/// Assumes an infinite number of possible values.
/// The values may be collectively included (aka whitelist) or collectively excluded (aka !whitelist).
#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
        let mut current = ranges.get(0).unwrap().to_owned();

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

/// ColumnDomains is internally represented as a normalized map of each column to its
///
/// respective allowable value domain(ValueSet). Conceptually, these ValueSet can be thought of
///
/// as being AND'ed together to form the representative predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Default)]
pub struct Predicate {
    exprs: Vec<Expr>,
    pushed_down_domains: ColumnDomains<Column>,
    limit: Option<usize>,
}

impl Predicate {
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn exprs(&self) -> &[Expr] {
        &self.exprs
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
        self.exprs = filters.to_vec();
        if let Some(ref expr) = conjunction(filters.to_vec()) {
            if let Ok(domains) = RowExpressionToDomainsVisitor::expr_to_column_domains(expr) {
                self.pushed_down_domains = domains;
            }
        }
        self
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryArgs {
    pub tenant: String,
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

#[derive(Debug, Clone)]
pub struct QueryExpr {
    pub filters: Vec<Expr>,
    pub df_schema: SchemaRef,
    pub table_schema: TskvTableSchema,
}

impl QueryExpr {
    pub fn encode(option: &QueryExpr) -> Result<Vec<u8>> {
        let mut buffer = vec![];

        buffer.append(&mut (option.filters.len() as u32).to_be_bytes().to_vec());
        for item in option.filters.iter() {
            let mut tmp = item
                .to_bytes()
                .map_err(|err| Error::InvalidQueryExprMsg {
                    err: err.to_string(),
                })?
                .to_vec();
            buffer.append(&mut (tmp.len() as u32).to_be_bytes().to_vec());
            buffer.append(&mut tmp);
        }

        let tmp = serde_json::to_string(option.df_schema.as_ref()).map_err(|err| {
            Error::InvalidQueryExprMsg {
                err: err.to_string(),
            }
        })?;
        buffer.append(&mut (tmp.len() as u32).to_be_bytes().to_vec());
        buffer.append(&mut tmp.into_bytes());

        let tmp = serde_json::to_string(&option.table_schema).map_err(|err| {
            Error::InvalidQueryExprMsg {
                err: err.to_string(),
            }
        })?;
        buffer.append(&mut (tmp.len() as u32).to_be_bytes().to_vec());
        buffer.append(&mut tmp.into_bytes());

        Ok(buffer)
    }

    pub fn decode(buf: &[u8]) -> Result<QueryExpr> {
        let decode_data_len_val = |reader: &mut BufReader<&[u8]>| -> Result<Vec<u8>> {
            let mut len_buf: [u8; 4] = [0; 4];
            reader.read_exact(&mut len_buf)?;

            let mut data_buf = vec![];
            data_buf.resize(u32::from_be_bytes(len_buf) as usize, 0);
            reader.read_exact(&mut data_buf)?;

            Ok(data_buf)
        };

        let mut buffer = BufReader::new(buf);

        let mut count_buf: [u8; 4] = [0; 4];
        buffer.read_exact(&mut count_buf)?;
        let count = u32::from_be_bytes(count_buf);
        let mut filters = Vec::with_capacity(count as usize);
        for _i in 0..count {
            let data_buf = decode_data_len_val(&mut buffer)?;
            let expr = Expr::from_bytes(&data_buf).map_err(|err| Error::InvalidQueryExprMsg {
                err: err.to_string(),
            })?;

            filters.push(expr);
        }

        let data_buf = decode_data_len_val(&mut buffer)?;
        let data = String::from_utf8(data_buf).map_err(|err| Error::InvalidQueryExprMsg {
            err: err.to_string(),
        })?;
        let df_schema =
            serde_json::from_str::<Schema>(&data).map_err(|err| Error::InvalidQueryExprMsg {
                err: err.to_string(),
            })?;
        let df_schema = Arc::new(df_schema);

        let data_buf = decode_data_len_val(&mut buffer)?;
        let data = String::from_utf8(data_buf).map_err(|err| Error::InvalidQueryExprMsg {
            err: err.to_string(),
        })?;
        let table_schema = serde_json::from_str::<TskvTableSchema>(&data).map_err(|err| {
            Error::InvalidQueryExprMsg {
                err: err.to_string(),
            }
        })?;

        Ok(QueryExpr {
            filters,
            df_schema,
            table_schema,
        })
    }
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
    fn test_of_ranges() {
        let f1 = Range::lt(&DataType::Float64, &ScalarValue::Float64(Some(-1000000.1)));
        let f2 = Range::gt(&DataType::Float64, &ScalarValue::Float64(Some(2.2)));
        let f3 = Range::eq(
            &DataType::Float64,
            &ScalarValue::Float64(Some(3333333333333333.3)),
        );

        let domain_1 = Domain::of_ranges(&[f1]).unwrap();
        let domain_2 = Domain::of_ranges(&[f2, f3]).unwrap();

        assert!(matches!(domain_1, Domain::Range(_)));

        assert!(match domain_2 {
            Domain::Range(val_set) => {
                // f2 and f3 will span to 1 range
                val_set.low_indexed_ranges.len() == 1
            }
            _ => false,
        });
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
