use std::{
    cmp::{self, Ordering},
    collections::{BTreeMap, HashMap, HashSet},
    hash::Hash,
    sync::Arc,
};

use datafusion::{
    arrow::datatypes::DataType,
    error::DataFusionError,
    logical_expr::{utils::expr_to_columns, Expr, Operator},
    logical_plan::combine_filters,
    scalar::ScalarValue,
};
use models::schema::{TableFiled, TableSchema};
use models::{Error, Result};

use trace::info;

use crate::helper::RowExpressionToDomainsVisitor;

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
        ColumnDomains {
            column_to_domain: None,
        }
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
}

#[derive(Debug, Default)]
pub struct Predicate {
    filters: Vec<Expr>,
    pushed_down_domains: Option<ColumnDomains<TableFiled>>,
    limit: Option<usize>,
    timeframe: TimeRange,
}

impl Predicate {
    pub fn new(
        filters: Vec<Expr>,
        pushed_down_domains: Option<ColumnDomains<TableFiled>>,
        limit: Option<usize>,
        timeframe: TimeRange,
    ) -> Self {
        Self {
            filters,
            pushed_down_domains,
            limit,
            timeframe,
        }
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn domains(&self) -> &Option<ColumnDomains<TableFiled>> {
        &self.pushed_down_domains
    }

    pub fn set_limit(mut self, limit: Option<usize>) -> Predicate {
        self.limit = limit;
        self
    }
    pub fn combine_expr(&self) -> Option<Expr> {
        let mut res: Option<Expr> = None;
        for i in &self.filters {
            if let Some(e) = res {
                res = Some(e.and(i.clone()))
            } else {
                res = Some(i.clone())
            }
        }
        res
    }

    pub fn split_expr(predicate: &Expr, predicates: &mut Vec<Expr>) {
        match predicate {
            Expr::BinaryExpr {
                right,
                op: Operator::And,
                left,
            } => {
                Self::split_expr(left, predicates);
                Self::split_expr(right, predicates);
            }
            other => predicates.push(other.clone()),
        }
    }
    pub fn primitive_binary_expr(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr { left, op, right } => {
                matches!(
                    (&**left, &**right),
                    (Expr::Column(_), Expr::Literal(_)) | (Expr::Literal(_), Expr::Column(_))
                ) && matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                )
            }
            _ => false,
        }
    }
    pub fn pushdown_exprs(mut self, filters: &[Expr]) -> Predicate {
        let mut exprs = vec![];
        filters
            .iter()
            .for_each(|expr| Self::split_expr(expr, &mut exprs));

        let mut pushdown: Vec<Expr> = vec![];
        let exprs_result = exprs
            .into_iter()
            .try_for_each::<_, Result<_, DataFusionError>>(|expr| {
                let mut columns = HashSet::new();
                expr_to_columns(&expr, &mut columns)?;

                if columns.len() == 1 && Self::primitive_binary_expr(&expr) {
                    pushdown.push(expr);
                }
                Ok(())
            });

        match exprs_result {
            Ok(()) => {
                self.filters.append(&mut pushdown);
            }
            Err(e) => {
                info!(
                    "Error, {}, building push-down predicates for filters: {:#?}. No
                predicates are pushed down",
                    e, filters
                );
            }
        }
        self
    }

    /// resolve and extract supported filter
    /// convert filter to ColumnDomains and set self
    pub fn extract_pushed_down_domains(
        mut self,
        filters: &[Expr],
        table_schema: &TableSchema,
    ) -> Predicate {
        if let Some(ref expr) = combine_filters(filters) {
            let domains_result = expr_to_domains(expr, table_schema);
            match domains_result {
                Ok(domains) => {
                    self.pushed_down_domains = Some(domains);
                }
                Err(e) => {
                    info!(
                        "Error, {}, building push-down predicates for filters: {:#?}. No
                    predicates are pushed down",
                        e, filters
                    );
                }
            }
        }
        self
    }

    /// resolve the time range based on the input expression
    pub fn with_time_frame(mut self, max_ts: i64, min_ts: i64) -> Self {
        self.timeframe = TimeRange::new(max_ts, min_ts);
        self
    }

    pub fn get_time_range(&self) -> (i64, i64) {
        (self.timeframe.min_ts, self.timeframe.max_ts)
    }
}

pub fn expr_to_domains(
    expr: &Expr,
    table_schema: &TableSchema,
) -> Result<ColumnDomains<TableFiled>, DataFusionError> {
    let fields = &table_schema.fields;
    let column_domains = RowExpressionToDomainsVisitor::expr_to_column_domains(expr)?;
    let resutl = column_domains.translate_column(|col| fields.get(&col.name).cloned());

    Ok(resutl)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_of_ranges() {
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

    #[tokio::test]
    async fn test_of_values() {
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
