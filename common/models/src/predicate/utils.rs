use std::ops::{Bound, RangeBounds};

use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::scalar::ScalarValue;

use super::domain::{ColumnDomains, TimeRange};
use crate::predicate::domain::{Domain, Range, ValueEntry};
use crate::schema::TIME_FIELD_NAME;

pub fn filter_to_time_ranges(time_domain: &ColumnDomains<String>) -> Vec<TimeRange> {
    if time_domain.is_none() {
        // Does not contain any data, and returns an empty array directly
        return vec![];
    }

    if time_domain.is_all() {
        // Include all data
        return vec![TimeRange::all()];
    }

    let mut time_ranges: Vec<TimeRange> = Vec::new();

    if let Some(time_domain) = time_domain.domains() {
        if let Some(domain) = time_domain.get(TIME_FIELD_NAME) {
            // Convert ScalarValue value to nanosecond timestamp
            let valid_and_generate_index_key = |v: &ScalarValue| {
                // Time can only be of type Timestamp
                assert!(matches!(v.get_datatype(), ArrowDataType::Timestamp(_, _)));
                unsafe { i64::try_from(v.clone()).unwrap_unchecked() }
            };

            match domain {
                Domain::Range(range_set) => {
                    for (_, range) in range_set.low_indexed_ranges().into_iter() {
                        let range: &Range = range;

                        let start_bound = range.start_bound();
                        let end_bound = range.end_bound();

                        // Convert the time value in Bound to timestamp
                        let translate_bound = |bound: Bound<&ScalarValue>| match bound {
                            Bound::Unbounded => Bound::Unbounded,
                            Bound::Included(v) => Bound::Included(valid_and_generate_index_key(v)),
                            Bound::Excluded(v) => Bound::Excluded(valid_and_generate_index_key(v)),
                        };

                        let range = (translate_bound(start_bound), translate_bound(end_bound));
                        time_ranges.push(range.into());
                    }
                }
                Domain::Equtable(vals) => {
                    if !vals.is_white_list() {
                        // eg. time != xxx
                        time_ranges.push(TimeRange::all());
                    } else {
                        // Contains the given value
                        for entry in vals.entries().into_iter() {
                            let entry: &ValueEntry = entry;

                            let ts = valid_and_generate_index_key(entry.value());

                            time_ranges.push(TimeRange::new(ts, ts));
                        }
                    }
                }
                Domain::All => time_ranges.push(TimeRange::all()),
                Domain::None => return vec![],
            }
        } else {
            time_ranges.push(TimeRange::all());
        }
    }

    time_ranges
}
