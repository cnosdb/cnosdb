use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Bound;

use arrow::datatypes::*;
use arrow_array::{downcast_primitive, Array, ArrayRef, PrimitiveArray, RecordBatch};
use stable_vec::StableVec;

use crate::error;
use crate::error::Result;

macro_rules! primitive_cut_record_batch_helper {
    ($t:ty, $batches:ident, $column_idx:ident) => {
        cut_record_batches_primitive::<$t>($batches, $column_idx)
    };
}

pub fn cut_record_batches(
    batches: &mut StableVec<VecDeque<RecordBatch>>,
    data_type: &arrow::datatypes::DataType,
    column_idx: usize,
) -> Result<Vec<RecordBatch>> {
    let res = downcast_primitive! {
        data_type => (primitive_cut_record_batch_helper, batches, column_idx),
        _ => return Err(error::Error::Unimplemented { msg: format!("cut record_batches not support {}", data_type)})
    };
    Ok(res)
}

fn find_cut_range<T: ArrowPrimitiveType>(arrays: &[(usize, ArrayRef)]) -> Vec<(usize, usize)> {
    let array = arrays[0]
        .1
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("find cut range");
    let mut begin = array.value(0);
    let mut end = Bound::Included(array.value(array.len() - 1));
    let mut min_idx: Vec<usize> = vec![0];

    for (idx, (_, array)) in arrays.iter().enumerate().skip(1) {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .expect("find cut range");

        let first = array.value(0);
        let last = array.value(array.len() - 1);

        if first.is_lt(begin) {
            // 开区间
            end = Bound::Excluded(begin);
            begin = first;
            min_idx.clear();
            min_idx.push(idx);
        } else if first.is_eq(begin) {
            min_idx.push(idx)
        } else {
            match end {
                Bound::Included(v) => {
                    if first.is_le(v) {
                        end = Bound::Excluded(first)
                    }
                }
                Bound::Excluded(v) => {
                    if first.lt(&v) {
                        end = Bound::Excluded(first)
                    }
                }
                Bound::Unbounded => {}
            }
        }

        match end {
            Bound::Included(v) => {
                if last.lt(&v) {
                    end = Bound::Included(last)
                }
            }
            Bound::Excluded(v) => {
                if last.lt(&v) {
                    end = Bound::Included(last)
                }
            }
            Bound::Unbounded => {}
        }
    }

    debug_assert!(!min_idx.is_empty());
    let mut res = Vec::with_capacity(min_idx.len());
    for i in min_idx {
        let array = arrays[i]
            .1
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .expect("find cut len");
        let len = cut_len_primitive(array, &end);
        res.push((arrays[i].0, len))
    }
    res
}

fn cut_record_batches_primitive<T: ArrowPrimitiveType>(
    batches: &mut StableVec<VecDeque<RecordBatch>>,
    column_idx: usize,
) -> Vec<RecordBatch> {
    let arrays = batches
        .iter()
        .map(|(idx, b)| (idx, b.front().unwrap().column(column_idx).clone()))
        .collect::<Vec<_>>();
    let to_cut = find_cut_range::<T>(arrays.as_slice());

    let mut merge = vec![];
    for (i, cut_len) in to_cut {
        let cache = batches.get_mut(i).unwrap();
        let batch = cache.pop_front().unwrap();
        merge.push(batch.slice(0, cut_len));
        if cut_len < batch.num_rows() {
            cache.push_front(batch.slice(cut_len, batch.num_rows() - cut_len))
        }
    }

    merge
}

fn cut_len_primitive<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    value: &Bound<T::Native>,
) -> usize {
    match value {
        Bound::Included(v) => match binary_search_primitive(array, v) {
            Ok(i) => last_eq(array, i) + 1,
            Err(i) => i,
        },
        Bound::Excluded(v) => match binary_search_primitive(array, v) {
            Ok(i) => first_eq(array, i),
            Err(i) => i,
        },
        Bound::Unbounded => array.len(),
    }
}

fn first_eq<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>, idx: usize) -> usize {
    let mut res = idx;
    if res >= array.len() {
        return idx;
    }

    let value = array.value(idx);
    loop {
        if res == 0 {
            return res;
        }
        if array.value(res - 1).eq(&value) {
            res -= 1;
        } else {
            return res;
        }
    }
}

fn last_eq<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>, idx: usize) -> usize {
    let mut res = idx;
    if res + 1 >= array.len() {
        return res;
    }

    let value = array.value(idx);
    loop {
        if res + 1 >= array.len() {
            return res;
        }
        if array.value(res + 1).eq(&value) {
            res += 1;
        } else {
            return res;
        }
    }
}

fn binary_search_primitive<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    value: &T::Native,
) -> std::result::Result<usize, usize> {
    let mut size = array.len();
    let mut left = 0;
    let mut right = size;

    match array.value(size - 1).compare(*value) {
        Ordering::Less => return Err(size),
        Ordering::Equal => {
            return Ok(size - 1);
        }
        Ordering::Greater => {
            size -= 1;
        }
    }

    while left < right {
        let mid = left + size / 2;

        // SAFETY: the while condition means `size` is strictly positive, so
        // `size/2 < size`. Thus `left + size/2 < left + size`, which
        // coupled with the `left + size <= self.len()` invariant means
        // we have `left + size/2 < self.len()`, and this is in-bounds.
        let mid_value = array.value(mid);
        let cmp = mid_value.compare(*value);

        if cmp == Ordering::Less {
            left = mid + 1;
        } else if cmp == Ordering::Greater {
            right = mid;
        } else {
            return Ok(mid);
        }
        size = right - left;
    }
    Err(left)
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use arrow_array::types::Int64Type;
    use arrow_array::{ArrayRef, Int64Array};

    use crate::reader::batch_cut::{binary_search_primitive, cut_len_primitive, find_cut_range};

    #[test]
    fn test_binary_search() {
        let array = Int64Array::from_iter_values([1, 1, 1, 1, 1, 1, 1, 1]);
        let res = binary_search_primitive(&array, &1);
        assert!(matches!(res, Ok(0..8)));

        let array = Int64Array::from_iter_values([1, 1, 1, 1, 1, 1, 1, 1]);
        let res = binary_search_primitive(&array, &2);
        assert!(matches!(res, Err(8)));

        let array = Int64Array::from_iter_values([1, 1, 1, 1, 2, 2, 2, 2]);
        let res = binary_search_primitive(&array, &1);
        assert!(matches!(res, Ok(0..4)));

        let array = Int64Array::from_iter_values([1, 1, 1, 1, 2, 2, 2, 5]);
        let res = binary_search_primitive(&array, &3);
        assert!(matches!(res, Err(7)));

        let array = Int64Array::from_iter_values([1, 1, 2, 2, 2, 5, 5, 5]);
        let res = binary_search_primitive(&array, &3);
        assert!(matches!(res, Err(5)));

        let array = Int64Array::from_iter_values([1, 1, 2, 2, 3, 3, 5, 5]);
        let res = binary_search_primitive(&array, &3);
        assert!(matches!(res, Ok(5..6)));
    }

    #[test]
    fn test_cut_len() {
        let array = Int64Array::from_iter_values([1, 1, 1, 1, 1, 1, 1, 1]);
        let value = Bound::Included(2);
        let cut_len = cut_len_primitive(&array, &value);
        assert_eq!(cut_len, 8);

        let array = Int64Array::from_iter_values([1, 1, 1, 1, 1, 1, 1, 1]);
        let value = Bound::Included(1);
        let cut_len = cut_len_primitive(&array, &value);
        assert_eq!(cut_len, 8);

        let array = Int64Array::from_iter_values([1, 1, 1, 1, 2, 2, 2, 2]);
        let value = Bound::Included(1);
        let cut_len = cut_len_primitive(&array, &value);
        assert_eq!(cut_len, 4);

        let array = Int64Array::from_iter_values([1, 1, 1, 1, 2, 2, 2, 2]);
        let value = Bound::Included(2);
        let cut_len = cut_len_primitive(&array, &value);
        assert_eq!(cut_len, 8);

        let array = Int64Array::from_iter_values([1, 1, 1, 1, 2, 2, 2, 2]);
        let value = Bound::Excluded(2);
        let cut_len = cut_len_primitive(&array, &value);
        assert_eq!(cut_len, 4);

        let array = Int64Array::from_iter_values([1, 1, 1, 1, 2, 2, 2, 2]);
        let value = Bound::Excluded(1);
        let cut_len = cut_len_primitive(&array, &value);
        assert_eq!(cut_len, 0);
    }

    #[test]
    fn test_find_cut_range() {
        let array1 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let array2 = Arc::new(Int64Array::from_iter_values([4, 5, 6]));
        let array3 = Arc::new(Int64Array::from_iter_values([7, 8, 9]));
        let arrays: Vec<(usize, ArrayRef)> = vec![(0, array1), (1, array2), (2, array3)];
        let res = find_cut_range::<Int64Type>(arrays.as_slice());
        assert_eq!(res, vec![(0, 3)]);

        let array1 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let array2 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let array3 = Arc::new(Int64Array::from_iter_values([4, 5, 6]));
        let arrays: Vec<(usize, ArrayRef)> = vec![(0, array1), (1, array2), (2, array3)];
        let res = find_cut_range::<Int64Type>(arrays.as_slice());
        assert_eq!(res, vec![(0, 3), (1, 3)]);

        let array1 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let array2 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let array3 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let arrays: Vec<(usize, ArrayRef)> = vec![(0, array1), (1, array2), (2, array3)];
        let res = find_cut_range::<Int64Type>(arrays.as_slice());
        assert_eq!(res, vec![(0, 3), (1, 3), (2, 3)]);

        let array1 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let array2 = Arc::new(Int64Array::from_iter_values([2, 3, 4]));
        let array3 = Arc::new(Int64Array::from_iter_values([3, 4, 5]));
        let arrays: Vec<(usize, ArrayRef)> = vec![(0, array1), (1, array2), (2, array3)];
        let res = find_cut_range::<Int64Type>(arrays.as_slice());
        assert_eq!(res, vec![(0, 1)]);

        let array1 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let array2 = Arc::new(Int64Array::from_iter_values([2, 3, 4]));
        let array3 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let arrays: Vec<(usize, ArrayRef)> = vec![(0, array1), (1, array2), (2, array3)];
        let res = find_cut_range::<Int64Type>(arrays.as_slice());
        assert_eq!(res, vec![(0, 1), (2, 1)]);
    }
}
