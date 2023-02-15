use std::fmt::Debug;
use std::{mem, ptr};

/// Removes all but the last of consecutive elements in the vector that resolve to the same
/// key.
///
/// If the vector is sorted, this removes all duplicates.
/// ```
#[inline]
pub fn dedup_front_by_key<T, F, K>(v: &mut Vec<T>, mut key: F)
where
    T: Debug,
    F: FnMut(&mut T) -> K,
    K: PartialEq,
{
    dedup_front_by(v, |a, b| key(a) == key(b))
}

/// Removes all but the last of consecutive elements in the vector satisfying a given equality
/// relation.
///
/// The `same_bucket` function is passed references to two elements from the vector and
/// must determine if the elements compare equal. The elements are passed in opposite order
/// from their order in the slice, so if `same_bucket(a, b)` returns `true`, `a` is removed.
///
/// If the vector is sorted, this removes all duplicates.
/// ```
pub fn dedup_front_by<T, F>(v: &mut Vec<T>, mut same_bucket: F)
where
    T: Debug,
    F: FnMut(&mut T, &mut T) -> bool,
{
    let len = v.len();
    if len <= 1 {
        return;
    }

    /* INVARIANT: vec.len() > read >= write > write-1 >= 0 */
    struct FillGapOnDrop<'a, T> {
        /* Offset of the element we want to check if it is duplicate */
        read: usize,
        /* Offset of the element we want to check with if it is duplicate */
        prev: usize,

        /* Offset of the place where we want to place the non-duplicate
         * when we find it. */
        write: usize,

        /* The Vec that would need correction if `same_bucket` panicked */
        vec: &'a mut Vec<T>,
    }

    impl<'a, T> Drop for FillGapOnDrop<'a, T> {
        fn drop(&mut self) {
            /* This code gets executed when `same_bucket` panics */

            /* SAFETY: invariant guarantees that `read - write`
             * and `len - read` never overflow and that the copy is always
             * in-bounds. */
            unsafe {
                let ptr = self.vec.as_mut_ptr();
                let len = self.vec.len();

                /* How many items were left when `same_bucket` panicked.
                 * Basically vec[read..].len() */
                let items_left = len.wrapping_sub(self.read);

                /* Pointer to first item in vec[write..write+items_left] slice */
                let dropped_ptr = ptr.add(self.write);
                /* Pointer to first item in vec[read..] slice */
                let valid_ptr = ptr.add(self.read);

                /* Copy `vec[read..]` to `vec[write..write+items_left]`.
                 * The slices can overlap, so `copy_nonoverlapping` cannot be used */
                ptr::copy(valid_ptr, dropped_ptr, items_left);

                /* How many items have been already dropped
                 * Basically vec[read..write].len() */
                let dropped = self.read.wrapping_sub(self.write);

                self.vec.set_len(len - dropped);
            }
        }
    }

    let mut gap = FillGapOnDrop {
        read: 1,
        prev: 0,
        write: 0,
        vec: v,
    };
    let ptr = gap.vec.as_mut_ptr();

    /* Drop items while going through Vec, it should be more efficient than
     * doing slice partition_dedup + truncate */

    /* SAFETY: Because of the invariant, read_ptr, prev_ptr and write_ptr
     * are always in-bounds and read_ptr never aliases prev_ptr */
    unsafe {
        while gap.read < len {
            let prev_ptr = ptr.add(gap.prev);
            let read_ptr = ptr.add(gap.read);

            if same_bucket(&mut *prev_ptr, &mut *read_ptr) {
                // Increase `gap.read` now since the drop may panic.
                gap.read += 1;
                /* We have found duplicate, drop it in-place */
                ptr::drop_in_place(read_ptr);
            } else {
                let read_ptr = ptr.add(gap.read.wrapping_sub(1));
                let write_ptr = ptr.add(gap.write);

                /* Because `read_ptr` can be equal to `write_ptr`, we either
                 * have to use `copy` or conditional `copy_nonoverlapping`.
                 * Looks like the first option is faster. */
                ptr::copy(read_ptr, write_ptr, 1);

                /* We have filled that place, so go further */
                gap.write += 1;
                gap.prev = gap.read;
                gap.read += 1;
            }
        }
        let final_ptr = ptr.add(gap.read.wrapping_sub(1));
        let write_ptr = ptr.add(gap.write);
        ptr::copy(final_ptr, write_ptr, 1);
        gap.write += 1;

        /* Technically we could let `gap` clean up with its Drop, but
         * when `same_bucket` is guaranteed to not panic, this bloats a little
         * the codegen, so we just do it manually */
        gap.vec.set_len(gap.write);
        mem::forget(gap);
    }
}

#[test]
fn test_dedup_by_key() {
    fn case(a: Vec<i32>, b: Vec<i32>) {
        let mut v = a;
        dedup_front_by_key(&mut v, |i| *i / 10);
        assert_eq!(v, b);
    }
    case(vec![], vec![]);
    case(vec![10], vec![10]);
    case(vec![10, 11], vec![11]);
    case(vec![10, 20, 30], vec![10, 20, 30]);
    case(vec![10, 11, 20, 30], vec![11, 20, 30]);
    case(vec![10, 20, 21, 30], vec![10, 21, 30]);
    case(vec![10, 20, 30, 31], vec![10, 20, 31]);
    case(vec![10, 11, 20, 21, 22, 30, 31], vec![11, 22, 31]);
}

#[test]
fn test_dedup_by() {
    let mut vec = vec!["foo", "bar", "Bar", "baz", "bar"];
    dedup_front_by(&mut vec, |a, b| a.eq_ignore_ascii_case(b));

    assert_eq!(vec, ["foo", "Bar", "baz", "bar"]);

    let mut vec = vec![
        ("foo", 1),
        ("foo", 2),
        ("foo", 3),
        ("bar", 3),
        ("bar", 4),
        ("bar", 5),
    ];
    dedup_front_by(&mut vec, |a, b| {
        a.0 == b.0 && {
            b.1 += a.1;
            true
        }
    });

    assert_eq!(vec, [("foo", 4), ("bar", 8)]);
}
