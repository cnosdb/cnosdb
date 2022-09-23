use std::fmt::Debug;

const ONE_MASK: [u8; 8] = [1, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7];
const ZERO_MASK: [u8; 8] = [
    255 - 1,
    255 - (1 << 1),
    255 - (1 << 2),
    255 - (1 << 3),
    255 - (1 << 4),
    255 - (1 << 5),
    255 - (1 << 6),
    255 - (1 << 7),
];

const HIGH_ONES_MASK: [u8; 8] = [0, 128, 192, 224, 240, 248, 252, 254];
const LOW_ONES_MASK: [u8; 8] = [
    0,
    1,
    (1 << 2) - 1,
    (1 << 3) - 1,
    (1 << 4) - 1,
    (1 << 5) - 1,
    (1 << 6) - 1,
    (1 << 7) - 1,
];

#[derive(Clone, Default, PartialEq, Eq)]
pub struct Bitmap(Vec<u8>);

impl Bitmap {
    pub fn new() -> Self {
        Self(vec![0_u8; 1])
    }

    pub fn with_data(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn get(&self, i: usize) -> bool {
        if Self::byte_index(i) > self.bytes_len() {
            return false;
        }
        self.0[i >> 3] & ONE_MASK[i & 7] != 0
    }

    pub fn set(&mut self, i: usize, non_null: bool) {
        self.ensure_length(i);
        if non_null {
            self.0[i >> 3] |= ONE_MASK[i & 7];
        } else {
            self.0[i >> 3] &= ZERO_MASK[i & 7];
        }
    }

    /// Delete a range ( [start, end] ) in current BitMap
    pub fn delete_range(&mut self, start: usize, mut end: usize) {
        if end < start {
            return;
        }
        let bit_count = self.bit_count();
        if end >= bit_count {
            end = bit_count - 1;
        }
        if start == 0 && end == bit_count - 1 {
            self.0.clear();
            return;
        }

        let start_del_idx = start >> 3;
        let end_del_idx = end >> 3;

        // If only needs a simple truncate.
        if start_del_idx == end_del_idx && end_del_idx == self.bytes_len() - 1 {
            let ptr = self.0.as_mut_ptr();
            unsafe {
                let curr = ptr.add(end_del_idx);
                *curr >>= end - start + 1;
                if *curr == 0 {
                    self.0.truncate(self.bytes_len() - 1);
                }
            }
            return;
        }

        // Remaining bit count at the start index (start % 8).
        let start_remain_bits = start & 7;
        // Remaining bit count at the end index (8 - ( end % 8 - 1)).
        let end_remain_bits = 8 - (end & 7) - 1;
        // Valid low bits at the start index.
        let mut low = start_remain_bits + end_remain_bits;

        match low.cmp(&8_usize) {
            std::cmp::Ordering::Less => {
                // Valid high bits num for the next indexs.
                let high = low;
                // Valid low bits num for the next indexs.
                low = 8 - low;

                let ptr = self.0.as_mut_ptr();
                // SAFETY: start_del_idx and end_del_idx is less than or equal to self.0.len().
                unsafe {
                    let next_ptr = ptr.add(end_del_idx);
                    let mut next = (*next_ptr & HIGH_ONES_MASK[end_remain_bits]) >> low;
                    let mut curr_ptr = ptr.add(start_del_idx);
                    *curr_ptr &= LOW_ONES_MASK[start_remain_bits];
                    *curr_ptr |= next;

                    if end_del_idx + 1 < self.bytes_len() {
                        *curr_ptr |= (*next_ptr.add(1) & LOW_ONES_MASK[low]) << high;

                        for i in 1..self.bytes_len() - end_del_idx {
                            curr_ptr = ptr.add(start_del_idx + i);

                            next = (*next_ptr.add(i) & HIGH_ONES_MASK[high]) >> low;
                            *curr_ptr &= LOW_ONES_MASK[low];
                            *curr_ptr |= next;

                            *curr_ptr |= (*next_ptr.add(i + 1) & LOW_ONES_MASK[low]) << high;
                        }
                    }
                }
                let new_len = self.bytes_len() + start_del_idx - end_del_idx;
                self.0.truncate(new_len)
            }
            std::cmp::Ordering::Equal => {
                // Valid low & high bits num for the next indexs.
                low = 4;

                let ptr = self.0.as_mut_ptr();
                // SAFETY: start_del_idx and end_del_idx is less than or equal to self.0.len().
                unsafe {
                    let next_ptr = ptr.add(end_del_idx);
                    let mut next = *next_ptr & HIGH_ONES_MASK[end_remain_bits];
                    let mut curr_ptr = ptr.add(start_del_idx);
                    *curr_ptr &= LOW_ONES_MASK[start_remain_bits];
                    *curr_ptr |= next;

                    if end_del_idx + 1 < self.bytes_len() {
                        for i in 1..self.bytes_len() - end_del_idx {
                            curr_ptr = ptr.add(start_del_idx + i);

                            next = *next_ptr.add(i) & HIGH_ONES_MASK[low];
                            *curr_ptr &= LOW_ONES_MASK[low];
                            *curr_ptr |= next;
                        }
                    }
                }
                let new_len = self.bytes_len() + start_del_idx - end_del_idx;
                self.0.truncate(new_len)
            }
            std::cmp::Ordering::Greater => {
                // Valid high bits num, for the next indexs.
                let high = low - 8;
                // Valid low bits num for the next indexs.
                low = 8 - high;

                let ptr = self.0.as_mut_ptr();
                // SAFETY: start_del_idx and end_del_idx is less than or equal to self.0.len().
                unsafe {
                    let next_ptr = ptr.add(end_del_idx);
                    let mut next = (*next_ptr & HIGH_ONES_MASK[end_remain_bits]) << high;
                    let prev = ptr.add(start_del_idx);
                    *prev &= LOW_ONES_MASK[start_remain_bits];
                    *prev |= next;
                    *next_ptr >>= low;

                    if end_del_idx + 1 < self.bytes_len() {
                        for i in 1..self.bytes_len() - end_del_idx {
                            next = (*next_ptr.add(i) & LOW_ONES_MASK[low]) << high;
                            *ptr.add(start_del_idx + i) |= next;
                            *next_ptr.add(i) >>= low;
                        }
                    }
                }
                let new_len = self.bytes_len() + start_del_idx - end_del_idx + 1;
                self.0.truncate(new_len)
            }
        }
    }

    /// The provided i devided by 8
    fn byte_index(i: usize) -> usize {
        i >> 3
    }

    /// Makes sure that self.0.len() is greater than i.
    fn ensure_length(&mut self, i: usize) {
        let new_len = Self::byte_index(i) + 1;
        if new_len > self.0.len() {
            self.0.resize(new_len, 0);
        }
    }

    pub fn has_null(&self, bit_count: usize) -> bool {
        for i in 0..self.bytes_len() - 1 {
            if self.0[i] != 255 {
                return true;
            }
        }
        self.0
            .last()
            .map(|v| *v | HIGH_ONES_MASK[8 - (bit_count & 7)] != 255)
            .unwrap()
    }

    /// Bit count of the current BitMap
    pub fn bit_count(&self) -> usize {
        self.bytes_len() * 8
    }

    /// Byte count of the curent BitMap
    pub fn bytes_len(&self) -> usize {
        self.0.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Debug for Bitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Bitmap ({})",
            self.0
                .iter()
                .map(|u| format!("{:b}", u))
                .collect::<Vec<String>>()
                .join(" ")
        )
    }
}

#[test]
pub fn test_bitmap_get_set() {
    let mut bitmap = Bitmap::new();

    bitmap.set(0, true);
    assert_eq!(bitmap.as_slice(), &[1]);
    assert!(bitmap.get(0));

    bitmap.set(3, true);
    bitmap.set(8, true);
    assert_eq!(bitmap.as_slice(), &[9, 1]);
    assert!(bitmap.get(3));
    assert!(bitmap.get(8));

    bitmap.set(8, false);
    assert_eq!(bitmap.as_slice(), &[9, 0]);
    assert!(!bitmap.get(8));
}

#[test]
pub fn test_bitmap_exclude() {
    let mut bitmap = Bitmap::new();
    for i in 0..30 {
        bitmap.set(i, true);
    }
    // [0b_1111_1111, 0b_1111_1111, 0b_1111_1111, 0_b_0011_1111]
    assert_eq!(bitmap.as_slice(), &[255, 255, 255, 63]);

    let mut bitmap_1 = bitmap.clone();
    bitmap_1.delete_range(3, 13);
    // [0b_1111_1111, 0b_1111_1111, 0b_1111_1111, 0_b_0011_1111]
    //   ----_-+++       ++--_----     ++++_++++       ++_++++
    // [0b_1111_1111, 0b_1111_1111, 0b_0000_0111]
    dbg!(&bitmap_1);
    assert_eq!(bitmap_1.as_slice(), &[255, 255, 7]);

    let mut bitmap_2 = bitmap.clone();
    bitmap_2.delete_range(4, 11);
    // [0b_1111_1111, 0b_1111_1111, 0b_1111_1111, 0b_0011_1111]
    //   ----_++++       ++++_----     ++++_++++       ++_++++
    // [0b_1111_1111, 0b_1111_1111, 0b_0011_1111]
    dbg!(&bitmap_2);
    assert_eq!(bitmap_2.as_slice(), &[255, 255, 63]);

    let mut bitmap_3 = bitmap.clone();
    bitmap_3.delete_range(5, 9);
    // [0b_1111_1111, 0b_1111_1111, 0b_1111_1111, 0b_0011_1111]
    //     ---+_++++     ++++_++--     ++++_++++       ++_++++
    // [0b_1111_1111, 0b_1111_1111, 0b_1111_1111, 0b_0000_0001]
    dbg!(&bitmap_3);
    assert_eq!(bitmap_3.as_slice(), &[255, 255, 255, 1]);

    let mut bitmap_4 = bitmap.clone();
    bitmap_4.delete_range(0, 2);
    // [0b_1111_1111, 0b_1111_1111, 0b_1111_1111, 0b_0011_1111]
    //     ++++_+---     ++++_++--     ++++_++++       ++_++++
    // [0b_1111_1111, 0b_1111_1111, 0b_1111_1111, 0b0000_0111]
    dbg!(&bitmap_4);
    assert_eq!(bitmap_4.as_slice(), &[255, 255, 255, 7]);

    let mut bitmap_5 = bitmap.clone();
    bitmap_5.delete_range(0, 10);
    // [0b_1111_1111, 0b_1111_1111, 0b_1111_1111, 0b_0011_1111]
    //     ----_----     ++++_+---     ++++_++++       ++_++++
    // [0b_1111_1111, 0b_1111_1111, 0b0000_0111]
    dbg!(&bitmap_5);
    assert_eq!(bitmap_5.as_slice(), &[255, 255, 7]);

    let mut bitmap_6 = bitmap.clone();
    bitmap_6.delete_range(28, 30);
    // [0b_1111_1111, 0b_1111_1111, 0b_1111_1111, 0b_0011_1111]
    //     ++++_++++     ++++_++++     ++++_++++       --_-+++
    // [0b_1111_1111, 0b_1111_1111, 0b_1111_1111, 0b_0000_0111]
    dbg!(&bitmap_6);
    assert_eq!(bitmap_6.as_slice(), &[255, 255, 255, 7]);

    let mut bitmap_7 = bitmap.clone();
    bitmap_7.delete_range(20, 30);
    // [0b_1111_1111, 0b_1111_1111, 0b_1111_1111, 0b_0011_1111]
    //     ++++_++++     ++++_++++     ----_++++       --_----
    // [0b_1111_1111, 0b_1111_1111, 0b_0000_1111]
    dbg!(&bitmap_7);
    assert_eq!(bitmap_7.as_slice(), &[255, 255, 15]);
}

#[test]
fn test_bitmap_has_null() {
    let mut bitmap = Bitmap::new();
    for i in 0..30 {
        bitmap.set(i, true);
    }
    assert!(!bitmap.has_null(30));
    bitmap.set(31, false);
    assert!(bitmap.has_null(31));
}
