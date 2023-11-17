#[derive(Debug, Default, Clone)]
pub struct BitSet {
    buffer: Vec<u8>,
    len: usize,
}

impl BitSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn to_vec(&self) -> Vec<bool> {
        let boolset = self
            .buffer
            .iter()
            .flat_map(|&byte| (0..8).map(move |i| (byte >> i) & 1 != 0))
            .collect::<Vec<_>>();
        boolset
    }

    pub fn new_without_check(len: usize, buffer: Vec<u8>) -> Self {
        Self { buffer, len }
    }

    pub fn with_size(count: usize) -> Self {
        let mut bitset = Self::default();
        bitset.append_unset(count);
        bitset
    }

    pub fn with_size_all_set(count: usize) -> Self {
        let mut bitset = Self::default();
        bitset.append_set(count);
        bitset
    }

    /// Create a bitset with a sequence of index number which means value exists.
    ///
    /// The length of the bit set is `len`, but if the max value in `offsets`
    /// is greater than `len`, then the max value will be the `len`.
    pub fn with_offsets(len: usize, offsets: &[usize]) -> Self {
        let mut bitset = match offsets.iter().max() {
            Some(&max_off) => BitSet::with_size(max_off.max(len)),
            None => BitSet::default(),
        };
        for off in offsets {
            bitset.set(*off);
        }

        bitset
    }

    pub fn append_unset(&mut self, count: usize) {
        self.len += count;
        let new_buf_len = (self.len + 7) >> 3;
        self.buffer.resize(new_buf_len, 0);
    }

    pub fn append_set(&mut self, count: usize) {
        let new_len = self.len + count;
        let new_buf_len = (new_len + 7) >> 3;

        let skew = self.len & 7;
        if skew != 0 {
            *self.buffer.last_mut().unwrap() |= 0xFF << skew;
        }

        self.buffer.resize(new_buf_len, 0xFF);

        let rem = new_len & 7;
        if rem != 0 {
            *self.buffer.last_mut().unwrap() &= (1 << rem) - 1;
        }

        self.len = new_len;
    }

    pub fn append_bits(&mut self, count: usize, to_set: &[u8]) {
        assert_eq!((count + 7) >> 3, to_set.len());

        let new_len = self.len + count;
        let new_buf_len = (new_len + 7) >> 3;
        self.buffer.reserve(new_buf_len - self.buffer.len());

        let whole_bytes = count >> 3;
        let overrun = count & 7;

        let skew = self.len & 7;
        if skew == 0 {
            self.buffer.extend_from_slice(&to_set[..whole_bytes]);
            if overrun > 0 {
                let masked = to_set[whole_bytes] & ((1 << overrun) - 1);
                self.buffer.push(masked)
            }

            self.len = new_len;
            debug_assert_eq!(self.buffer.len(), new_buf_len);
            return;
        }

        for to_set_byte in &to_set[..whole_bytes] {
            let low = *to_set_byte << skew;
            let high = *to_set_byte >> (8 - skew);

            *self.buffer.last_mut().unwrap() |= low;
            self.buffer.push(high);
        }

        if overrun > 0 {
            let masked = to_set[whole_bytes] & ((1 << overrun) - 1);
            let low = masked << skew;
            *self.buffer.last_mut().unwrap() |= low;

            if overrun > 8 - skew {
                let high = masked >> (8 - skew);
                self.buffer.push(high)
            }
        }

        self.len = new_len;
        debug_assert_eq!(self.buffer.len(), new_buf_len);
    }

    pub fn set(&mut self, idx: usize) {
        assert!(idx <= self.len);

        let byte_idx = idx >> 3;
        let bit_idx = idx & 7;
        self.buffer[byte_idx] |= 1 << bit_idx;
    }

    pub fn get(&self, idx: usize) -> bool {
        assert!(idx <= self.len);

        let byte_idx = idx >> 3;
        let bit_idx = idx & 7;
        (self.buffer[byte_idx] >> bit_idx) & 1 != 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn byte_len(&self) -> usize {
        self.buffer.len()
    }

    pub fn bytes(&self) -> &[u8] {
        &self.buffer
    }

    pub fn is_all_set(&self) -> bool {
        if self.len == 0 {
            return true;
        }

        let full_blocks = (self.len / 8).saturating_sub(1);
        if !self.buffer.iter().take(full_blocks).all(|&v| v == u8::MAX) {
            return false;
        }

        let mask = match self.len % 8 {
            1..=8 => !(0xFF << (self.len % 8)), // LSB mask
            0 => 0xFF,
            _ => unreachable!(),
        };
        *self.buffer.last().unwrap() == mask
    }

    pub fn is_all_unset(&self) -> bool {
        self.buffer.iter().all(|&v| v == 0)
    }
}

impl PartialEq for BitSet {
    fn eq(&self, other: &Self) -> bool {
        if self.len != other.len {
            return false;
        }
        if self.len == 0 {
            return true;
        }
        let bound = self.len >> 3;
        if self.buffer[..bound] != other.buffer[..bound] {
            return false;
        }
        let mask = 0xFF >> (8 - (self.len & 7));
        (self.buffer[bound] & mask) == (other.buffer[bound] & mask)
    }
}

#[derive(Debug, Default, Clone)]
pub struct ImmutBitSet<'a> {
    buffer: &'a [u8],
    len: usize,
}

impl<'a> ImmutBitSet<'a> {
    pub fn new_without_check(len: usize, buffer: &'a [u8]) -> Self {
        Self { buffer, len }
    }

    pub fn get(&self, idx: usize) -> bool {
        assert!(idx <= self.len);

        let byte_idx = idx >> 3;
        let bit_idx = idx & 7;
        (self.buffer[byte_idx] >> bit_idx) & 1 != 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn byte_len(&self) -> usize {
        self.buffer.len()
    }

    pub fn bytes(&self) -> &[u8] {
        self.buffer
    }

    pub fn is_all_set(&self) -> bool {
        if self.len == 0 {
            return false;
        }

        let full_blocks = (self.len / 8).saturating_sub(1);
        if !self.buffer.iter().take(full_blocks).all(|&v| v == u8::MAX) {
            return false;
        }

        let mask = match self.len % 8 {
            1..=8 => !(0xFF << (self.len % 8)), // LSB mask
            0 => 0xFF,
            _ => unreachable!(),
        };
        *self.buffer.last().unwrap() == mask
    }

    pub fn is_all_unset(&self) -> bool {
        self.buffer.iter().all(|&v| v == 0)
    }
}

impl<'a> PartialEq for ImmutBitSet<'a> {
    fn eq(&self, other: &Self) -> bool {
        if self.len != other.len {
            return false;
        }
        if self.len == 0 {
            return true;
        }
        let bound = self.len >> 3;
        if self.buffer[..bound] != other.buffer[..bound] {
            return false;
        }
        let mask = 0xFF >> (8 - (self.len & 7));
        (self.buffer[bound] & mask) == (other.buffer[bound] & mask)
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::*;
    use rand::rngs::OsRng;

    use super::*;

    /// Computes a compacted representation of a given bool array
    fn compact_bools(bools: &[bool]) -> Vec<u8> {
        bools
            .chunks(8)
            .map(|x| {
                let mut collect = 0_u8;
                for (idx, set) in x.iter().enumerate() {
                    if *set {
                        collect |= 1 << idx
                    }
                }
                collect
            })
            .collect()
    }

    fn iter_set_bools(bools: &[bool]) -> impl Iterator<Item = usize> + '_ {
        bools
            .iter()
            .enumerate()
            .filter(|&(_, y)| *y)
            .map(|(x, _)| x)
    }

    #[test]
    fn test_compact_bools() {
        let bools = &[
            false, false, true, true, false, false, true, false, true, false,
        ];
        let collected = compact_bools(bools);
        let indexes: Vec<_> = iter_set_bools(bools).collect();
        assert_eq!(collected.as_slice(), &[0b01001100, 0b00000001]);
        assert_eq!(indexes.as_slice(), &[2, 3, 6, 8])
    }

    #[test]
    fn test_bit_mask() {
        let mut mask = BitSet::new();

        mask.append_bits(8, &[0b11111111]);
        let d1 = mask.buffer.clone();

        mask.append_bits(3, &[0b01010010]);
        let d2 = mask.buffer.clone();

        mask.append_bits(5, &[0b00010100]);
        let d3 = mask.buffer.clone();

        mask.append_bits(2, &[0b11110010]);
        let d4 = mask.buffer.clone();

        mask.append_bits(15, &[0b11011010, 0b01010101]);
        let d5 = mask.buffer.clone();

        assert_eq!(d1.as_slice(), &[0b11111111]);
        assert_eq!(d2.as_slice(), &[0b11111111, 0b00000010]);
        assert_eq!(d3.as_slice(), &[0b11111111, 0b10100010]);
        assert_eq!(d4.as_slice(), &[0b11111111, 0b10100010, 0b00000010]);
        assert_eq!(
            d5.as_slice(),
            &[0b11111111, 0b10100010, 0b01101010, 0b01010111, 0b00000001]
        );

        assert!(mask.get(0));
        assert!(!mask.get(8));
        assert!(mask.get(9));
        assert!(mask.get(19));

        let immut_mask = ImmutBitSet::new_without_check(mask.len(), &mask.buffer);
        assert!(immut_mask.get(0));
        assert!(!immut_mask.get(8));
        assert!(immut_mask.get(9));
        assert!(immut_mask.get(19));
    }

    fn make_rng() -> StdRng {
        let seed = OsRng.next_u64();
        println!("Seed: {seed}");
        StdRng::seed_from_u64(seed)
    }

    #[test]
    fn test_append_fuzz() {
        let mut mask = BitSet::new();
        let mut all_bools = vec![];
        let mut rng = make_rng();

        for _ in 0..100 {
            let len = (rng.next_u32() % 32) as usize;
            let set = rng.next_u32() & 1 == 0;

            match set {
                true => mask.append_set(len),
                false => mask.append_unset(len),
            }

            all_bools.extend(std::iter::repeat(set).take(len));

            let collected = compact_bools(&all_bools);
            assert_eq!(mask.buffer, collected);
        }
    }

    #[test]
    #[should_panic = "idx <= self.len"]
    fn test_bitset_set_get_out_of_bounds() {
        let mut v = BitSet::with_size(4);

        // The bitset is of length 4, which is backed by a single byte with 8
        // bits of storage capacity.
        //
        // Accessing bits past the 4 the bitset "contains" should not succeed.

        v.get(5);
        v.set(5);
    }

    #[test]
    fn test_all_set_unset() {
        for i in 1..100 {
            let mut v = BitSet::new();
            v.append_set(i);
            assert!(v.is_all_set());
            assert!(!v.is_all_unset());
        }
    }

    #[test]
    fn test_all_set_unset_multi_byte() {
        let mut v = BitSet::new();

        // Bitmap is composed of entirely set bits.
        v.append_set(100);
        assert!(v.is_all_set());
        assert!(!v.is_all_unset());

        // Now the bitmap is neither composed of entirely set, nor entirely
        // unset bits.
        v.append_unset(1);
        assert!(!v.is_all_set());
        assert!(!v.is_all_unset());

        let mut v = BitSet::new();

        // Bitmap is composed of entirely unset bits.
        v.append_unset(100);
        assert!(!v.is_all_set());
        assert!(v.is_all_unset());

        // And once again, it is neither all set, nor all unset.
        v.append_set(1);
        assert!(!v.is_all_set());
        assert!(!v.is_all_unset());
    }

    #[test]
    fn test_all_set_unset_single_byte() {
        let mut v = BitSet::new();

        // Bitmap is composed of entirely set bits.
        v.append_set(2);
        assert!(v.is_all_set());
        assert!(!v.is_all_unset());

        // Now the bitmap is neither composed of entirely set, nor entirely
        // unset bits.
        v.append_unset(1);
        assert!(!v.is_all_set());
        assert!(!v.is_all_unset());

        let mut v = BitSet::new();

        // Bitmap is composed of entirely unset bits.
        v.append_unset(2);
        assert!(!v.is_all_set());
        assert!(v.is_all_unset());

        // And once again, it is neither all set, nor all unset.
        v.append_set(1);
        assert!(!v.is_all_set());
        assert!(!v.is_all_unset());
    }

    #[test]
    fn test_all_set_unset_empty() {
        let v1 = BitSet::new();
        assert!(!v1.is_all_set());
        assert!(v1.is_all_unset());

        let v2 = ImmutBitSet::new_without_check(8, &[0]);
        assert!(!v2.is_all_set());
        assert!(v2.is_all_unset());
    }

    #[test]
    fn test_with_offsets() {
        let b = BitSet::with_offsets(0, &[1, 2, 3]);
        assert_eq!(b.len, 3);
        assert_eq!(b.buffer, vec![0b_0000_1110]);

        let b = BitSet::with_offsets(10, &[1, 2, 3]);
        assert_eq!(b.len, 10);
        assert_eq!(b.buffer, vec![0b_0000_1110, 0b_0000_0000]);
    }

    #[test]
    fn test_eq() {
        {
            let buffer_1 = vec![0b_0000_1110];
            let buffer_2 = vec![0b_0110_1110];
            let a1 = BitSet::new_without_check(5, buffer_1.clone());
            let b1 = BitSet::new_without_check(5, buffer_2.clone());
            assert_eq!(a1, b1);
            let a2 = ImmutBitSet::new_without_check(5, &buffer_1);
            let b2 = ImmutBitSet::new_without_check(5, &buffer_2);
            assert_eq!(a2, b2);
        }
        {
            let buffer_1 = vec![0b_0000_1110];
            let buffer_2 = vec![0b_0111_1110];
            let a1 = BitSet::new_without_check(5, buffer_1.clone());
            let b1 = BitSet::new_without_check(5, buffer_2.clone());
            assert_ne!(a1, b1);
            let a2 = ImmutBitSet::new_without_check(5, &buffer_1);
            let b2 = ImmutBitSet::new_without_check(5, &buffer_2);
            assert_ne!(a2, b2);
        }
        {
            let buffer_1 = vec![0b_0000_1110, 0b_0000_0011];
            let buffer_2 = vec![0b_0000_1110, 0b_0110_0011];
            let a1 = BitSet::new_without_check(5, buffer_1.clone());
            let b1 = BitSet::new_without_check(5, buffer_2.clone());
            assert_eq!(a1, b1);
            let a2 = ImmutBitSet::new_without_check(5, &buffer_1);
            let b2 = ImmutBitSet::new_without_check(5, &buffer_2);
            assert_eq!(a2, b2);
        }
    }
}
