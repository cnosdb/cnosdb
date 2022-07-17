use std::{fmt::Display, mem::size_of, ops::Index};

use models::{Timestamp, ValueType};
use protos::models::FieldType;

use super::coders;
use crate::memcache::{BoolCell, Byte, DataType, F64Cell, I64Cell, StrCell, U64Cell};

#[derive(Debug, Clone, PartialEq)]
pub enum DataBlock {
    U64 { ts: Vec<i64>, val: Vec<u64> },
    I64 { ts: Vec<i64>, val: Vec<i64> },
    Str { ts: Vec<i64>, val: Vec<Byte> },
    F64 { ts: Vec<i64>, val: Vec<f64> },
    Bool { ts: Vec<i64>, val: Vec<bool> },
}

impl DataBlock {
    pub fn new(size: usize, field_type: ValueType) -> Self {
        match field_type {
            ValueType::Unsigned => {
                Self::U64 { ts: Vec::with_capacity(size), val: Vec::with_capacity(size) }
            },
            ValueType::Integer => {
                Self::I64 { ts: Vec::with_capacity(size), val: Vec::with_capacity(size) }
            },
            ValueType::Float => {
                Self::F64 { ts: Vec::with_capacity(size), val: Vec::with_capacity(size) }
            },
            ValueType::String => {
                Self::Str { ts: Vec::with_capacity(size), val: Vec::with_capacity(size) }
            },
            ValueType::Boolean => {
                Self::Bool { ts: Vec::with_capacity(size), val: Vec::with_capacity(size) }
            },
            ValueType::Unknown => {
                todo!()
            },
        }
    }

    /// Inserts new timestamp and value wrapped by `DataType` to this `DataBlock`.
    pub fn insert(&mut self, data: &DataType) {
        match data {
            DataType::Bool(item) => {
                if let Self::Bool { ts, val, .. } = self {
                    ts.push(item.ts);
                    val.push(item.val);
                }
            },
            DataType::U64(item) => {
                if let Self::U64 { ts, val, .. } = self {
                    ts.push(item.ts);
                    val.push(item.val);
                }
            },
            DataType::I64(item) => {
                if let Self::I64 { ts, val, .. } = self {
                    ts.push(item.ts);
                    val.push(item.val);
                }
            },
            DataType::Str(item) => {
                if let Self::Str { ts, val, .. } = self {
                    ts.push(item.ts);
                    val.push(item.val.clone());
                }
            },
            DataType::F64(item) => {
                if let Self::F64 { ts, val, .. } = self {
                    ts.push(item.ts);
                    val.push(item.val);
                }
            },
        }
    }

    pub fn time_range(&self) -> Option<(Timestamp, Timestamp)> {
        if self.len() == 0 {
            return None;
        }
        let end = self.len();
        match self {
            DataBlock::U64 { ts, .. } => Some((ts[0].to_owned(), ts[end - 1].to_owned())),
            DataBlock::I64 { ts, .. } => Some((ts[0].to_owned(), ts[end - 1].to_owned())),
            DataBlock::Str { ts, .. } => Some((ts[0].to_owned(), ts[end - 1].to_owned())),
            DataBlock::F64 { ts, .. } => Some((ts[0].to_owned(), ts[end - 1].to_owned())),
            DataBlock::Bool { ts, .. } => Some((ts[0].to_owned(), ts[end - 1].to_owned())),
        }
    }

    /// Returns (`timestamp[start]`, `timestamp[end]`) from this `DataBlock` at the specified
    /// indexes.
    pub fn time_range_by_range(&self, start: usize, end: usize) -> (i64, i64) {
        match self {
            DataBlock::U64 { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()),
            DataBlock::I64 { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()),
            DataBlock::Str { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()),
            DataBlock::F64 { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()),
            DataBlock::Bool { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()),
        }
    }

    /// Inserts new timestamps and values wrapped by `&[DataType]` to this `DataBlock`.
    pub fn batch_insert(&mut self, cells: &[DataType]) {
        for iter in cells.iter() {
            self.insert(iter);
        }
    }

    /// Returns the length of the timestamps array of this `DataBlock`.
    pub fn len(&self) -> usize {
        match &self {
            Self::U64 { ts, .. } => ts.len(),
            Self::I64 { ts, .. } => ts.len(),
            Self::F64 { ts, .. } => ts.len(),
            Self::Str { ts, .. } => ts.len(),
            Self::Bool { ts, .. } => ts.len(),
        }
    }

    /// Returns the `ValueType` by this `DataBlock` variant.
    pub fn field_type(&self) -> ValueType {
        match &self {
            DataBlock::U64 { .. } => ValueType::Unsigned,
            DataBlock::I64 { .. } => ValueType::Integer,
            DataBlock::Str { .. } => ValueType::String,
            DataBlock::F64 { .. } => ValueType::Float,
            DataBlock::Bool { .. } => ValueType::Boolean,
        }
    }

    /// Returns a new `DataType` by this `DataBlock` variant.
    pub fn get_type(&self) -> DataType {
        match &self {
            DataBlock::U64 { .. } => DataType::U64(U64Cell::default()),
            DataBlock::I64 { .. } => DataType::I64(I64Cell::default()),
            DataBlock::Str { .. } => DataType::Str(StrCell::default()),
            DataBlock::F64 { .. } => DataType::F64(F64Cell::default()),
            DataBlock::Bool { .. } => DataType::Bool(BoolCell::default()),
        }
    }

    /// Returns a slice containing the entire timestamps of this `DataBlock`.
    pub fn ts(&self) -> &[i64] {
        match self {
            DataBlock::U64 { ts, .. } => ts.as_slice(),
            DataBlock::I64 { ts, .. } => ts.as_slice(),
            DataBlock::Str { ts, .. } => ts.as_slice(),
            DataBlock::F64 { ts, .. } => ts.as_slice(),
            DataBlock::Bool { ts, .. } => ts.as_slice(),
        }
    }

    /// Returns whether all elements of this `DataBlock` has been iterated
    /// ( `DataBlock::*index == DataBlock::ts.len()` )
    pub fn is_empty(&self) -> bool {
        match &self {
            DataBlock::U64 { ts, .. } => ts.len() > 0,
            DataBlock::I64 { ts, .. } => ts.len() > 0,
            DataBlock::Str { ts, .. } => ts.len() > 0,
            DataBlock::F64 { ts, .. } => ts.len() > 0,
            DataBlock::Bool { ts, .. } => ts.len() > 0,
        }
    }

    /// Returns the (ts, val) wrapped by `DataType` at the index 'i'
    pub fn get(&self, i: usize) -> Option<DataType> {
        match self {
            DataBlock::U64 { ts, val, .. } => {
                if ts.len() <= i {
                    None
                } else {
                    Some(DataType::U64(U64Cell { ts: ts[i], val: val[i] }))
                }
            },
            DataBlock::I64 { ts, val, .. } => {
                if ts.len() <= i {
                    None
                } else {
                    Some(DataType::I64(I64Cell { ts: ts[i], val: val[i] }))
                }
            },
            DataBlock::Str { ts, val, .. } => {
                if ts.len() <= i {
                    None
                } else {
                    Some(DataType::Str(StrCell { ts: ts[i], val: val[i].clone() }))
                }
            },
            DataBlock::F64 { ts, val, .. } => {
                if ts.len() <= i {
                    None
                } else {
                    Some(DataType::F64(F64Cell { ts: ts[i], val: val[i] }))
                }
            },
            DataBlock::Bool { ts, val, .. } => {
                if ts.len() <= i {
                    None
                } else {
                    Some(DataType::Bool(BoolCell { ts: ts[i], val: val[i] }))
                }
            },
        }
    }

    /// Set the (ts, val) wrapped by `DataType` at the index 'i'
    pub fn set(&mut self, i: usize, data_type: DataType) {
        match (self, data_type) {
            (DataBlock::U64 { ts, val, .. }, DataType::U64(c)) => {
                ts[i] = c.ts;
                val[i] = c.val;
            },
            (DataBlock::I64 { ts, val, .. }, DataType::I64(c)) => {
                ts[i] = c.ts;
                val[i] = c.val;
            },
            (DataBlock::Str { ts, val, .. }, DataType::Str(c)) => {
                ts[i] = c.ts;
                val[i] = c.val;
            },
            (DataBlock::F64 { ts, val, .. }, DataType::F64(c)) => {
                ts[i] = c.ts;
                val[i] = c.val;
            },
            (DataBlock::Bool { ts, val, .. }, DataType::Bool(c)) => {
                ts[i] = c.ts;
                val[i] = c.val;
            },
            _ => {},
        }
    }

    /// Append a `DataBlock` into this `DataBlock`, sorted by timestamp,
    /// if two (timestamp, value) conflict with the same timestamp, use the last value.
    pub fn merge(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        if self.field_type() != other.field_type() {
            return;
        }
        let (smin_ts, smax_ts) = self.time_range_by_range(0, self.len());
        let (min_ts, max_ts) = other.time_range_by_range(0, other.len());

        let i_ts_sli = self.ts();
        let ts_sli = other.ts();
        let mut new_blk = Self::new(self.len() + other.len(), self.field_type());
        let (mut i, mut j, mut k) = (0_usize, 0_usize, 0_usize);
        while i < i_ts_sli.len() && j < ts_sli.len() {
            match i_ts_sli[i].cmp(&ts_sli[j]) {
                std::cmp::Ordering::Less => {
                    new_blk.set(k, self.get(i).expect("checked index i"));
                    i += 1;
                },
                std::cmp::Ordering::Equal => {
                    new_blk.set(k, other.get(j).expect("checked index j"));
                    i += 1;
                    j += 1;
                },
                std::cmp::Ordering::Greater => {
                    new_blk.set(k, other.get(j).expect("checked index j"));
                    j += 1;
                },
            }
            k += 1;
        }
    }

    /// Merges many `DataBlock`s into one `DataBlock`, sorted by timestamp,
    /// if many (timestamp, value) conflict with the same timestamp, use the last value.
    pub fn merge_blocks(mut blocks: Vec<Self>) -> Self {
        if blocks.len() == 1 {
            return blocks.remove(0);
        }

        let mut res =
            Self::new(blocks.first().unwrap().len(), blocks.first().unwrap().field_type());
        // [(DataBlock)]
        let mut buf = vec![None; blocks.len()];
        let mut offsets = vec![0_usize; blocks.len()];
        loop {
            match Self::rebuild_vec(&mut blocks, &mut buf, &mut offsets) {
                Some(min) => {
                    let mut data = None;
                    for item in &mut buf {
                        if let Some(it) = item {
                            if it.timestamp() == min {
                                data = item.take();
                            }
                        }
                    }
                    if let Some(it) = data {
                        res.insert(&it);
                    }
                },
                None => return res,
            }
        }
    }

    /// Remote (ts, val) in this `DataBlock` where ts is greater equal than min_ts
    /// and ts is less than the max_ts
    pub fn exclude(&mut self, min_ts: Timestamp, max_ts: Timestamp) {
        fn binary_earch(sli: &[i64], ts: &i64) -> usize {
            match sli.binary_search(ts) {
                Ok(i) => i,
                Err(i) => i,
            }
        }
        let ts_sli = self.ts();
        let min_idx = binary_earch(ts_sli, &min_ts);
        let max_idx = binary_earch(ts_sli, &max_ts);

        fn exclude_fast<T: Sized + Copy>(v: &mut Vec<T>, min_idx: usize, max_idx: usize) {
            let a = v.as_mut_ptr();
            unsafe {
                let b = a.add(min_idx);
                let c = a.add(max_idx);
                c.copy_to(b, v.len() - max_idx);
                v.set_len(v.len() + min_idx - max_idx);
            }
        }

        fn exclude_slow(v: &mut Vec<Byte>, min_idx: usize, max_idx: usize) {
            let len = v.len() + min_idx - max_idx;
            for i in min_idx..len {
                v[i] = v[max_idx - min_idx + i].clone();
            }
            v.truncate(len);
        }

        match self {
            DataBlock::U64 { ts, val } => {
                exclude_fast(ts, min_idx, max_idx);
                exclude_fast(val, min_idx, max_idx);
            },
            DataBlock::I64 { ts, val } => {
                exclude_fast(ts, min_idx, max_idx);
                exclude_fast(val, min_idx, max_idx);
            },
            DataBlock::Str { ts, val } => {
                exclude_fast(ts, min_idx, max_idx);
                exclude_slow(val, min_idx, max_idx);
            },
            DataBlock::F64 { ts, val } => {
                exclude_fast(ts, min_idx, max_idx);
                exclude_fast(val, min_idx, max_idx);
            },
            DataBlock::Bool { ts, val } => {
                exclude_fast(ts, min_idx, max_idx);
                exclude_fast(val, min_idx, max_idx);
            },
        }
    }

    /// Extract `DataBlock`s to `DataType`s,
    /// returns the minimum timestamp in a series of `DataBlock`s
    fn rebuild_vec(blocks: &mut [Self],
                   dst: &mut Vec<Option<DataType>>,
                   offsets: &mut [usize])
                   -> Option<i64> {
        let mut min_ts = None;
        for (i, (block, dst)) in blocks.iter_mut().zip(dst).enumerate() {
            if dst.is_none() {
                *dst = block.get(offsets[i]);
                offsets[i] += 1;
            }

            if let Some(pair) = dst {
                match min_ts {
                    Some(min) => {
                        if pair.timestamp() < min {
                            min_ts = Some(pair.timestamp());
                        }
                    },
                    None => min_ts = Some(pair.timestamp()),
                }
            };
        }
        min_ts
    }

    // todo:
    /// Encodes timestamps and values of this `DataBlock` to bytes.
    pub fn encode(&self,
                  start: usize,
                  end: usize)
                  -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
        let mut ts_buf = vec![];
        let mut data_buf = vec![];
        match self {
            DataBlock::Bool { ts, val, .. } => {
                coders::timestamp::encode(&ts[start..end], &mut ts_buf)?;
                coders::boolean::encode(&val[start..end], &mut data_buf)?;
            },
            DataBlock::U64 { ts, val, .. } => {
                coders::timestamp::encode(&ts[start..end], &mut ts_buf)?;
                coders::unsigned::encode(&val[start..end], &mut data_buf)?;
            },
            DataBlock::I64 { ts, val, .. } => {
                coders::timestamp::encode(&ts[start..end], &mut ts_buf)?;
                coders::integer::encode(&val[start..end], &mut data_buf)?;
            },
            DataBlock::Str { ts, val, .. } => {
                coders::timestamp::encode(&ts[start..end], &mut ts_buf)?;
                let strs: Vec<&[u8]> = val.iter().map(|str| &str[..]).collect();
                coders::string::encode(&strs[start..end], &mut data_buf)?;
            },
            DataBlock::F64 { ts, val, .. } => {
                coders::timestamp::encode(&ts[start..end], &mut ts_buf)?;
                coders::float::encode(&val[start..end], &mut data_buf)?;
            },
        }
        Ok((ts_buf, data_buf))
    }

    pub fn decode() {}
}

impl Display for DataBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataBlock::U64 { ts, val } => {
                if ts.len() > 0 {
                    write!(f,
                           "U64 {{ len: {}, min_ts: {}, max_ts: {} }}",
                           ts.len(),
                           ts.first().unwrap(),
                           ts.last().unwrap())
                } else {
                    write!(f, "U64 {{ len: {}, min_ts: NONE, max_ts: NONE }}", ts.len())
                }
            },
            DataBlock::I64 { ts, val } => {
                if ts.len() > 0 {
                    write!(f,
                           "I64 {{ len: {}, min_ts: {}, max_ts: {} }}",
                           ts.len(),
                           ts.first().unwrap(),
                           ts.last().unwrap())
                } else {
                    write!(f, "I64 {{ len: {}, min_ts: NONE, max_ts: NONE }}", ts.len())
                }
            },
            DataBlock::Str { ts, val } => {
                if ts.len() > 0 {
                    write!(f,
                           "Str {{ len: {}, min_ts: {}, max_ts: {} }}",
                           ts.len(),
                           ts.first().unwrap(),
                           ts.last().unwrap())
                } else {
                    write!(f, "Str {{ len: {}, min_ts: NONE, max_ts: NONE }}", ts.len())
                }
            },
            DataBlock::F64 { ts, val } => {
                if ts.len() > 0 {
                    write!(f,
                           "F64 {{ len: {}, min_ts: {}, max_ts: {} }}",
                           ts.len(),
                           ts.first().unwrap(),
                           ts.last().unwrap())
                } else {
                    write!(f, "F64 {{ len: {}, min_ts: NONE, max_ts: NONE }}", ts.len())
                }
            },
            DataBlock::Bool { ts, val } => {
                if ts.len() > 0 {
                    write!(f,
                           "Bool {{ len: {}, min_ts: {}, max_ts: {} }}",
                           ts.len(),
                           ts.first().unwrap(),
                           ts.last().unwrap())
                } else {
                    write!(f, "Bool {{ len: {}, min_ts: NONE, max_ts: NONE }}", ts.len())
                }
            },
        }
    }
}

#[cfg(test)]
mod test {
    use std::mem::size_of;

    use crate::tsm::DataBlock;

    #[test]
    fn test_merge_blocks() {
        let res = DataBlock::merge_blocks(vec![DataBlock::U64 { ts: vec![1, 2, 3, 4, 5],
                                                                val: vec![10, 20, 30, 40, 50] },
                                               DataBlock::U64 { ts: vec![2, 3, 4],
                                                                val: vec![12, 13, 15] },]);

        assert_eq!(res, DataBlock::U64 { ts: vec![1, 2, 3, 4, 5], val: vec![10, 12, 13, 15, 50] },);
    }

    #[test]
    fn test_append_block() {
        // TODO
    }

    #[test]
    fn test_data_block_exclude() {
        let mut blk = DataBlock::U64 { ts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                                       val: vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19] };
        blk.exclude(2, 8);
        dbg!(&blk);
        assert_eq!(blk, DataBlock::U64 { ts: vec![0, 1, 8, 9], val: vec![10, 11, 18, 19] });

        let mut blk = DataBlock::Str { ts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                                       val: vec![vec![10],
                                                 vec![11],
                                                 vec![12],
                                                 vec![13],
                                                 vec![14],
                                                 vec![15],
                                                 vec![16],
                                                 vec![17],
                                                 vec![18],
                                                 vec![19]] };
        blk.exclude(2, 8);
        dbg!(&blk);
        assert_eq!(blk,
                   DataBlock::Str { ts: vec![0, 1, 8, 9],
                                    val: vec![vec![10], vec![11], vec![18], vec![19]] })
    }
}
