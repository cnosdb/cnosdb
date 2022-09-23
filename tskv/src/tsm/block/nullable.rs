use std::{fmt::Debug, iter::Peekable};

use models::{Timestamp, ValueType};
use trace::error;

use super::{binary_search, bitmap, exclude_fast, exclude_slow};
use crate::{
    memcache::{DataType, FieldVal},
    tsm::{
        codec::{self, DataBlockEncoding, Encoding},
        Bitmap, ByTimeRange, DataBlock,
    },
    TimeRange,
};

#[derive(Debug, Clone, PartialEq)]
pub enum PrimitiveDataBlock {
    U64(Vec<u64>),
    I64(Vec<i64>),
    Str(Vec<Vec<u8>>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
}

impl PrimitiveDataBlock {
    pub fn new(size: usize, field_type: ValueType) -> Self {
        match field_type {
            ValueType::Float => PrimitiveDataBlock::F64(Vec::<f64>::with_capacity(size)),
            ValueType::Integer => PrimitiveDataBlock::I64(Vec::<i64>::with_capacity(size)),
            ValueType::Unsigned => PrimitiveDataBlock::U64(Vec::<u64>::with_capacity(size)),
            ValueType::Boolean => PrimitiveDataBlock::Bool(Vec::<bool>::with_capacity(size)),
            ValueType::String => PrimitiveDataBlock::Str(Vec::<Vec<u8>>::with_capacity(size)),
            ValueType::Unknown => {
                error!("Initialize PrimitiveDataBlock with ValueType::Unknown, using PrimitiveDataBlock::F64");
                PrimitiveDataBlock::F64(Vec::<f64>::with_capacity(size))
            }
        }
    }
    pub fn with_field_values(field_type: ValueType, field_values: Vec<FieldVal>) -> Self {
        let mut blk = Self::new(field_values.len(), field_type);
        for v in field_values {
            blk.insert(v);
        }
        blk
    }

    pub fn insert(&mut self, value: FieldVal) {
        match (self, value) {
            (Self::Bool(val), FieldVal::Boolean(val_in)) => {
                val.push(val_in);
            }
            (Self::U64(val), FieldVal::Unsigned(val_in)) => {
                val.push(val_in);
            }
            (Self::I64(val), FieldVal::Integer(val_in)) => {
                val.push(val_in);
            }
            (Self::Str(val), FieldVal::Bytes(val_in)) => {
                val.push(val_in);
            }
            (Self::F64(val), FieldVal::Float(val_in)) => {
                val.push(val_in);
            }
            (self_other, value_other) => {
                let self_value_type = match self_other {
                    PrimitiveDataBlock::U64(_) => "U64",
                    PrimitiveDataBlock::I64(_) => "I64",
                    PrimitiveDataBlock::Str(_) => "Str",
                    PrimitiveDataBlock::F64(_) => "F64",
                    PrimitiveDataBlock::Bool(_) => "Bool",
                };
                let field_val_value_type = match value_other {
                    FieldVal::Float(_) => "Float",
                    FieldVal::Integer(_) => "Integer",
                    FieldVal::Unsigned(_) => "Unsigned",
                    FieldVal::Boolean(_) => "Boolean",
                    FieldVal::Bytes(_) => "Bytes",
                };
                error!(
                    "Ignored unmatched value type of 'Self::{}' and 'FieldVal::{}'.",
                    self_value_type, field_val_value_type
                );
            }
        }
    }

    pub fn get(&self, i: usize) -> Option<FieldVal> {
        match self {
            PrimitiveDataBlock::U64(val) => val.get(i).map(|v| FieldVal::Unsigned(*v)),
            PrimitiveDataBlock::I64(val) => val.get(i).map(|v| FieldVal::Integer(*v)),
            PrimitiveDataBlock::Str(val) => val.get(i).map(|v| FieldVal::Bytes(v.clone())),
            PrimitiveDataBlock::F64(val) => val.get(i).map(|v| FieldVal::Float(*v)),
            PrimitiveDataBlock::Bool(val) => val.get(i).map(|v| FieldVal::Boolean(*v)),
        }
    }

    pub fn data_value(&self, index: usize, ts: Timestamp) -> DataType {
        match self {
            PrimitiveDataBlock::U64(val) => DataType::U64(ts, val[index]),
            PrimitiveDataBlock::I64(val) => DataType::I64(ts, val[index]),
            PrimitiveDataBlock::Str(val) => DataType::Str(ts, val[index].clone()),
            PrimitiveDataBlock::F64(val) => DataType::F64(ts, val[index]),
            PrimitiveDataBlock::Bool(val) => DataType::Bool(ts, val[index]),
        }
    }

    pub fn field_type(&self) -> ValueType {
        match self {
            PrimitiveDataBlock::U64(_) => ValueType::Unsigned,
            PrimitiveDataBlock::I64(_) => ValueType::Integer,
            PrimitiveDataBlock::Str(_) => ValueType::String,
            PrimitiveDataBlock::F64(_) => ValueType::Float,
            PrimitiveDataBlock::Bool(_) => ValueType::Boolean,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            PrimitiveDataBlock::U64(val) => val.len(),
            PrimitiveDataBlock::I64(val) => val.len(),
            PrimitiveDataBlock::Str(val) => val.len(),
            PrimitiveDataBlock::F64(val) => val.len(),
            PrimitiveDataBlock::Bool(val) => val.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NullableDataBlock {
    timestamps: Vec<Timestamp>,
    field_values: PrimitiveDataBlock,
    encodings: DataBlockEncoding,
    nulls_bitmap: Option<Bitmap>,
}

impl NullableDataBlock {
    pub fn new(capacity: usize, field_type: ValueType, encodings: DataBlockEncoding) -> Self {
        Self {
            timestamps: Vec::with_capacity(capacity),
            field_values: PrimitiveDataBlock::new(capacity, field_type),
            encodings,
            nulls_bitmap: None,
        }
    }

    pub fn with(
        timestamps: Vec<Timestamp>,
        field_values: PrimitiveDataBlock,
        encodings: DataBlockEncoding,
        nulls_bitmap: Option<Bitmap>,
    ) -> Self {
        Self {
            timestamps,
            field_values,
            encodings,
            nulls_bitmap,
        }
    }

    pub fn append_value(&mut self, timestamp: Timestamp, field_value: Option<FieldVal>) {
        if let Some(val) = field_value {
            self.field_values.insert(val);
            self.appended_non_null(timestamp);
        } else {
            self.append_null(timestamp);
        }
    }

    fn appended_non_null(&mut self, timestamp: Timestamp) {
        if self.nulls_bitmap.is_none() {
            self.nulls_bitmap = Some(Bitmap::new());
        }
        self.nulls_bitmap
            .as_mut()
            .unwrap()
            .set(self.timestamps.len(), true);

        self.timestamps.push(timestamp);
    }

    pub fn append_null(&mut self, timestamp: Timestamp) {
        if self.nulls_bitmap.is_none() {
            self.nulls_bitmap = Some(Bitmap::new());
        }

        self.nulls_bitmap
            .as_mut()
            .unwrap()
            .set(self.timestamps.len(), false);

        self.timestamps.push(timestamp);
    }

    pub fn is_not_null(&self, index: usize) -> bool {
        if let Some(bitmap) = self.nulls_bitmap.as_ref() {
            bitmap.get(index)
        } else {
            true
        }
    }

    /// Encodes timestamps, values of this `NullableDataBlock` to bytes.
    pub fn encode(
        &self,
        encoding: DataBlockEncoding,
    ) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
        let (ts_enc, val_enc) = encoding.split();
        let ts_codec = codec::get_ts_codec(ts_enc);

        let mut ts_buf = vec![];
        let mut data_buf = vec![];
        ts_codec.encode(&self.timestamps, &mut ts_buf)?;

        match &self.field_values {
            PrimitiveDataBlock::Bool(val) => {
                let val_codec = codec::get_bool_codec(val_enc);
                val_codec.encode(&val[..], &mut data_buf)?;
            }
            PrimitiveDataBlock::U64(val) => {
                let val_codec = codec::get_u64_codec(val_enc);
                val_codec.encode(&val[..], &mut data_buf)?;
            }
            PrimitiveDataBlock::I64(val) => {
                let val_codec = codec::get_i64_codec(val_enc);
                val_codec.encode(&val[..], &mut data_buf)?;
            }
            PrimitiveDataBlock::Str(val) => {
                let slices: Vec<&[u8]> = val.iter().map(|str| &str[..]).collect();
                let val_codec = codec::get_str_codec(val_enc);
                val_codec.encode(&slices[..], &mut data_buf)?;
            }
            PrimitiveDataBlock::F64(val) => {
                let val_codec = codec::get_f64_codec(val_enc);
                val_codec.encode(&val[..], &mut data_buf)?;
            }
        }

        Ok((ts_buf, data_buf))
    }

    pub fn iter(&self) -> NullableDataBlockIterator<'_> {
        NullableDataBlockIterator {
            inner: self,
            iter_index: 0,
            iter_index_d: 0,
        }
    }

    /// Returns the `ValueType` by this `NullableDataBlock` variant.
    pub fn field_type(&self) -> ValueType {
        match &self.field_values {
            PrimitiveDataBlock::U64 { .. } => ValueType::Unsigned,
            PrimitiveDataBlock::I64 { .. } => ValueType::Integer,
            PrimitiveDataBlock::Str { .. } => ValueType::String,
            PrimitiveDataBlock::F64 { .. } => ValueType::Float,
            PrimitiveDataBlock::Bool { .. } => ValueType::Boolean,
        }
    }

    pub fn timestamps(&self) -> &[Timestamp] {
        &self.timestamps
    }

    pub fn field_values(&self) -> &PrimitiveDataBlock {
        &self.field_values
    }

    pub fn encodings(&self) -> DataBlockEncoding {
        self.encodings
    }

    pub fn set_encodings(&mut self, encodings: DataBlockEncoding) {
        self.encodings = encodings;
    }

    pub fn bitmap(&self) -> Option<&Bitmap> {
        self.nulls_bitmap.as_ref()
    }

    pub fn bitmap_pruned_raw(&self) -> Option<&[u8]> {
        if let Some(bitmap) = self.nulls_bitmap.as_ref() {
            if bitmap.has_null(self.len()) {
                return Some(bitmap.as_slice());
            }
        }
        None
    }

    pub fn set_bitmap(&mut self, bitmap: Option<Bitmap>) {
        self.nulls_bitmap = bitmap;
    }

    pub fn prune_bitmap(&mut self) {
        if let Some(bitmap) = self.nulls_bitmap.as_ref() {
            if !bitmap.has_null(self.len()) {
                self.nulls_bitmap = None;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    /// Remove (ts, val) in this `NullableDataBlock` where index is greater equal than `min`
    /// and less than `max`.
    ///
    /// **Panics** if min or max is out of range of the ts or val in this `DataBlock`.
    fn exclude_by_index(&mut self, min: usize, max: usize) {
        exclude_fast(&mut self.timestamps, min, max);
        match &mut self.field_values {
            PrimitiveDataBlock::U64(val) => {
                exclude_fast(val, min, max);
            }
            PrimitiveDataBlock::I64(val) => {
                exclude_fast(val, min, max);
            }
            PrimitiveDataBlock::Str(val) => {
                exclude_slow(val, min, max);
            }
            PrimitiveDataBlock::F64(val) => {
                exclude_fast(val, min, max);
            }
            PrimitiveDataBlock::Bool(val) => {
                exclude_fast(val, min, max);
            }
        }
    }

    /// Merges one or many `NullableDataBlock`s into some `NullableDataBlock` with fixed length,
    /// sorted by timestamp, if many (timestamp, value) conflict with the same
    /// timestamp, use the last value.
    pub fn merge_blocks(mut blocks: Vec<Self>, max_block_size: u32) -> Vec<NullableDataBlock> {
        if blocks.is_empty() {
            return Vec::with_capacity(0);
        }
        if blocks.len() == 1 {
            return vec![blocks.remove(0)];
        }
        let last_blk = blocks.last().unwrap();
        let mut blk_builder = NullableDataBlock::new(
            max_block_size as usize,
            last_blk.field_type(),
            last_blk.encodings(),
        );
        let mut blk_builder_has_null_flag = false;
        let mut ret_blks: Vec<NullableDataBlock> = Vec::new();

        let mut blk_iters: Vec<Peekable<NullableDataBlockIterator>> = blocks
            .iter()
            .filter(|b| !b.is_empty())
            .map(|b| b.iter().peekable())
            .collect();
        let mut min_ts;
        let mut blk_iter_offsets = vec![0; blk_iters.len()];
        let mut val_set_flag: bool;
        loop {
            min_ts = Timestamp::MAX;
            blk_iter_offsets.clear();
            for (i, iter) in blk_iters.iter_mut().enumerate() {
                if let Some((ts, val)) = iter.peek() {
                    match min_ts.cmp(ts) {
                        std::cmp::Ordering::Less => continue,
                        std::cmp::Ordering::Equal => {
                            blk_iter_offsets.push(i);
                        }
                        std::cmp::Ordering::Greater => {
                            min_ts = *ts;
                            blk_iter_offsets.clear();
                            blk_iter_offsets.push(i);
                        }
                    }
                }
            }
            if blk_iter_offsets.is_empty() {
                break;
            }

            val_set_flag = false;
            for off in blk_iter_offsets.iter().rev() {
                // SAFETY: offset[i] is always less than blk_iters.len().
                let blk_iter = unsafe { blk_iters.get_unchecked_mut(*off) };
                if !val_set_flag {
                    let (ts, val) = blk_iter.peek_mut().unwrap();
                    if let Some(v) = val {
                        blk_builder.append_value(*ts, val.take());
                        val_set_flag = true;
                    }
                }
                blk_iter.next();
            }

            if max_block_size != 0 && blk_builder.len() >= max_block_size as usize {
                if !blk_builder_has_null_flag {
                    blk_builder.set_bitmap(None);
                }
                blk_builder_has_null_flag = false;
                let field_typ = blk_builder.field_type();
                let field_enc = blk_builder.encodings();
                let blk = std::mem::replace(
                    &mut blk_builder,
                    NullableDataBlock::new(0, field_typ, field_enc),
                );
                ret_blks.push(blk);
            }
        }

        if !blk_builder.is_empty() {
            if !blk_builder_has_null_flag {
                blk_builder.set_bitmap(None);
            }
            ret_blks.push(blk_builder);
        }
        ret_blks
    }
}

impl ByTimeRange for NullableDataBlock {
    fn time_range(&self) -> Option<TimeRange> {
        if self.timestamps.is_empty() {
            return None;
        }
        let end = self.timestamps.len();
        Some((self.timestamps[0], self.timestamps[end - 1]).into())
    }

    /// Returns (`timestamp[start]`, `timestamp[end]`) from this `NullableDataBlock` at the specified
    /// indexes.
    fn time_range_by_range(&self, start: usize, end: usize) -> TimeRange {
        (
            self.timestamps[start].to_owned(),
            self.timestamps[end - 1].to_owned(),
        )
            .into()
    }

    /// Remove (ts, val) in this `NullableDataBlock` where ts is greater equal than min_ts
    /// and ts is less equal than the max_ts
    fn exclude(&mut self, time_range: &TimeRange) {
        if self.is_empty() {
            return;
        }
        let TimeRange { min_ts, max_ts } = *time_range;

        let ts_sli = self.timestamps();
        let (min_idx, has_min) = binary_search(ts_sli, &min_ts);
        let (mut max_idx, has_max) = binary_search(ts_sli, &max_ts);
        // If ts_sli doesn't contain supported time range then return.
        if min_idx > max_idx || min_idx == max_idx && !has_min && !has_max {
            return;
        }
        if max_idx < ts_sli.len() {
            max_idx += 1;
        }

        self.exclude_by_index(min_idx, max_idx);
    }
}

impl From<DataBlock> for NullableDataBlock {
    fn from(data_block: DataBlock) -> Self {
        let (timestamps, field_values, code_type_id) = match data_block {
            DataBlock::U64 {
                ts,
                val,
                code_type_id,
            } => (ts, PrimitiveDataBlock::U64(val), code_type_id),
            DataBlock::I64 {
                ts,
                val,
                code_type_id,
            } => (ts, PrimitiveDataBlock::I64(val), code_type_id),
            DataBlock::Str {
                ts,
                val,
                code_type_id,
            } => (ts, PrimitiveDataBlock::Str(val), code_type_id),
            DataBlock::F64 {
                ts,
                val,
                code_type_id,
            } => (ts, PrimitiveDataBlock::F64(val), code_type_id),
            DataBlock::Bool {
                ts,
                val,
                code_type_id,
            } => (ts, PrimitiveDataBlock::Bool(val), code_type_id),
        };
        Self {
            timestamps,
            field_values,
            encodings: DataBlockEncoding(code_type_id),
            nulls_bitmap: None,
        }
    }
}

impl PartialEq for NullableDataBlock {
    fn eq(&self, other: &Self) -> bool {
        if self.timestamps == other.timestamps && self.field_values == other.field_values {
            match (self.nulls_bitmap.as_ref(), other.nulls_bitmap.as_ref()) {
                (None, None) => true,
                (None, Some(bitmap)) => bitmap.is_empty(),
                (Some(bitmap), None) => bitmap.is_empty(),
                (Some(b1), Some(b2)) => b1 == b2,
            }
        } else {
            false
        }
    }
}

pub struct NullableDataBlockIterator<'a> {
    inner: &'a NullableDataBlock,

    iter_index: usize,
    iter_index_d: usize,
}

impl<'a> Iterator for NullableDataBlockIterator<'a> {
    type Item = (Timestamp, Option<FieldVal>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.iter_index >= self.inner.len() {
            return None;
        }
        // assert!(self.inner.timestamps.len() > self.iter_index);
        if let Some(ts) = self.inner.timestamps.get(self.iter_index) {
            if self.inner.is_not_null(self.iter_index) {
                let val = self.inner.field_values.get(self.iter_index_d);
                self.iter_index += 1;
                self.iter_index_d += 1;
                return Some((*ts, val));
            } else {
                self.iter_index += 1;
                return Some((*ts, None));
            }
        }

        return None;
    }
}

#[cfg(test)]
pub mod nullable_block_tests {
    use models::{Timestamp, ValueType};

    use crate::{memcache::FieldVal, tsm::codec::DataBlockEncoding};

    use super::{Bitmap, NullableDataBlock};

    pub(crate) fn check_nullable_data_block(
        block: &NullableDataBlock,
        pattern: &[(Timestamp, Option<FieldVal>)],
    ) {
        let block_1: Vec<(Timestamp, Option<FieldVal>)> = block.iter().collect();
        assert_eq!(&block_1, pattern);
    }

    #[test]
    fn test_iterate_nullable_blocks() {
        let mut blk = NullableDataBlock::new(10, ValueType::Integer, DataBlockEncoding::default());
        blk.append_value(1, Some(FieldVal::Integer(1)));
        blk.append_value(2, None);
        blk.append_value(3, Some(FieldVal::Integer(3)));
        blk.append_value(4, None);

        let mut iter = blk.iter();
        assert_eq!(iter.next(), Some((1, Some(FieldVal::Integer(1)))));
        assert_eq!(iter.next(), Some((2, None)));
        assert_eq!(iter.next(), Some((3, Some(FieldVal::Integer(3)))));
        assert_eq!(iter.next(), Some((4, None)));
    }

    #[test]
    fn test_merge_nullable_blocks() {
        let mut blk1 = NullableDataBlock::new(10, ValueType::Integer, DataBlockEncoding::default());
        let mut blk2 = NullableDataBlock::new(10, ValueType::Integer, DataBlockEncoding::default());

        blk1.append_value(1, Some(FieldVal::Integer(1)));
        blk1.append_value(2, Some(FieldVal::Integer(2)));
        blk1.append_value(3, Some(FieldVal::Integer(3)));
        blk1.append_value(4, Some(FieldVal::Integer(4)));
        blk1.append_value(5, Some(FieldVal::Integer(5)));

        blk2.append_value(2, Some(FieldVal::Integer(12)));
        blk2.append_value(3, Some(FieldVal::Integer(13)));
        blk2.append_value(4, Some(FieldVal::Integer(14)));

        let mut blk3 = NullableDataBlock::new(10, ValueType::Integer, DataBlockEncoding::default());
        blk3.append_value(1, Some(FieldVal::Integer(1)));
        blk3.append_value(2, Some(FieldVal::Integer(12)));
        blk3.append_value(3, Some(FieldVal::Integer(13)));
        blk3.append_value(4, Some(FieldVal::Integer(14)));
        blk3.append_value(5, Some(FieldVal::Integer(5)));
        blk3.prune_bitmap();

        assert_eq!(
            NullableDataBlock::merge_blocks(vec![blk1, blk2], 0),
            vec![blk3]
        );
    }
}
