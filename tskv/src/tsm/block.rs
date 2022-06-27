// use std::fmt::Error;

use models::ValueType;
use protos::models::FieldType;

use super::coders;
use crate::{
    error::{Error, Result},
    memcache::{BoolCell, Byte, DataType, F64Cell, I64Cell, StrCell, U64Cell},
};

#[derive(Debug, Clone, PartialEq)]
pub enum DataBlock {
    U64Block { index: u32, ts: Vec<i64>, val: Vec<u64> },
    I64Block { index: u32, ts: Vec<i64>, val: Vec<i64> },
    StrBlock { index: u32, ts: Vec<i64>, val: Vec<Byte> },
    F64Block { index: u32, ts: Vec<i64>, val: Vec<f64> },
    BoolBlock { index: u32, ts: Vec<i64>, val: Vec<bool> },
}

impl DataBlock {
    pub fn new(size: usize, field_type: ValueType) -> Self {
        match field_type {
            ValueType::Unsigned => Self::U64Block { index: 0,
                                                    ts: Vec::with_capacity(size),
                                                    val: Vec::with_capacity(size) },
            ValueType::Integer => Self::I64Block { index: 0,
                                                   ts: Vec::with_capacity(size),
                                                   val: Vec::with_capacity(size) },
            ValueType::Float => Self::F64Block { index: 0,
                                                 ts: Vec::with_capacity(size),
                                                 val: Vec::with_capacity(size) },
            ValueType::String => Self::StrBlock { index: 0,
                                                  ts: Vec::with_capacity(size),
                                                  val: Vec::with_capacity(size) },
            ValueType::Boolean => Self::BoolBlock { index: 0,
                                                    ts: Vec::with_capacity(size),
                                                    val: Vec::with_capacity(size) },
            ValueType::Unknown => {
                todo!()
            },
        }
    }
    pub fn insert(&mut self, data: DataType) {
        match data {
            DataType::Bool(item) => {
                if let Self::BoolBlock { ts, val, index } = self {
                    ts.push(item.ts);
                    val.push(item.val);
                }
            },
            DataType::U64(item) => {
                if let Self::U64Block { ts, val, index } = self {
                    ts.push(item.ts);
                    val.push(item.val);
                }
            },
            DataType::I64(item) => {
                if let Self::I64Block { ts, val, index } = self {
                    ts.push(item.ts);
                    val.push(item.val);
                }
            },
            DataType::Str(item) => {
                if let Self::StrBlock { ts, val, index } = self {
                    ts.push(item.ts);
                    val.push(item.val);
                }
            },
            DataType::F64(item) => {
                if let Self::F64Block { ts, val, index } = self {
                    ts.push(item.ts);
                    val.push(item.val);
                }
            },
        }
    }

    pub fn time_range(&self, start: usize, end: usize) -> (i64, i64) {
        match self {
            DataBlock::U64Block { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()),
            DataBlock::I64Block { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()),
            DataBlock::StrBlock { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()),
            DataBlock::F64Block { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()),
            DataBlock::BoolBlock { ts, .. } => (ts[start].to_owned(), ts[end - 1].to_owned()),
        }
    }
    pub fn batch_insert(&mut self, cells: &Vec<DataType>) {
        for iter in cells.iter() {
            match iter {
                DataType::U64(item) => {
                    if let Self::U64Block { ts, val, index } = self {
                        ts.push(item.ts);
                        val.push(item.val);
                    }
                },
                DataType::I64(item) => {
                    if let Self::I64Block { ts, val, index } = self {
                        ts.push(item.ts);
                        val.push(item.val);
                    }
                },
                DataType::Str(item) => {
                    if let Self::StrBlock { ts, val, index } = self {
                        ts.push(item.ts);
                        val.push(item.val.clone());
                    }
                },
                DataType::F64(item) => {
                    if let Self::F64Block { ts, val, index } = self {
                        ts.push(item.ts);
                        val.push(item.val);
                    }
                },
                DataType::Bool(item) => {
                    if let Self::BoolBlock { ts, val, index } = self {
                        ts.push(item.ts);
                        val.push(item.val);
                    }
                },
                _ => todo!(),
            }
        }
    }

    pub fn len(&self) -> usize {
        match &self {
            Self::U64Block { ts, .. } => ts.len(),
            Self::I64Block { ts, .. } => ts.len(),
            Self::F64Block { ts, .. } => ts.len(),
            Self::StrBlock { ts, .. } => ts.len(),
            Self::BoolBlock { ts, .. } => ts.len(),
        }
    }
    pub fn field_type(&self) -> ValueType {
        match &self {
            DataBlock::U64Block { .. } => ValueType::Unsigned,
            DataBlock::I64Block { .. } => ValueType::Integer,
            DataBlock::StrBlock { .. } => ValueType::String,
            DataBlock::F64Block { .. } => ValueType::Float,
            DataBlock::BoolBlock { .. } => ValueType::Boolean,
        }
    }
    pub fn get_type(&self) -> DataType {
        match &self {
            DataBlock::U64Block { index, ts, val } => DataType::U64(U64Cell::default()),
            DataBlock::I64Block { index, ts, val } => DataType::I64(I64Cell::default()),
            DataBlock::StrBlock { index, ts, val } => DataType::Str(StrCell::default()),
            DataBlock::F64Block { index, ts, val } => DataType::F64(F64Cell::default()),
            DataBlock::BoolBlock { index, ts, val } => DataType::Bool(BoolCell::default()),
        }
    }
    pub fn is_empty(&self) -> bool {
        match &self {
            DataBlock::U64Block { index, ts, val } => *index == ts.len() as u32,
            DataBlock::I64Block { index, ts, val } => *index == ts.len() as u32,
            DataBlock::StrBlock { index, ts, val } => *index == ts.len() as u32,
            DataBlock::F64Block { index, ts, val } => *index == ts.len() as u32,
            DataBlock::BoolBlock { index, ts, val } => *index == ts.len() as u32,
        }
    }
    pub fn next(&mut self) -> Option<DataType> {
        if self.is_empty() {
            return None;
        }
        match self {
            DataBlock::U64Block { index, ts, val } => {
                let i = *index as usize;
                *index += 1;
                Some(DataType::U64(U64Cell { ts: ts[i], val: val[i] }))
            },
            DataBlock::I64Block { index, ts, val } => {
                let i = *index as usize;
                *index += 1;
                Some(DataType::I64(I64Cell { ts: ts[i], val: val[i] }))
            },
            DataBlock::StrBlock { index, ts, val } => {
                let i = *index as usize;
                *index += 1;
                Some(DataType::Str(StrCell { ts: ts[i], val: val[i].clone() }))
            },
            DataBlock::F64Block { index, ts, val } => {
                let i = *index as usize;
                *index += 1;
                Some(DataType::F64(F64Cell { ts: ts[i], val: val[i] }))
            },
            DataBlock::BoolBlock { index, ts, val } => {
                let i = *index as usize;
                *index += 1;
                Some(DataType::Bool(BoolCell { ts: ts[i], val: val[i] }))
            },
        }
    }
    // last write win
    pub fn merge_blocks(mut blocks: Vec<Self>) -> Self {
        if blocks.len() == 1 {
            return blocks.remove(0);
        }

        let mut res =
            Self::new(blocks.first().unwrap().len(), blocks.first().unwrap().field_type());
        let mut buf = vec![None; blocks.len()];
        loop {
            match Self::rebuild_vec(&mut blocks, &mut buf) {
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
                        res.insert(it);
                    }
                },
                None => return res,
            }
        }
    }
    fn rebuild_vec(blocks: &mut [Self], dst: &mut Vec<Option<DataType>>) -> Option<i64> {
        let mut min_ts = None;
        for (block, dst) in blocks.iter_mut().zip(dst) {
            if dst.is_none() {
                *dst = block.next();
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
    pub fn encode(&mut self, start: usize, end: usize) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut ts_buf = vec![];
        let mut data_buf = vec![];
        match self {
            DataBlock::BoolBlock { ts, val, .. } => {
                coders::timestamp::encode(&ts[start..end], &mut ts_buf)
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
                coders::boolean::encode(&val[start..end], &mut data_buf)
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            },
            DataBlock::U64Block { ts, val, .. } => {
                coders::timestamp::encode(&ts[start..end], &mut ts_buf)
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
                coders::unsigned::encode(&val[start..end], &mut data_buf)
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            },
            DataBlock::I64Block { ts, val, .. } => {
                coders::timestamp::encode(&ts[start..end], &mut ts_buf)
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
                coders::integer::encode(&val[start..end], &mut data_buf)
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            },
            DataBlock::StrBlock { ts, val, .. } => {
                coders::timestamp::encode(&ts[start..end], &mut ts_buf)
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
                let strs: Vec<&[u8]> = val.iter().map(|str| &str[..]).collect();
                coders::string::encode(&strs[start..end], &mut data_buf)
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            },
            DataBlock::F64Block { ts, val, .. } => {
                coders::timestamp::encode(&ts[start..end], &mut ts_buf)
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
                coders::float::encode(&val[start..end], &mut data_buf)
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            },
        }
        Ok((ts_buf, data_buf))
    }
    pub fn decode() {}
}

#[test]
fn merge_blocks() {
    let res = DataBlock::merge_blocks(vec![DataBlock::U64Block { index: 0,
                                                                 ts: vec![1, 2, 3, 4, 5],
                                                                 val: vec![10, 20, 30, 40, 50] },
                                           DataBlock::U64Block { index: 0,
                                                                 ts: vec![2, 3, 4],
                                                                 val: vec![12, 13, 15] },]);

    assert_eq!(res,
               DataBlock::U64Block { index: 0,
                                     ts: vec![1, 2, 3, 4, 5],
                                     val: vec![10, 12, 13, 15, 50] },);
}
