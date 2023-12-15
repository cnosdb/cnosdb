use std::fmt::Display;
use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};
use minivec::{mini_vec, MiniVec};

use crate::{PhysicalDType, Timestamp, ValueType};

#[derive(Debug, Clone)]
pub enum FieldVal {
    Float(f64),
    Integer(i64),
    Unsigned(u64),
    Boolean(bool),
    Bytes(MiniVec<u8>),
}

impl FieldVal {
    pub fn value_type(&self) -> PhysicalDType {
        match self {
            FieldVal::Float(..) => PhysicalDType::Float,
            FieldVal::Integer(..) => PhysicalDType::Integer,
            FieldVal::Unsigned(..) => PhysicalDType::Unsigned,
            FieldVal::Boolean(..) => PhysicalDType::Boolean,
            FieldVal::Bytes(..) => PhysicalDType::String,
        }
    }

    pub fn data_value(&self, ts: i64) -> DataType {
        match self {
            FieldVal::Float(val) => DataType::F64(ts, *val),
            FieldVal::Integer(val) => DataType::I64(ts, *val),
            FieldVal::Unsigned(val) => DataType::U64(ts, *val),
            FieldVal::Boolean(val) => DataType::Bool(ts, *val),
            FieldVal::Bytes(val) => DataType::Str(ts, val.clone()),
        }
    }

    pub fn new(val: MiniVec<u8>, vtype: ValueType) -> FieldVal {
        match vtype {
            ValueType::Unsigned => {
                let mut rdr = Cursor::new(val);
                let val = rdr.read_u64::<BigEndian>().unwrap();
                FieldVal::Unsigned(val)
            }
            ValueType::Integer => {
                let mut rdr = Cursor::new(val);
                let val = rdr.read_i64::<BigEndian>().unwrap();
                FieldVal::Integer(val)
            }
            ValueType::Float => {
                let mut rdr = Cursor::new(val);
                let val = rdr.read_f64::<BigEndian>().unwrap();
                FieldVal::Float(val)
            }
            ValueType::Boolean => {
                let mut rdr = Cursor::new(val);
                let val = rdr.read_u8().unwrap() != 0;
                FieldVal::Boolean(val)
            }
            ValueType::String => FieldVal::Bytes(val),
            _ => todo!(),
        }
    }

    pub fn heap_size(&self) -> usize {
        if let FieldVal::Bytes(val) = self {
            val.capacity()
        } else {
            0
        }
    }
}

impl Display for FieldVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldVal::Unsigned(val) => write!(f, "{}", val),
            FieldVal::Integer(val) => write!(f, "{}", val),
            FieldVal::Float(val) => write!(f, "{}", val),
            FieldVal::Boolean(val) => write!(f, "{}", val),
            FieldVal::Bytes(val) => write!(f, "{:?})", val),
        }
    }
}

impl PartialEq for FieldVal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldVal::Unsigned(a), FieldVal::Unsigned(b)) => a == b,
            (FieldVal::Integer(a), FieldVal::Integer(b)) => a == b,
            (FieldVal::Float(a), FieldVal::Float(b)) => a.eq(b),
            (FieldVal::Boolean(a), FieldVal::Boolean(b)) => a == b,
            (FieldVal::Bytes(a), FieldVal::Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for FieldVal {}

#[derive(Debug, Clone)]
pub enum DataType {
    U64(i64, u64),
    I64(i64, i64),
    Str(i64, MiniVec<u8>),
    F64(i64, f64),
    Bool(i64, bool),
}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp().eq(&other.timestamp())
    }
}

impl Eq for DataType {}

impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Only care about timestamps when comparing
impl Ord for DataType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp().cmp(&other.timestamp())
    }
}

impl DataType {
    pub fn new(vtype: ValueType, ts: i64) -> Self {
        match vtype {
            ValueType::Unsigned => DataType::U64(ts, 0),
            ValueType::Integer => DataType::I64(ts, 0),
            ValueType::Float => DataType::F64(ts, 0.0),
            ValueType::Boolean => DataType::Bool(ts, false),
            ValueType::String => DataType::Str(ts, mini_vec![]),
            _ => todo!(),
        }
    }
    pub fn timestamp(&self) -> i64 {
        match *self {
            DataType::U64(ts, ..) => ts,
            DataType::I64(ts, ..) => ts,
            DataType::Str(ts, ..) => ts,
            DataType::F64(ts, ..) => ts,
            DataType::Bool(ts, ..) => ts,
        }
    }

    pub fn with_field_val(ts: Timestamp, field_val: FieldVal) -> Self {
        match field_val {
            FieldVal::Float(val) => Self::F64(ts, val),
            FieldVal::Integer(val) => Self::I64(ts, val),
            FieldVal::Unsigned(val) => Self::U64(ts, val),
            FieldVal::Boolean(val) => Self::Bool(ts, val),
            FieldVal::Bytes(val) => Self::Str(ts, val),
        }
    }

    pub fn to_bytes(&self) -> MiniVec<u8> {
        match self {
            DataType::U64(t, val) => {
                let mut buf = mini_vec![0; 16];
                buf[0..8].copy_from_slice(t.to_be_bytes().as_slice());
                buf[8..16].copy_from_slice(val.to_be_bytes().as_slice());
                buf
            }
            DataType::I64(t, val) => {
                let mut buf = mini_vec![0; 16];
                buf[0..8].copy_from_slice(t.to_be_bytes().as_slice());
                buf[8..16].copy_from_slice(val.to_be_bytes().as_slice());
                buf
            }
            DataType::F64(t, val) => {
                let mut buf = mini_vec![0; 16];
                buf[0..8].copy_from_slice(t.to_be_bytes().as_slice());
                buf[8..16].copy_from_slice(val.to_be_bytes().as_slice());
                buf
            }
            DataType::Str(t, val) => {
                let buf_len = 8 + val.len();
                let mut buf = mini_vec![0; buf_len];
                buf[0..8].copy_from_slice(t.to_be_bytes().as_slice());
                buf[8..buf_len].copy_from_slice(val);
                buf
            }
            DataType::Bool(t, val) => {
                let mut buf = mini_vec![0; 9];
                buf[0..8].copy_from_slice(t.to_be_bytes().as_slice());
                buf[8] = if *val { 1_u8 } else { 0_u8 };
                buf
            }
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::U64(ts, val) => write!(f, "({}, {})", ts, val),
            DataType::I64(ts, val) => write!(f, "({}, {})", ts, val),
            DataType::Str(ts, val) => write!(f, "({}, {:?})", ts, val),
            DataType::F64(ts, val) => write!(f, "({}, {})", ts, val),
            DataType::Bool(ts, val) => write!(f, "({}, {})", ts, val),
        }
    }
}
