use models::column_data::{ColumnDataResult, DataTypeMissMatchSnafu, UnsupportedDataTypeSnafu};
use models::field_value::FieldVal;
use models::schema::tskv_table_schema::TableColumn;
use models::PhysicalDType;
use utils::bitset::BitSet;

use crate::{TskvError, TskvResult};

#[derive(Debug, Clone, PartialEq)]
pub struct MutableColumnRef<'a> {
    pub column_desc: TableColumn,
    pub column_data: ColumnDataRef<'a>,
}

impl<'a> MutableColumnRef<'a> {
    pub fn empty(column_desc: TableColumn) -> TskvResult<MutableColumnRef<'a>> {
        let column_type = column_desc.column_type.to_physical_data_type();
        let data = ColumnDataRef::new(column_type)
            .map_err(|e| TskvError::ColumnDataError { source: e })?;
        let column = Self {
            column_desc,
            column_data: data,
        };
        Ok(column)
    }

    pub fn push_ts(&mut self, value: i64) -> TskvResult<()> {
        self.column_data
            .push_ts(value)
            .map_err(|e| TskvError::ColumnDataError { source: e })
    }

    pub fn push(&mut self, value: Option<&'a FieldVal>) -> TskvResult<()> {
        self.column_data
            .push(value)
            .map_err(|e| TskvError::ColumnDataError { source: e })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDataRef<'a> {
    pub valid: BitSet,
    pub primary_data: PrimaryColumnDataRef<'a>,
}

impl<'a> ColumnDataRef<'a> {
    pub fn new(column_type: PhysicalDType) -> ColumnDataResult<ColumnDataRef<'a>> {
        let valid = BitSet::new();
        let primary_data = match column_type {
            PhysicalDType::Float => PrimaryColumnDataRef::F64(vec![], f64::MAX, f64::MIN),
            PhysicalDType::Integer => PrimaryColumnDataRef::I64(vec![], i64::MAX, i64::MIN),
            PhysicalDType::Unsigned => PrimaryColumnDataRef::U64(vec![], u64::MAX, u64::MIN),
            PhysicalDType::Boolean => PrimaryColumnDataRef::Bool(vec![], false, true),
            PhysicalDType::String => {
                PrimaryColumnDataRef::String(vec![], "".as_bytes(), "".as_bytes())
            }
            PhysicalDType::Unknown => {
                return Err(UnsupportedDataTypeSnafu {
                    dt: "unknown".to_string(),
                }
                .build())
            }
        };
        let data = ColumnDataRef {
            valid,
            primary_data,
        };
        Ok(data)
    }

    pub fn push_ts(&mut self, val: i64) -> ColumnDataResult<()> {
        let data_len = self.valid.len();
        match &mut self.primary_data {
            PrimaryColumnDataRef::I64(ref mut values, min, max) => {
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                values.push(val);
                self.valid.append_unset_and_set(data_len);
            }

            _ => {
                return Err(DataTypeMissMatchSnafu {
                    column_type: self.primary_data.physical_dtype(),
                    field_val: Some(FieldVal::Integer(val)),
                }
                .build())
            }
        }
        Ok(())
    }

    pub fn push(&mut self, value: Option<&'a FieldVal>) -> ColumnDataResult<()> {
        let data_len = self.valid.len();
        match (&mut self.primary_data, value) {
            (PrimaryColumnDataRef::F64(ref mut values, min, max), Some(FieldVal::Float(val))) => {
                if *max < *val {
                    *max = *val;
                }
                if *min > *val {
                    *min = *val;
                }
                values.push(*val);
                self.valid.append_unset_and_set(data_len);
            }
            (PrimaryColumnDataRef::F64(..), None) => {
                self.valid.append_unset(1);
            }

            (PrimaryColumnDataRef::I64(ref mut values, min, max), Some(FieldVal::Integer(val))) => {
                if *max < *val {
                    *max = *val;
                }
                if *min > *val {
                    *min = *val;
                }
                values.push(*val);
                self.valid.append_unset_and_set(data_len);
            }
            (PrimaryColumnDataRef::I64(..), None) => {
                self.valid.append_unset(1);
            }

            (
                PrimaryColumnDataRef::U64(ref mut values, min, max),
                Some(FieldVal::Unsigned(val)),
            ) => {
                if *max < *val {
                    *max = *val;
                }
                if *min > *val {
                    *min = *val;
                }
                values.push(*val);
                self.valid.append_unset_and_set(data_len);
            }
            (PrimaryColumnDataRef::U64(..), None) => {
                self.valid.append_unset(1);
            }

            (
                PrimaryColumnDataRef::String(ref mut values, min, max),
                Some(FieldVal::Bytes(val)),
            ) => {
                let val = val.as_slice();
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                values.push(val);
                self.valid.append_unset_and_set(data_len);
            }
            (PrimaryColumnDataRef::String(..), None) => {
                self.valid.append_unset(1);
            }

            (
                PrimaryColumnDataRef::Bool(ref mut values, min, max),
                Some(FieldVal::Boolean(val)),
            ) => {
                if !(*max) & val {
                    *max = *val;
                }
                if *min & !val {
                    *min = *val;
                }
                values.push(*val);
                self.valid.append_unset_and_set(data_len);
            }
            (PrimaryColumnDataRef::Bool(..), None) => {
                self.valid.append_unset(1);
            }

            _ => {
                return Err(DataTypeMissMatchSnafu {
                    column_type: self.primary_data.physical_dtype(),
                    field_val: value.cloned(),
                }
                .build())
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PrimaryColumnDataRef<'a> {
    ///   array   min, max
    F64(Vec<f64>, f64, f64),
    I64(Vec<i64>, i64, i64),
    U64(Vec<u64>, u64, u64),
    String(Vec<&'a [u8]>, &'a [u8], &'a [u8]),
    Bool(Vec<bool>, bool, bool),
}

impl<'a> PrimaryColumnDataRef<'a> {
    pub fn physical_dtype(&self) -> PhysicalDType {
        match self {
            PrimaryColumnDataRef::F64(..) => PhysicalDType::Float,
            PrimaryColumnDataRef::I64(..) => PhysicalDType::Integer,
            PrimaryColumnDataRef::U64(..) => PhysicalDType::Unsigned,
            PrimaryColumnDataRef::String(..) => PhysicalDType::String,
            PrimaryColumnDataRef::Bool(..) => PhysicalDType::Boolean,
        }
    }
}
