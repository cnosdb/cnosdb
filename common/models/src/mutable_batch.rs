use std::collections::HashMap;

use utils::bitset::BitSet;

use crate::field_value::FieldVal;
use crate::schema::PhysicalCType;
use crate::{Error, PhysicalDType, Result};

#[derive(Debug, Default, Clone)]
pub struct MutableBatch {
    /// Map of column name to index in `MutableBatch::columns`
    pub column_names: HashMap<String, usize>,

    /// Columns contained within this MutableBatch
    pub columns: Vec<Column>,

    /// The number of rows in this MutableBatch
    pub row_count: usize,
}

impl MutableBatch {
    pub fn new() -> Self {
        Self {
            column_names: Default::default(),
            columns: Default::default(),
            row_count: 0,
        }
    }

    pub fn column_mut(&mut self, name: &str, col_type: PhysicalCType) -> Result<&mut Column> {
        let columns_len = self.columns.len();
        let column_idx = self
            .column_names
            .raw_entry_mut()
            .from_key(name)
            .or_insert_with(|| (name.to_string(), columns_len))
            .1;
        if *column_idx == columns_len {
            self.columns.push(Column::new(self.row_count, col_type)?);
        }

        Ok(&mut self.columns[*column_idx] as _)
    }

    pub fn finish(&mut self) {
        for column in self.columns.iter_mut() {
            if column.valid.len() < self.row_count {
                column
                    .valid
                    .append_unset(self.row_count - column.valid.len());
                match column.data {
                    ColumnData::F64(ref mut value) => {
                        if !value.is_empty() {
                            value.append(&mut vec![0.0; self.row_count - value.len()]);
                        }
                    }
                    ColumnData::I64(ref mut value) => {
                        if !value.is_empty() {
                            value.append(&mut vec![0; self.row_count - value.len()]);
                        }
                    }
                    ColumnData::U64(ref mut value) => {
                        if !value.is_empty() {
                            value.append(&mut vec![0; self.row_count - value.len()]);
                        }
                    }
                    ColumnData::String(ref mut value) => {
                        if !value.is_empty() {
                            value.append(&mut vec![String::new(); self.row_count - value.len()]);
                        }
                    }
                    ColumnData::Bool(ref mut value) => {
                        if !value.is_empty() {
                            value.append(&mut vec![false; self.row_count - value.len()]);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub column_type: PhysicalCType,
    pub valid: BitSet,
    pub data: ColumnData,
}

impl Column {
    pub fn new(row_count: usize, column_type: PhysicalCType) -> Result<Column> {
        let mut valid = BitSet::new();
        valid.append_unset(row_count);

        let data = match column_type {
            PhysicalCType::Tag => ColumnData::String(vec![String::new(); row_count]),
            PhysicalCType::Time(_) => ColumnData::I64(vec![0; row_count]),
            PhysicalCType::Field(field_type) => match field_type {
                PhysicalDType::Unknown => {
                    return Err(Error::Common {
                        content: "Unknown field type".to_string(),
                    });
                }
                PhysicalDType::Float => ColumnData::F64(vec![0.0; row_count]),
                PhysicalDType::Integer => ColumnData::I64(vec![0; row_count]),
                PhysicalDType::Unsigned => ColumnData::U64(vec![0; row_count]),
                PhysicalDType::Boolean => ColumnData::Bool(vec![false; row_count]),
                PhysicalDType::String => ColumnData::String(vec![String::new(); row_count]),
            },
        };
        Ok(Self {
            column_type,
            valid,
            data,
        })
    }
    pub fn push(&mut self, value: Option<FieldVal>) {
        match (&mut self.data, value) {
            (ColumnData::F64(ref mut value), Some(FieldVal::Float(val))) => {
                value.push(val);

                self.valid.append_set(1);
            }
            (ColumnData::F64(ref mut value), None) => {
                value.push(0.0);
                self.valid.append_unset(1);
            }
            (ColumnData::I64(ref mut value), Some(FieldVal::Integer(val))) => {
                value.push(val);
                self.valid.append_set(1);
            }
            (ColumnData::I64(ref mut value), None) => {
                value.push(0);
                self.valid.append_unset(1);
            }
            (ColumnData::U64(ref mut value), Some(FieldVal::Unsigned(val))) => {
                value.push(val);
                self.valid.append_set(1);
            }
            (ColumnData::U64(ref mut value), None) => {
                value.push(0);
                self.valid.append_unset(1);
            }
            //todo: need to change string to Bytes type in ColumnData
            (ColumnData::String(ref mut value), Some(FieldVal::Bytes(val))) => {
                value.push(String::from_utf8(val.to_vec()).unwrap());
                self.valid.append_set(1);
            }
            (ColumnData::String(ref mut value), None) => {
                value.push(String::new());
                self.valid.append_unset(1);
            }
            (ColumnData::Bool(ref mut value), Some(FieldVal::Boolean(val))) => {
                value.push(val);
                self.valid.append_set(1);
            }
            (ColumnData::Bool(ref mut value), None) => {
                value.push(false);
                self.valid.append_unset(1);
            }
            _ => {
                panic!("Column type mismatch")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ColumnData {
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
    String(Vec<String>),
    Bool(Vec<bool>),
}
