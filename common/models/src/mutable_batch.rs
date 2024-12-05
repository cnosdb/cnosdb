use std::collections::HashMap;

use crate::column_data_ref::{ColumnDataRef, PrimaryColumnDataRef};
use crate::errors::CommonSnafu;
use crate::field_value::FieldVal;
use crate::schema::tskv_table_schema::PhysicalCType;
use crate::ModelResult;

#[derive(Debug, Default, Clone)]
pub struct MutableBatch<'a> {
    /// Map of column name to index in `MutableBatch::columns`
    pub column_names: HashMap<String, usize>,

    /// Columns contained within this MutableBatch
    pub columns: Vec<Column<'a>>,

    /// The number of rows in this MutableBatch
    pub row_count: usize,
}

impl<'a> MutableBatch<'a> {
    pub fn new() -> Self {
        Self {
            column_names: Default::default(),
            columns: Default::default(),
            row_count: 0,
        }
    }

    pub fn column_mut(
        &mut self,
        name: &str,
        col_type: PhysicalCType,
    ) -> ModelResult<&mut Column<'a>> {
        let column_idx = match self.column_names.get(name) {
            Some(column_idx) => *column_idx,
            None => {
                let column_idx = self.columns.len();
                self.column_names.insert(name.to_string(), column_idx);
                self.columns.push(Column::new(self.row_count, col_type)?);
                column_idx
            }
        };

        Ok(&mut self.columns[column_idx] as _)
    }

    pub fn finish(&mut self) {
        for column in self.columns.iter_mut() {
            if column.column_data.valid.len() < self.row_count {
                column
                    .column_data
                    .valid
                    .append_n(self.row_count - column.column_data.valid.len(), false);
                match column.column_data.primary_data {
                    PrimaryColumnDataRef::F64(ref mut value, ..) => {
                        if !value.is_empty() {
                            value.append(&mut vec![0.0; self.row_count - value.len()]);
                        }
                    }
                    PrimaryColumnDataRef::I64(ref mut value, ..) => {
                        if !value.is_empty() {
                            value.append(&mut vec![0; self.row_count - value.len()]);
                        }
                    }
                    PrimaryColumnDataRef::U64(ref mut value, ..) => {
                        if !value.is_empty() {
                            value.append(&mut vec![0; self.row_count - value.len()]);
                        }
                    }
                    PrimaryColumnDataRef::String(ref mut value, ..) => {
                        if !value.is_empty() {
                            value.append(&mut vec!["".as_bytes(); self.row_count - value.len()]);
                        }
                    }
                    PrimaryColumnDataRef::Bool(ref mut value, ..) => {
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
pub struct Column<'a> {
    pub column_type: PhysicalCType,
    pub column_data: ColumnDataRef<'a>,
}

impl<'a> Column<'a> {
    pub fn new(row_count: usize, column_type: PhysicalCType) -> ModelResult<Column<'a>> {
        let data = ColumnDataRef::new(column_type.to_physical_data_type(), row_count)
            .map_err(|e| CommonSnafu { msg: e.to_string() }.build())?;
        Ok(Self {
            column_type,
            column_data: data,
        })
    }
    pub fn push(&mut self, value: Option<&'a FieldVal>) -> ModelResult<()> {
        self.column_data
            .push(value)
            .map_err(|e| CommonSnafu { msg: e.to_string() }.build())
    }
}
