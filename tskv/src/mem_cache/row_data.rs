use std::cmp;
use std::mem::size_of_val;

use flatbuffers::{ForwardsUOffset, Vector};
use minivec::MiniVec;
use models::field_value::FieldVal;
use models::schema::tskv_table_schema::TskvTableSchema;
use protos::models::{Column, FieldType};
use skiplist::OrderedSkipList;
use snafu::OptionExt;
use trace::error;
use utils::bitset::ImmutBitSet;
use utils::precision::{timestamp_convert, Precision};

use crate::database::FbSchema;
use crate::error::{CommonSnafu, FieldsIsEmptySnafu, TskvResult};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RowData {
    pub ts: i64,
    pub fields: Vec<Option<FieldVal>>,
}

impl RowData {
    pub fn point_to_row_data(
        schema: &TskvTableSchema,
        from_precision: Precision,
        columns: &Vector<ForwardsUOffset<Column>>,
        fb_schema: &FbSchema,
        row_idx: Vec<usize>,
    ) -> TskvResult<Vec<RowData>> {
        let fields_id = schema.fields_id();
        let mut res = Vec::with_capacity(row_idx.len());
        for row_count in row_idx.into_iter() {
            let mut fields = vec![None; fields_id.len()];
            let mut has_fields = false;
            for field_id in fb_schema.field_indexes.iter() {
                let column = columns.get(*field_id);
                let column_name = column.name_ext()?;
                let column_nullbit = column.nullbit_ext()?;
                match column.field_type() {
                    FieldType::Integer => {
                        let len = column.int_values_len()?;
                        let column_nullbits =
                            ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                        if !column_nullbits.get(row_count) {
                            continue;
                        }
                        let val = column.int_values()?.get(row_count);
                        match schema.column(column_name) {
                            None => {
                                error!("column {} not found in schema", column_name);
                            }
                            Some(column) => {
                                let field_id = column.id;
                                let field_idx = fields_id.get(&field_id).unwrap();
                                fields[*field_idx] = Some(FieldVal::Integer(val));
                                has_fields = true;
                            }
                        }
                    }
                    FieldType::Float => {
                        let len = column.float_values_len()?;
                        let column_nullbits =
                            ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                        if !column_nullbits.get(row_count) {
                            continue;
                        }
                        let val = column.float_values()?.get(row_count);
                        match schema.column(column_name) {
                            None => {
                                error!("column {} not found in schema", column_name);
                            }
                            Some(column) => {
                                let field_id = column.id;
                                let field_idx = fields_id.get(&field_id).unwrap();
                                fields[*field_idx] = Some(FieldVal::Float(val));
                                has_fields = true;
                            }
                        }
                    }
                    FieldType::Unsigned => {
                        let len = column.uint_values_len()?;
                        let column_nullbits =
                            ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                        if !column_nullbits.get(row_count) {
                            continue;
                        }
                        let val = column.uint_values()?.get(row_count);
                        match schema.column(column_name) {
                            None => {
                                error!("column {} not found in schema", column_name);
                            }
                            Some(column) => {
                                let field_id = column.id;
                                let field_idx = fields_id.get(&field_id).unwrap();
                                fields[*field_idx] = Some(FieldVal::Unsigned(val));
                                has_fields = true;
                            }
                        }
                    }
                    FieldType::Boolean => {
                        let len = column.bool_values_len()?;
                        let column_nullbits =
                            ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                        if !column_nullbits.get(row_count) {
                            continue;
                        }
                        let val = column.bool_values()?.get(row_count);
                        match schema.column(column_name) {
                            None => {
                                error!("column {} not found in schema", column_name);
                            }
                            Some(column) => {
                                let field_id = column.id;
                                let field_idx = fields_id.get(&field_id).unwrap();
                                fields[*field_idx] = Some(FieldVal::Boolean(val));
                                has_fields = true;
                            }
                        }
                    }
                    FieldType::String => {
                        let len = column.string_values_len()?;
                        let column_nullbits =
                            ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                        if !column_nullbits.get(row_count) {
                            continue;
                        }
                        let val = column.string_values()?.get(row_count);
                        match schema.column(column_name) {
                            None => {
                                error!("column {} not found in schema", column_name);
                            }
                            Some(column) => {
                                let field_id = column.id;
                                let field_idx = fields_id.get(&field_id).unwrap();
                                fields[*field_idx] =
                                    Some(FieldVal::Bytes(MiniVec::from(val.as_bytes())));
                                has_fields = true;
                            }
                        }
                    }
                    _ => {
                        error!("unsupported field type");
                    }
                }
            }

            if !has_fields {
                return Err(FieldsIsEmptySnafu.build());
            }

            let ts_column = columns.get(fb_schema.time_index);
            let ts = ts_column.int_values()?.get(row_count);
            let to_precision = schema.time_column_precision();
            let ts = timestamp_convert(from_precision, to_precision, ts).context(CommonSnafu {
                reason: "timestamp overflow".to_string(),
            })?;
            res.push(RowData { ts, fields });
        }
        Ok(res)
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for i in self.fields.iter() {
            match i {
                None => {
                    size += size_of_val(i);
                }
                Some(v) => {
                    size += size_of_val(i) + v.heap_size();
                }
            }
        }
        size += size_of_val(&self.ts);
        size += size_of_val(&self.fields);
        size
    }
}

impl PartialOrd for RowData {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.ts.cmp(&other.ts))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct OrderedRowsData {
    rows: OrderedSkipList<RowData>,
}

impl OrderedRowsData {
    pub fn new() -> Self {
        let mut rows: OrderedSkipList<RowData> = OrderedSkipList::new();
        unsafe { rows.sort_by(|a: &RowData, b: &RowData| a.partial_cmp(b).unwrap()) }
        Self { rows }
    }

    pub fn get_rows(self) -> OrderedSkipList<RowData> {
        self.rows
    }

    pub fn get_ref_rows(&self) -> &OrderedSkipList<RowData> {
        &self.rows
    }

    pub fn clear(&mut self) {
        self.rows.clear()
    }

    pub fn insert(&mut self, row: RowData) {
        self.rows.insert(row);
    }

    pub fn retain(&mut self, mut f: impl FnMut(&RowData) -> bool) {
        self.rows.retain(|row| f(row));
    }
}

impl Default for OrderedRowsData {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for OrderedRowsData {
    fn clone(&self) -> Self {
        let mut clone_rows: OrderedSkipList<RowData> = OrderedSkipList::new();
        unsafe { clone_rows.sort_by(|a: &RowData, b: &RowData| a.partial_cmp(b).unwrap()) }
        self.rows.iter().for_each(|row| {
            clone_rows.insert(row.clone());
        });
        Self { rows: clone_rows }
    }
}
