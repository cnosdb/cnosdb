use std::collections::HashMap;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{SchemaRef, TimeUnit};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use models::mutable_batch::{ColumnData, MutableBatch};
use models::schema::{PhysicalCType as ColumnType, TskvTableSchemaRef};
use models::PhysicalDType as ValueType;
use protos::models::{
    Column as FbColumn, ColumnBuilder, ColumnType as FbColumnType, FieldType, PointsBuilder,
    TableBuilder, ValuesBuilder,
};
use utils::bitset::BitSet;

use crate::{Error, FieldValue, Line, Result};

pub fn line_to_batches(lines: &[Line]) -> Result<HashMap<String, MutableBatch>> {
    let mut batches = HashMap::new();
    for line in lines.iter() {
        let table = line.table;
        let (_, batch) = batches
            .raw_entry_mut()
            .from_key(table)
            .or_insert_with(|| (table.to_string(), MutableBatch::new()));
        let row_count = batch.row_count;
        for (tag_key, tag_value) in line.tags.iter() {
            let col = batch
                .column_mut(tag_key, ColumnType::Tag)
                .map_err(|e| Error::Common {
                    content: format!("Error getting column: {}", e),
                })?;
            match &mut col.data {
                ColumnData::String(data) => {
                    data.resize(row_count + 1, String::new());
                    data[row_count] = tag_value.to_string();
                    col.valid.append_unset(row_count - col.valid.len());
                    col.valid.append_set(1);
                }
                _ => {
                    return Err(Error::Common {
                        content: "Expected string column".to_string(),
                    });
                }
            }
        }
        for (field_key, field_value) in line.fields.iter() {
            match field_value {
                FieldValue::U64(value) => {
                    let col = batch
                        .column_mut(field_key, ColumnType::Field(ValueType::Unsigned))
                        .map_err(|e| Error::Common {
                            content: format!("Error getting column: {}", e),
                        })?;
                    match &mut col.data {
                        ColumnData::U64(data) => {
                            data.resize(row_count + 1, 0);
                            data[row_count] = *value;
                            col.valid.append_unset(row_count - col.valid.len());
                            col.valid.append_set(1);
                        }
                        _ => {
                            return Err(Error::Common {
                                content: "Expected u64 column".to_string(),
                            });
                        }
                    }
                }
                FieldValue::I64(value) => {
                    let col = batch
                        .column_mut(field_key, ColumnType::Field(ValueType::Integer))
                        .map_err(|e| Error::Common {
                            content: format!("Error getting column: {}", e),
                        })?;
                    match &mut col.data {
                        ColumnData::I64(data) => {
                            data.resize(row_count + 1, 0);
                            data[row_count] = *value;
                            col.valid.append_unset(row_count - col.valid.len());
                            col.valid.append_set(1);
                        }
                        _ => {
                            return Err(Error::Common {
                                content: "Expected i64 column".to_string(),
                            });
                        }
                    }
                }
                FieldValue::Str(value) => {
                    let col = batch
                        .column_mut(field_key, ColumnType::Field(ValueType::String))
                        .map_err(|e| Error::Common {
                            content: format!("Error getting column: {}", e),
                        })?;
                    match &mut col.data {
                        ColumnData::String(data) => {
                            data.resize(row_count + 1, String::new());
                            data[row_count] = String::from_utf8(value.to_vec()).unwrap();
                            col.valid.append_unset(row_count - col.valid.len());
                            col.valid.append_set(1);
                        }
                        _ => {
                            return Err(Error::Common {
                                content: "Expected string column".to_string(),
                            });
                        }
                    }
                }
                FieldValue::F64(value) => {
                    let col = batch
                        .column_mut(field_key, ColumnType::Field(ValueType::Float))
                        .map_err(|e| Error::Common {
                            content: format!("Error getting column: {}", e),
                        })?;
                    match &mut col.data {
                        ColumnData::F64(data) => {
                            data.resize(row_count + 1, 0.0);
                            data[row_count] = *value;
                            col.valid.append_unset(row_count - col.valid.len());
                            col.valid.append_set(1);
                        }
                        _ => {
                            return Err(Error::Common {
                                content: "Expected f64 column".to_string(),
                            });
                        }
                    }
                }
                FieldValue::Bool(value) => {
                    let col = batch
                        .column_mut(field_key, ColumnType::Field(ValueType::Boolean))
                        .map_err(|e| Error::Common {
                            content: format!("Error getting column: {}", e),
                        })?;
                    match &mut col.data {
                        ColumnData::Bool(data) => {
                            data.resize(row_count + 1, false);
                            data[row_count] = *value;
                            col.valid.append_unset(row_count - col.valid.len());
                            col.valid.append_set(1);
                        }
                        _ => {
                            return Err(Error::Common {
                                content: "Expected bool column".to_string(),
                            });
                        }
                    }
                }
            }
        }

        let time = line.timestamp;
        let col = batch
            .column_mut("time", ColumnType::default_time())
            .map_err(|e| Error::Common {
                content: format!("Error getting column: {}", e),
            })?;
        match col.data {
            ColumnData::I64(ref mut data) => {
                data.resize(row_count + 1, 0);
                data[row_count] = time;
                col.valid.append_unset(row_count - col.valid.len());
                col.valid.append_set(1);
            }
            _ => {
                return Err(Error::Common {
                    content: "Expected i64 type as time column".to_string(),
                });
            }
        }
        batch.row_count += 1;
    }
    batches.iter_mut().for_each(|(_, batch)| {
        batch.finish();
    });
    Ok(batches)
}

pub fn mutable_batches_to_point(db: &str, batches: HashMap<String, MutableBatch>) -> Vec<u8> {
    let fbb = &mut FlatBufferBuilder::new();
    let mut tables = Vec::with_capacity(batches.len());
    for (table_name, table_batch) in batches {
        let mut columns = Vec::with_capacity(table_batch.columns.len());
        let mut columns_names = table_batch
            .column_names
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect::<Vec<_>>();
        columns_names.sort_by(|a, b| a.0.cmp(b.0));
        for (field_name, field_idx) in columns_names {
            let column = table_batch.columns.get(*field_idx).unwrap();
            let fb_column_type = match column.column_type {
                ColumnType::Tag => FbColumnType::Tag,
                ColumnType::Field(_) => FbColumnType::Field,
                ColumnType::Time(_) => FbColumnType::Time,
            };

            let (field_type, values) = match column.data {
                ColumnData::F64(ref values) => {
                    let values = fbb.create_vector(values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_float_value(values);
                    (FieldType::Float, values_builder.finish())
                }
                ColumnData::I64(ref values) => {
                    let values = fbb.create_vector(values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_int_value(values);
                    (FieldType::Integer, values_builder.finish())
                }
                ColumnData::U64(ref values) => {
                    let values = fbb.create_vector(values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_uint_value(values);
                    (FieldType::Unsigned, values_builder.finish())
                }
                ColumnData::String(ref values) => {
                    let values = values
                        .iter()
                        .map(|s| fbb.create_string(s))
                        .collect::<Vec<_>>();
                    let values = fbb.create_vector(&values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_string_value(values);
                    (FieldType::String, values_builder.finish())
                }
                ColumnData::Bool(ref values) => {
                    let values = fbb.create_vector(values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_bool_value(values);
                    (FieldType::Boolean, values_builder.finish())
                }
            };
            let column_name = fbb.create_string(field_name);
            let nullbits = fbb.create_vector(column.valid.bytes());
            let mut column_builder = ColumnBuilder::new(fbb);
            column_builder.add_field_type(field_type);
            column_builder.add_column_type(fb_column_type);
            column_builder.add_name(column_name);
            column_builder.add_nullbits(nullbits);
            column_builder.add_col_values(values);

            columns.push(column_builder.finish());
        }
        let columns = fbb.create_vector(&columns);
        let table_name = fbb.create_string(&table_name);
        let mut table_builder = TableBuilder::new(fbb);
        table_builder.add_columns(columns);
        table_builder.add_tab(table_name);
        table_builder.add_num_rows(table_batch.row_count as u64);
        tables.push(table_builder.finish());
    }
    let tables = fbb.create_vector(&tables);
    let db = fbb.create_string(db);
    let mut point_builder = PointsBuilder::new(fbb);
    point_builder.add_tables(tables);
    point_builder.add_db(db);
    let points = point_builder.finish();
    fbb.finish(points, None);
    let data = fbb.finished_data().to_vec();
    data
}

pub fn arrow_array_to_points(
    columns: Vec<ArrayRef>,
    schema: SchemaRef,
    table_schema: TskvTableSchemaRef,
    len: usize,
) -> Result<Vec<u8>> {
    let mut fbb = FlatBufferBuilder::new();
    let table_name = table_schema.name.as_str();
    let mut fb_columns = Vec::new();
    for (column, schema) in columns.iter().zip(schema.fields.iter()) {
        let col_name = schema.name().as_str();
        let column_schema = table_schema.column(col_name).ok_or_else(|| Error::Common {
            content: format!("column {} not found in table {}", col_name, table_name),
        })?;
        let fb_column = match column_schema.column_type.to_physical_type() {
            ColumnType::Tag => build_string_column(column, col_name, FbColumnType::Tag, &mut fbb)?,
            ColumnType::Time(ref time_unit) => {
                build_timestamp_column(column, col_name, time_unit, &mut fbb)?
            }
            ColumnType::Field(value_type) => match value_type {
                ValueType::Unknown => {
                    return Err(Error::Common {
                        content: format!("column {} type is unknown", col_name),
                    });
                }
                ValueType::Float => build_f64_column(column, col_name, &mut fbb)?,
                ValueType::Integer => build_i64_column(column, col_name, &mut fbb)?,
                ValueType::Unsigned => build_u64_column(column, col_name, &mut fbb)?,
                ValueType::Boolean => build_bool_column(column, col_name, &mut fbb)?,
                ValueType::String => {
                    build_string_column(column, col_name, FbColumnType::Field, &mut fbb)?
                }
            },
        };
        fb_columns.push(fb_column);
    }
    let columns = fbb.create_vector(&fb_columns);
    let table_name = fbb.create_string(table_name);
    let mut table_builder = TableBuilder::new(&mut fbb);
    table_builder.add_columns(columns);
    table_builder.add_tab(table_name);
    table_builder.add_num_rows(len as u64);
    let table = table_builder.finish();
    let tables = fbb.create_vector(&[table]);
    let db = fbb.create_string(table_schema.db.as_str());
    let mut point_builder = PointsBuilder::new(&mut fbb);
    point_builder.add_tables(tables);
    point_builder.add_db(db);
    let points = point_builder.finish();
    fbb.finish(points, None);
    let data = fbb.finished_data().to_vec();
    Ok(data)
}

pub fn build_string_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    fb_column_type: FbColumnType,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let values = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(Error::Common {
            content: format!("column {} is not string", col_name),
        })?;
    let mut nullbits = BitSet::new();
    let mut col_values = Vec::with_capacity(values.len());
    values.iter().for_each(|value| {
        if let Some(value) = value {
            nullbits.append_set(1);
            col_values.push(fbb.create_string(value));
        } else {
            nullbits.append_unset(1);
            col_values.push(fbb.create_string(""));
        }
    });
    let nullbits = fbb.create_vector(nullbits.bytes());
    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_string_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(fb_column_type);
    column_builder.add_field_type(FieldType::String);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}

pub fn build_timestamp_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    time_unit: &TimeUnit,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let (nullbits, col_values) = match time_unit {
        TimeUnit::Second => {
            return Err(Error::Common {
                content: "time column not support second".to_string(),
            });
        }
        TimeUnit::Millisecond => {
            let values = column
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or(Error::Common {
                    content: format!("column {} is not int64", col_name),
                })?;
            let mut nullbits = BitSet::new();
            let mut col_values = Vec::with_capacity(values.len());
            values.iter().for_each(|value| {
                if let Some(value) = value {
                    nullbits.append_set(1);
                    col_values.push(value);
                } else {
                    nullbits.append_unset(1);
                    col_values.push(0);
                }
            });
            (nullbits, col_values)
        }
        TimeUnit::Microsecond => {
            let values = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or(Error::Common {
                    content: format!("column {} is not int64", col_name),
                })?;
            let mut nullbits = BitSet::new();
            let mut col_values = Vec::with_capacity(values.len());
            values.iter().for_each(|value| {
                if let Some(value) = value {
                    nullbits.append_set(1);
                    col_values.push(value);
                } else {
                    nullbits.append_unset(1);
                    col_values.push(0);
                }
            });
            (nullbits, col_values)
        }
        TimeUnit::Nanosecond => {
            let values = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or(Error::Common {
                    content: format!("column {} is not int64", col_name),
                })?;
            let mut nullbits = BitSet::new();
            let mut col_values = Vec::with_capacity(values.len());
            values.iter().for_each(|value| {
                if let Some(value) = value {
                    nullbits.append_set(1);
                    col_values.push(value);
                } else {
                    nullbits.append_unset(1);
                    col_values.push(0);
                }
            });
            (nullbits, col_values)
        }
    };
    let nullbits = fbb.create_vector(nullbits.bytes());
    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_int_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(FbColumnType::Time);
    column_builder.add_field_type(FieldType::Integer);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}

pub fn build_i64_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let values = column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(Error::Common {
            content: format!("column {} is not int64", col_name),
        })?;
    let mut nullbits = BitSet::new();
    let mut col_values = Vec::with_capacity(values.len());
    values.iter().for_each(|value| {
        if let Some(value) = value {
            nullbits.append_set(1);
            col_values.push(value);
        } else {
            nullbits.append_unset(1);
            col_values.push(0);
        }
    });
    let nullbits = fbb.create_vector(nullbits.bytes());
    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_int_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(FbColumnType::Field);
    column_builder.add_field_type(FieldType::Integer);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}

pub fn build_f64_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let values = column
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or(Error::Common {
            content: format!("column {} is not float64", col_name),
        })?;
    let mut nullbits = BitSet::new();
    let mut col_values = Vec::with_capacity(values.len());
    values.iter().for_each(|value| {
        if let Some(value) = value {
            nullbits.append_set(1);
            col_values.push(value);
        } else {
            nullbits.append_unset(1);
            col_values.push(0.0);
        }
    });
    let nullbits = fbb.create_vector(nullbits.bytes());
    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_float_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(FbColumnType::Field);
    column_builder.add_field_type(FieldType::Float);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}

pub fn build_u64_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let values = column
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or(Error::Common {
            content: format!("column {} is not uint64", col_name),
        })?;
    let mut nullbits = BitSet::new();
    let mut col_values = Vec::with_capacity(values.len());
    values.iter().for_each(|value| {
        if let Some(value) = value {
            nullbits.append_set(1);
            col_values.push(value);
        } else {
            nullbits.append_unset(1);
            col_values.push(0);
        }
    });
    let nullbits = fbb.create_vector(nullbits.bytes());
    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_uint_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(FbColumnType::Field);
    column_builder.add_field_type(FieldType::Unsigned);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}

pub fn build_bool_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let values = column
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or(Error::Common {
            content: format!("column {} is not bool", col_name),
        })?;
    let mut nullbits = BitSet::new();
    let mut col_values = Vec::with_capacity(values.len());
    values.iter().for_each(|value| {
        if let Some(value) = value {
            nullbits.append_set(1);
            col_values.push(value);
        } else {
            nullbits.append_unset(1);
            col_values.push(false);
        }
    });
    let nullbits = fbb.create_vector(nullbits.bytes());
    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_bool_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(FbColumnType::Field);
    column_builder.add_field_type(FieldType::Boolean);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}
