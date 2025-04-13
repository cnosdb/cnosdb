use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Fields, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};

use crate::instance::CnosdbColumnType;

const NULL: &str = "NULL";

pub fn convert_batches(batches: Vec<RecordBatch>) -> Result<Vec<Vec<String>>, ArrowError> {
    if batches.is_empty() {
        Ok(vec![])
    } else {
        let schema = batches[0].schema();
        let mut rows = vec![];
        for batch in batches {
            // Verify schema
            if !equivalent_names_and_types(&schema, batch.schema()) {
                return Err(ArrowError::SchemaError(format!(
                    "Schema mismatch. Previously had\n{:#?}\n\nGot:\n{:#?}",
                    &schema,
                    batch.schema()
                )));
            }

            let new_rows = convert_batch(batch)?.into_iter().flat_map(expand_row);
            rows.extend(new_rows);
        }
        Ok(rows)
    }
}

fn convert_batch(batch: RecordBatch) -> Result<Vec<Vec<String>>, ArrowError> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|col| {
                    let options = FormatOptions::default().with_null(NULL);
                    let formatter = ArrayFormatter::try_new(col, &options)?;
                    value_to_string(col, row, formatter)
                })
                .collect::<Result<Vec<String>, ArrowError>>()
        })
        .collect()
}

/// special case rows that have newlines in them (like explain plans)
//
/// Transform inputs into one cell per line
fn expand_row(mut row: Vec<String>) -> Vec<Vec<String>> {
    use std::iter::once;

    // check last cell
    if let Some(cell) = row.pop() {
        let lines: Vec<_> = cell.split('\n').filter(|l| !l.is_empty()).collect();

        // no newlines in last cell
        if lines.len() < 2 {
            row.push(cell);
            return vec![row];
        }

        // form new rows with each additional line
        let new_lines: Vec<_> = lines
            .into_iter()
            .map(|l| {
                // replace any leading spaces with '-' as
                // `sqllogictest` ignores whitespace differences
                let content = l.trim_start();
                let new_prefix = "-".repeat(l.len() - content.len());
                vec![format!("{new_prefix}{content}")]
            })
            .collect();

        once(row).chain(new_lines).collect::<Vec<_>>()
    } else {
        vec![row]
    }
}

pub fn value_to_string(
    col: &ArrayRef,
    row: usize,
    formatter: ArrayFormatter,
) -> Result<String, ArrowError> {
    let mut res = formatter.value(row).try_to_string()?;
    if col.data_type().equals_datatype(&DataType::Utf8) {
        res = escape_string(&res);
    }
    Ok(res)
}

fn escape_string(s: &str) -> String {
    let mut res = String::with_capacity(s.len() + 2);
    res.push('"');
    for c in s.chars() {
        if c == '"' {
            res.push('\\');
        }
        res.push(c);
    }
    res.push('"');
    res
}

/// Check two schemas for being equal for field names/types
fn equivalent_names_and_types(schema: &SchemaRef, other: SchemaRef) -> bool {
    if schema.fields().len() != other.fields().len() {
        return false;
    }
    let self_fields = schema.fields().iter();
    let other_fields = other.fields().iter();
    self_fields
        .zip(other_fields)
        .all(|(f1, f2)| f1.name() == f2.name() && f1.data_type() == f2.data_type())
}

/// Converts columns to a result as expected by sqllogicteset.
pub fn convert_schema_to_types(columns: &Fields) -> Vec<CnosdbColumnType> {
    columns
        .iter()
        .map(|f| f.data_type())
        .map(|data_type| match data_type {
            DataType::Boolean => CnosdbColumnType::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => CnosdbColumnType::Integer,
            DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => CnosdbColumnType::Float,
            DataType::Utf8 | DataType::LargeUtf8 => CnosdbColumnType::Text,
            DataType::Timestamp(_, _) => CnosdbColumnType::Timestamp,
            _ => CnosdbColumnType::Another,
        })
        .collect()
}
