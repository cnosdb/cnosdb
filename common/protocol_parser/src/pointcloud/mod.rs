use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use models::codec::Encoding::Default;
use models::schema::{TableColumn, TskvTableSchema};

use self::parser::{Error, FileFormat, PointCloud, Result};
use crate::pointcloud::parser::ColumnData;

mod off;
pub mod parser;
mod ply;
mod stl;

pub fn req_to_pointcloud(file: &str, format: FileFormat) -> Result<PointCloud> {
    let mut datapoint = match format {
        FileFormat::PLY => parse_ply(file)?,
        FileFormat::STL => parse_stl(file)?,
        FileFormat::OFF => parse_off(file)?,
        FileFormat::UNKNOWN => return Err(Error::ErrorFormat),
    };

    datapoint.finish();
    Ok(datapoint)
}

pub fn parse_ply(file: &str) -> Result<PointCloud> {
    let mut c = Cursor::new(file);
    ply::parse(&mut c).map_err(|e| e.into())
}

pub fn parse_off(file: &str) -> Result<PointCloud> {
    off::parse(file).map_err(|e| e.into())
}
pub fn parse_stl(file: &str) -> Result<PointCloud> {
    let mut c = Cursor::new(file);
    stl::parse(&mut c).map_err(|e| e.into())
}

// this fun will generate vertex and face Recordbatch
pub fn trans_pc_to_rc(
    data: PointCloud,
    tenant: &str,
    database: &str,
    table_name: &str,
) -> Result<(TskvTableSchema, RecordBatch, TskvTableSchema, RecordBatch)> {
    // write vertex to database
    let mut vertex_schema_field = Vec::new();
    let mut columns = Vec::new();
    let mut col_idx = 0;

    for (col_name, column) in data.vertex_element {
        vertex_schema_field.push(TableColumn::new(
            col_idx,
            col_name,
            column.col_type,
            Default,
        ));
        match column.data {
            ColumnData::I64(col) => columns.push(Arc::new(Int64Array::from(col)) as ArrayRef),
            ColumnData::F64(col) => columns.push(Arc::new(Float64Array::from(col)) as ArrayRef),
            _ => (),
        }
        col_idx += 1;
    }

    assert!(!vertex_schema_field.is_empty());
    let vertex_schema = TskvTableSchema::new(
        tenant.to_string(),
        database.to_string(),
        format!("{}_vertex", table_name),
        vertex_schema_field,
    );
    let vertex_record = RecordBatch::try_new(vertex_schema.to_arrow_schema(), columns).unwrap();

    // write face to database
    let mut face_schema_field = Vec::new();
    let mut columns = Vec::new();
    col_idx = 0;

    for (col_name, column) in data.face_element {
        face_schema_field.push(TableColumn::new(
            col_idx,
            col_name,
            column.col_type,
            Default,
        ));
        match column.data {
            ColumnData::I64(col) => columns.push(Arc::new(Int64Array::from(col)) as ArrayRef),
            ColumnData::U64(col) => columns.push(Arc::new(UInt64Array::from(col)) as ArrayRef),
            ColumnData::F64(col) => columns.push(Arc::new(Float64Array::from(col)) as ArrayRef),
        }
        col_idx += 1;
    }

    assert!(!face_schema_field.is_empty());
    let face_schema = TskvTableSchema::new(
        tenant.to_string(),
        database.to_string(),
        format!("{}_face", table_name),
        face_schema_field,
    );
    let face_record = RecordBatch::try_new(face_schema.to_arrow_schema(), columns).unwrap();

    Ok((vertex_schema, vertex_record, face_schema, face_record))
}
