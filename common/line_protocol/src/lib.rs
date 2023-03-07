use std::collections::HashMap;

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use models::schema::FieldValue;
// use mut_batch::mutable_batch::MutableBatch;
use snafu::Snafu;

mod parser;
pub use parser::{Line, Parser};
use protos::models::{
    FieldBuilder, FieldType, Point, PointArgs, Points, PointsArgs, TableBuilder, TagBuilder,
};
use protos::{build_fb_schema_offset, init_field, init_tags, FbSchema};
use utils::bitset::BitSet;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Error: pos: {}, in: '{}'", pos, content))]
    Parse { pos: usize, content: String },
}

pub fn line_protocol_to_lines(lines: &str, default_time: i64) -> Result<Vec<Line>> {
    let parser = Parser::new(default_time);
    parser.parse(lines)
}

pub fn line_to_point<'a, 'fbb: 'mut_fbb, 'mut_fbb>(
    line: &'a Line,
    schema: &FbSchema,
    fbb: &'mut_fbb mut FlatBufferBuilder<'fbb>,
) -> WIPOffset<Point<'fbb>> {
    let mut tags = Vec::with_capacity(schema.tag_names().len());
    init_tags(fbb, &mut tags, schema.tag_names().len());

    let mut tags_nullbit = BitSet::with_size(schema.tag_names().len());

    for (k, v) in line.tags.iter() {
        let fbv = fbb.create_vector(v.as_bytes());

        let idx = match schema.tag_names().get(k) {
            None => continue,
            Some(v) => *v,
        };

        let mut tag_builder = TagBuilder::new(fbb);
        tag_builder.add_value(fbv);

        tags[idx] = tag_builder.finish();
        tags_nullbit.set(idx);
    }

    let mut fields = Vec::with_capacity(schema.field().len());
    init_field(fbb, &mut fields, schema.field().len());

    let mut fields_nullbits = BitSet::with_size(schema.field().len());

    for (k, v) in line.fields.iter() {
        let idx = match schema.field().get(k) {
            None => continue,
            Some(v) => *v,
        };

        let fbv = match v {
            FieldValue::U64(field_val) => fbb.create_vector(&field_val.to_be_bytes()),
            FieldValue::I64(field_val) => fbb.create_vector(&field_val.to_be_bytes()),
            FieldValue::Str(field_val) => fbb.create_vector(field_val),
            FieldValue::F64(field_val) => fbb.create_vector(&field_val.to_be_bytes()),
            FieldValue::Bool(field_val) => {
                if *field_val {
                    fbb.create_vector(&[1_u8][..])
                } else {
                    fbb.create_vector(&[0_u8][..])
                }
            }
        };
        let mut field_builder = FieldBuilder::new(fbb);
        field_builder.add_value(fbv);

        fields[idx] = field_builder.finish();
        fields_nullbits.set(idx)
    }

    let point_args = PointArgs {
        tags: Some(fbb.create_vector(&tags)),
        tags_nullbit: Some(fbb.create_vector(tags_nullbit.bytes())),
        fields: Some(fbb.create_vector(&fields)),
        fields_nullbit: Some(fbb.create_vector(fields_nullbits.bytes())),
        timestamp: line.timestamp,
    };

    Point::create(fbb, &point_args)
}

// Construct table_name -> Schema based on all lines
pub fn build_schema<'a>(lines: &'a [Line]) -> HashMap<&'a str, FbSchema<'a>> {
    let mut schemas = HashMap::new();
    for line in lines {
        let table = line.measurement;
        let schema: &mut FbSchema = schemas.entry(table).or_default();
        for (tag_key, _) in line.tags.iter() {
            schema.add_tag(tag_key);
        }
        for (k, v) in line.fields.iter() {
            match v {
                FieldValue::U64(_) => schema.add_filed(k, FieldType::Unsigned),
                FieldValue::I64(_) => schema.add_filed(k, FieldType::Integer),
                FieldValue::Str(_) => schema.add_filed(k, FieldType::String),
                FieldValue::F64(_) => schema.add_filed(k, FieldType::Float),
                FieldValue::Bool(_) => schema.add_filed(k, FieldType::Boolean),
            };
        }
    }
    schemas
}

// Walk through all the rows, divided by table name， table_name -> line index
pub fn build_table_line_index<'a>(lines: &'a [Line]) -> HashMap<&'a str, Vec<usize>> {
    let mut schemas = HashMap::new();
    for (idx, line) in lines.iter().enumerate() {
        let idxs: &mut Vec<usize> = schemas.entry(line.measurement).or_default();
        idxs.push(idx);
    }
    schemas
}

pub fn parse_lines_to_points<'a>(db: &'a str, lines: &'a [Line]) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    let table_line_index = build_table_line_index(lines);
    let schemas = build_schema(lines);

    let mut table_offsets = Vec::with_capacity(schemas.len());
    for (table_name, idxs) in table_line_index {
        let num_rows = idxs.len();
        let mut points = Vec::with_capacity(idxs.len());
        let schema = schemas.get(table_name).unwrap();
        for idx in idxs {
            points.push(line_to_point(&lines[idx], schema, &mut fbb))
        }
        let fb_schema = build_fb_schema_offset(&mut fbb, schema);

        let point = fbb.create_vector(&points);
        let tab = fbb.create_vector(table_name.as_bytes());

        let mut table_builder = TableBuilder::new(&mut fbb);

        table_builder.add_points(point);
        table_builder.add_schema(fb_schema);
        table_builder.add_tab(tab);
        table_builder.add_num_rows(num_rows as u64);

        table_offsets.push(table_builder.finish());
    }

    let fbb_db = fbb.create_vector(db.as_bytes());
    let points_raw = fbb.create_vector(&table_offsets);
    let points = Points::create(
        &mut fbb,
        &PointsArgs {
            db: Some(fbb_db),
            tables: Some(points_raw),
        },
    );
    fbb.finish(points, None);
    fbb.finished_data().to_vec()
}
