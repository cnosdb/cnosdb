mod generated;
pub mod prompb;
use crate::models::{
    Field, FieldBuilder, FieldType, Points, Schema, SchemaBuilder, Table, Tag, TagBuilder,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
pub use generated::*;
use snafu::Snafu;
use std::collections::HashMap;

pub mod models_helper;

type PointsResult<T> = Result<T, PointsError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum PointsError {
    #[snafu(display("{}", msg))]
    Points { msg: String },
}

pub fn fb_table_name(table: &Table) -> PointsResult<String> {
    unsafe {
        Ok(String::from_utf8_unchecked(
            table
                .tab()
                .ok_or(PointsError::Points {
                    msg: "table missing table name in point".to_string(),
                })?
                .bytes()
                .into(),
        ))
    }
}

pub fn schema_tag_name<'a>(schema: &'a Schema) -> PointsResult<Vec<&'a str>> {
    Ok(schema
        .tag_name()
        .unwrap_or_default()
        .iter()
        .collect::<Vec<_>>())
}

pub fn schema_field_name<'a>(schema: &'a Schema) -> PointsResult<Vec<&'a str>> {
    Ok(schema
        .field_name()
        .unwrap_or_default()
        .iter()
        .collect::<Vec<_>>())
}

pub fn schema_field_type(schema: &Schema) -> PointsResult<Vec<FieldType>> {
    Ok(schema
        .field_type()
        .unwrap_or_default()
        .iter()
        .collect::<Vec<_>>())
}

pub fn get_db_from_fb_points(fb_points: &Points) -> PointsResult<String> {
    unsafe {
        Ok(String::from_utf8_unchecked(
            fb_points
                .db()
                .ok_or(PointsError::Points {
                    msg: "write point miss database name".to_string(),
                })?
                .bytes()
                .into(),
        ))
    }
}

#[derive(Default)]
pub struct FbSchema<'a> {
    tag_name: HashMap<&'a str, usize>,
    field: HashMap<&'a str, usize>,
    field_type: Vec<FieldType>,
}

impl<'a> FbSchema<'a> {
    pub fn new(
        tag_name: HashMap<&'a str, usize>,
        field: HashMap<&'a str, usize>,
        field_type: Vec<FieldType>,
    ) -> Self {
        Self {
            tag_name,
            field,
            field_type,
        }
    }

    pub fn add_tag(&mut self, tag_key: &'a str) {
        let len = self.tag_name.len();
        self.tag_name.entry(tag_key).or_insert(len);
    }

    pub fn add_filed(&mut self, field_key: &'a str, field_type: FieldType) {
        self.field.entry(field_key).or_insert_with(|| {
            self.field_type.push(field_type);
            self.field_type.len() - 1
        });
    }

    pub fn tag_len(&self) -> usize {
        self.tag_name.len()
    }

    pub fn field_len(&self) -> usize {
        self.field.len()
    }

    pub fn tag_names(&self) -> &HashMap<&str, usize> {
        &self.tag_name
    }

    pub fn field(&self) -> &HashMap<&str, usize> {
        &self.field
    }

    pub fn field_type(&self) -> &[FieldType] {
        &self.field_type
    }
}

pub fn init_tags<'fbb: 'mut_fbb, 'mut_fbb>(
    fbb: &'mut_fbb mut FlatBufferBuilder<'fbb>,
    tags: &mut Vec<WIPOffset<Tag<'fbb>>>,
    len: usize,
) {
    for _ in 0..len {
        let fbv = fbb.create_vector("".as_bytes());
        let mut tag_builder = TagBuilder::new(fbb);
        tag_builder.add_value(fbv);

        tags.push(tag_builder.finish());
    }
}

pub fn init_field<'fbb: 'mut_fbb, 'mut_fbb>(
    fbb: &'mut_fbb mut FlatBufferBuilder<'fbb>,
    fields: &mut Vec<WIPOffset<Field<'fbb>>>,
    len: usize,
) {
    for _ in 0..len {
        let fbv = fbb.create_vector("".as_bytes());
        let mut field_builder = FieldBuilder::new(fbb);
        field_builder.add_value(fbv);

        fields.push(field_builder.finish());
    }
}

pub fn build_fb_schema_offset<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    schema: &FbSchema,
) -> WIPOffset<Schema<'fbb>> {
    let mut tags_name = schema.tag_names().iter().collect::<Vec<_>>();
    tags_name.sort_by(|a, b| a.1.cmp(b.1));
    let tags_name = tags_name
        .iter()
        .map(|item| fbb.create_string(item.0))
        .collect::<Vec<_>>();

    let mut fields_name = schema.field().iter().collect::<Vec<_>>();
    fields_name.sort_by(|a, b| a.1.cmp(b.1));
    let fields_name = fields_name
        .iter()
        .map(|item| fbb.create_string(item.0))
        .collect::<Vec<_>>();

    let tags_name = fbb.create_vector(&tags_name);
    let fields_name = fbb.create_vector(&fields_name);
    let field_type = fbb.create_vector(schema.field_type());

    let mut schema_builder = SchemaBuilder::new(fbb);
    schema_builder.add_tag_name(tags_name);
    schema_builder.add_field_name(fields_name);
    schema_builder.add_field_type(field_type);

    schema_builder.finish()
}
