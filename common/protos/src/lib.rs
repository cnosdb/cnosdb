mod generated;
pub use generated::*;
use utils::bitset::ImmutBitSet;
pub mod models_helper;
pub mod prompb;
pub mod test_helper;

use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, WIPOffset};
use generated::models::Point;
use snafu::Snafu;
use utils::{bitset::BitSet, BkdrHasher};

use crate::models::{
    Field, FieldBuilder, FieldType, Points, Schema, SchemaBuilder, Table, Tag, TagBuilder,
};

type PointsResult<T> = Result<T, PointsError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum PointsError {
    #[snafu(display("{}", msg))]
    Points { msg: String },

    #[snafu(display("Flatbuffers 'Points' missing database name (db)"))]
    PointsMissingDatabaseName,

    #[snafu(display("Flatbuffers 'Points' missing tables data (tables)"))]
    PointsMissingTables,

    #[snafu(display("Flatbuffers 'Table' missing table name (tab)"))]
    TableMissingName,

    #[snafu(display("Flatbuffers 'Table' missing schema"))]
    TableMissingSchema,

    #[snafu(display("Flatbuffers 'Table' missing points data (points)"))]
    TableMissingPoints,

    #[snafu(display("Flatbuffers 'Point' missing tags data (tags)"))]
    PointMissingTags,

    #[snafu(display("Flatbuffers 'Point' missing tags bitset (tags_nullbit)"))]
    PointMissingTagsNullbit,

    #[snafu(display("Flatbuffers 'Tag' missing value"))]
    TagMissingValue,
}

#[derive(Debug, PartialEq, Clone)]
pub enum FieldValue {
    U64(u64),
    I64(i64),
    Str(Vec<u8>),
    F64(f64),
    Bool(bool),
}

impl<'a> Schema<'a> {
    pub fn tag_name_ext(&'a self) -> Vec<&'a str> {
        self.tag_name()
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    pub fn field_name_ext(&'a self) -> Vec<&'a str> {
        self.field_name()
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    pub fn field_type_ext(&self) -> Vec<FieldType> {
        self.field_type()
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }
}

impl<'a> Points<'a> {
    pub fn db_ext(&'a self) -> PointsResult<&'a str> {
        unsafe {
            Ok(std::str::from_utf8_unchecked(
                self.db()
                    .ok_or(PointsError::PointsMissingDatabaseName)?
                    .bytes(),
            ))
        }
    }

    pub fn tables_iter_ext(&'a self) -> PointsResult<impl Iterator<Item = Table<'a>>> {
        Ok(self
            .tables()
            .ok_or(PointsError::PointsMissingTables)?
            .iter())
    }
}

impl Point<'_> {
    pub fn hash_id_ext(&self, tab: &str, schema: &Schema) -> PointsResult<u64> {
        let mut hasher = BkdrHasher::new();
        hasher.hash_with(tab.as_bytes());
        if let Some(tag_name) = schema.tag_name() {
            let bitset_bytes = self
                .tags_nullbit()
                .ok_or(PointsError::PointMissingTagsNullbit)?
                .bytes();
            let tags = self.tags().ok_or(PointsError::PointMissingTags)?;
            let bitset = ImmutBitSet::new_without_check(tag_name.len(), bitset_bytes);
            for (i, (k, v)) in tag_name.iter().zip(tags).enumerate() {
                if !bitset.get(i) {
                    continue;
                }
                hasher.hash_with(k.as_bytes());
                hasher.hash_with(v.value().ok_or(PointsError::TagMissingValue)?.bytes());
            }
        }

        Ok(hasher.number())
    }
}

impl<'a> Display for Points<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "==============================")?;
        writeln!(f, "Database: {}", self.db_ext().unwrap_or("{!BAD_DB_NAME}"))?;
        writeln!(f, "------------------------------")?;
        match self.tables_iter_ext() {
            Ok(tables) => {
                for table in tables {
                    write!(
                        f,
                        "Table: {}",
                        table.tab_ext().unwrap_or("{!BAD_TABLE_NAME}")
                    )?;
                    writeln!(f, "{}", table)?;
                    writeln!(f, "------------------------------")?;
                }
            }
            Err(_) => {
                writeln!(f, "No tables")?;
            }
        }

        Ok(())
    }
}

impl<'a> Table<'a> {
    pub fn tab_ext(&'a self) -> PointsResult<&'a str> {
        unsafe {
            Ok(std::str::from_utf8_unchecked(
                self.tab().ok_or(PointsError::TableMissingName)?.bytes(),
            ))
        }
    }

    pub fn schema_ext(&'a self) -> PointsResult<Schema<'a>> {
        self.schema().ok_or(PointsError::TableMissingSchema)
    }

    pub fn points_iter_ext(&'a self) -> PointsResult<impl Iterator<Item = Point<'a>>> {
        Ok(self.points().ok_or(PointsError::TableMissingPoints)?.iter())
    }
}

impl<'a> Display for Table<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let schema = match self.schema_ext() {
            Ok(s) => s,
            Err(_) => {
                writeln!(f, "{{!BAD_TABLE_SCHEMA}}")?;
                return Ok(());
            }
        };
        let tag_names = schema.tag_name_ext();
        let field_keys = schema.field_name_ext();
        let field_types = schema.field_type_ext();
        let points = match self.points_iter_ext() {
            Ok(p) => p,
            Err(_) => {
                writeln!(f, "{{!BAD_TABLE_POINTS}}")?;
                return Ok(());
            }
        };
        for point in points {
            write!(f, "\nTimestamp: {}", point.timestamp())?;

            // Do not use point.tags_iter_ext() in order to print more information.
            if let Some(tags) = point.tags() {
                // Tags[${tags_len}]
                // { ${tag_name}, ${tag_val} }, { ... }, ...
                write!(f, "\nTags[{}]: ", tags.len())?;
                if let Some(tags_bitset_bytes) = point.tags_nullbit() {
                    let tags_bitset =
                        ImmutBitSet::new_without_check(tag_names.len(), tags_bitset_bytes.bytes());
                    let (mut tags_i, tags_last_i) = (0_usize, tags.len());
                    let mut tags_iter = tags.iter();
                    for (i, tag_name) in tag_names.iter().enumerate() {
                        if !tags_bitset.get(i) {
                            continue;
                        }
                        write!(f, "{{ '{tag_name}': ")?;
                        if let Some(tag) = tags_iter.next() {
                            tags_i += 1;
                            if let Some(val) = tag.value() {
                                let tag_val = unsafe { std::str::from_utf8_unchecked(val.bytes()) };
                                write!(f, "'{tag_val}' }}")?;
                            } else {
                                write!(f, "{{!EMPTY_TAG_VAL}} }}")?;
                            }
                        } else {
                            write!(f, "{{!BAD_TAG_VAL}} }}")?;
                        }
                        if tags_i < tags_last_i {
                            write!(f, ", ")?;
                        }
                    }
                } else {
                    write!(f, "{{!BAD_TAGS_NO_BITSET}}")?;
                }
            } else {
                writeln!(f, "Tags[0]")?;
            }

            // Do not use point.fields_iter_ext() in order to print more information.
            if let Some(fields) = point.fields() {
                // Fields[${fields_len}]
                // { ${field_key}, ${field_val}, ${field_type} }, { ... }, ...
                //
                write!(f, "\nFields[{}]: ", fields.len())?;

                if let Some(fields_bitset_bytes) = point.fields_nullbit() {
                    let fields_bitset = ImmutBitSet::new_without_check(
                        field_keys.len(),
                        fields_bitset_bytes.bytes(),
                    );
                    let (mut fields_i, fields_last_i) = (0_usize, fields.len());
                    let mut fields_iter = fields.iter();
                    for (i, field_key) in field_keys.iter().enumerate() {
                        if !fields_bitset.get(i) {
                            continue;
                        }
                        write!(f, "{{ {}: ", field_key)?;
                        if let Some(field) = fields_iter.next() {
                            fields_i += 1;

                            if let Some(val) = field.value() {
                                let val_bytes = val.bytes();
                                let field_type = field_types[i];
                                match field_type {
                                    FieldType::Integer => {
                                        if val_bytes.len() >= 8 {
                                            let val = unsafe {
                                                i64::from_be_bytes(
                                                    *(val_bytes.as_ptr() as *const [u8; 8]),
                                                )
                                            };
                                            write!(f, "{}, ", val)?;
                                        } else {
                                            write!(f, "{{!BAD_INTEGER_VALUE}}, ")?;
                                        }
                                    }
                                    FieldType::Unsigned => {
                                        if val_bytes.len() >= 8 {
                                            let val = unsafe {
                                                u64::from_be_bytes(
                                                    *(val_bytes.as_ptr() as *const [u8; 8]),
                                                )
                                            };
                                            write!(f, "{}, ", val)?;
                                        } else {
                                            write!(f, "{{!BAD_UNSIGNED_VALUE}}, ")?;
                                        }
                                    }
                                    FieldType::Float => {
                                        if val_bytes.len() >= 8 {
                                            let val = unsafe {
                                                f64::from_be_bytes(
                                                    *(val_bytes.as_ptr() as *const [u8; 8]),
                                                )
                                            };
                                            write!(f, "{}, ", val)?;
                                        } else {
                                            write!(f, "{{!BAD_FLOAT_VALUE}}")?;
                                        }
                                    }
                                    FieldType::Boolean => {
                                        if val_bytes.is_empty() {
                                            write!(f, "{{!BAD_BOOLEAN_VALUE}}")?;
                                        } else if val_bytes[0] == 1 {
                                            write!(f, "true, ")?;
                                        } else {
                                            write!(f, "false, ")?;
                                        }
                                    }
                                    FieldType::String => match std::str::from_utf8(val_bytes) {
                                        Ok(s) => write!(f, "'{s}', ")?,
                                        Err(e) => write!(f, "{{!BAD_STRING_VALUE: {e}}}")?,
                                    },
                                    _ => {
                                        write!(f, "{{!UNKNOWN_FIELD_TYPE}}, ")?;
                                    }
                                }
                                write!(
                                    f,
                                    "{} }}",
                                    field_type.variant_name().unwrap_or("{!BAD_FIELD_TYPE}")
                                )?;
                            } // End if let Some(val) = field.value()
                        } else {
                            write!(f, "{{!BAD_FIELD_VAL}}")?;
                        } // End if let Some(field) = fields_iter.next()
                        if fields_i < fields_last_i {
                            write!(f, ", ")?;
                        }
                    } // End loop field_keys.iter().enumerate()
                } else {
                    write!(f, "{{!BAD_FIELDS_NO_BITSET}}")?;
                }
            } else {
                writeln!(f, "Fields[0]")?;
            }
        }

        Ok(())
    }
}

impl<'a> Point<'a> {
    pub fn tags_iter_ext(&'a self, schema: &'a Schema<'a>) -> Option<PointTagsIterator<'a>> {
        let names_bytes = schema.tag_name();
        let bitset_bytes = self.tags_nullbit();
        let values_bytes = self.tags();
        if names_bytes.is_none() || bitset_bytes.is_none() || values_bytes.is_none() {
            return None;
        }

        let tag_names_iter = unsafe { names_bytes.unwrap_unchecked().iter() };
        let bitset = ImmutBitSet::new_without_check(tag_names_iter.len(), unsafe {
            bitset_bytes.unwrap_unchecked().bytes()
        });
        let tag_values_iter = unsafe { values_bytes.unwrap_unchecked().iter() };

        Some(PointTagsIterator {
            bitset,
            tag_names_iter,
            tag_values_iter,
            i: 0,
        })
    }

    pub fn fields_iter_ext(&'a self, schema: &'a Schema<'a>) -> Option<PointFieldsIterator<'a>> {
        let names_bytes = schema.field_name();
        let bitset_bytes = self.fields_nullbit();
        let types_bytes = schema.field_type();
        let values_bytes = self.fields();
        if names_bytes.is_none()
            || bitset_bytes.is_none()
            || types_bytes.is_none()
            || values_bytes.is_none()
        {
            return None;
        }

        let field_names_iter = unsafe { names_bytes.unwrap_unchecked().iter() };
        let bitset = ImmutBitSet::new_without_check(field_names_iter.len(), unsafe {
            bitset_bytes.unwrap_unchecked().bytes()
        });
        let field_types_iter = unsafe { types_bytes.unwrap_unchecked().iter() };
        let field_value_iter = unsafe { values_bytes.unwrap_unchecked().iter() };
        Some(PointFieldsIterator {
            bitset,
            field_names_iter,
            field_types_iter,
            field_values_iter: field_value_iter,
            i: 0,
        })
    }
}

pub struct PointTagsIterator<'a> {
    bitset: ImmutBitSet<'a>,
    tag_names_iter: flatbuffers::VectorIter<'a, ForwardsUOffset<&'a str>>,
    tag_values_iter: flatbuffers::VectorIter<'a, ForwardsUOffset<Tag<'a>>>,
    i: usize,
}

impl<'a> Iterator for PointTagsIterator<'a> {
    type Item = (&'a str, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.bitset.len() {
            return None;
        }
        while self.i < self.bitset.len() && !self.bitset.get(self.i) {
            let _ = self.tag_names_iter.next();
            self.i += 1;
        }
        if self.i >= self.bitset.len() {
            return None;
        }

        let tag_name = match self.tag_names_iter.next() {
            Some(k) => k,
            None => return None,
        };
        let tag_value = match self.tag_values_iter.next() {
            Some(t) => match t.value().map(|v| v.bytes()) {
                Some(b) => b,
                None => return None,
            },
            None => return None,
        };

        self.i += 1;
        Some((tag_name, tag_value))
    }
}

pub struct PointFieldsIterator<'a> {
    bitset: ImmutBitSet<'a>,
    field_names_iter: flatbuffers::VectorIter<'a, ForwardsUOffset<&'a str>>,
    field_types_iter: flatbuffers::VectorIter<'a, FieldType>,
    field_values_iter: flatbuffers::VectorIter<'a, ForwardsUOffset<Field<'a>>>,
    i: usize,
}

impl<'a> Iterator for PointFieldsIterator<'a> {
    type Item = (&'a str, FieldType, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.bitset.len() {
            return None;
        }
        while self.i < self.bitset.len() && !self.bitset.get(self.i) {
            let _ = self.field_names_iter.next();
            let _ = self.field_types_iter.next();
            self.i += 1;
        }
        if self.i >= self.bitset.len() {
            return None;
        }

        let field_name = match self.field_names_iter.next() {
            Some(n) => n,
            None => return None,
        };
        let field_type = match self.field_types_iter.next() {
            Some(t) => t,
            None => return None,
        };
        let field_value = match self.field_values_iter.next() {
            Some(f) => match f.value().map(|v| v.bytes()) {
                Some(b) => b,
                None => return None,
            },
            None => return None,
        };

        self.i += 1;
        Some((field_name, field_type, field_value))
    }
}

// TODO(zipper): `FbSchema` is not declared in fbs, may we rename it as `SchemaExt`.
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

    pub fn add_field(&mut self, field_key: &'a str, field_type: FieldType) {
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

pub fn init_fields<'fbb: 'mut_fbb, 'mut_fbb>(
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

pub fn init_tags_and_nullbits<'a, 'fbb: 'mut_fbb, 'mut_fbb>(
    fbb: &'mut_fbb mut FlatBufferBuilder<'fbb>,
    schema: &'a FbSchema<'a>,
) -> (Vec<WIPOffset<Tag<'fbb>>>, BitSet) {
    let mut tags = Vec::with_capacity(schema.tag_names().len());
    init_tags(fbb, &mut tags, schema.tag_names().len());

    let tags_nullbit = BitSet::with_size(schema.tag_names().len());
    (tags, tags_nullbit)
}

pub fn init_fields_and_nullbits<'a, 'fbb: 'mut_fbb, 'mut_fbb>(
    fbb: &'mut_fbb mut FlatBufferBuilder<'fbb>,
    schema: &'a FbSchema,
) -> (Vec<WIPOffset<Field<'fbb>>>, BitSet) {
    let mut fields = Vec::with_capacity(schema.field().len());
    init_fields(fbb, &mut fields, schema.field().len());

    let fields_nullbits = BitSet::with_size(schema.field().len());
    (fields, fields_nullbits)
}

pub fn insert_tag<'a, 'fbb: 'mut_fbb, 'mut_fbb>(
    fbb: &'mut_fbb mut FlatBufferBuilder<'fbb>,
    tags: &mut [WIPOffset<Tag<'fbb>>],
    tag_nullbits: &'a mut BitSet,
    schema: &'a FbSchema,
    tag_key: &str,
    tag_value: &str,
) {
    let tag_index = match schema.tag_names().get(tag_key) {
        None => return,
        Some(v) => *v,
    };

    let tag_value = fbb.create_vector(tag_value.as_bytes());

    let mut tag_builder = TagBuilder::new(fbb);
    tag_builder.add_value(tag_value);
    tags[tag_index] = tag_builder.finish();

    tag_nullbits.set(tag_index);
}

pub fn insert_field<'a, 'fbb: 'mut_fbb, 'mut_fbb>(
    fbb: &'mut_fbb mut FlatBufferBuilder<'fbb>,
    fields: &mut [WIPOffset<Field<'fbb>>],
    field_nullbits: &'a mut BitSet,
    schema: &'a FbSchema,
    field_key: &str,
    field_value: &FieldValue,
) {
    let field_index = match schema.field().get(field_key) {
        None => return,
        Some(v) => *v,
    };

    let field_value = match field_value {
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
    field_builder.add_value(field_value);
    fields[field_index] = field_builder.finish();

    field_nullbits.set(field_index);
}

#[cfg(test)]
pub mod test {
    use utils::bitset::BitSet;

    use crate::{
        models::{FieldType, Points},
        test_helper::{PointsDescription, TableDescription},
    };

    #[test]
    fn test_fb_models_ext_methods() {
        let points = PointsDescription::default();
        let points_bytes = points.as_fb_bytes();

        let fb_points = flatbuffers::root::<Points>(&points_bytes).unwrap();
        assert_eq!(points.database.as_str(), fb_points.db_ext().unwrap());

        assert!(fb_points.tables().is_some());
        assert_eq!(points.table_descs.len(), fb_points.tables().unwrap().len());
        for (table, fb_table) in points
            .table_descs
            .iter()
            .zip(fb_points.tables_iter_ext().unwrap())
        {
            assert_eq!(table.table.as_str(), fb_table.tab_ext().unwrap());
            let TableDescription {
                table: _,
                schema_tags: pt_sch_tags,
                schema_fields: pt_sch_fields,
                schema_field_types: pt_sch_field_types,
                point_timestamps: pt_ts_desc,
                point_tags: pt_tag_desc,
                point_tag_bitsets: pt_tag_bitsets,
                point_fields: pt_field_desc,
                point_field_bitsets: pt_field_bitsets,
            } = table;

            assert!(fb_table.schema().is_some());
            let fb_schema = fb_table.schema().unwrap();
            assert_eq!(pt_sch_tags.len(), fb_schema.tag_name_ext().len());
            assert_eq!(pt_sch_fields.len(), fb_schema.field_name_ext().len());
            assert_eq!(pt_sch_field_types.len(), fb_schema.field_type_ext().len());
            for (pt_tag_name, fb_tag_name) in
                pt_sch_tags.iter().zip(fb_schema.tag_name_ext().iter())
            {
                assert_eq!(pt_tag_name.as_str(), *fb_tag_name);
            }
            for (pt_field_name, fb_field_name) in
                pt_sch_fields.iter().zip(fb_schema.field_name_ext().iter())
            {
                assert_eq!(pt_field_name.as_str(), *fb_field_name);
            }
            for (pt_field_type, fb_field_type) in pt_sch_field_types
                .iter()
                .zip(fb_schema.field_type_ext().iter())
            {
                assert_eq!(*pt_field_type, *fb_field_type);
            }

            assert!(fb_table.points().is_some());
            for (i, fb_point) in fb_table.points_iter_ext().unwrap().enumerate() {
                let (pt_ts, pt_tags, pt_fields) =
                    (pt_ts_desc[i], &pt_tag_desc[i], &pt_field_desc[i]);
                let (pt_tags_bitset, pt_fields_bitset) = (&pt_tag_bitsets[i], &pt_field_bitsets[i]);

                assert_eq!(fb_point.timestamp(), pt_ts);

                let fb_tags = fb_point.tags().unwrap();
                assert_eq!(fb_tags.len(), pt_tags.len());
                let fb_tag_nullbit = fb_point.tags_nullbit().unwrap().bytes().to_vec();
                let tags_bitset = BitSet::new_without_check(pt_sch_tags.len(), fb_tag_nullbit);
                assert_eq!(&tags_bitset, pt_tags_bitset);

                let fb_fields = fb_point.fields().unwrap();
                assert_eq!(fb_fields.len(), pt_fields.len());
                let fb_field_nullbit = fb_point.fields_nullbit().unwrap().bytes().to_vec();
                let fields_bitset =
                    BitSet::new_without_check(pt_sch_fields.len(), fb_field_nullbit);
                assert_eq!(&fields_bitset, pt_fields_bitset);

                if let Some(fields_iter) = fb_point.fields_iter_ext(&fb_schema) {
                    for (
                        (fb_field_name, fb_field_type, fb_field_value),
                        (pt_field_name, pt_field_type, pt_field_value),
                    ) in fields_iter.zip(pt_fields.iter())
                    {
                        assert_eq!(fb_field_name, pt_field_name);
                        assert_eq!(
                            &fb_field_type, pt_field_type,
                            "field type not match: {fb_field_name} - {pt_field_name}"
                        );
                        match fb_field_type {
                            FieldType::Float => {
                                let val_fb = unsafe {
                                    f64::from_be_bytes(*(fb_field_value.as_ptr() as *const [u8; 8]))
                                };
                                let val_pt = unsafe {
                                    f64::from_be_bytes(
                                        *(pt_field_value.as_slice().as_ptr() as *const [u8; 8]),
                                    )
                                };
                                assert_eq!(
                                    val_fb, val_pt,
                                    "field value not match: {fb_field_name} - {pt_field_name}"
                                );
                            }
                            FieldType::Integer => {
                                let val_fb = unsafe {
                                    i64::from_be_bytes(*(fb_field_value.as_ptr() as *const [u8; 8]))
                                };
                                let val_pt = unsafe {
                                    i64::from_be_bytes(
                                        *(pt_field_value.as_slice().as_ptr() as *const [u8; 8]),
                                    )
                                };
                                assert_eq!(
                                    val_fb, val_pt,
                                    "field value not match: {fb_field_name} - {pt_field_name}"
                                );
                            }
                            FieldType::Unsigned => {
                                let val_fb = unsafe {
                                    u64::from_be_bytes(*(fb_field_value.as_ptr() as *const [u8; 8]))
                                };
                                let val_pt = unsafe {
                                    u64::from_be_bytes(
                                        *(pt_field_value.as_slice().as_ptr() as *const [u8; 8]),
                                    )
                                };
                                assert_eq!(
                                    val_fb, val_pt,
                                    "field value not match: {fb_field_name} - {pt_field_name}"
                                );
                            }
                            FieldType::Boolean => {
                                let val_fb = fb_field_value[0] == 1;
                                let val_pt = pt_field_value[0] == 1;
                                assert_eq!(
                                    val_fb, val_pt,
                                    "field value not match: {fb_field_name} - {pt_field_name}"
                                );
                            }
                            FieldType::String => {
                                let val_fb = std::str::from_utf8(fb_field_value).unwrap();
                                let val_pt = std::str::from_utf8(pt_field_value).unwrap();
                                assert_eq!(
                                    val_fb, val_pt,
                                    "field value not match: {fb_field_name} - {pt_field_name}"
                                );
                            }
                            _ => {
                                panic!("Unknown field type: {fb_field_name} - {pt_field_name}");
                            }
                        }
                    }
                }
                let fb_field_nullbit = fb_point.fields_nullbit().unwrap().bytes().to_vec();
                let field_nullbit =
                    BitSet::new_without_check(pt_sch_fields.len(), fb_field_nullbit);
                assert_eq!(&field_nullbit, pt_fields_bitset);
            }
        }
    }

    #[test]
    #[ignore = "Checked by human"]
    fn test_format_fb_model_points() {
        let points = PointsDescription::default();
        let points_bytes = points.as_fb_bytes();
        let fb_points = flatbuffers::root::<Points>(&points_bytes).unwrap();
        let fb_points_str = format!("{fb_points}");
        // println!("{fb_points}");
        assert_eq!(
            &fb_points_str,
            r#"==============================
Database: test_database
------------------------------
Table: test_table_1
Timestamp: 1009
Tags[3]: { 'ta': 'high' }, { 'tb': 'user1.host1' }, { 'tc': 'A' }
Fields[5]: { f1: -2, Float }, { f2: 2, Integer }, { f3: 3, Unsigned }, { f4: false, Boolean }, { f5: 'dd', String }
Timestamp: 1013
Tags[1]: { 'tb': 'user2.host2' }
Fields[4]: { f1: 5, Float }, { f2: 7, Integer }, { f3: 11, Unsigned }, { f5: 'alpha', String }
Timestamp: 1019
Tags[2]: { 'ta': 'low' }, { 'tc': 'B' }
Fields[4]: { f1: 13, Float }, { f2: 17, Integer }, { f3: 19, Unsigned }, { f4: true, Boolean }
Timestamp: 1021
Tags[1]: { 'tc': 'A' }
Fields[3]: { f1: 23, Float }, { f2: 29, Integer }, { f4: false, Boolean }
Timestamp: 1031
Tags[2]: { 'tb': 'user1.host2' }, { 'tc': 'C' }
Fields[4]: { f2: 31, Integer }, { f3: 37, Unsigned }, { f4: true, Boolean }, { f5: 'hello?', String }
------------------------------
Table: test_table_2
Timestamp: 2003
Tags[3]: { 'ta': 'high' }, { 'tb': 'user1.host1' }, { 'tc': 'A' }
Fields[5]: { f1: -2, Float }, { f2: 2, Integer }, { f3: 3, Unsigned }, { f4: false, Boolean }, { f5: 'dd', String }
Timestamp: 2011
Tags[1]: { 'tb': 'user2.host2' }
Fields[4]: { f1: 5, Float }, { f2: 7, Integer }, { f3: 11, Unsigned }, { f5: 'alpha', String }
Timestamp: 2017
Tags[2]: { 'ta': 'low' }, { 'tc': 'B' }
Fields[4]: { f1: 13, Float }, { f2: 17, Integer }, { f3: 19, Unsigned }, { f4: true, Boolean }
Timestamp: 2027
Tags[1]: { 'tc': 'A' }
Fields[3]: { f1: 23, Float }, { f2: 29, Integer }, { f4: false, Boolean }
Timestamp: 2029
Tags[2]: { 'tb': 'user1.host2' }, { 'tc': 'C' }
Fields[4]: { f2: 31, Integer }, { f3: 37, Unsigned }, { f4: true, Boolean }, { f5: 'hello?', String }
------------------------------
"#
        );
    }
}
