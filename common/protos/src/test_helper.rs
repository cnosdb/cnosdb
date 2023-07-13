use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use utils::bitset::BitSet;

use crate::{
    models::{
        FieldBuilder, FieldType, Point, PointBuilder, Points, PointsBuilder, Schema, SchemaBuilder,
        Table, TableBuilder, TagBuilder,
    },
    PointsError,
};

/// Wraps $e.to_string()
macro_rules! s {
    ($e: expr) => {
        $e.to_string()
    };
}

pub fn add_schema_to_fbb<'fbb: 'a, 'a, T: AsRef<str>>(
    fbb: &'a mut FlatBufferBuilder<'fbb>,
    schema_tags: &[T],
    schema_fields: &[T],
    schema_field_types: &[FieldType],
) -> WIPOffset<Schema<'fbb>> {
    let tag_name = schema_tags
        .iter()
        .map(|s| fbb.create_string(s.as_ref()))
        .collect::<Vec<WIPOffset<&str>>>();
    let tag_name = fbb.create_vector(&tag_name);
    let field_name = schema_fields
        .iter()
        .map(|s| fbb.create_string(s.as_ref()))
        .collect::<Vec<WIPOffset<&str>>>();
    let field_name = fbb.create_vector(&field_name);
    let field_type = fbb.create_vector_from_iter(schema_field_types.iter());
    let mut builder = SchemaBuilder::new(fbb);
    builder.add_tag_name(tag_name);
    builder.add_field_name(field_name);
    builder.add_field_type(field_type);
    builder.finish()
}

pub fn add_point_to_fbb<'fbb: 'a, 'a, T: AsRef<str>>(
    fbb: &'a mut FlatBufferBuilder<'fbb>,
    schema_tags: &[T],
    schema_fields: &[T],
    pt_timestamp: i64,
    pt_tags: &[(T, T)],
    pt_fields: &[(T, FieldType, Vec<u8>)],
) -> WIPOffset<Point<'fbb>> {
    let mut tags = Vec::with_capacity(pt_tags.len());
    let mut tags_nullbit = BitSet::with_size(schema_tags.len());
    for (i, sch_tag) in schema_tags.iter().map(|s| s.as_ref()).enumerate() {
        for (tag_key, tag_val) in pt_tags.iter().map(|(k, v)| (k.as_ref(), v.as_ref())) {
            if sch_tag == tag_key {
                let tag_value = fbb.create_vector(tag_val.as_bytes());
                let mut tag_builder = TagBuilder::new(fbb);
                tag_builder.add_value(tag_value);
                tags.push(tag_builder.finish());
                tags_nullbit.set(i);
                break;
            }
        }
    }
    let tags = fbb.create_vector(&tags);
    let tags_nullbit = fbb.create_vector(tags_nullbit.bytes());

    let mut fields = Vec::with_capacity(pt_fields.len());
    let mut fields_nullbit = BitSet::with_size(schema_fields.len());
    for (i, sch_field) in schema_fields.iter().map(|s| s.as_ref()).enumerate() {
        for (field_key, _, field_val) in pt_fields.iter().map(|(k, t, v)| (k.as_ref(), t, v)) {
            if *sch_field == *field_key {
                let field_value = fbb.create_vector(field_val.as_slice());
                let mut field_builder = FieldBuilder::new(fbb);
                field_builder.add_value(field_value);
                fields.push(field_builder.finish());
                fields_nullbit.set(i);
                break;
            }
        }
    }
    let fields = fbb.create_vector(&fields);
    let fields_nullbit = fbb.create_vector(fields_nullbit.bytes());

    let mut builder = PointBuilder::new(fbb);
    builder.add_timestamp(pt_timestamp);
    builder.add_tags(tags);
    builder.add_tags_nullbit(tags_nullbit);
    builder.add_fields(fields);
    builder.add_fields_nullbit(fields_nullbit);
    builder.finish()
}

pub fn add_table_to_fbb<'fbb: 'a, 'a, T: AsRef<str>>(
    fbb: &'a mut FlatBufferBuilder<'fbb>,
    table: T,
    schema: WIPOffset<Schema<'fbb>>,
    points: WIPOffset<Vector<'fbb, ForwardsUOffset<Point<'fbb>>>>,
    num_rows: u64,
) -> WIPOffset<Table<'fbb>> {
    let tab = fbb.create_vector(table.as_ref().as_bytes());

    let mut builder = TableBuilder::new(fbb);
    builder.add_tab(tab);
    builder.add_schema(schema);
    builder.add_points(points);
    builder.add_num_rows(num_rows);
    builder.finish()
}

pub struct PointsDescription {
    pub database: String,
    pub table_descs: Vec<TableDescription>,
}

impl PointsDescription {
    pub fn new(database: &str) -> Self {
        Self {
            database: s!(database),
            table_descs: vec![],
        }
    }

    pub fn as_fb_bytes(&self) -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();
        let points = self.write_fbb(&mut fbb);
        fbb.finish(points, None);
        fbb.finished_data().to_vec()
    }

    pub fn write_fbb<'fbb: 'a, 'a>(
        &self,
        fbb: &'a mut FlatBufferBuilder<'fbb>,
    ) -> WIPOffset<Points<'fbb>> {
        let mut table_vec = Vec::with_capacity(self.table_descs.len());
        for table_desc in self.table_descs.iter() {
            table_vec.push(table_desc.write_fbb(fbb));
        }
        let table_vec = fbb.create_vector(&table_vec);
        let database = fbb.create_vector(self.database.as_bytes());
        let mut points = PointsBuilder::new(fbb);
        points.add_db(database);
        points.add_tables(table_vec);
        points.finish()
    }
}

impl Default for PointsDescription {
    fn default() -> Self {
        let table_1 = TableDescription {
            table: s!("test_table_1"),
            ..Default::default()
        };
        let table_2 = TableDescription {
            table: s!("test_table_2"),
            point_timestamps: vec![2003, 2011, 2017, 2027, 2029],
            ..Default::default()
        };
        assert_eq!(
            table_2.point_timestamps.len(),
            table_2.point_field_bitsets.len()
        );
        Self {
            database: s!("test_database"),
            table_descs: vec![table_1, table_2],
        }
    }
}

impl<'a> From<Points<'a>> for PointsDescription {
    #[allow(unused)]
    fn from(points: Points<'a>) -> Self {
        todo!()
    }
}

pub struct TableDescription {
    pub table: String,
    pub schema_tags: Vec<String>,
    pub schema_fields: Vec<String>,
    pub schema_field_types: Vec<FieldType>,
    pub point_timestamps: Vec<i64>,
    pub point_tags: Vec<Vec<(String, String)>>,
    pub point_tag_bitsets: Vec<BitSet>,
    pub point_fields: Vec<Vec<(String, FieldType, Vec<u8>)>>,
    pub point_field_bitsets: Vec<BitSet>,
}

impl TableDescription {
    pub fn new(table: &str) -> Self {
        Self {
            table: s!(table),
            ..Default::default()
        }
    }

    pub fn as_fb_bytes(&self) -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();
        let table = self.write_fbb(&mut fbb);
        fbb.finish(table, None);
        fbb.finished_data().to_vec()
    }

    pub fn write_fbb<'fbb: 'a, 'a>(
        &self,
        fbb: &'a mut FlatBufferBuilder<'fbb>,
    ) -> WIPOffset<Table<'fbb>> {
        let schema = add_schema_to_fbb(
            fbb,
            &self.schema_tags,
            &self.schema_fields,
            &self.schema_field_types,
        );
        let mut point_vec = Vec::with_capacity(self.point_timestamps.len());
        for (ts, (tags, fields)) in self
            .point_timestamps
            .iter()
            .zip(self.point_tags.iter().zip(self.point_fields.iter()))
        {
            let point = add_point_to_fbb(
                fbb,
                &self.schema_tags,
                &self.schema_fields,
                *ts,
                tags,
                fields,
            );
            point_vec.push(point);
        }
        let point_vec = fbb.create_vector(&point_vec);
        add_table_to_fbb(
            fbb,
            &self.table,
            schema,
            point_vec,
            self.point_timestamps.len() as u64,
        )
    }

    pub fn schema_field_iter(&self) -> impl Iterator<Item = (&String, &FieldType)> {
        self.schema_fields
            .iter()
            .zip(self.schema_field_types.iter())
    }

    pub fn reset_points(&mut self, timestamps: &[i64]) {
        let mut tag_generator = TagSetGenerator::default();
        let mut field_generator = FieldValueGenerator::default();

        let tag_bitset_offsets: Vec<usize> = (0..self.schema_tags.len()).collect();
        let default_tag_bitset = BitSet::with_offsets(self.schema_tags.len(), &tag_bitset_offsets);
        let field_bitset_offsets: Vec<usize> = (0..self.schema_fields.len()).collect();
        let default_field_bitset =
            BitSet::with_offsets(self.schema_fields.len(), &field_bitset_offsets);

        self.point_timestamps = timestamps.to_vec();
        self.point_tags.clear();
        self.point_tag_bitsets.clear();
        self.point_fields.clear();
        self.point_field_bitsets.clear();
        for _ in timestamps {
            let mut point_fields = vec![];
            for (field_name, field_type) in self.schema_field_iter() {
                point_fields.push(field_generator.next_field_value(field_name, *field_type));
            }
            self.point_tags.push(tag_generator.next_tag_set());
            self.point_tag_bitsets.push(default_tag_bitset.clone());
            self.point_fields.push(point_fields);
            self.point_field_bitsets.push(default_field_bitset.clone());
        }
    }
}

const TA: &str = "ta";
const TB: &str = "tb";
const TC: &str = "tc";

const F1: &str = "f1";
const F2: &str = "f2";
const F3: &str = "f3";
const F4: &str = "f4";
const F5: &str = "f5";

impl Default for TableDescription {
    #[rustfmt::skip]
    fn default() -> Self {
        let all_tags = 3;
        let all_fields = 5;
        Self {
            table: s!("test_table"),
            schema_tags: vec![s!(TA), s!(TB), s!(TC)],
            schema_fields: vec![s!(F1), s!(F2), s!(F3), s!(F4), s!(F5)],
            schema_field_types: vec![FieldType::Float, FieldType::Integer, FieldType::Unsigned, FieldType::Boolean, FieldType::String],
            point_timestamps: vec![1009, 1013, 1019, 1021, 1031],
            point_tags: vec![
                vec![(s!(TA), s!("high")), (s!(TB), s!("user1.host1")), (s!(TC), s!("A"))],
                vec![(s!(TB), s!("user2.host2"))],
                vec![(s!(TA), s!("low")), (s!(TC), s!("B"))],
                vec![(s!(TC), s!("A"))],
                vec![(s!(TB), s!("user1.host2")), (s!(TC), s!("C"))],
            ],
            point_tag_bitsets: vec![
                BitSet::with_offsets(all_tags, &[0, 1, 2]),
                BitSet::with_offsets(all_tags, &[1]),
                BitSet::with_offsets(all_tags, &[0, 2]),
                BitSet::with_offsets(all_tags, &[2]),
                BitSet::with_offsets(all_tags, &[1, 2]),
            ],
            point_fields: vec![
                vec![
                    (s!(F1), FieldType::Float, (-2.0_f64).to_be_bytes().to_vec()),
                    (s!(F2), FieldType::Integer, 2_i64.to_be_bytes().to_vec()),
                    (s!(F3), FieldType::Unsigned, 3_u64.to_be_bytes().to_vec()),
                    (s!(F4), FieldType::Boolean, vec![0_u8]),
                    (s!(F5), FieldType::String, "dd".as_bytes().to_vec()),
                ],
                vec![
                    (s!(F1), FieldType::Float, 5.0_f64.to_be_bytes().to_vec()),
                    (s!(F2), FieldType::Integer, 7_i64.to_be_bytes().to_vec()),
                    (s!(F3), FieldType::Unsigned, 11_u64.to_be_bytes().to_vec()),
                    (s!(F5), FieldType::String, "alpha".as_bytes().to_vec()),
                ],
                vec![
                    (s!(F1), FieldType::Float, 13.0_f64.to_be_bytes().to_vec()),
                    (s!(F2), FieldType::Integer, 17_i64.to_be_bytes().to_vec()),
                    (s!(F3), FieldType::Unsigned, 19_u64.to_be_bytes().to_vec()),
                    (s!(F4), FieldType::Boolean, vec![1_u8]),
                ],
                vec![
                    (s!(F1), FieldType::Float, 23.0_f64.to_be_bytes().to_vec()),
                    (s!(F2), FieldType::Integer, 29_i64.to_be_bytes().to_vec()),
                    (s!(F4), FieldType::Boolean, vec![0_u8]),
                ],
                vec![
                    (s!(F2), FieldType::Integer, 31_i64.to_be_bytes().to_vec()),
                    (s!(F3), FieldType::Unsigned, 37_u64.to_be_bytes().to_vec()),
                    (s!(F4), FieldType::Boolean, vec![1_u8]),
                    (s!(F5), FieldType::String, "hello?".as_bytes().to_vec()),
                ],
            ],
            point_field_bitsets: vec![
                BitSet::with_offsets(all_fields, &[0, 1, 2, 3, 4]),
                BitSet::with_offsets(all_fields, &[0, 1, 2, 4]),
                BitSet::with_offsets(all_fields, &[0, 1, 2, 3]),
                BitSet::with_offsets(all_fields, &[0, 1, 3]),
                BitSet::with_offsets(all_fields, &[1, 2, 3, 4]),
            ],
        }
    }
}

impl<'a> TryFrom<Table<'a>> for TableDescription {
    type Error = PointsError;

    #[allow(dead_code, unused_mut, unused_variables)]
    fn try_from(fb_table: Table<'a>) -> Result<Self, Self::Error> {
        let table = fb_table.tab_ext()?.to_string();
        let schema = fb_table.schema_ext()?;
        let schema_tags = schema
            .tag_name_ext()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let schema_fields = schema
            .field_name_ext()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let schema_field_types = schema.field_type_ext();
        let mut point_timestamps = vec![];
        let mut point_tags = vec![];
        let mut point_tag_bitsets = vec![];
        let mut point_fields = vec![];
        let mut point_field_bitsets = vec![];
        for fb_point in fb_table.points_iter_ext()? {
            point_timestamps.push(fb_point.timestamp());

            let mut tags = vec![];
            let mut tags_bitset = BitSet::new();
            if let Some(tags) = fb_point.tags() {
                for tag in tags {
                    todo!()
                }
            }
            point_tags.push(tags);
            point_tag_bitsets.push(tags_bitset);

            let mut fields = vec![];
            let mut fields_bitset = BitSet::new();
            if let Some(fields) = fb_point.fields() {
                for field in fields {
                    todo!()
                }
            }
            point_fields.push(fields);
            point_field_bitsets.push(fields_bitset);
        }

        Ok(Self {
            table,
            schema_tags,
            schema_fields,
            schema_field_types,
            point_timestamps,
            point_tags,
            point_tag_bitsets,
            point_fields,
            point_field_bitsets,
        })
    }
}

pub struct TagSetGenerator {
    pub tag_keys: Vec<String>,
    pub tag_values: Vec<Vec<String>>,
    pub tag_values_i: Vec<usize>,
}

impl TagSetGenerator {
    pub fn next_tag_set(&mut self) -> Vec<(String, String)> {
        let mut tag_set = Vec::with_capacity(self.tag_keys.len());
        for (i, (tag_key, tag_val_pool)) in
            self.tag_keys.iter().zip(self.tag_values.iter()).enumerate()
        {
            self.tag_values_i[i] = (self.tag_values_i[i] + 1) % tag_val_pool.len();
            tag_set.push((tag_key.clone(), tag_val_pool[self.tag_values_i[i]].clone()));
        }
        tag_set
    }
}

impl Default for TagSetGenerator {
    fn default() -> Self {
        Self {
            tag_keys: vec![s!(TA), s!(TB), s!(TC)],
            tag_values: vec![
                vec![s!("taa"), s!("tab"), s!("tac"), s!("tad"), s!("tae")],
                vec![s!("tba"), s!("tbb"), s!("tbc"), s!("tbd"), s!("tbe")],
                vec![s!("tca"), s!("tcb"), s!("tcc"), s!("tcd"), s!("tcd")],
            ],
            tag_values_i: vec![0, 0, 0],
        }
    }
}

pub struct FieldValueGenerator {
    pub f64_pool: Vec<f64>,
    pub f64_i: usize,

    pub i64_pool: Vec<i64>,
    pub i64_i: usize,

    pub u64_pool: Vec<u64>,
    pub u64_i: usize,

    pub bool_pool: Vec<bool>,
    pub bool_i: usize,

    pub string_pool: Vec<String>,
    pub string_i: usize,
}

#[rustfmt::skip]
impl FieldValueGenerator {
    pub fn next_field_value(&mut self, field_name: &str, field_type: FieldType) -> (String, FieldType, Vec<u8>) {
        match field_type {
            FieldType::Float => self.next_f64(field_name),
            FieldType::Integer => self.next_i64(field_name),
            FieldType::Unsigned => self.next_u64(field_name),
            FieldType::Boolean => self.next_bool(field_name),
            FieldType::String => self.next_string(field_name),
            _ => panic!("Unexpected field_type 'Unknown'"),
        }
    }

    pub fn next_f64(&mut self, field_name: &str) -> (String, FieldType, Vec<u8>) {
        self.f64_i = (self.f64_i + 1) % self.f64_pool.len();
        let val = self.f64_pool[self.f64_i % self.f64_pool.len()];
        (s!(field_name), FieldType::Float, val.to_be_bytes().to_vec())
    }

    pub fn next_i64(&mut self, field_name: &str) -> (String, FieldType, Vec<u8>) {
        self.i64_i = (self.i64_i + 1) % self.i64_pool.len();
        let val = self.i64_pool[self.i64_i % self.i64_pool.len()];
        (s!(field_name), FieldType::Integer, val.to_be_bytes().to_vec())
    }

    pub fn next_u64(&mut self, field_name: &str) -> (String, FieldType, Vec<u8>) {
        self.u64_i = (self.u64_i + 1) % self.u64_pool.len();
        let val = self.u64_pool[self.u64_i % self.u64_pool.len()];
        (s!(field_name), FieldType::Unsigned, val.to_be_bytes().to_vec())
    }

    pub fn next_bool(&mut self, field_name: &str) -> (String, FieldType, Vec<u8>) {
        self.bool_i = (self.bool_i + 1) % self.bool_pool.len();
        let val = self.bool_pool[self.bool_i % self.bool_pool.len()];
        (s!(field_name), FieldType::Boolean, if val { vec![1_u8] } else { vec![0_u8] })
    }

    pub fn next_string(&mut self, field_name: &str) -> (String, FieldType, Vec<u8>) {
        self.string_i = (self.string_i + 1) % self.string_pool.len();
        let val = &self.string_pool[self.string_i % self.string_pool.len()];
        (s!(field_name), FieldType::String, val.as_bytes().to_vec())
    }
}

#[rustfmt::skip]
impl Default for FieldValueGenerator {
    fn default() -> Self {
        Self {
            f64_pool: vec![2_f64, 3.0, 5.0, 7.0, 11.0, 13.0, 17.0, 19.0, 23.0, 29.0, 31.0, 37.0, 41.0, 43.0, 47.0, 53.0, 59.0, 61.0, 67.0, 71.0],
            f64_i: 0,
            i64_pool: vec![2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71],
            i64_i: 0,
            u64_pool: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
            u64_i: 0,
            bool_pool: vec![false, false, true, true, false, true, false, true, false, false, false, true, false, true, false, false, false, true],
            bool_i: 0,
            string_pool: vec![s!("slings"), s!("arrows"), s!("outrageous"), s!("fortune"), s!("arms"), s!("sea"), s!("troubles"), s!("opposing")],
            string_i: 0,
        }
    }
}
