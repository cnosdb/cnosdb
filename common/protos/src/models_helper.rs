use protobuf::Message;
#[cfg(feature = "test")]
pub use test::*;

use crate::models::Points;
use crate::FieldType;

pub fn print_points(points: Points) {
    if let Some(db) = points.db() {
        println!(
            "Database: {}",
            String::from_utf8(db.bytes().to_vec()).unwrap()
        );
    }
    if let Some(tables) = points.tables() {
        for table in tables {
            for point in table.points().unwrap_or_default() {
                print!("\nTimestamp: {}", point.timestamp());
                if let Some(tags) = point.tags() {
                    print!("\nTags[{}]: ", tags.len());
                    for (tag_idx, tag) in tags.iter().enumerate() {
                        if let Some(key) = table.schema().map(|schema| schema.tag_name()).unwrap() {
                            let key = key.get(tag_idx);
                            print!("{{ {}: ", key)
                        } else {
                            print!("{{ EMPTY_TAG_KEY: ")
                        }
                        if let Some(val) = tag.value() {
                            print!("{} }}", String::from_utf8(val.bytes().to_vec()).unwrap())
                        } else {
                            print!("EMPTY_TAG_VAL }}")
                        }
                        if tag_idx < tags.len() - 1 {
                            print!(", ")
                        }
                    }
                } else {
                    println!("Tags[0]")
                }
                if let Some(fields) = point.fields() {
                    print!("\nFields[{}]: ", fields.len());
                    for (field_idx, field) in fields.iter().enumerate() {
                        if let Some(name) =
                            table.schema().map(|schema| schema.field_name()).unwrap()
                        {
                            let name = name.get(field_idx);
                            print!("{{ {}: ", name)
                        } else {
                            print!("{{ EMPTY_FIELD_NAME: ");
                        }
                        if let Some(val) = field.value() {
                            let val_bytes = val.bytes().to_vec();
                            let field_type =
                                table.schema().unwrap().field_type().unwrap().get(field_idx);
                            match field_type {
                                FieldType::Integer => {
                                    let val = unsafe {
                                        i64::from_be_bytes(
                                            *(&val_bytes as *const _ as *const [u8; 8]),
                                        )
                                    };
                                    print!("{}, ", val);
                                }
                                FieldType::Unsigned => {
                                    let val = unsafe {
                                        u64::from_be_bytes(
                                            *(&val_bytes as *const _ as *const [u8; 8]),
                                        )
                                    };
                                    print!("{}, ", val);
                                }
                                FieldType::Float => {
                                    let val = unsafe {
                                        f64::from_be_bytes(
                                            *(&val_bytes as *const _ as *const [u8; 8]),
                                        )
                                    };
                                    print!("{}, ", val);
                                }
                                FieldType::Boolean => {
                                    if val_bytes[0] == 1 {
                                        print!("true, ");
                                    } else {
                                        print!("false, ");
                                    }
                                }
                                FieldType::String => {
                                    print!("{}, ", String::from_utf8(val_bytes).unwrap())
                                }
                                _ => {
                                    print!("UNKNOWN_FIELD_VAL, ");
                                }
                            }
                            print!("{} }}", field_type.0)
                        }
                        if field_idx < fields.len() - 1 {
                            print!(", ")
                        }
                    }
                } else {
                    println!("Fields[0]");
                }
                println!();
            }
        }
    }
}

pub fn parse_proto_bytes<T>(bytes: &[u8]) -> Result<T, protobuf::Error>
where
    T: Message,
{
    T::parse_from_bytes(bytes)
}

pub fn to_proto_bytes<T>(msg: T) -> Result<Vec<u8>, protobuf::Error>
where
    T: Message,
{
    msg.write_to_bytes()
}

pub fn to_prost_bytes<T>(msg: T) -> Vec<u8>
where
    T: prost::Message,
{
    msg.encode_to_vec()
}

pub fn parse_prost_bytes<T>(bytes: &[u8]) -> Result<T, prost::DecodeError>
where
    T: prost::Message,
    T: Default,
{
    T::decode(bytes)
}

pub fn bincode<T>(bytes: &[u8]) -> Result<T, prost::DecodeError>
where
    T: prost::Message,
    T: Default,
{
    T::decode(bytes)
}

#[cfg(feature = "test")]
mod test {
    use crate::{build_fb_schema_offset, init_fields, init_tags, FbSchema};
    use chrono::prelude::*;
    use flatbuffers::{self, ForwardsUOffset, Vector, WIPOffset};
    use std::collections::HashMap;
    use utils::bitset::BitSet;

    use crate::models::*;

    pub fn create_tags<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        tags_raw: &[(&str, &str)],
        schema: &FbSchema,
    ) -> (
        WIPOffset<Vector<'a, ForwardsUOffset<Tag<'a>>>>,
        WIPOffset<Vector<'a, u8>>,
    ) {
        let mut tags = Vec::with_capacity(schema.tag_names().len());
        init_tags(fbb, &mut tags, schema.tag_names().len());

        let mut tags_nullbit = BitSet::with_size(schema.tag_names().len());

        for (k, v) in tags_raw {
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

        let tags = fbb.create_vector(&tags);
        let tags_nullbit = fbb.create_vector(tags_nullbit.bytes());

        (tags, tags_nullbit)
    }

    pub fn create_fields<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        fields_raw: &[(&str, &[u8])],
        schema: &FbSchema,
    ) -> (
        WIPOffset<Vector<'a, ForwardsUOffset<Field<'a>>>>,
        WIPOffset<Vector<'a, u8>>,
    ) {
        let mut fields = Vec::with_capacity(schema.field().len());
        init_fields(fbb, &mut fields, schema.field_len());

        let mut fields_nullbits = BitSet::with_size(schema.field_len());

        for (name, value) in fields_raw {
            let idx = match schema.field().get(name) {
                None => continue,
                Some(v) => *v,
            };

            let fbv = fbb.create_vector(value);

            let mut field_builder = FieldBuilder::new(fbb);
            field_builder.add_value(fbv);

            fields[idx] = field_builder.finish();
            fields_nullbits.set(idx)
        }

        let fields = fbb.create_vector(&fields);
        let fields_nullbits = fbb.create_vector(fields_nullbits.bytes());

        (fields, fields_nullbits)
    }

    pub fn create_point<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        timestamp: i64,
        tags: WIPOffset<Vector<ForwardsUOffset<Tag<'a>>>>,
        tags_nullbit: WIPOffset<Vector<'a, u8>>,
        fields: WIPOffset<Vector<ForwardsUOffset<Field<'a>>>>,
        fields_nullbit: WIPOffset<Vector<'a, u8>>,
    ) -> WIPOffset<Point<'a>> {
        let mut point_builder = PointBuilder::new(fbb);
        point_builder.add_timestamp(timestamp);
        point_builder.add_tags(tags);
        point_builder.add_tags_nullbit(tags_nullbit);
        point_builder.add_fields(fields);
        point_builder.add_fields_nullbit(fields_nullbit);
        point_builder.finish()
    }

    /// Generate `num` points, tags and fields are always the same value.
    ///
    /// - Database: "db0"
    /// - table,ta=a,tb=b fa=100i,fb=1000 1
    /// - table,ta=a,tb=b fa=100i,fb=1000 2
    /// - ...
    /// - table,ta=a,tb=b fa=100i,fb=1000 num-1
    #[allow(clippy::too_many_arguments)]
    pub fn create_const_points<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        schema: FbSchema,
        database: &str,
        table: &str,
        tags: Vec<(&str, &str)>,
        fields: Vec<(&str, &[u8])>,
        start_timestamp: i64,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let db = fbb.create_vector(database.as_bytes());

        let mut points = vec![];
        for timestamp in start_timestamp..start_timestamp + num as i64 {
            let (tags, tags_bullbit) = create_tags(fbb, &tags, &schema);

            let (fields, field_bullbits) = create_fields(fbb, &fields, &schema);

            points.push(create_point(
                fbb,
                timestamp,
                tags,
                tags_bullbit,
                fields,
                field_bullbits,
            ))
        }
        let fb_schema = build_fb_schema_offset(fbb, &schema);

        let point = fbb.create_vector(&points);
        let tab = fbb.create_vector(table.as_bytes());

        let mut table_builder = TableBuilder::new(fbb);

        table_builder.add_points(point);
        table_builder.add_schema(fb_schema);
        table_builder.add_tab(tab);
        table_builder.add_num_rows(num as u64);

        let table = table_builder.finish();
        let table_offsets = fbb.create_vector(&[table]);

        Points::create(
            fbb,
            &PointsArgs {
                db: Some(db),
                tables: Some(table_offsets),
            },
        )
    }

    pub fn create_random_points_with_delta<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let db = fbb.create_vector("db".as_bytes());
        let mut tags_names: HashMap<&str, usize> = HashMap::new();
        tags_names.insert("ta", 0);
        tags_names.insert("tb", 1);

        let mut fields: HashMap<&str, usize> = HashMap::new();
        fields.insert("fa", 0);
        fields.insert("fb", 1);

        let schema = FbSchema {
            tag_name: tags_names,
            field: fields,
            field_type: vec![FieldType::Integer, FieldType::Float],
        };

        let area = ["a".to_string(), "b".to_string(), "c".to_string()];
        let mut points = vec![];
        for i in 0..num {
            let timestamp = if i <= num / 2 {
                Local::now().timestamp_nanos()
            } else {
                1
            };
            let tav = area[rand::random::<usize>() % 3].clone();
            let tbv = area[rand::random::<usize>() % 3].clone();
            let (tags, tags_nullbit) = create_tags(
                fbb,
                vec![
                    ("ta", ("a".to_string() + &tav).as_str()),
                    ("tb", ("b".to_string() + &tbv).as_str()),
                ]
                .as_slice(),
                &schema,
            );

            let fav = rand::random::<i64>().to_be_bytes();
            let fbv = rand::random::<i64>().to_be_bytes();
            let (fields, fields_nullbits) = create_fields(
                fbb,
                vec![("fa", fav.as_slice()), ("fb", fbv.as_slice())].as_slice(),
                &schema,
            );

            points.push(create_point(
                fbb,
                timestamp,
                tags,
                tags_nullbit,
                fields,
                fields_nullbits,
            ))
        }

        let fb_schema = build_fb_schema_offset(fbb, &schema);

        let point = fbb.create_vector(&points);
        let tab = fbb.create_vector("table".as_bytes());

        let mut table_builder = TableBuilder::new(fbb);

        table_builder.add_points(point);
        table_builder.add_schema(fb_schema);
        table_builder.add_tab(tab);
        table_builder.add_num_rows(num as u64);

        let table = table_builder.finish();
        let table_offsets = fbb.create_vector(&[table]);

        Points::create(
            fbb,
            &PointsArgs {
                db: Some(db),
                tables: Some(table_offsets),
            },
        )
    }

    pub fn create_random_points_include_delta<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let db = fbb.create_vector("db".as_bytes());
        let mut tags_names: HashMap<&str, usize> = HashMap::new();
        tags_names.insert("ta", 0);
        tags_names.insert("tb", 1);

        let mut fields: HashMap<&str, usize> = HashMap::new();
        fields.insert("fa", 0);
        fields.insert("fb", 1);
        fields.insert("fc", 2);

        let schema = FbSchema {
            tag_name: tags_names,
            field: fields,
            field_type: vec![FieldType::Integer, FieldType::Float, FieldType::Float],
        };

        let area = ["a".to_string(), "b".to_string(), "c".to_string()];
        let mut points = vec![];
        for i in 0..num {
            let timestamp = if i % 2 == 0 {
                Local::now().timestamp_nanos()
            } else {
                i64::MIN
            };
            let tav = area[rand::random::<usize>() % 3].clone();
            let tbv = area[rand::random::<usize>() % 3].clone();
            let (tags, tags_nullbit) = if rand::random::<i64>() % 2 == 0 {
                create_tags(
                    fbb,
                    vec![
                        ("ta", ("a".to_string() + &tav).as_str()),
                        ("tb", ("b".to_string() + &tbv).as_str()),
                    ]
                    .as_slice(),
                    &schema,
                )
            } else {
                create_tags(
                    fbb,
                    vec![
                        ("ta", ("a".to_string() + &tav).as_str()),
                        ("tb", ("b".to_string() + &tbv).as_str()),
                        ("tc", ("c".to_string() + &tbv).as_str()),
                    ]
                    .as_slice(),
                    &schema,
                )
            };

            let fav = rand::random::<f64>().to_be_bytes();
            let fbv = rand::random::<i64>().to_be_bytes();
            let (fields, fields_nullbit) = if rand::random::<i64>() % 2 == 0 {
                create_fields(
                    fbb,
                    vec![("fa", fav.as_slice()), ("fb", fbv.as_slice())].as_slice(),
                    &schema,
                )
            } else {
                create_fields(
                    fbb,
                    vec![
                        ("fa", fav.as_slice()),
                        ("fb", fbv.as_slice()),
                        ("fc", fbv.as_slice()),
                    ]
                    .as_slice(),
                    &schema,
                )
            };

            points.push(create_point(
                fbb,
                timestamp,
                tags,
                tags_nullbit,
                fields,
                fields_nullbit,
            ))
        }
        let fb_schema = build_fb_schema_offset(fbb, &schema);

        let point = fbb.create_vector(&points);
        let tab = fbb.create_vector("table".as_bytes());

        let mut table_builder = TableBuilder::new(fbb);

        table_builder.add_points(point);
        table_builder.add_schema(fb_schema);
        table_builder.add_tab(tab);
        table_builder.add_num_rows(num as u64);

        let table = table_builder.finish();
        let table_offsets = fbb.create_vector(&[table]);

        Points::create(
            fbb,
            &PointsArgs {
                db: Some(db),
                tables: Some(table_offsets),
            },
        )
    }

    pub fn create_big_random_points<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let db = fbb.create_vector("db".as_bytes());
        let mut tags_names: HashMap<&str, usize> = HashMap::new();
        tags_names.insert("tag", 0);

        let mut fields: HashMap<&str, usize> = HashMap::new();
        fields.insert("field_integer", 0);
        fields.insert("field_float", 1);

        let schema = FbSchema {
            tag_name: tags_names,
            field: fields,
            field_type: vec![FieldType::Integer, FieldType::Float],
        };

        let mut points = vec![];
        for _ in 0..num {
            let timestamp = Local::now().timestamp_nanos();
            let mut tags = vec![];
            let tav = rand::random::<u8>().to_string();
            for _ in 0..19999 {
                tags.push(("tag", tav.as_str()));
            }
            let (tags, tags_nullbit) = create_tags(fbb, &tags, &schema);

            let mut fields = vec![];
            let fav = rand::random::<i64>().to_be_bytes();
            let fbv = rand::random::<f64>().to_be_bytes();
            for _ in 0..19999 {
                fields.push(("field_integer", fav.as_slice()));
                fields.push(("field_float", fbv.as_slice()));
            }
            let (fields, fields_nullbit) = create_fields(fbb, &fields, &schema);

            points.push(create_point(
                fbb,
                timestamp,
                tags,
                tags_nullbit,
                fields,
                fields_nullbit,
            ))
        }
        let fb_schema = build_fb_schema_offset(fbb, &schema);

        let point = fbb.create_vector(&points);
        let tab = fbb.create_vector("table".as_bytes());

        let mut table_builder = TableBuilder::new(fbb);

        table_builder.add_points(point);
        table_builder.add_schema(fb_schema);
        table_builder.add_tab(tab);
        table_builder.add_num_rows(num as u64);

        let table = table_builder.finish();
        let table_offsets = fbb.create_vector(&[table]);

        Points::create(
            fbb,
            &PointsArgs {
                db: Some(db),
                tables: Some(table_offsets),
            },
        )
    }

    pub fn create_dev_ops_points<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        num: usize,
        database: &str,
        table: &str,
    ) -> WIPOffset<Points<'a>> {
        let db = fbb.create_vector(database.as_bytes());

        let mut tags_names: HashMap<&str, usize> = HashMap::new();
        tags_names.insert("region", 0);
        tags_names.insert("host", 1);

        let mut fields: HashMap<&str, usize> = HashMap::new();
        fields.insert("cpu", 0);
        fields.insert("mem", 1);

        let schema = FbSchema {
            tag_name: tags_names,
            field: fields,
            field_type: vec![FieldType::Float, FieldType::Float],
        };

        #[rustfmt::skip]
            let tag_key_values = [
                ("region", vec!["rg_1", "rg_2", "rg_3", "rg_4", "rg_5", "rg_6"]),
                ("host", vec!["192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.4", "192.168.0.5", "192.168.0.6"]),
            ];
        let field_keys = ["cpu", "mem"];

        let now = Local::now().timestamp_nanos();
        let mut points = vec![];
        for i in 0..num as i64 {
            let timestamp = now + i;

            #[rustfmt::skip]
                let (tags, tags_nullbit) = create_tags(fbb, vec![
                    (tag_key_values[0].0, tag_key_values[0].1[i as usize / tag_key_values[0].1.len() % tag_key_values[0].1.len()]),
                    (tag_key_values[1].0, tag_key_values[1].1[i as usize % tag_key_values[1].1.len()]),
                ].as_slice(), &schema);

            let mut fields = vec![];
            let fv = rand::random::<f64>().to_be_bytes();
            for fk in field_keys {
                fields.push((fk, fv.as_slice()));
            }
            let (fields, fields_nullbit) = create_fields(fbb, &fields, &schema);

            points.push(create_point(
                fbb,
                timestamp,
                tags,
                tags_nullbit,
                fields,
                fields_nullbit,
            ))
        }
        let fb_schema = build_fb_schema_offset(fbb, &schema);

        let point = fbb.create_vector(&points);
        let tab = fbb.create_vector(table.as_bytes());

        let mut table_builder = TableBuilder::new(fbb);

        table_builder.add_points(point);
        table_builder.add_schema(fb_schema);
        table_builder.add_tab(tab);
        table_builder.add_num_rows(num as u64);

        let table = table_builder.finish();
        let table_offsets = fbb.create_vector(&[table]);

        Points::create(
            fbb,
            &PointsArgs {
                db: Some(db),
                tables: Some(table_offsets),
            },
        )
    }
}
