use protobuf::Message;
#[cfg(feature = "test")]
pub use test::*;

use crate::{
    models::{FieldType, Points},
    prompb::remote::ReadRequest,
};

pub fn print_points(points: Points) {
    if let Some(db) = points.db() {
        println!(
            "Database: {}",
            String::from_utf8(db.bytes().to_vec()).unwrap()
        );
    }
    if let Some(points) = points.points() {
        for point in points.iter() {
            print!("\nTimestamp: {}", point.timestamp());
            if let Some(tags) = point.tags() {
                print!("\nTags[{}]: ", tags.len());
                for (tag_idx, tag) in tags.iter().enumerate() {
                    if let Some(key) = tag.key() {
                        print!("{{ {}: ", String::from_utf8(key.bytes().to_vec()).unwrap())
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
                    if let Some(name) = field.name() {
                        print!("{{ {}: ", String::from_utf8(name.bytes().to_vec()).unwrap())
                    } else {
                        print!("{{ EMPTY_FIELD_NAME: ");
                    }
                    if let Some(val) = field.value() {
                        let val_bytes = val.bytes();
                        match field.type_() {
                            FieldType::Integer => {
                                let val = unsafe {
                                    i64::from_be_bytes(*(val_bytes as *const _ as *const [u8; 8]))
                                };
                                print!("{}, ", val);
                            }
                            FieldType::Unsigned => {
                                let val = unsafe {
                                    u64::from_be_bytes(*(val_bytes as *const _ as *const [u8; 8]))
                                };
                                print!("{}, ", val);
                            }
                            FieldType::Float => {
                                let val = unsafe {
                                    f64::from_be_bytes(*(val_bytes as *const _ as *const [u8; 8]))
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
                                print!("{}, ", String::from_utf8(val_bytes.to_vec()).unwrap())
                            }
                            _ => {
                                print!("UNKNOWN_FIELD_VAL, ");
                            }
                        }
                        print!("{} }}", field.type_().0)
                    } else {
                        print!("EMPTY_FIELD_VAL, {} }}", field.type_().0);
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

#[cfg(feature = "test")]
mod test {
    use chrono::prelude::*;
    use flatbuffers::{self, ForwardsUOffset, InvalidFlatbuffer, Vector, WIPOffset};

    use crate::models::{self, *};

    pub fn create_tags<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        tags: &[(&str, &str)],
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<Tag<'a>>>> {
        let mut vec = vec![];
        for (k, v) in tags.iter() {
            let k = fbb.create_vector(k.as_bytes());
            let v = fbb.create_vector(v.as_bytes());
            let mut tag_builder = TagBuilder::new(fbb);
            tag_builder.add_key(k);
            tag_builder.add_value(v);
            vec.push(tag_builder.finish());
        }
        fbb.create_vector(&vec)
    }

    pub fn create_fields<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        fields: &[(&str, FieldType, &[u8])],
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<Field<'a>>>> {
        let mut vec = vec![];
        for (name, ft, val) in fields.iter() {
            let name = fbb.create_vector(name.as_bytes());
            let val = fbb.create_vector(val);
            let mut field_builder = FieldBuilder::new(fbb);
            field_builder.add_name(name);
            field_builder.add_type_(*ft);
            field_builder.add_value(val);
            vec.push(field_builder.finish());
        }
        fbb.create_vector(&vec)
    }

    pub fn create_point<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        timestamp: i64,
        db: WIPOffset<Vector<u8>>,
        table: WIPOffset<Vector<u8>>,
        tags: WIPOffset<Vector<ForwardsUOffset<Tag>>>,
        fields: WIPOffset<Vector<ForwardsUOffset<Field>>>,
    ) -> WIPOffset<Point<'a>> {
        let mut point_builder = PointBuilder::new(fbb);
        point_builder.add_db(db);
        point_builder.add_tab(table);
        point_builder.add_tags(tags);
        point_builder.add_fields(fields);
        point_builder.add_timestamp(timestamp);
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
        database: &str,
        table: &str,
        tags: Vec<(&str, &str)>,
        fields: Vec<(&str, FieldType, &[u8])>,
        start_timestamp: i64,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let db = fbb.create_vector(database.as_bytes());
        let mut points = vec![];
        for timestamp in start_timestamp..start_timestamp + num as i64 {
            let tags = create_tags(fbb, &tags);

            let fields = create_fields(fbb, &fields);

            let table = fbb.create_vector(table.as_bytes());
            points.push(create_point(
                fbb,
                timestamp,
                db.clone(),
                table,
                tags,
                fields,
            ))
        }
        let points = fbb.create_vector(&points);
        Points::create(
            fbb,
            &PointsArgs {
                db: Some(db),
                points: Some(points),
            },
        )
    }

    pub fn create_random_points_with_delta<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let db = fbb.create_vector("db".as_bytes());
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
            let tags = create_tags(
                fbb,
                vec![
                    ("ta", ("a".to_string() + &tav).as_str()),
                    ("tb", ("b".to_string() + &tbv).as_str()),
                ]
                .as_slice(),
            );

            let fav = rand::random::<i64>().to_be_bytes();
            let fbv = rand::random::<i64>().to_be_bytes();
            let fields = create_fields(
                fbb,
                vec![
                    ("fa", FieldType::Integer, fav.as_slice()),
                    ("fb", FieldType::Float, fbv.as_slice()),
                ]
                .as_slice(),
            );

            let table = fbb.create_vector("table".as_bytes());
            points.push(create_point(
                fbb,
                timestamp,
                db.clone(),
                table,
                tags,
                fields,
            ))
        }

        let points = fbb.create_vector(&points);
        Points::create(
            fbb,
            &PointsArgs {
                db: Some(db),
                points: Some(points),
            },
        )
    }

    pub fn create_random_points_include_delta<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let db = fbb.create_vector("db".as_bytes());
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
            let tags = if rand::random::<i64>() % 2 == 0 {
                create_tags(
                    fbb,
                    vec![
                        ("ta", ("a".to_string() + &tav).as_str()),
                        ("tb", ("b".to_string() + &tbv).as_str()),
                    ]
                    .as_slice(),
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
                )
            };

            let fav = rand::random::<f64>().to_be_bytes();
            let fbv = rand::random::<i64>().to_be_bytes();
            let fields = if rand::random::<i64>() % 2 == 0 {
                create_fields(
                    fbb,
                    vec![
                        ("fa", FieldType::Integer, fav.as_slice()),
                        ("fb", FieldType::Float, fbv.as_slice()),
                    ]
                    .as_slice(),
                )
            } else {
                create_fields(
                    fbb,
                    vec![
                        ("fa", FieldType::Integer, fav.as_slice()),
                        ("fb", FieldType::Float, fbv.as_slice()),
                        ("fc", FieldType::Float, fbv.as_slice()),
                    ]
                    .as_slice(),
                )
            };

            let table = fbb.create_vector("table".as_bytes());
            points.push(create_point(
                fbb,
                timestamp,
                db.clone(),
                table,
                tags,
                fields,
            ))
        }
        let points = fbb.create_vector(&points);
        Points::create(
            fbb,
            &PointsArgs {
                db: Some(db),
                points: Some(points),
            },
        )
    }

    pub fn create_big_random_points<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let db = fbb.create_vector("db".as_bytes());
        let mut points = vec![];
        for _ in 0..num {
            let timestamp = Local::now().timestamp_nanos();
            let mut tags = vec![];
            let tav = rand::random::<u8>().to_string();
            for _ in 0..19999 {
                tags.push(("tag", tav.as_str()));
            }
            let tags = create_tags(fbb, &tags);

            let mut fields = vec![];
            let fav = rand::random::<i64>().to_be_bytes();
            let fbv = rand::random::<f64>().to_be_bytes();
            for _ in 0..19999 {
                fields.push(("field_integer", FieldType::Integer, fav.as_slice()));
                fields.push(("field_float", FieldType::Float, fbv.as_slice()));
            }
            let fields = create_fields(fbb, &fields);

            let table = fbb.create_vector("table".as_bytes());
            points.push(create_point(
                fbb,
                timestamp,
                db.clone(),
                table,
                tags,
                fields,
            ));
        }
        let points = fbb.create_vector(&points);
        Points::create(
            fbb,
            &PointsArgs {
                db: Some(db),
                points: Some(points),
            },
        )
    }

    pub fn create_dev_ops_points<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        num: usize,
        database: &str,
        table: &str,
    ) -> WIPOffset<Points<'a>> {
        #[rustfmt::skip]
        let tag_key_values = [
            ("region", vec!["rg_1", "rg_2", "rg_3", "rg_4", "rg_5", "rg_6"]),
            ("host", vec!["192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.4", "192.168.0.5", "192.168.0.6"]),
        ];
        let field_keys = ["cpu", "mem"];

        let now = Local::now().timestamp_nanos();
        let database = fbb.create_vector(database.as_bytes());
        let table = fbb.create_vector(table.as_bytes());
        let mut points = vec![];
        for i in 0..num as i64 {
            let timestamp = now + i;

            #[rustfmt::skip]
            let tags = create_tags(fbb, vec![
                (tag_key_values[0].0, tag_key_values[0].1[i as usize / tag_key_values[0].1.len() % tag_key_values[0].1.len()]),
                (tag_key_values[1].0, tag_key_values[1].1[i as usize % tag_key_values[1].1.len()]),
            ].as_slice());

            let mut fields = vec![];
            let fv = rand::random::<f64>().to_be_bytes();
            for fk in field_keys {
                fields.push((fk, FieldType::Float, fv.as_slice()));
            }
            let fields = create_fields(fbb, &fields);

            points.push(create_point(
                fbb,
                timestamp,
                database.clone(),
                table,
                tags,
                fields,
            ));
        }
        let points = fbb.create_vector(&points);
        Points::create(
            fbb,
            &PointsArgs {
                db: Some(database),
                points: Some(points),
            },
        )
    }

    pub fn get_db_from_fb_points(fb_points: Points) -> String {
        unsafe {
            let db = fb_points.db().unwrap().bytes().to_vec();
            String::from_utf8_unchecked(db)
        }
    }

    pub fn get_db_from_flatbuffers(points: &[u8]) -> Result<String, InvalidFlatbuffer> {
        let fb_points = flatbuffers::root::<Points>(points)?;
        Ok(get_db_from_fb_points(fb_points))
    }
}
