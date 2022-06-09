#[cfg(feature = "test")]
pub use test::*;

#[cfg(feature = "test")]
mod test {
    use chrono::prelude::*;
    use flatbuffers::{self, ForwardsUOffset, Vector, WIPOffset};

    use crate::models::{self, *};

    pub fn create_tags<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        tags: Vec<(&str, &str)>)
        -> WIPOffset<flatbuffers::Vector<'a, ForwardsUOffset<models::Tag<'a>>>> {
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
        fields: Vec<(&str, models::FieldType, &[u8])>)
        -> WIPOffset<flatbuffers::Vector<'a, ForwardsUOffset<models::Field<'a>>>> {
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

    pub fn create_point<'a>(fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
                            timestamp: u64,
                            tags: WIPOffset<flatbuffers::Vector<ForwardsUOffset<models::Tag>>>,
                            fields: WIPOffset<flatbuffers::Vector<ForwardsUOffset<models::Field>>>)
                            -> WIPOffset<models::Point<'a>> {
        let mut point_builder = PointBuilder::new(fbb);
        point_builder.add_tags(tags);
        point_builder.add_fields(fields);
        point_builder.add_timestamp(timestamp);
        point_builder.finish()
    }

    pub fn create_random_points<'a>(fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
                                    num: usize)
                                    -> WIPOffset<Points<'a>> {
        let mut points = vec![];
        for _ in 0..num {
            let timestamp = Local::now().timestamp_millis() as u64;

            let tav = rand::random::<u8>().to_string();
            let tbv = rand::random::<u8>().to_string();
            let tags = create_tags(fbb,
                                   vec![("ta", &("a".to_string() + &tav)),
                                        ("tb", &("b".to_string() + &tbv))]);

            let fav = rand::random::<f64>().to_be_bytes();
            let fbv = rand::random::<i64>().to_be_bytes();
            let fields = create_fields(fbb,
                                       vec![("fa", models::FieldType::Integer, fav.as_slice()),
                                            ("fb", models::FieldType::Float, fbv.as_slice()),]);
            points.push(create_point(fbb, timestamp, tags, fields))
        }
        let points = fbb.create_vector(&points);
        models::Points::create(fbb, &models::PointsArgs { points: Some(points) })
    }
}
