pub use test::*;

pub fn print_points(points: crate::models::Points) {
    println!("{points}")
}

pub fn parse_proto_bytes<T>(bytes: &[u8]) -> Result<T, protobuf::Error>
where
    T: protobuf::Message,
{
    T::parse_from_bytes(bytes)
}

pub fn to_proto_bytes<T>(msg: T) -> Result<Vec<u8>, protobuf::Error>
where
    T: protobuf::Message,
{
    msg.write_to_bytes()
}

pub fn to_prost_bytes<T>(msg: &T) -> Vec<u8>
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

mod test {
    use std::collections::HashMap;

    use chrono::prelude::*;
    use flatbuffers::{self, WIPOffset};
    use utils::bitset::BitSet;

    use crate::models::*;

    /// Generate `num` points, tags and fields are always the same value.
    ///
    /// - Database: "db0"
    /// - table,ta=a,tb=b fa=100i,fb=1000 1
    /// - table,ta=a,tb=b fa=100i,fb=1000 2
    /// - ...
    /// - table,ta=a,tb=b fa=100i,fb=1000 num-1
    pub fn create_const_points<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        database: &str,
        table: &str,
        tags: Vec<(&str, &str)>,
        fields: Vec<(&str, &[u8])>,
        fields_type: HashMap<&str, FieldType>,
        start_timestamp: i64,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let nullbits = BitSet::with_size_all_set(num);
        let fb_nullbits = fbb.create_vector(nullbits.bytes());
        let db = fbb.create_string(database);
        let table = fbb.create_string(table);
        let mut columns = vec![];

        for (tag_key, tag_value) in tags {
            let col_name = fbb.create_string(tag_key);
            let mut col_values = vec![];
            for _ in 0..num {
                col_values.push(fbb.create_string(tag_value));
            }
            let values = fbb.create_vector(&col_values);
            let mut values_builder = ValuesBuilder::new(fbb);
            values_builder.add_string_value(values);
            let values = values_builder.finish();
            let mut col_builder = ColumnBuilder::new(fbb);
            col_builder.add_field_type(FieldType::String);
            col_builder.add_column_type(ColumnType::Tag);
            col_builder.add_name(col_name);
            col_builder.add_col_values(values);
            col_builder.add_nullbits(fb_nullbits);
            let col = col_builder.finish();
            columns.push(col);
        }

        for (field_name, field_value) in fields.iter() {
            let col_name = fbb.create_string(field_name);
            match *fields_type.get(field_name).unwrap() {
                FieldType::Float => {
                    let mut col_values = vec![];
                    for _ in 0..num {
                        col_values.push(f64::from_be_bytes((*field_value).try_into().unwrap()));
                    }
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_float_value(values);
                    let values = values_builder.finish();
                    let mut col_builder = ColumnBuilder::new(fbb);
                    col_builder.add_field_type(FieldType::Float);
                    col_builder.add_column_type(ColumnType::Field);
                    col_builder.add_name(col_name);
                    col_builder.add_col_values(values);
                    col_builder.add_nullbits(fb_nullbits);
                    let col = col_builder.finish();
                    columns.push(col);
                }
                FieldType::Integer => {
                    let mut col_values = vec![];
                    for _ in 0..num {
                        col_values.push(i64::from_be_bytes((*field_value).try_into().unwrap()));
                    }
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_int_value(values);
                    let values = values_builder.finish();
                    let mut col_builder = ColumnBuilder::new(fbb);
                    col_builder.add_field_type(FieldType::Integer);
                    col_builder.add_column_type(ColumnType::Field);
                    col_builder.add_name(col_name);
                    col_builder.add_col_values(values);
                    col_builder.add_nullbits(fb_nullbits);
                    let col = col_builder.finish();
                    columns.push(col);
                }
                FieldType::String => {
                    let mut col_values = vec![];
                    for _ in 0..num {
                        col_values
                            .push(fbb.create_string(std::str::from_utf8(field_value).unwrap()));
                    }
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_string_value(values);
                    let values = values_builder.finish();
                    let mut col_builder = ColumnBuilder::new(fbb);
                    col_builder.add_field_type(FieldType::String);
                    col_builder.add_column_type(ColumnType::Field);
                    col_builder.add_name(col_name);
                    col_builder.add_col_values(values);
                    col_builder.add_nullbits(fb_nullbits);
                    let col = col_builder.finish();
                    columns.push(col);
                }
                FieldType::Unsigned => {
                    let mut col_values = vec![];
                    for _ in 0..num {
                        col_values.push(u64::from_be_bytes((*field_value).try_into().unwrap()));
                    }
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_uint_value(values);
                    let values = values_builder.finish();
                    let mut col_builder = ColumnBuilder::new(fbb);
                    col_builder.add_field_type(FieldType::Unsigned);
                    col_builder.add_column_type(ColumnType::Field);
                    col_builder.add_name(col_name);
                    col_builder.add_col_values(values);
                    col_builder.add_nullbits(fb_nullbits);
                    let col = col_builder.finish();
                    columns.push(col);
                }
                FieldType::Boolean => {
                    let mut col_values = vec![];
                    for _ in 0..num {
                        col_values.push(field_value[0] != 0);
                    }
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_bool_value(values);
                    let values = values_builder.finish();
                    let mut col_builder = ColumnBuilder::new(fbb);
                    col_builder.add_field_type(FieldType::Boolean);
                    col_builder.add_column_type(ColumnType::Field);
                    col_builder.add_name(col_name);
                    col_builder.add_col_values(values);
                    col_builder.add_nullbits(fb_nullbits);
                    let col = col_builder.finish();
                    columns.push(col);
                }
                _ => {
                    panic!("not support field type:{:?}", fields_type[field_name])
                }
            }
        }
        let mut time_col_values = vec![];

        for _ in 0..num {
            time_col_values.push(start_timestamp + num as i64);
        }
        let values = fbb.create_vector(&time_col_values);
        let mut values_builder = ValuesBuilder::new(fbb);
        values_builder.add_int_value(values);
        let values = values_builder.finish();
        let name = fbb.create_string("time");
        let mut col_builder = ColumnBuilder::new(fbb);
        col_builder.add_field_type(FieldType::Boolean);
        col_builder.add_column_type(ColumnType::Field);
        col_builder.add_name(name);
        col_builder.add_col_values(values);
        col_builder.add_nullbits(fb_nullbits);
        let col = col_builder.finish();
        columns.push(col);

        let columns = fbb.create_vector(&columns);
        let mut table_builder = TableBuilder::new(fbb);
        table_builder.add_tab(table);
        table_builder.add_columns(columns);
        table_builder.add_num_rows(num as u64);
        let table = table_builder.finish();
        let tables = vec![table];
        let tables = fbb.create_vector(&tables);

        let mut points_builder = PointsBuilder::new(fbb);
        points_builder.add_db(db);
        points_builder.add_tables(tables);

        points_builder.finish()
    }

    pub fn create_random_points_with_delta<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let mut tags_names: HashMap<&str, usize> = HashMap::new();
        tags_names.insert("ta", 0);
        tags_names.insert("tb", 1);

        let mut fields: HashMap<&str, usize> = HashMap::new();
        fields.insert("fa", 0);
        fields.insert("fb", 1);

        let fields_type = HashMap::from([("fa", FieldType::Integer), ("fb", FieldType::Float)]);

        let mut time_values = Vec::new();
        let mut tags_values = HashMap::new();
        let mut fields_values = HashMap::new();

        let area = ["a".to_string(), "b".to_string(), "c".to_string()];
        for i in 0..num {
            let timestamp = if i <= num / 2 {
                Local::now().timestamp_nanos_opt().unwrap()
            } else {
                1
            };
            time_values.push(timestamp);

            let tav = area[rand::random::<usize>() % 3].clone();
            let tbv = area[rand::random::<usize>() % 3].clone();
            let entry = tags_values.entry("ta").or_insert(vec![]);
            entry.push("a".to_string() + &tav);
            let entry = tags_values.entry("tb").or_insert(vec![]);
            entry.push("b".to_string() + &tbv);

            let entry = fields_values.entry("fa").or_insert(vec![]);
            entry.push(rand::random::<i64>().to_be_bytes());
            let entry = fields_values.entry("fb").or_insert(vec![]);
            entry.push(rand::random::<f64>().to_be_bytes());
        }

        create_points(
            fbb,
            "db",
            "table",
            tags_values
                .iter()
                .map(|(k, v)| (*k, v.iter().map(|v| v.as_str()).collect()))
                .collect(),
            fields_values
                .iter()
                .map(|(k, v)| (*k, v.iter().map(|v| v.as_slice()).collect()))
                .collect(),
            fields_type,
            &time_values,
            num,
        )
    }

    pub fn create_random_points_include_delta<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        database: &str,
        table: &str,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let mut tags_names: HashMap<&str, usize> = HashMap::new();
        tags_names.insert("ta", 0);
        tags_names.insert("tb", 1);

        let mut fields: HashMap<&str, usize> = HashMap::new();
        fields.insert("fa", 0);
        fields.insert("fb", 1);
        fields.insert("fc", 2);

        let fields_type = HashMap::from([
            ("fa", FieldType::Integer),
            ("fb", FieldType::Float),
            ("fc", FieldType::Float),
        ]);

        let mut time_values = Vec::new();
        let mut tags_values = HashMap::new();
        let mut fields_values = HashMap::new();

        let area = ["a".to_string(), "b".to_string(), "c".to_string()];
        for i in 0..num {
            let timestamp = if i % 2 == 0 {
                Local::now().timestamp_nanos_opt().unwrap()
            } else {
                i64::MIN
            };
            time_values.push(timestamp);
            let tav = area[rand::random::<usize>() % 3].clone();
            let tbv = area[rand::random::<usize>() % 3].clone();
            if rand::random::<i64>() % 2 == 0 {
                let entry = tags_values.entry("ta").or_insert(vec![]);
                entry.push("a".to_string() + &tav);
                let entry = tags_values.entry("tb").or_insert(vec![]);
                entry.push("b".to_string() + &tbv);
                let entry = tags_values.entry("tc").or_insert(vec![]);
                entry.push("".to_string());
            } else {
                let entry = tags_values.entry("ta").or_insert(vec![]);
                entry.push("a".to_string() + &tav);
                let entry = tags_values.entry("tb").or_insert(vec![]);
                entry.push("b".to_string() + &tbv);
                let entry = tags_values.entry("tc").or_insert(vec![]);
                entry.push("c".to_string() + &tbv);
            }

            let fav = rand::random::<f64>().to_be_bytes();
            let fbv = rand::random::<i64>().to_be_bytes();

            if rand::random::<i64>() % 2 == 0 {
                let entry = fields_values.entry("fa").or_insert(vec![]);
                entry.push(fbv);
                let entry = fields_values.entry("fb").or_insert(vec![]);
                entry.push(fav);
                let entry = fields_values.entry("fc").or_insert(vec![]);
                entry.push(0.0_f64.to_be_bytes());
            } else {
                let entry = fields_values.entry("fa").or_insert(vec![]);
                entry.push(fbv);
                let entry = fields_values.entry("fb").or_insert(vec![]);
                entry.push(fav);
                let entry = fields_values.entry("fc").or_insert(vec![]);
                entry.push(fav);
            }
        }
        create_points(
            fbb,
            database,
            table,
            tags_values
                .iter()
                .map(|(k, v)| (*k, v.iter().map(|v| v.as_str()).collect()))
                .collect(),
            fields_values
                .iter()
                .map(|(k, v)| (*k, v.iter().map(|v| v.as_slice()).collect()))
                .collect(),
            fields_type,
            &time_values,
            num,
        )
    }

    pub fn create_big_random_points<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        _table: &str,
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let _db = fbb.create_vector("db".as_bytes());
        let mut tags_names: HashMap<&str, usize> = HashMap::new();
        tags_names.insert("tag", 0);

        let mut fields: HashMap<&str, usize> = HashMap::new();
        fields.insert("field_integer", 0);
        fields.insert("field_float", 1);

        let fields_type = HashMap::from([
            ("field_integer", FieldType::Integer),
            ("field_float", FieldType::Float),
        ]);

        let mut time_values = Vec::new();
        let mut tags_values = HashMap::new();
        let mut fields_values = HashMap::new();
        let tav = rand::random::<u8>().to_string();
        let fav = rand::random::<i64>().to_be_bytes();
        let fbv = rand::random::<f64>().to_be_bytes();

        for _ in 0..num {
            let timestamp = Local::now().timestamp_nanos_opt().unwrap();
            time_values.push(timestamp);
            let entry = tags_values.entry("tag").or_insert(vec![]);
            for _ in 0..19999 {
                entry.push(tav.as_str());
            }

            let entry = fields_values.entry("field_integer").or_insert(vec![]);
            for _ in 0..19999 {
                entry.push(fav.as_slice());
            }
            let entry = fields_values.entry("field_float").or_insert(vec![]);
            for _ in 0..19999 {
                entry.push(fbv.as_slice());
            }
        }
        create_points(
            fbb,
            "db",
            "table",
            tags_values,
            fields_values,
            fields_type,
            &time_values,
            num,
        )
    }

    pub fn create_points<'a>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        database: &str,
        table: &str,
        tags: HashMap<&str, Vec<&str>>,
        fields: HashMap<&str, Vec<&[u8]>>,
        fields_type: HashMap<&str, FieldType>,
        time: &[i64],
        num: usize,
    ) -> WIPOffset<Points<'a>> {
        let nullbits = BitSet::with_size_all_set(num);
        let fb_nullbits = fbb.create_vector(nullbits.bytes());
        let db = fbb.create_string(database);
        let table = fbb.create_string(table);
        let mut columns = vec![];

        for (tag_key, tag_value) in tags.iter() {
            let col_name = fbb.create_string(tag_key);
            let mut col_values = vec![];
            for value in tag_value {
                col_values.push(fbb.create_string(value));
            }
            let values = fbb.create_vector(&col_values);
            let mut values_builder = ValuesBuilder::new(fbb);
            values_builder.add_string_value(values);
            let values = values_builder.finish();
            let mut col_builder = ColumnBuilder::new(fbb);
            col_builder.add_field_type(FieldType::String);
            col_builder.add_column_type(ColumnType::Tag);
            col_builder.add_name(col_name);
            col_builder.add_col_values(values);
            col_builder.add_nullbits(fb_nullbits);
            let col = col_builder.finish();
            columns.push(col);
        }

        for (field_name, field_value) in fields.iter() {
            let col_name = fbb.create_string(field_name);
            match *fields_type.get(field_name).unwrap() {
                FieldType::Float => {
                    let mut col_values = vec![];
                    for value in field_value {
                        col_values.push(f64::from_be_bytes((*value).try_into().unwrap()));
                    }
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_float_value(values);
                    let values = values_builder.finish();
                    let mut col_builder = ColumnBuilder::new(fbb);
                    col_builder.add_field_type(FieldType::Float);
                    col_builder.add_column_type(ColumnType::Field);
                    col_builder.add_name(col_name);
                    col_builder.add_col_values(values);
                    col_builder.add_nullbits(fb_nullbits);
                    let col = col_builder.finish();
                    columns.push(col);
                }
                FieldType::Integer => {
                    let mut col_values = vec![];
                    for value in field_value {
                        col_values.push(i64::from_be_bytes((*value).try_into().unwrap()));
                    }
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_int_value(values);
                    let values = values_builder.finish();
                    let mut col_builder = ColumnBuilder::new(fbb);
                    col_builder.add_field_type(FieldType::Integer);
                    col_builder.add_column_type(ColumnType::Field);
                    col_builder.add_name(col_name);
                    col_builder.add_col_values(values);
                    col_builder.add_nullbits(fb_nullbits);
                    let col = col_builder.finish();
                    columns.push(col);
                }
                FieldType::String => {
                    let mut col_values = vec![];
                    for value in field_value {
                        col_values.push(fbb.create_string(std::str::from_utf8(value).unwrap()));
                    }
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_string_value(values);
                    let values = values_builder.finish();
                    let mut col_builder = ColumnBuilder::new(fbb);
                    col_builder.add_field_type(FieldType::String);
                    col_builder.add_column_type(ColumnType::Field);
                    col_builder.add_name(col_name);
                    col_builder.add_col_values(values);
                    col_builder.add_nullbits(fb_nullbits);
                    let col = col_builder.finish();
                    columns.push(col);
                }
                FieldType::Unsigned => {
                    let mut col_values = vec![];
                    for value in field_value {
                        col_values.push(u64::from_be_bytes((*value).try_into().unwrap()));
                    }
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_uint_value(values);
                    let values = values_builder.finish();
                    let mut col_builder = ColumnBuilder::new(fbb);
                    col_builder.add_field_type(FieldType::Unsigned);
                    col_builder.add_column_type(ColumnType::Field);
                    col_builder.add_name(col_name);
                    col_builder.add_col_values(values);
                    col_builder.add_nullbits(fb_nullbits);
                    let col = col_builder.finish();
                    columns.push(col);
                }
                FieldType::Boolean => {
                    let mut col_values = vec![];
                    for value in field_value {
                        col_values.push(value[0] != 0);
                    }
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_bool_value(values);
                    let values = values_builder.finish();
                    let mut col_builder = ColumnBuilder::new(fbb);
                    col_builder.add_field_type(FieldType::Boolean);
                    col_builder.add_column_type(ColumnType::Field);
                    col_builder.add_name(col_name);
                    col_builder.add_col_values(values);
                    col_builder.add_nullbits(fb_nullbits);
                    let col = col_builder.finish();
                    columns.push(col);
                }
                _ => {
                    panic!("Unsupported field type");
                }
            }
        }
        let mut time_col_values = vec![];
        for value in time {
            time_col_values.push(value);
        }

        let values = fbb.create_vector(&time_col_values);
        let mut values_builder = ValuesBuilder::new(fbb);
        values_builder.add_int_value(values);
        let values = values_builder.finish();
        let name = fbb.create_string("time");
        let mut col_builder = ColumnBuilder::new(fbb);
        col_builder.add_field_type(FieldType::Integer);
        col_builder.add_column_type(ColumnType::Time);
        col_builder.add_name(name);
        col_builder.add_col_values(values);
        col_builder.add_nullbits(fb_nullbits);
        let col = col_builder.finish();
        columns.push(col);

        let columns = fbb.create_vector(&columns);
        let mut table_builder = TableBuilder::new(fbb);
        table_builder.add_tab(table);
        table_builder.add_columns(columns);
        table_builder.add_num_rows(num as u64);
        let table = table_builder.finish();
        let tables = vec![table];
        let tables = fbb.create_vector(&tables);

        let mut points_builder = PointsBuilder::new(fbb);
        points_builder.add_db(db);
        points_builder.add_tables(tables);

        points_builder.finish()
    }
}
