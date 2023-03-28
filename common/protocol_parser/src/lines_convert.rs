use std::collections::HashMap;

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use protos::models::{FieldType, Point, PointArgs, Points, PointsArgs, TableBuilder};
use protos::{
    build_fb_schema_offset, init_fields_and_nullbits, init_tags_and_nullbits, insert_field,
    insert_tag, FbSchema,
};

use crate::{FieldValue, Line};

pub fn line_to_point<'a, 'fbb: 'mut_fbb, 'mut_fbb>(
    line: &'a Line,
    schema: &FbSchema,
    fbb: &'mut_fbb mut FlatBufferBuilder<'fbb>,
) -> WIPOffset<Point<'fbb>> {
    let (mut tags, mut tags_nullbit) = init_tags_and_nullbits(fbb, schema);

    line.tags.iter().for_each(|(k, v)| {
        insert_tag(fbb, &mut tags, &mut tags_nullbit, schema, k, v);
    });

    let (mut fields, mut fields_nullbits) = init_fields_and_nullbits(fbb, schema);

    line.fields.iter().for_each(|(k, v)| {
        insert_field(fbb, &mut fields, &mut fields_nullbits, schema, k, v);
    });

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
        let table = line.table;
        let schema: &mut FbSchema = schemas.entry(table).or_default();
        for (tag_key, _) in line.tags.iter() {
            schema.add_tag(tag_key);
        }
        for (k, v) in line.fields.iter() {
            match v {
                FieldValue::U64(_) => schema.add_field(k, FieldType::Unsigned),
                FieldValue::I64(_) => schema.add_field(k, FieldType::Integer),
                FieldValue::Str(_) => schema.add_field(k, FieldType::String),
                FieldValue::F64(_) => schema.add_field(k, FieldType::Float),
                FieldValue::Bool(_) => schema.add_field(k, FieldType::Boolean),
            };
        }
    }
    schemas
}

// Walk through all the rows, divided by table name， table_name -> line index
pub fn build_table_line_index<'a>(lines: &'a [Line]) -> HashMap<&'a str, Vec<usize>> {
    let mut schemas = HashMap::new();
    for (idx, line) in lines.iter().enumerate() {
        let idxs: &mut Vec<usize> = schemas.entry(line.table).or_default();
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

#[cfg(test)]
mod test {
    use protos::models::Points;
    use protos::FieldValue::{F64, I64};

    use crate::lines_convert::parse_lines_to_points;
    use crate::Line;

    #[test]
    fn test_parse_line() {
        let line1 = Line {
            hash_id: 0,
            table: "test0",
            tags: vec![("ta", "a1"), ("tb", "b1")],
            fields: vec![("fa", F64(1.0)), ("fb", I64(1))],
            timestamp: 1,
        };

        let line2 = Line {
            hash_id: 0,
            table: "test0",
            tags: vec![("ta", "a2"), ("tb", "b2")],
            fields: vec![("fa", F64(2.0)), ("fb", I64(2))],
            timestamp: 2,
        };

        let line3 = Line {
            hash_id: 0,
            table: "test1",
            tags: vec![("ta", "a2"), ("tb", "b2")],
            fields: vec![("fa", F64(2.0)), ("fb", I64(2))],
            timestamp: 3,
        };

        let lines = vec![line1, line2, line3];

        let points = parse_lines_to_points("test", &lines);

        let fb_points = flatbuffers::root::<Points>(&points).unwrap();

        assert_eq!(
            String::from_utf8(fb_points.db().unwrap().bytes().to_vec()).unwrap(),
            "test"
        );

        let mut res_table_names = vec![];
        let expect_table_names = vec!["test0".to_string(), "test1".to_string()];

        let mut res_tag_names = vec![];
        let expected_tag_names = vec!["ta", "ta", "tb", "tb"];
        let mut res_field_names = vec![];
        let expected_field_names = vec!["fa", "fa", "fb", "fb"];
        let mut res_tag_values = vec![];
        let expected_tag_values = vec![
            "a1".to_string(),
            "a2".to_string(),
            "a2".to_string(),
            "b1".to_string(),
            "b2".to_string(),
            "b2".to_string(),
        ];
        for table in fb_points.tables().unwrap() {
            res_table_names.push(String::from_utf8(table.tab().unwrap().bytes().to_vec()).unwrap());
            let schema = table.schema().unwrap();
            res_tag_names.extend(schema.tag_name().unwrap().iter());
            res_field_names.extend(schema.field_name().unwrap().iter());
            for point in table.points().unwrap() {
                res_tag_values.extend(
                    point.tags().unwrap().iter().map(|tag| {
                        String::from_utf8(tag.value().unwrap().bytes().to_vec()).unwrap()
                    }),
                );
            }
        }

        res_table_names.sort();
        assert_eq!(expect_table_names, res_table_names);
        res_tag_names.sort();
        res_field_names.sort();
        res_tag_values.sort();
        assert_eq!(res_field_names, expected_field_names);
        assert_eq!(res_tag_names, expected_tag_names);
        assert_eq!(res_tag_values, expected_tag_values);
    }
}
