use std::io::{Error, Read};

use models::field_value::FieldVal;
use ply::ply::Property;
use ply_rs as ply;

use super::parser::PointCloud;
pub fn parse<T: Read>(f: &mut T) -> Result<PointCloud, Error> {
    let inner = ply::parser::Parser::<ply::ply::DefaultElement>::new();
    let ply = inner.read_ply(f)?;

    let mut data_point = PointCloud::default();

    for (name, payload) in ply.payload {
        if name.eq("vertex") {
            // for vertex
            for iter in payload {
                for (col, value) in iter {
                    match value {
                        Property::Float(num) => {
                            data_point.insert_to_vertex(&col, FieldVal::Float(num as f64))
                        }
                        Property::UChar(num) => {
                            data_point.insert_to_vertex(&col, FieldVal::Unsigned(num as u64))
                        }
                        _ => (),
                    }
                }
                data_point.vertex_row_count += 1;
            }
        } else if name.eq("face") {
            // for face
            for iter in payload {
                for (col, value) in iter {
                    match value {
                        Property::ListInt(list) => {
                            for (idx, int) in list.iter().enumerate() {
                                data_point.insert_to_face(
                                    format!("v-{}", idx).as_str(),
                                    FieldVal::Integer(*int as i64),
                                )
                            }
                        }
                        Property::UChar(uchar) => {
                            data_point.insert_to_face(&col, FieldVal::Unsigned(uchar as u64));
                        }
                        _ => (),
                    }
                }
                data_point.face_row_count += 1;
            }
        } else {
            // for custom
        }
    }
    Ok(data_point)
}
