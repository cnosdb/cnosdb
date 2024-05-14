use std::io::{Error, Read, Seek};

use models::field_value::FieldVal;
use stl_io;

use super::parser::PointCloud;

pub fn parse<T: Read + Seek>(f: &mut T) -> Result<PointCloud, Error> {
    let mesh = stl_io::read_stl(f)?;

    let mut data_point = PointCloud::default();
    for vertex in mesh.vertices {
        data_point.insert_to_vertex("x", FieldVal::Float(vertex[0] as f64));
        data_point.insert_to_vertex("y", FieldVal::Float(vertex[1] as f64));
        data_point.insert_to_vertex("z", FieldVal::Float(vertex[2] as f64));

        data_point.vertex_row_count += 1;
    }

    for face in mesh.faces {
        data_point.insert_to_face("v-0", FieldVal::Unsigned(face.vertices[0] as u64));
        data_point.insert_to_face("v-1", FieldVal::Unsigned(face.vertices[1] as u64));
        data_point.insert_to_face("v-2", FieldVal::Unsigned(face.vertices[2] as u64));

        data_point.face_row_count += 1;
    }

    Ok(data_point)
}
