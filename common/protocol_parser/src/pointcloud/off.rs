use models::field_value::FieldVal;
use off_rs::parser::options::Options;
use off_rs::{self, Error};

use super::parser::PointCloud;
pub fn parse(f: &str) -> Result<PointCloud, Error> {
    let mesh = off_rs::parse(f, Options::default())?;

    let mut data_point = PointCloud::default();
    for vertex in mesh.vertices {
        data_point.insert_to_vertex("x", FieldVal::Float(vertex.position.x as f64));
        data_point.insert_to_vertex("y", FieldVal::Float(vertex.position.y as f64));
        data_point.insert_to_vertex("z", FieldVal::Float(vertex.position.z as f64));

        if let Some(color) = vertex.color {
            data_point.insert_to_vertex("red", FieldVal::Float(color.red as f64));
            data_point.insert_to_vertex("blue", FieldVal::Float(color.blue as f64));
            data_point.insert_to_vertex("green", FieldVal::Float(color.green as f64));
        }
        data_point.vertex_row_count += 1;
    }

    for face in mesh.faces {
        for (idx, vertex_index) in face.vertices.iter().enumerate() {
            data_point.insert_to_face(
                format!("v-{}", idx).as_str(),
                FieldVal::Unsigned(*vertex_index as u64),
            );
        }

        if let Some(color) = face.color {
            data_point.insert_to_face("red", FieldVal::Float(color.red as f64));
            data_point.insert_to_face("blue", FieldVal::Float(color.blue as f64));
            data_point.insert_to_face("green", FieldVal::Float(color.green as f64));
        }
        data_point.face_row_count += 1;
    }

    Ok(data_point)
}
