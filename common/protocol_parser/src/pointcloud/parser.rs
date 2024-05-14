use std::collections::BTreeMap;

use models::field_value::FieldVal;
use models::schema::ColumnType;
use models::ValueType;
use snafu::Snafu;

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum Error {
    #[snafu(display("{}", content))]
    Common { content: String },

    #[snafu(display("Invalid pointcloud file format"))]
    ErrorFormat,

    #[snafu(display("io error"))]
    IOError,

    #[snafu(display("parse error"))]
    ParseError,
}

impl From<std::io::Error> for Error {
    fn from(_value: std::io::Error) -> Self {
        Self::IOError
    }
}

impl From<off_rs::Error> for Error {
    fn from(value: off_rs::Error) -> Self {
        match value {
            off_rs::Error::IOError(_) => Self::IOError,
            off_rs::Error::ParserError(_) => Self::ParseError,
        }
    }
}

pub struct PointCloud {
    pub vertex_element: BTreeMap<String, Column>,
    pub face_element: BTreeMap<String, Column>,
    // WIP: (ply file has some custom properties)
    pub custom_element: BTreeMap<String, Column>,

    pub vertex_row_count: usize,
    pub face_row_count: usize,
    pub custom_row_count: usize,
}

impl PointCloud {
    pub fn new() -> Self {
        Self {
            vertex_element: BTreeMap::new(),
            face_element: BTreeMap::new(),
            custom_element: BTreeMap::new(),

            vertex_row_count: 0,
            face_row_count: 0,
            custom_row_count: 0,
        }
    }
    pub fn insert_to_vertex(&mut self, column: &str, value: FieldVal) {
        let col = match self.vertex_element.get_mut(column) {
            Some(idx) => idx,
            None => {
                let col = Column::new(self.vertex_row_count, &value);
                self.vertex_element.insert(column.to_string(), col);
                self.vertex_element.get_mut(column).unwrap()
            }
        };

        col.append(self.vertex_row_count);
        col.push(value);
    }

    pub fn insert_to_face(&mut self, column: &str, value: FieldVal) {
        let col = match self.face_element.get_mut(column) {
            Some(idx) => idx,
            None => {
                let col = Column::new(self.face_row_count, &value);
                self.face_element.insert(column.to_string(), col);
                self.face_element.get_mut(column).unwrap()
            }
        };

        col.append(self.face_row_count);
        col.push(value);
    }

    pub fn finish(&mut self) {
        // to make all the columns have the same length
        for face in self.face_element.values_mut() {
            face.append(self.face_row_count);
        }
    }
}

impl Default for PointCloud {
    fn default() -> Self {
        Self::new()
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub enum FileFormat {
    PLY = 1,
    OFF,
    STL,
    UNKNOWN,
}

impl From<String> for FileFormat {
    fn from(value: String) -> Self {
        match value.as_str() {
            "ply" => Self::PLY,
            "off" => Self::OFF,
            "stl" => Self::STL,
            _ => Self::UNKNOWN,
        }
    }
}

impl From<&str> for FileFormat {
    fn from(value: &str) -> Self {
        match value {
            "ply" => Self::PLY,
            "off" => Self::OFF,
            "stl" => Self::STL,
            _ => Self::UNKNOWN,
        }
    }
}

#[derive(Debug)]
pub struct Column {
    pub col_type: ColumnType,
    pub data: ColumnData,
}

impl Column {
    pub fn push(&mut self, value: FieldVal) {
        assert!(self.check(&value));
        self.data.push(value);
    }

    pub fn new(row_count: usize, value: &FieldVal) -> Self {
        let data = ColumnData::new(row_count, value);

        let col_type = match value {
            FieldVal::Float(_) => ColumnType::Field(ValueType::Float),
            FieldVal::Unsigned(_) => ColumnType::Field(ValueType::Unsigned),
            FieldVal::Integer(_) => ColumnType::Field(ValueType::Integer),
            _ => unimplemented!(),
        };

        Self { col_type, data }
    }

    pub fn append(&mut self, row_count: usize) {
        self.data.append(row_count)
    }

    fn check(&self, value: &FieldVal) -> bool {
        matches!(
            (&self.col_type, value),
            (ColumnType::Field(ValueType::Float), FieldVal::Float(_))
                | (
                    ColumnType::Field(ValueType::Unsigned),
                    FieldVal::Unsigned(_)
                )
                | (ColumnType::Field(ValueType::Integer), FieldVal::Integer(_))
        )
    }
}

#[derive(Debug)]
pub enum ColumnData {
    F64(Vec<Option<f64>>),
    U64(Vec<Option<u64>>),
    I64(Vec<Option<i64>>),
}

impl ColumnData {
    pub fn push(&mut self, value: FieldVal) {
        match (self, value) {
            (ColumnData::F64(col), FieldVal::Float(val)) => {
                col.push(Some(val));
            }
            (ColumnData::U64(col), FieldVal::Unsigned(val)) => col.push(Some(val)),
            (ColumnData::I64(col), FieldVal::Integer(val)) => col.push(Some(val)),
            _ => (),
        }
    }

    pub fn new(row_count: usize, value: &FieldVal) -> Self {
        match value {
            FieldVal::Float(_) => ColumnData::F64(vec![None; row_count]),
            FieldVal::Unsigned(_) => ColumnData::U64(vec![None; row_count]),
            FieldVal::Integer(_) => ColumnData::I64(vec![None; row_count]),
            _ => unimplemented!(),
        }
    }

    pub fn append(&mut self, row_count: usize) {
        match self {
            ColumnData::F64(col) => col.append(&mut vec![None; row_count - col.len()]),
            ColumnData::U64(col) => col.append(&mut vec![None; row_count - col.len()]),
            ColumnData::I64(col) => col.append(&mut vec![None; row_count - col.len()]),
        }
    }
}
