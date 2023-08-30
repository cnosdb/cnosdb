use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone, Eq, Hash)]
#[non_exhaustive]
pub struct Geometry {
    pub sub_type: GeometryType,
    pub srid: i16,
}

impl Geometry {
    pub fn new_with_srid(sub_type: GeometryType, srid: i16) -> Self {
        Self { sub_type, srid }
    }
}

impl Display for Geometry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Geometry({}, {})", self.sub_type, self.srid)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone, Eq, Hash)]
pub enum GeometryType {
    Point,
    Linestring,
    Polygon,
    Multipoint,
    Multilinestring,
    Multipolygon,
    Geometrycollection,
}

impl Display for GeometryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Point => write!(f, "POINT"),
            Self::Linestring => write!(f, "LINESTRING"),
            Self::Polygon => write!(f, "POLYGON"),
            Self::Multipoint => write!(f, "MULTIPOINT"),
            Self::Multilinestring => write!(f, "MULTILINESTRING"),
            Self::Multipolygon => write!(f, "MULTIPOLYGON"),
            Self::Geometrycollection => write!(f, "GEOMETRYCOLLECTION"),
        }
    }
}
