use std::fmt::Display;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone, Eq, Hash, Ord, PartialOrd)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone, Eq, Hash, Ord, PartialOrd)]
pub enum GeometryType {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
}

impl Display for GeometryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Point => write!(f, "POINT"),
            Self::LineString => write!(f, "LINESTRING"),
            Self::Polygon => write!(f, "POLYGON"),
            Self::MultiPoint => write!(f, "MULTIPOINT"),
            Self::MultiLineString => write!(f, "MULTILINESTRING"),
            Self::MultiPolygon => write!(f, "MULTIPOLYGON"),
            Self::GeometryCollection => write!(f, "GEOMETRYCOLLECTION"),
        }
    }
}

impl FromStr for GeometryType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "POINT" => Ok(Self::Point),
            "LINESTRING" => Ok(Self::LineString),
            "POLYGON" => Ok(Self::Polygon),
            "MULTIPOINT" => Ok(Self::MultiPoint),
            "MULTILINESTRING" => Ok(Self::MultiLineString),
            "MULTIPOLYGON" => Ok(Self::MultiPolygon),
            "GEOMETRYCOLLECTION" => Ok(Self::GeometryCollection),
            other => {
                Err(format!("Invalid geometry type: {}, excepted: POINT | LINESTRING | POLYGON | MULTIPOINT | MULTILINESTRING | MULTIPOLYGON | GEOMETRYCOLLECTION", other))
            }
        }
    }
}
