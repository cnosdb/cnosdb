use datafusion::arrow::array::Float64Builder;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ScalarUDF;
use geo::{
    Distance as _, Euclidean, Geometry, Line, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon, Triangle,
};
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use crate::geometry_binary_op;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<ScalarUDF> {
    let udf = geometry_binary_op!("ST_Distance", distance, DataType::Float64, Float64Builder);
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn distance(geo_l: &Geometry, geo_r: &Geometry) -> DFResult<f64> {
    let distance = match (geo_l, geo_r) {
        (Geometry::Point(p), other) => point_distance(p, other)?,
        (Geometry::Line(p), other) => line_distance(p, other)?,
        (Geometry::LineString(p), other) => line_string_distance(p, other)?,
        (Geometry::Polygon(p), other) => polygon_distance(p, other)?,
        (Geometry::MultiPoint(p), other) => multi_point_distance(p, other)?,
        (Geometry::MultiLineString(p), other) => multi_line_string_distance(p, other)?,
        (Geometry::MultiPolygon(p), other) => multi_polygon_distance(p, other)?,
        (Geometry::Triangle(p), other) => triangle_distance(p, other)?,
        (geo_l, geo_r) => {
            return Err(DataFusionError::Execution(format!(
                "Calculating the distance between {:?} and {:?} is not supported",
                geo_l, geo_r
            )));
        }
    };

    Ok(distance)
}

fn point_distance(point: &Point, other: &Geometry) -> DFResult<f64> {
    let dist = match other {
        Geometry::Point(rhs) => Euclidean.distance(point, rhs),
        Geometry::Line(rhs) => Euclidean.distance(point, rhs),
        Geometry::LineString(rhs) => Euclidean.distance(point, rhs),
        Geometry::Polygon(rhs) => Euclidean.distance(point, rhs),
        Geometry::MultiPoint(rhs) => Euclidean.distance(point, rhs),
        Geometry::MultiLineString(rhs) => Euclidean.distance(point, rhs),
        Geometry::MultiPolygon(rhs) => Euclidean.distance(point, rhs),
        Geometry::GeometryCollection(_) | Geometry::Rect(_) | Geometry::Triangle(_) => {
            return Err(DataFusionError::Execution(format!(
                "Calculating the distance between POINT and {:?} is not supported",
                other
            )));
        }
    };

    Ok(dist)
}

fn line_distance(point: &Line, other: &Geometry) -> DFResult<f64> {
    let dist = match other {
        Geometry::Point(rhs) => Euclidean.distance(point, rhs),
        Geometry::Line(rhs) => Euclidean.distance(point, rhs),
        Geometry::LineString(rhs) => Euclidean.distance(point, rhs),
        Geometry::Polygon(rhs) => Euclidean.distance(point, rhs),
        Geometry::MultiPolygon(rhs) => Euclidean.distance(point, rhs),
        Geometry::MultiPoint(_)
        | Geometry::MultiLineString(_)
        | Geometry::GeometryCollection(_)
        | Geometry::Rect(_)
        | Geometry::Triangle(_) => {
            return Err(DataFusionError::Execution(format!(
                "Calculating the distance between LINE and {:?} is not supported",
                other
            )));
        }
    };

    Ok(dist)
}

fn line_string_distance(point: &LineString, other: &Geometry) -> DFResult<f64> {
    let dist = match other {
        Geometry::Point(rhs) => Euclidean.distance(point, rhs),
        Geometry::Line(rhs) => Euclidean.distance(point, rhs),
        Geometry::LineString(rhs) => Euclidean.distance(point, rhs),
        Geometry::Polygon(rhs) => Euclidean.distance(point, rhs),
        Geometry::MultiPolygon(_)
        | Geometry::MultiPoint(_)
        | Geometry::MultiLineString(_)
        | Geometry::GeometryCollection(_)
        | Geometry::Rect(_)
        | Geometry::Triangle(_) => {
            return Err(DataFusionError::Execution(format!(
                "Calculating the distance between LINESTRING and {:?} is not supported",
                other
            )));
        }
    };

    Ok(dist)
}

fn polygon_distance(point: &Polygon, other: &Geometry) -> DFResult<f64> {
    let dist = match other {
        Geometry::Point(rhs) => Euclidean.distance(point, rhs),
        Geometry::Line(rhs) => Euclidean.distance(point, rhs),
        Geometry::LineString(rhs) => Euclidean.distance(point, rhs),
        Geometry::Polygon(rhs) => Euclidean.distance(point, rhs),
        Geometry::MultiPolygon(_)
        | Geometry::MultiPoint(_)
        | Geometry::MultiLineString(_)
        | Geometry::GeometryCollection(_)
        | Geometry::Rect(_)
        | Geometry::Triangle(_) => {
            return Err(DataFusionError::Execution(format!(
                "Calculating the distance between POLYGON and {:?} is not supported",
                other
            )));
        }
    };

    Ok(dist)
}

fn multi_polygon_distance(point: &MultiPolygon, other: &Geometry) -> DFResult<f64> {
    let dist = match other {
        Geometry::Point(rhs) => Euclidean.distance(point, rhs),
        Geometry::Line(rhs) => Euclidean.distance(point, rhs),
        Geometry::LineString(_)
        | Geometry::Polygon(_)
        | Geometry::MultiPolygon(_)
        | Geometry::MultiPoint(_)
        | Geometry::MultiLineString(_)
        | Geometry::GeometryCollection(_)
        | Geometry::Rect(_)
        | Geometry::Triangle(_) => {
            return Err(DataFusionError::Execution(format!(
                "Calculating the distance between MULTIPOLYGON and {:?} is not supported",
                other
            )));
        }
    };

    Ok(dist)
}

fn multi_point_distance(point: &MultiPoint, other: &Geometry) -> DFResult<f64> {
    let dist = match other {
        Geometry::Point(rhs) => Euclidean.distance(point, rhs),
        Geometry::Line(_)
        | Geometry::LineString(_)
        | Geometry::Polygon(_)
        | Geometry::MultiPolygon(_)
        | Geometry::MultiPoint(_)
        | Geometry::MultiLineString(_)
        | Geometry::GeometryCollection(_)
        | Geometry::Rect(_)
        | Geometry::Triangle(_) => {
            return Err(DataFusionError::Execution(format!(
                "Calculating the distance between MULTIPOINT and {:?} is not supported",
                other
            )));
        }
    };

    Ok(dist)
}

fn multi_line_string_distance(point: &MultiLineString, other: &Geometry) -> DFResult<f64> {
    let dist = match other {
        Geometry::Point(rhs) => Euclidean.distance(point, rhs),
        Geometry::Line(_)
        | Geometry::LineString(_)
        | Geometry::Polygon(_)
        | Geometry::MultiPolygon(_)
        | Geometry::MultiPoint(_)
        | Geometry::MultiLineString(_)
        | Geometry::GeometryCollection(_)
        | Geometry::Rect(_)
        | Geometry::Triangle(_) => {
            return Err(DataFusionError::Execution(format!(
                "Calculating the distance between MULTILINESTRING and {:?} is not supported",
                other
            )));
        }
    };

    Ok(dist)
}

fn triangle_distance(point: &Triangle, other: &Geometry) -> DFResult<f64> {
    let dist = match other {
        Geometry::Point(rhs) => Euclidean.distance(point, rhs),
        Geometry::Line(_)
        | Geometry::LineString(_)
        | Geometry::Polygon(_)
        | Geometry::MultiPolygon(_)
        | Geometry::MultiPoint(_)
        | Geometry::MultiLineString(_)
        | Geometry::GeometryCollection(_)
        | Geometry::Rect(_)
        | Geometry::Triangle(_) => {
            return Err(DataFusionError::Execution(format!(
                "Calculating the distance between TRIANGLE and {:?} is not supported",
                other
            )));
        }
    };

    Ok(dist)
}
