use std::sync::Arc;

use datafusion::arrow::array::{downcast_array, ArrayRef, Float64Builder, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_plan::functions::make_scalar_function;
use geo::{
    EuclideanDistance, Geometry, Line, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon, Triangle,
};
use geozero::wkt::WktStr;
use geozero::ToGeo;
use spi::query::function::FunctionMetadataManager;
use spi::Result;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let fun = make_scalar_function(func);

    let signature = Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Float64)));

    ScalarUDF::new("st_distance", &signature, &return_type, &fun)
}

fn func(args: &[ArrayRef]) -> DFResult<ArrayRef> {
    let geo1 = args[0].as_ref();
    let geo2 = args[1].as_ref();

    let mut builder = Float64Builder::with_capacity(geo1.len());

    let geo1 = downcast_array::<StringArray>(geo1);
    let geo2 = downcast_array::<StringArray>(geo2);

    geo1.iter()
        .zip(geo2.iter())
        .try_for_each(|(l, r)| {
            match (l, r) {
                (None, _) | (_, None) => builder.append_null(),
                (Some(l), Some(r)) => {
                    let wkt_l = WktStr(l);
                    let wkt_r = WktStr(r);

                    let geo_l = wkt_l
                        .to_geo()
                        .map_err(|err| DataFusionError::Execution(err.to_string()))?;
                    let geo_r = wkt_r
                        .to_geo()
                        .map_err(|err| DataFusionError::Execution(err.to_string()))?;

                    let distance = distance(&geo_l, &geo_r)?;

                    builder.append_value(distance)
                }
            };

            Ok::<(), DataFusionError>(())
        })
        .map_err(|err| DataFusionError::Execution(err.to_string()))?;

    let result = builder.finish();

    Ok(Arc::new(result))
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
        Geometry::Point(rhs) => point.euclidean_distance(rhs),
        Geometry::Line(rhs) => point.euclidean_distance(rhs),
        Geometry::LineString(rhs) => point.euclidean_distance(rhs),
        Geometry::Polygon(rhs) => point.euclidean_distance(rhs),
        Geometry::MultiPoint(rhs) => point.euclidean_distance(rhs),
        Geometry::MultiLineString(rhs) => point.euclidean_distance(rhs),
        Geometry::MultiPolygon(rhs) => point.euclidean_distance(rhs),
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
        Geometry::Point(rhs) => point.euclidean_distance(rhs),
        Geometry::Line(rhs) => point.euclidean_distance(rhs),
        Geometry::LineString(rhs) => point.euclidean_distance(rhs),
        Geometry::Polygon(rhs) => point.euclidean_distance(rhs),
        Geometry::MultiPolygon(rhs) => point.euclidean_distance(rhs),
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
        Geometry::Point(rhs) => point.euclidean_distance(rhs),
        Geometry::Line(rhs) => point.euclidean_distance(rhs),
        Geometry::LineString(rhs) => point.euclidean_distance(rhs),
        Geometry::Polygon(rhs) => point.euclidean_distance(rhs),
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
        Geometry::Point(rhs) => point.euclidean_distance(rhs),
        Geometry::Line(rhs) => point.euclidean_distance(rhs),
        Geometry::LineString(rhs) => point.euclidean_distance(rhs),
        Geometry::Polygon(rhs) => point.euclidean_distance(rhs),
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
        Geometry::Point(rhs) => point.euclidean_distance(rhs),
        Geometry::Line(rhs) => point.euclidean_distance(rhs),
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
        Geometry::Point(rhs) => point.euclidean_distance(rhs),
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
        Geometry::Point(rhs) => point.euclidean_distance(rhs),
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
        Geometry::Point(rhs) => point.euclidean_distance(rhs),
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
