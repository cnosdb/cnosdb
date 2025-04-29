mod st_area;
mod st_asbinary;
mod st_binary_op;
mod st_distance;
mod st_geomfromwkb;

use datafusion::error::DataFusionError;
use geo::Geometry;
use geozero::wkt::Wkt;
use geozero::ToGeo;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

pub fn register_udfs(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    st_distance::register_udf(func_manager)?;
    st_geomfromwkb::register_udf(func_manager)?;
    st_asbinary::register_udf(func_manager)?;
    st_area::register_udf(func_manager)?;
    st_binary_op::register_udf(func_manager)?;
    Ok(())
}

pub fn str_to_geo(wkt: &str) -> Result<Geometry, DataFusionError> {
    Wkt(wkt)
        .to_geo()
        .map_err(|err| DataFusionError::Execution(err.to_string()))
}

/// $FUNC_NAME: &str
/// $RES_TYPE: datafusion::DataType
/// $OP: fn(&Geometry) -> Result<$RES_TYPE, DataFusionError>
/// $RES_ARRAY_BUILDER: impl ArrayBuilder
#[macro_export]
macro_rules! geometry_unary_op {
    ($FUNC_NAME:expr, $OP:ident,
    $RES_TYPE: expr,
    $RES_ARRAY_BUILDER: ident) => {{
        use std::sync::Arc;

        use datafusion::arrow::array::{downcast_array, StringArray};
        use datafusion::common::DataFusionError;
        use datafusion::error::Result as DFResult;
        use datafusion::logical_expr::{
            ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
        };
        use $crate::extension::expr::scalar_function::gis::str_to_geo;

        #[derive(Debug)]
        pub struct UserDefinedFunctionImpl {
            signature: Signature,
        }
        impl UserDefinedFunctionImpl {
            pub fn new() -> Self {
                Self {
                    signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
                }
            }
        }
        impl ScalarUDFImpl for UserDefinedFunctionImpl {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &str {
                $FUNC_NAME
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
                Ok($RES_TYPE)
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                let args = ColumnarValue::values_to_arrays(&args.args)?;
                let mut builder = $RES_ARRAY_BUILDER::new();
                let geo = args[0].as_ref();
                let geo = downcast_array::<StringArray>(geo);
                geo.iter().try_for_each(|g| {
                    match g {
                        None => builder.append_null(),
                        Some(g) => {
                            let geo = str_to_geo(g)?;
                            let v = $OP(&geo)?;
                            builder.append_value(v);
                        }
                    }
                    Ok::<(), DataFusionError>(())
                })?;
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }

        ScalarUDF::new_from_impl(UserDefinedFunctionImpl::new())
    }};
}

/// $FUNC_NAME: &str
/// $RES_TYPE: datafusion::DataType
/// $OP: fn(&Geometry, &Geometry) -> Result<$RES_TYPE, DataFusionError>
/// $RES_ARRAY_BUILDER: impl ArrayBuilder
#[macro_export]
macro_rules! geometry_binary_op {
    ($FUN_NAME: expr, $OP:ident,
    $RES_TYPE: expr,
    $RES_ARRAY_BUILDER: ident) => {{
        use std::sync::Arc;

        use datafusion::arrow::array::{downcast_array, StringArray};
        use datafusion::error::Result as DFResult;
        use datafusion::logical_expr::{
            ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
        };
        use $crate::extension::expr::scalar_function::gis::str_to_geo;

        #[derive(Debug)]
        pub struct UserDefinedFunctionImpl {
            signature: Signature,
        }
        impl UserDefinedFunctionImpl {
            pub fn new() -> Self {
                Self {
                    signature: Signature::exact(
                        vec![DataType::Utf8, DataType::Utf8],
                        Volatility::Immutable,
                    ),
                }
            }
        }
        impl ScalarUDFImpl for UserDefinedFunctionImpl {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &str {
                $FUN_NAME
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
                Ok($RES_TYPE)
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                let args = ColumnarValue::values_to_arrays(&args.args)?;
                let mut builder = $RES_ARRAY_BUILDER::new();
                let geo1 = args[0].as_ref();
                let geo2 = args[1].as_ref();
                let geo1 = downcast_array::<StringArray>(geo1);
                let geo2 = downcast_array::<StringArray>(geo2);
                geo1.iter().zip(geo2.iter()).try_for_each(|(l, r)| {
                    match (l, r) {
                        (None, _) | (_, None) => builder.append_null(),
                        (Some(l), Some(r)) => {
                            let geo_l = str_to_geo(l)?;
                            let geo_r = str_to_geo(r)?;
                            let v = $OP(&geo_l, &geo_r)?;
                            builder.append_value(v);
                        }
                    }
                    Ok::<(), DataFusionError>(())
                })?;
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }

        ScalarUDF::new_from_impl(UserDefinedFunctionImpl::new())
    }};
}

#[cfg(test)]
mod tests {
    use geo::{line_string, point, Distance, Euclidean, Geometry, Point};
    use geozero::wkb::Wkb;
    use geozero::wkt::Wkt;
    use geozero::{CoordDimensions, ToGeo, ToWkb, ToWkt};

    #[test]
    fn test_from_wkb() {
        let p = point!(x: 1.2, y: 1.3);
        let pe: Geometry = p.into();
        let bytes = pe.to_wkb(CoordDimensions::xy()).unwrap();

        let wkb = Wkb(bytes);

        let geo = wkb.to_geo().unwrap();

        match geo {
            Geometry::Point(p) => {
                assert_eq!(Point::new(1.2, 1.3), p)
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_from_wkt() {
        // POINT
        // LINESTRING
        // POLYGON
        // MULTIPOINT
        // MULTILINESTRING
        // MULTIPOLYGON
        // GEOMETRYCOLLECTION
        // https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
        let wht = Wkt("POINT(1.2 1.3)");

        let geo = wht.to_geo().unwrap();

        match geo {
            Geometry::Point(p) => {
                assert_eq!(Point::new(1.2, 1.3), p)
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_to_wkt() {
        let p = point!(x: 1.2, y: 1.3);
        let pe: Geometry = p.into();

        let wkt = pe.to_wkt().unwrap();

        assert_eq!("POINT(1.2 1.3)", &wkt)
    }

    /// Calculate the minimum euclidean distance between geometries
    #[test]
    fn test_euclidean_distance() {
        let p = point!(x: 1.2, y: 1.3);

        let ls = line_string![
            (x: -21.95156, y: 64.1446),
            (x: -21.951, y: 64.14479),
            (x: -21.95044, y: 64.14527),
            (x: -21.951445, y: 64.145508),
        ];

        let dist = Euclidean.distance(&ls, &p);

        assert_eq!(66.9734009226357, dist)
    }
}
