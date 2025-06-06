use std::fmt::{Debug, Display};

use datafusion::arrow::array::{downcast_array, Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use geo::Geometry;
use geozero::wkt::Wkt;
use geozero::ToGeo;
use models::gis::data_type::GeometryType;
use spi::QueryError;

use super::AssertExpr;

#[derive(Debug)]
pub struct AssertGeomType {
    geom_with_idx: Vec<(GeometryType, usize)>,
}

impl AssertGeomType {
    pub fn new(geom_with_idx: Vec<(GeometryType, usize)>) -> Self {
        Self { geom_with_idx }
    }
}

impl AssertExpr for AssertGeomType {
    fn assert(&self, batch: &RecordBatch) -> DFResult<()> {
        for (sub_type, idx) in &self.geom_with_idx {
            let column = batch.column(*idx).as_ref();
            check_wkt(sub_type, column)?;
        }

        Ok(())
    }
}

impl Display for AssertGeomType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = self
            .geom_with_idx
            .iter()
            .map(|(_, idx)| idx.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "AssertGeomType({})", str)
    }
}

macro_rules! define_check_wkt {
    ($(
        $sub_type:ident $(= $string_keyword:expr)?
    ),*) => {
            fn check_wkt(sub_type: &GeometryType, array: &dyn Array) -> DFResult<()> {
                let str_array = downcast_array::<StringArray>(array);

                match sub_type {
                    $(GeometryType::$sub_type => {
                        for ele in str_array.iter().flatten() {
                            let geom = Wkt(ele).to_geo().map_err(|err| {
                                DataFusionError::External(Box::new(QueryError::InvalidGeometryType {
                                    reason: format!("{}, expect {}, got {:?}", err, stringify!($sub_type), ele),
                                }))
                            })?;

                            if let Geometry::$sub_type(_) = &geom {
                                continue;
                            }

                            return Err(DataFusionError::External(Box::new(
                                QueryError::InvalidGeometryType {
                                    reason: format!("expect {}, got {:?}", stringify!($sub_type), geom),
                                },
                            )));
                        }
                    }),*
                }

                Ok(())
            }
    };
}

define_check_wkt!(
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection
);
