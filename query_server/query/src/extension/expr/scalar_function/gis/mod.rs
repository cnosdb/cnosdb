mod st_distance;

use spi::query::function::FunctionMetadataManager;
use spi::Result;

pub fn register_udfs(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    st_distance::register_udf(func_manager)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use geo::{line_string, point, EuclideanDistance, Geometry, Point};
    use geozero::wkb::Wkb;
    use geozero::wkt::WktStr;
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
        let wht = WktStr("POINT(1.2 1.3)");

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

        let dist = ls.euclidean_distance(&p);

        assert_eq!(66.9734009226357, dist)
    }
}
