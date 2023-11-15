use datafusion::arrow::array::BooleanBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use geo::{Contains, Geometry, Intersects, Relate};
use spi::query::function::FunctionMetadataManager;
use spi::{DFResult, Result};

use crate::geometry_binary_op;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    let udf = geometry_binary_op!("ST_Equals", equals, DataType::Boolean, BooleanBuilder);
    func_manager.register_udf(udf)?;
    let udf = geometry_binary_op!("ST_Contains", contains, DataType::Boolean, BooleanBuilder);
    func_manager.register_udf(udf)?;
    let udf = geometry_binary_op!(
        "ST_Intersects",
        intersects,
        DataType::Boolean,
        BooleanBuilder
    );
    func_manager.register_udf(udf)?;
    let udf = geometry_binary_op!("ST_Disjoint", disjoint, DataType::Boolean, BooleanBuilder);
    func_manager.register_udf(udf)?;
    let udf = geometry_binary_op!("ST_Within", with_in, DataType::Boolean, BooleanBuilder);
    func_manager.register_udf(udf)?;
    Ok(())
}

fn equals(l: &Geometry, r: &Geometry) -> DFResult<bool> {
    Ok(l.relate(r).is_within() && r.relate(l).is_within())
}

fn contains(l: &Geometry, r: &Geometry) -> DFResult<bool> {
    Ok(l.contains(r))
}

fn intersects(l: &Geometry, r: &Geometry) -> DFResult<bool> {
    Ok(l.intersects(r))
}

fn disjoint(l: &Geometry, r: &Geometry) -> DFResult<bool> {
    Ok(l.relate(r).is_disjoint())
}

fn with_in(l: &Geometry, r: &Geometry) -> DFResult<bool> {
    Ok(l.relate(r).is_within())
}
