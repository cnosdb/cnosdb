use std::any::Any;
use std::sync::Arc;

pub use binary::BinaryStatistics;
pub use boolean::BooleanStatistics;
use models::PhysicalDType;
pub use primitive::PrimitiveStatistics;

use crate::error::Result;
use crate::tsm2::page::PageStatistics;
use crate::Error;

mod binary;
mod boolean;
mod primitive;

/// A trait used to describe specific statistics. Each physical type has its own struct.
/// Match the [`Statistics::physical_type`] to each type and downcast accordingly.
pub trait Statistics: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    fn physical_type(&self) -> &PhysicalDType;

    fn null_count(&self) -> Option<i64>;
}

impl PartialEq for &dyn Statistics {
    fn eq(&self, other: &Self) -> bool {
        self.physical_type() == other.physical_type() && {
            match self.physical_type() {
                PhysicalDType::Boolean => {
                    self.as_any().downcast_ref::<BooleanStatistics>().unwrap()
                        == other.as_any().downcast_ref::<BooleanStatistics>().unwrap()
                }
                PhysicalDType::Integer => {
                    self.as_any()
                        .downcast_ref::<PrimitiveStatistics<i64>>()
                        .unwrap()
                        == other
                            .as_any()
                            .downcast_ref::<PrimitiveStatistics<i64>>()
                            .unwrap()
                }
                PhysicalDType::Unsigned => {
                    self.as_any()
                        .downcast_ref::<PrimitiveStatistics<u64>>()
                        .unwrap()
                        == other
                            .as_any()
                            .downcast_ref::<PrimitiveStatistics<u64>>()
                            .unwrap()
                }
                PhysicalDType::Float => {
                    self.as_any()
                        .downcast_ref::<PrimitiveStatistics<f64>>()
                        .unwrap()
                        == other
                            .as_any()
                            .downcast_ref::<PrimitiveStatistics<f64>>()
                            .unwrap()
                }
                PhysicalDType::String => {
                    self.as_any().downcast_ref::<BinaryStatistics>().unwrap()
                        == other.as_any().downcast_ref::<BinaryStatistics>().unwrap()
                }
                _ => {
                    panic!("Unexpected data type")
                }
            }
        }
    }
}

/// Deserializes a raw statistics into [`Statistics`].
/// # Error
/// This function errors if it is not possible to read the statistics to the
/// corresponding `physical_type`.
pub fn deserialize_statistics(
    statistics: &PageStatistics,
    primitive_type: PhysicalDType,
) -> Result<Arc<dyn Statistics>> {
    match primitive_type {
        PhysicalDType::Boolean => boolean::read(statistics),
        PhysicalDType::Integer => primitive::read::<i64>(statistics, primitive_type),
        PhysicalDType::Unsigned => primitive::read::<u64>(statistics, primitive_type),
        PhysicalDType::Float => primitive::read::<f64>(statistics, primitive_type),
        PhysicalDType::String => binary::read(statistics),
        _ => Err(Error::OutOfSpec {
            reason: "unknown data type".to_string(),
        }),
    }
}

/// Serializes [`Statistics`] into a raw parquet statistics.
pub fn serialize_statistics(statistics: &dyn Statistics) -> PageStatistics {
    match statistics.physical_type() {
        PhysicalDType::Boolean => boolean::write(statistics.as_any().downcast_ref().unwrap()),
        PhysicalDType::Integer => {
            primitive::write::<i64>(statistics.as_any().downcast_ref().unwrap())
        }
        PhysicalDType::Unsigned => {
            primitive::write::<u64>(statistics.as_any().downcast_ref().unwrap())
        }
        PhysicalDType::Float => {
            primitive::write::<f64>(statistics.as_any().downcast_ref().unwrap())
        }
        PhysicalDType::String => binary::write(statistics.as_any().downcast_ref().unwrap()),
        _ => {
            panic!("Unexpected data type")
        }
    }
}
