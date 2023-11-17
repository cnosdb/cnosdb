use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use models::PhysicalDType;

use super::Statistics;
use crate::error::{Error, Result};
use crate::tsm2::page::PageStatistics;
use crate::tsm2::types;

#[derive(Clone, PartialEq, Eq)]
pub struct PrimitiveStatistics<T: types::NativeType> {
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min_value: Option<T>,
    pub max_value: Option<T>,
}

impl<T: types::NativeType> Debug for PrimitiveStatistics<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveStatistics")
            .field("null_count", &self.null_count)
            .field("distinct_count", &self.distinct_count)
            .field("min_value", &self.min_value)
            .field("max_value", &self.max_value)
            .finish()
    }
}

impl<T: types::NativeType> Statistics for PrimitiveStatistics<T> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn physical_type(&self) -> &PhysicalDType {
        &T::TYPE
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
    }
}

pub fn read<T: types::NativeType>(
    v: &PageStatistics,
    _primitive_type: PhysicalDType,
) -> Result<Arc<dyn Statistics>> {
    if let Some(ref v) = v.max_value {
        if v.len() != std::mem::size_of::<T>() {
            return Err(Error::OutOfSpec {
                reason: "The max_value of statistics MUST be plain encoded".to_string(),
            });
        }
    };
    if let Some(ref v) = v.min_value {
        if v.len() != std::mem::size_of::<T>() {
            return Err(Error::OutOfSpec {
                reason: "The min_value of statistics MUST be plain encoded".to_string(),
            });
        }
    };

    Ok(Arc::new(PrimitiveStatistics::<T> {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.as_ref().map(|x| types::decode(x)),
        min_value: v.min_value.as_ref().map(|x| types::decode(x)),
    }))
}

pub fn write<T: types::NativeType>(v: &PrimitiveStatistics<T>) -> PageStatistics {
    PageStatistics {
        primitive_type: T::TYPE,
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.map(|x| x.to_le_bytes().as_ref().to_vec()),
        min_value: v.min_value.map(|x| x.to_le_bytes().as_ref().to_vec()),
    }
}
