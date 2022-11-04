use coordinator::errors;
use models::define_result;
use snafu::Snafu;

use crate::utils::point_util::PointUtilError;

pub mod sink;
pub mod tskv_sink;

define_result!(DataSourceError);

#[derive(Debug, Snafu)]
pub enum DataSourceError {
    #[snafu(display("Point util error, err: {}", source))]
    PointUtil { source: PointUtilError },

    #[snafu(display("Tskv operator, err: {}", source))]
    Tskv { source: tskv::Error },

    #[snafu(display("Coordinator operator, err: {}", source))]
    Coordinator { source: errors::CoordinatorError },
}
