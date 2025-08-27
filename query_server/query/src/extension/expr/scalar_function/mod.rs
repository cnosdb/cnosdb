mod duration_in;
#[cfg(test)]
mod example;
mod gapfill;
mod gauge;
mod gis;
mod interpolate;
mod locf;
mod state_at;
mod utils;

use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

pub const TIME_WINDOW_GAPFILL: &str = "time_window_gapfill";
pub const LOCF: &str = "locf";
pub const INTERPOLATE: &str = "interpolate";
pub const DURATION_IN: &str = "duration_in";
pub const STATE_AT: &str = "state_at";

pub fn register_udfs(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    // extend function...
    // eg.
    //   example::register_udf(func_manager)?;
    gapfill::register_udf(func_manager)?;
    locf::register_udf(func_manager)?;
    interpolate::register_udf(func_manager)?;
    gauge::register_udfs(func_manager)?;
    duration_in::register_udf(func_manager)?;
    state_at::register_udf(func_manager)?;
    gis::register_udfs(func_manager)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use spi::query::function::FunctionMetadataManager;

    use super::example;
    use crate::function::simple_func_manager::SimpleFunctionMetadataManager;

    #[tokio::test]
    async fn test_example() {
        let mut func_manager = SimpleFunctionMetadataManager::default();

        let expect_udf = example::register_udf(&mut func_manager);

        assert!(expect_udf.is_ok(), "register_udf error.");

        let expect_udf = expect_udf.unwrap();

        let result_udf = func_manager.udf(expect_udf.name());

        assert!(result_udf.is_ok(), "not get result from func manager.");

        assert_eq!(&expect_udf, result_udf.unwrap().as_ref());
    }
}
