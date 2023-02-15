#[cfg(test)]
mod example;

use spi::query::function::FunctionMetadataManager;
use spi::Result;

pub fn register_udfs(_func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    // extend function...
    // eg.
    //   example::register_udf(func_manager)?;
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

        let result_udf = func_manager.udf(&expect_udf.name);

        assert!(result_udf.is_ok(), "not get result from func manager.");

        assert_eq!(&expect_udf, result_udf.unwrap().as_ref());
    }
}
