use coordinator::service::CoordinatorRef;
use datafusion::scalar::ScalarValue;
use spi::query::variable::SystemVariableManager;
use spi::Result;

pub fn register_variable(
    var_manager: &mut dyn SystemVariableManager,
    coord: CoordinatorRef,
) -> Result<()> {
    let cluster_name = coord.meta_manager().cluster();

    let value = ScalarValue::Utf8(Some(cluster_name));

    var_manager.register_variable("cluster_name".into(), value)?;

    Ok(())
}
