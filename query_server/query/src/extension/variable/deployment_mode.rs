use coordinator::service::CoordinatorRef;
use datafusion::scalar::ScalarValue;
use spi::query::variable::SystemVariableManager;
use spi::Result;

pub fn register_variable(
    var_manager: &mut dyn SystemVariableManager,
    coord: CoordinatorRef,
) -> Result<()> {
    let deployment_mode = coord.meta_manager().deployment_mode();

    let value = ScalarValue::Utf8(Some(deployment_mode));

    var_manager.register_variable("deployment_mode".into(), value)?;

    Ok(())
}
