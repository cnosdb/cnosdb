use coordinator::service::CoordinatorRef;
use datafusion::scalar::ScalarValue;
use spi::query::variable::SystemVariableManager;
use spi::Result;

pub fn register_variable(
    var_manager: &mut dyn SystemVariableManager,
    coord: CoordinatorRef,
) -> Result<()> {
    let node_id = coord.meta_manager().node_id();

    let value = ScalarValue::UInt64(Some(node_id));

    var_manager.register_variable("node_id".into(), value)?;

    Ok(())
}
