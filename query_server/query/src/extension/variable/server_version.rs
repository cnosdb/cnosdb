use datafusion::scalar::ScalarValue;
use spi::query::variable::SystemVariableManager;
use spi::QueryResult;
use version::workspace_version;

pub fn register_variable(var_manager: &mut dyn SystemVariableManager) -> QueryResult<()> {
    let value = ScalarValue::Utf8(Some(workspace_version().to_string()));

    var_manager.register_variable("server_version".into(), value)?;

    Ok(())
}
