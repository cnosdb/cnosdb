use config::VERSION;
use datafusion::scalar::ScalarValue;
use spi::query::variable::SystemVariableManager;
use spi::QueryResult;

pub fn register_variable(var_manager: &mut dyn SystemVariableManager) -> QueryResult<()> {
    let value = ScalarValue::Utf8(Some(VERSION.clone()));

    var_manager.register_variable("server_version".into(), value)?;

    Ok(())
}
