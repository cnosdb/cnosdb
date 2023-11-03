use datafusion::scalar::ScalarValue;
use spi::query::variable::SystemVariableManager;
use spi::Result;

pub fn register_variable(var_manager: &mut dyn SystemVariableManager) -> Result<()> {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN");

    let value = ScalarValue::Utf8(Some(version.to_string()));

    var_manager.register_variable("server_version".into(), value)?;

    Ok(())
}
