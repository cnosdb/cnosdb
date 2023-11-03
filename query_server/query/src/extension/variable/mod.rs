mod server_version;

use spi::query::variable::SystemVariableManager;
use spi::Result;

pub fn load_all_system_vars(var_manager: &mut dyn SystemVariableManager) -> Result<()> {
    // load all system variables
    server_version::register_variable(var_manager)?;
    Ok(())
}
