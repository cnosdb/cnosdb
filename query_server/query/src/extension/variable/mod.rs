use spi::query::variable::SystemVariableManager;
use spi::Result;

pub fn load_all_system_vars(_var_manager: &mut dyn SystemVariableManager) -> Result<()> {
    // TODO: load all system variables
    Ok(())
}
