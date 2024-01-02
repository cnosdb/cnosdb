mod cluster_name;
mod deployment_mode;
mod node_id;
mod server_version;

use coordinator::service::CoordinatorRef;
use spi::query::variable::SystemVariableManager;
use spi::Result;

pub fn load_all_system_vars(
    var_manager: &mut dyn SystemVariableManager,
    coord: CoordinatorRef,
) -> Result<()> {
    // load all system variables
    server_version::register_variable(var_manager)?;
    deployment_mode::register_variable(var_manager, coord.clone())?;
    node_id::register_variable(var_manager, coord.clone())?;
    cluster_name::register_variable(var_manager, coord.clone())?;
    Ok(())
}
