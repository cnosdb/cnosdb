use std::fmt::Display;
use std::sync::Arc;

use datafusion::scalar::ScalarValue;
use datafusion::variable::VarProvider;

use crate::DFResult;

pub type SystemVariableManagerRef = Arc<dyn SystemVariableManager + Send + Sync>;
pub type VarProviderRef = Arc<dyn VarProvider + Send + Sync>;

const SYS_VAR_PREFIX: &str = "@@";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VarName(String);

pub trait SystemVariableManager: VarProvider {
    fn register_variable(&mut self, name: VarName, value: ScalarValue) -> DFResult<()>;
}

pub fn normalize_var_name(var_name: &str) -> VarName {
    VarName(
        var_name
            .strip_prefix(SYS_VAR_PREFIX)
            .unwrap_or(var_name)
            .to_lowercase(),
    )
}

impl From<&str> for VarName {
    fn from(var_name: &str) -> Self {
        normalize_var_name(var_name)
    }
}

impl Display for VarName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", SYS_VAR_PREFIX, self.0)
    }
}
