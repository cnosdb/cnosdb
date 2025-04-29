use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::scalar::ScalarValue;
use datafusion::variable::VarProvider;
use spi::query::variable::{normalize_var_name, SystemVariableManager, VarName};

pub type SimpleSystemVarManagerRef = Arc<SimpleSystemVarManager>;

#[derive(Debug, Default)]
pub struct SimpleSystemVarManager {
    vars: HashMap<VarName, ScalarValue>,
}

impl VarProvider for SimpleSystemVarManager {
    fn get_value(&self, var_names: Vec<String>) -> DFResult<ScalarValue> {
        let name = normalize_var_name(&var_names[0]);

        self.vars
            .get(&name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("System variable not found: {}", name)))
    }

    fn get_type(&self, var_names: &[String]) -> Option<DataType> {
        let name = normalize_var_name(&var_names[0]);

        self.vars.get(&name).map(ScalarValue::data_type)
    }
}

impl SystemVariableManager for SimpleSystemVarManager {
    fn register_variable(&mut self, name: VarName, value: ScalarValue) -> DFResult<()> {
        if let Some(value) = self.vars.insert(name.clone(), value) {
            return Err(DataFusionError::Internal(format!(
                "Variable {} already registered with value {:?}",
                name, value
            )));
        }

        Ok(())
    }
}
