//! Cnosdb Configuration Options

use std::collections::HashMap;
use std::env;

use datafusion::arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;

pub const TARGET_PARTITIONS: &str = "target_partitions";

/// Definition of a configuration option
pub struct ConfigDefinition {
    /// key used to identifier this configuration option
    key: String,
    /// Description to be used in generated documentation
    description: String,
    /// Data type of this option
    data_type: DataType,
    /// Default value
    default_value: ScalarValue,
}

impl ConfigDefinition {
    /// Create a configuration option definition
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        data_type: DataType,
        default_value: ScalarValue,
    ) -> Self {
        Self {
            key: name.into(),
            description: description.into(),
            data_type,
            default_value,
        }
    }

    /// Create a configuration option definition with a boolean value
    pub fn new_bool(
        key: impl Into<String>,
        description: impl Into<String>,
        default_value: bool,
    ) -> Self {
        Self::new(
            key,
            description,
            DataType::Boolean,
            ScalarValue::Boolean(Some(default_value)),
        )
    }

    /// Create a configuration option definition with a u64 value
    pub fn new_u64(
        key: impl Into<String>,
        description: impl Into<String>,
        default_value: u64,
    ) -> Self {
        Self::new(
            key,
            description,
            DataType::UInt64,
            ScalarValue::UInt64(Some(default_value)),
        )
    }

    /// Create a configuration option definition with a u64 value
    pub fn new_string(
        key: impl Into<String>,
        description: impl Into<String>,
        default_value: impl Into<String>,
    ) -> Self {
        Self::new(
            key,
            description,
            DataType::UInt64,
            ScalarValue::Utf8(Some(default_value.into())),
        )
    }
}

/// Contains definitions for all built-in configuration options
pub struct BuiltInConfigs {
    /// Configuration option definitions
    config_definitions: Vec<ConfigDefinition>,
}

impl Default for BuiltInConfigs {
    fn default() -> Self {
        Self::new()
    }
}

impl BuiltInConfigs {
    /// Create a new BuiltInConfigs struct containing definitions for all built-in
    /// configuration options
    pub fn new() -> Self {
        Self {
            config_definitions: vec![
                ConfigDefinition::new_u64(
                    TARGET_PARTITIONS,
                    "Number of partitions for query execution. Increasing partitions can increase concurrency.",
                    0_u64,
                ),
            ],
        }
    }

    /// Generate documentation that can be included in the user guide
    pub fn generate_config_markdown() -> String {
        use std::fmt::Write as _;
        let configs = Self::new();
        let mut docs = "| key | type | default | description |\n".to_string();
        docs += "|-----|------|---------|-------------|\n";
        let mut config_definitions = configs.config_definitions;
        config_definitions.sort_by_key(|c| c.key.clone());
        for config in config_definitions {
            let _ = writeln!(
                &mut docs,
                "| {} | {} | {} | {} |",
                config.key, config.data_type, config.default_value, config.description
            );
        }
        docs
    }
}

/// Configuration options struct. This can contain values for built-in and custom options
#[derive(Debug, Clone)]
pub struct ConfigOptions {
    options: HashMap<String, ScalarValue>,
}

impl Default for ConfigOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigOptions {
    /// Create new ConfigOptions struct
    pub fn new() -> Self {
        let mut options = HashMap::new();
        let built_in = BuiltInConfigs::new();
        for config_def in &built_in.config_definitions {
            options.insert(config_def.key.clone(), config_def.default_value.clone());
        }
        Self { options }
    }

    /// Create new ConfigOptions struct, taking values from environment variables where possible.
    pub fn from_env() -> Self {
        let mut options = HashMap::new();
        let built_in = BuiltInConfigs::new();
        for config_def in &built_in.config_definitions {
            let config_value = {
                let mut env_key = config_def.key.replace('.', "_");
                env_key.make_ascii_uppercase();
                match env::var(&env_key) {
                    Ok(value) => {
                        match ScalarValue::try_from_string(value.clone(), &config_def.data_type) {
                            Ok(parsed) => parsed,
                            Err(_) => {
                                println!("Warning: could not parse environment variable {}={} to type {}.", env_key, value, config_def.data_type);
                                config_def.default_value.clone()
                            }
                        }
                    }
                    Err(_) => config_def.default_value.clone(),
                }
            };
            options.insert(config_def.key.clone(), config_value);
        }
        Self { options }
    }

    /// set a configuration option
    pub fn set(&mut self, key: &str, value: ScalarValue) {
        self.options.insert(key.to_string(), value);
    }

    /// set a boolean configuration option
    pub fn set_bool(&mut self, key: &str, value: bool) {
        self.set(key, ScalarValue::Boolean(Some(value)))
    }

    /// set a `u64` configuration option
    pub fn set_u64(&mut self, key: &str, value: u64) {
        self.set(key, ScalarValue::UInt64(Some(value)))
    }

    /// get a configuration option
    pub fn get(&self, key: &str) -> Option<ScalarValue> {
        self.options.get(key).cloned()
    }

    /// get a String configuration option
    pub fn get_string(&self, key: &str) -> Option<String> {
        match self.get(key) {
            Some(ScalarValue::Utf8(b)) => b,
            _ => None,
        }
    }

    /// get a boolean configuration option
    pub fn get_bool(&self, key: &str) -> bool {
        match self.get(key) {
            Some(ScalarValue::Boolean(Some(b))) => b,
            _ => false,
        }
    }

    /// get a u64 configuration option
    pub fn get_u64(&self, key: &str) -> u64 {
        match self.get(key) {
            Some(ScalarValue::UInt64(Some(n))) => n,
            _ => 0,
        }
    }

    /// Access the underlying hashmap
    pub fn options(&self) -> &HashMap<String, ScalarValue> {
        &self.options
    }
}
