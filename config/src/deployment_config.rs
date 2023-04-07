use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentConfig {
    #[serde(default = "DeploymentConfig::default_mode")]
    pub mode: String,
    #[serde(default = "DeploymentConfig::default_cpu")]
    pub cpu: usize,
    #[serde(default = "DeploymentConfig::default_memory")]
    pub memory: usize,
}

impl DeploymentConfig {
    pub fn default_mode() -> String {
        "query_tskv".to_string()
    }

    pub fn default_cpu() -> usize {
        get_sys_cpu()
    }

    pub fn default_memory() -> usize {
        get_sys_mem()
    }
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        Self {
            mode: Self::default_mode(),
            cpu: Self::default_cpu(),
            memory: Self::default_memory(),
        }
    }
}

impl CheckConfig for DeploymentConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("deployment".to_string());
        let mut ret = CheckConfigResult::default();

        match self.mode.as_str() {
            "query_tskv" | "tskv" | "query" | "singleton" => {}
            other_mode => {
                ret.add_error(CheckConfigItemResult {
                    config: config_name.clone(),
                    item: "mode".to_string(),
                    message: format!("'mode' {} is not supported, 'mode' must be one of [query_tskv, query, tskv, singleton]", other_mode)
                });
            }
        }

        if self.cpu == 0 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "cpu".to_string(),
                message: "'cpu' can not be zero".to_string(),
            });
        }

        if self.memory == 0 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name,
                item: "memory".to_string(),
                message: "'memory' can not be zero".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}

fn get_sys_cpu() -> usize {
    let mut num_cpus: usize = 4;
    if let Ok(cpu_info) = sys_info::cpu_num() {
        num_cpus = cpu_info as usize;
    }

    num_cpus
}

fn get_sys_mem() -> usize {
    let mut mem: usize = 16 * 1024 * 1024;
    if let Ok(mem_info) = sys_info::mem_info() {
        mem = mem_info.total as usize;
    }

    mem / 1024 / 1024
}
