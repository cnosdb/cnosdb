use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub enum DeploymentMode {
    Tskv,
    Query,
    Singleton,
    QueryTskv,
}
impl Display for DeploymentMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tskv => write!(f, "tskv"),
            Self::QueryTskv => write!(f, "query tskv"),
            Self::Singleton => write!(f, "singleton"),
            Self::Query => write!(f, "query"),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Default, PartialOrd, PartialEq, Ord, Eq)]
pub struct Deployment {
    pub mode: Option<DeploymentMode>,
    pub cpu: Option<usize>,
    pub memory: Option<usize>,
}

impl Deployment {
    pub fn cpu_or_default(&self) -> usize {
        self.cpu.unwrap_or(4)
    }

    pub fn memory_or_default(&self) -> usize {
        self.memory.unwrap_or(16)
    }
}

pub trait SetDeployment {
    fn set_mode(&mut self, mode: DeploymentMode);
    fn set_cpu(&mut self, cpu: usize);
    fn set_memory(&mut self, memory: usize);
}

impl SetDeployment for Option<Deployment> {
    fn set_mode(&mut self, mode: DeploymentMode) {
        match self {
            Some(deplyment) => deplyment.mode = Some(mode),
            None => {
                let mut deployment = Deployment::default();
                deployment.set_mode(mode);
                *self = Some(deployment);
            }
        }
    }

    fn set_cpu(&mut self, cpu: usize) {
        match self {
            Some(deplyment) => deplyment.cpu = Some(cpu),
            None => {
                let mut deployment = Deployment::default();
                deployment.set_cpu(cpu);
                *self = Some(deployment);
            }
        }
    }

    fn set_memory(&mut self, memory: usize) {
        match self {
            Some(deplyment) => deplyment.cpu = Some(memory),
            None => {
                let mut deployment = Deployment::default();
                deployment.set_memory(memory);
                *self = Some(deployment);
            }
        }
    }
}

impl SetDeployment for Deployment {
    fn set_mode(&mut self, mode: DeploymentMode) {
        self.mode = Some(mode);
    }
    fn set_cpu(&mut self, cpu: usize) {
        self.cpu = Some(cpu);
    }
    fn set_memory(&mut self, memory: usize) {
        self.memory = Some(memory)
    }
}
