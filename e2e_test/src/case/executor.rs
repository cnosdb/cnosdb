use core::panic;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use tokio::runtime::Runtime;

use super::step::{Step, StepPtr};
use super::CaseFlowControl;
use crate::cluster_def::{CnosdbClusterDefinition, DataNodeDefinition};
use crate::utils::global::E2eContext;
use crate::utils::{
    run_cluster_with_customized_configs, Client, CnosdbDataTestHelper, CnosdbMetaTestHelper,
    FnUpdateCnosdbConfig, FnUpdateMetaStoreConfig,
};

/// The base directory for e2e test.
/// The test directory will be created under this directory: '{E2E_TEST_BASE_DIR}/{case_group}/{case_name}'.
const E2E_TEST_BASE_DIR: &str = "/tmp/e2e_test/";

/// E2e test executor, hold the `CaseContext`.
pub struct E2eExecutor {
    case_context: CaseContext,

    is_singleton: bool,
    /// Whether the cluster is installed.
    /// If not, the executor will initialize directories when calling `startup()`.
    is_installed: bool,
}

impl E2eExecutor {
    pub fn case_context(&self) -> &CaseContext {
        &self.case_context
    }
    pub fn case_context_mut(&mut self) -> &mut CaseContext {
        &mut self.case_context
    }

    pub fn cluster_definition(&self) -> &CnosdbClusterDefinition {
        &self.case_context.cluster_definition
    }

    pub fn set_update_meta_config_fn_vec(
        &mut self,
        update_meta_config_fn_vec: Vec<Option<FnUpdateMetaStoreConfig>>,
    ) {
        self.case_context.update_meta_config_fn_vec = update_meta_config_fn_vec;
    }

    pub fn add_update_meta_config_fn(&mut self, update_meta_config_fn: FnUpdateMetaStoreConfig) {
        self.case_context
            .update_meta_config_fn_vec
            .push(Some(update_meta_config_fn));
    }

    pub fn set_update_data_config_fn_vec(
        &mut self,
        update_data_config_fn_vec: Vec<Option<FnUpdateCnosdbConfig>>,
    ) {
        self.case_context.update_data_config_fn_vec = update_data_config_fn_vec;
    }

    pub fn add_update_data_config_fn(&mut self, update_data_config_fn: FnUpdateCnosdbConfig) {
        self.case_context
            .update_data_config_fn_vec
            .push(Some(update_data_config_fn));
    }
}

impl E2eExecutor {
    pub fn new_cluster(
        context: &E2eContext,
        cluster_definition: Arc<CnosdbClusterDefinition>,
    ) -> Self {
        let case_group = context.case_group();
        let case_name = context.case_name();
        let case_id = format!("{case_group}.{case_name}");
        let runtime = context.runtime();

        let test_dir = PathBuf::from(format!("{E2E_TEST_BASE_DIR}/{case_group}/{case_name}",));
        let is_singleton = cluster_definition.meta_cluster_def.is_empty()
            && cluster_definition.data_cluster_def.len() == 1;

        Self {
            case_context: CaseContext {
                case_group,
                case_name,
                case_id,
                runtime,
                cluster_definition,
                update_meta_config_fn_vec: vec![],
                update_data_config_fn_vec: vec![],

                test_dir,
                meta: None,
                data: None,
                global_variables: HashMap::new(),
                context_variables: HashMap::new(),
                last_step_variables: HashMap::new(),
            },

            is_singleton,
            is_installed: false,
        }
    }

    /// Startup the cluster, will initialize directories the first time called.
    pub fn startup(&mut self) {
        println!("Starting e2e executor\n{}", self.case_context());
        if !self.is_installed {
            self.case_context.close_processes();
            sleep(Duration::from_secs(3));
            self.case_context.init_directory();
        }
        self.case_context.run(!self.is_installed);
        self.is_installed = true;
    }

    /// Shutdown the cluster and restart it, will not initialize directories
    /// even if they are not exist.
    pub fn restart(&mut self, update_config: bool) {
        self.shutdown();
        self.case_context.run(update_config);
    }

    /// Shutdown the cluster, close all processes.
    pub fn shutdown(&mut self) {
        self.case_context.close_processes();
    }

    /// Execute a list of test steps. If self is not installed, it will call `startup()`
    /// before executing steps.
    pub fn execute_steps(&mut self, steps: &[StepPtr]) {
        println!("# Executing steps: {}", self.case_context.case_id());

        for (i, step) in steps.iter().enumerate() {
            step.set_id(i);
        }

        if !self.is_installed {
            self.startup();
        }

        for step in steps {
            println!(
                "### Executing step: case: '{}', step: [{}]-'{}'",
                self.case_context.case_id(),
                step.id(),
                step.name(),
            );
            match step.execute(&mut self.case_context) {
                CaseFlowControl::Continue => continue,
                CaseFlowControl::Break => break,
                CaseFlowControl::Error(err) => {
                    panic!(
                        "- Test failed: case: '{}', step: [{}]-'{}' at {}\n  - Error: {err}",
                        self.case_context.case_id(),
                        step.id(),
                        step.name(),
                        step.location(),
                    );
                }
            }
        }

        println!("# Execute steps completed: {}", self.case_context.case_id());
    }

    /// Execute one test step.
    pub fn execute_step<S: Step>(&mut self, step: S) -> CaseFlowControl {
        println!(
            "### Executing step: {}[{}]-{} at {}",
            self.case_context.case_id(),
            step.id(),
            step.name(),
            step.location(),
        );
        step.execute(&mut self.case_context)
    }
}

impl Drop for E2eExecutor {
    fn drop(&mut self) {
        println!("Dropping e2e executor\n{}", self.case_context());
    }
}

/// E2e test case context, hold the runtime, cluster definition, and test directories.
pub struct CaseContext {
    runtime: Arc<Runtime>,
    case_group: Arc<String>,
    case_name: Arc<String>,
    case_id: String,
    test_dir: PathBuf,
    cluster_definition: Arc<CnosdbClusterDefinition>,

    /// Functions to update meta node config before startup.
    update_meta_config_fn_vec: Vec<Option<FnUpdateMetaStoreConfig>>,
    /// Functions to update data node config before startup.
    update_data_config_fn_vec: Vec<Option<FnUpdateCnosdbConfig>>,
    /// Meta cluster instances.
    meta: Option<CnosdbMetaTestHelper>,
    /// Data cluster instances.
    data: Option<CnosdbDataTestHelper>,

    global_variables: HashMap<String, String>,
    context_variables: HashMap<String, String>,
    last_step_variables: HashMap<String, String>,
}

impl CaseContext {
    pub fn runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    pub fn case_group(&self) -> &str {
        self.case_group.as_str()
    }

    pub fn case_name(&self) -> &str {
        self.case_name.as_str()
    }

    pub fn case_id(&self) -> &str {
        &self.case_id
    }

    pub fn test_dir(&self) -> &PathBuf {
        &self.test_dir
    }

    pub fn cluster_definition(&self) -> &CnosdbClusterDefinition {
        &self.cluster_definition
    }

    pub fn data_definition(&self, i: usize) -> &DataNodeDefinition {
        match self.cluster_definition.data_cluster_def.get(i) {
            Some(d) => d,
            None => panic!("Data node config of index {i} not found"),
        }
    }

    pub fn meta_opt(&self) -> Option<&CnosdbMetaTestHelper> {
        self.meta.as_ref()
    }

    pub fn data_opt(&self) -> Option<&CnosdbDataTestHelper> {
        self.data.as_ref()
    }

    pub fn meta(&self) -> &CnosdbMetaTestHelper {
        match self.meta_opt() {
            Some(d) => d,
            None => {
                panic!("Cnosdb meta server is not in the e2e context, cluster may be not started")
            }
        }
    }

    pub fn data(&self) -> &CnosdbDataTestHelper {
        match self.data_opt() {
            Some(d) => d,
            None => panic!(
                "Cnosdb data server cannot be found in case context, cluster may be not started"
            ),
        }
    }

    pub fn meta_mut(&mut self) -> &mut CnosdbMetaTestHelper {
        match self.meta.as_mut() {
            Some(d) => d,
            None => {
                panic!("Cnosdb meta server is not in the e2e context, cluster may be not started")
            }
        }
    }

    pub fn data_mut(&mut self) -> &mut CnosdbDataTestHelper {
        match self.data.as_mut() {
            Some(d) => d,
            None => panic!(
                "Cnosdb data server cannot be found in case context, cluster may be not started"
            ),
        }
    }

    pub fn meta_client(&self) -> Arc<Client> {
        self.meta().client.clone()
    }

    pub fn data_client(&self, index: usize) -> Arc<Client> {
        self.data().data_node_clients[index].clone()
    }

    pub fn global_variables(&self) -> &HashMap<String, String> {
        &self.global_variables
    }

    pub fn global_variables_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.global_variables
    }

    pub fn context_variables(&self) -> &HashMap<String, String> {
        &self.context_variables
    }

    pub fn context_variables_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.context_variables
    }

    pub fn add_context_variable(&mut self, key: &str, value: &str) {
        self.last_step_variables
            .insert(key.to_string(), value.to_string());
        self.context_variables
            .insert(key.to_string(), value.to_string());
    }

    pub fn last_step_variables(&self) -> &HashMap<String, String> {
        &self.last_step_variables
    }

    pub fn last_step_variables_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.last_step_variables
    }
}

impl CaseContext {
    /// Drop self.meta and self.data, close all processes,
    /// if any process closed then wait for 5 seconds.
    pub fn close_processes(&mut self) {
        // Drop meta and data, got whether any process was dropped.
        let has_meta_proc = self.meta.take().is_some();
        let has_data_proc = self.data.take().is_some();
        if has_meta_proc || has_data_proc {
            sleep(Duration::from_secs(5));
        }
    }

    /// Initialize the test directory, remove the old one if exists.
    pub fn init_directory(&self) {
        println!("- Removing test directory: {}", self.test_dir.display());
        let _ = std::fs::remove_dir_all(&self.test_dir);
        println!("- Creating test directory: {}", self.test_dir.display());
        std::fs::create_dir_all(&self.test_dir).unwrap();
    }

    /// Close existing processes and run the cluster(or singleton data node).
    pub fn run(&mut self, generate_config: bool) {
        self.close_processes();

        let (meta, data) = run_cluster_with_customized_configs(
            &self.test_dir,
            self.runtime.clone(),
            &self.cluster_definition,
            generate_config && !self.cluster_definition.meta_cluster_def.is_empty(),
            generate_config,
            &self.update_meta_config_fn_vec,
            &self.update_data_config_fn_vec,
        );
        self.meta = meta;
        self.data = data;
    }
}

impl std::fmt::Display for CaseContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "# Case: {}.{}", self.case_group, self.case_name)?;
        writeln!(f, "# TestDirectory: {}", self.test_dir.display())?;
        write!(f, "# ClusterDefinition:\n{}", self.cluster_definition)?;
        if !self.global_variables.is_empty()
            || !self.context_variables.is_empty()
            || !self.last_step_variables.is_empty()
        {
            writeln!(f, "# Variables:")?;
            if !self.global_variables.is_empty() {
                writeln!(f, "## Global Variables:")?;
                for (k, v) in self.global_variables.iter() {
                    writeln!(f, "  - {k}: {v}")?;
                }
            }
            if !self.context_variables.is_empty() {
                writeln!(f, "## Context Variables:")?;
                for (k, v) in self.context_variables.iter() {
                    writeln!(f, "  - {k}: {v}")?;
                }
            }
            if !self.last_step_variables.is_empty() {
                writeln!(f, "## LastStep Variables:")?;
                for (k, v) in self.last_step_variables.iter() {
                    writeln!(f, "  - {k}: {v}")?;
                }
            }
        }

        Ok(())
    }
}
