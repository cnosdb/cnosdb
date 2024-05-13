use core::panic;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Runtime;

use super::step::Step;
use super::{CaseFlowControl, CnosdbAuth};
use crate::cluster_def::{CnosdbClusterDefinition, DataNodeDefinition};
use crate::utils::{
    kill_all, run_cluster, run_singleton, Client, CnosdbDataTestHelper, CnosdbMetaTestHelper,
};
use crate::E2eResult;

const WAIT_BEFORE_RESTART_SECONDS: u64 = 1;

// TODO(zipper): This module also needs test.

pub struct E2eExecutor {
    case_group: String,
    case_name: String,
    runtime: Arc<Runtime>,
    cluster_definition: CnosdbClusterDefinition,

    test_dir: PathBuf,
    is_singleton: bool,
}

impl E2eExecutor {
    pub fn new_cluster(
        case_group: &str,
        case_name: &str,
        cluster_definition: CnosdbClusterDefinition,
    ) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);

        let test_dir = PathBuf::from(format!("/tmp/e2e_test/{case_group}/{case_name}"));
        let is_singleton = cluster_definition.meta_cluster_def.is_empty()
            && cluster_definition.data_cluster_def.len() == 1;

        Self {
            case_group: case_group.to_string(),
            case_name: case_name.to_string(),
            runtime,
            cluster_definition,

            test_dir,
            is_singleton,
        }
    }

    pub fn new_singleton(
        case_group: &str,
        case_name: &str,
        data_node_def: DataNodeDefinition,
    ) -> Self {
        Self::new_cluster(
            case_group,
            case_name,
            CnosdbClusterDefinition {
                meta_cluster_def: vec![],
                data_cluster_def: vec![data_node_def],
            },
        )
    }

    fn install_singleton(&self) -> CnosdbDataTestHelper {
        let _ = std::fs::remove_dir_all(&self.test_dir);
        std::fs::create_dir_all(&self.test_dir).unwrap();

        kill_all();

        run_singleton(
            &self.test_dir,
            &self.cluster_definition.data_cluster_def[0],
            false,
            true,
        )
    }

    fn install_cluster(&self) -> (Option<CnosdbMetaTestHelper>, Option<CnosdbDataTestHelper>) {
        let _ = std::fs::remove_dir_all(&self.test_dir);
        std::fs::create_dir_all(&self.test_dir).unwrap();

        kill_all();

        run_cluster(
            &self.test_dir,
            self.runtime.clone(),
            &self.cluster_definition,
            true,
            true,
        )
    }

    pub fn execute_steps(&self, steps: &[Box<dyn Step>]) {
        println!("Test begin: {}_{}", self.case_group, self.case_name);

        let mut context = E2eContext::from(self);
        if self.is_singleton {
            let data = self.install_singleton();
            context.with_data(Some(data));
        } else {
            let (meta, data) = self.install_cluster();
            context.with_meta(meta);
            context.with_data(data);
        }

        for step in steps {
            println!("- Executing step: [{}]-{}", step.id(), step.name());
            match step.execute(&mut context) {
                CaseFlowControl::Continue => continue,
                CaseFlowControl::Break => break,
            }
        }

        println!("Test complete: {}.{}", self.case_group, self.case_name);
    }

    #[deprecated]
    pub fn execute_steps_legacy(&self, steps: &[StepLegacy]) {
        println!("Test begin: {}_{}", self.case_group, self.case_name);
        if self.is_singleton {
            let mut data = self.install_singleton();
            self.execute_steps_legacy_inner(&mut data, steps);
        } else {
            let (_meta, data) = self.install_cluster();
            let mut data = data.unwrap();
            self.execute_steps_legacy_inner(&mut data, steps);
        }
        println!("Test complete: {}.{}", self.case_group, self.case_name);
    }

    #[deprecated]
    fn execute_steps_legacy_inner(&self, data: &mut CnosdbDataTestHelper, steps: &[StepLegacy]) {
        let fail_msg = |i: usize, step: &StepLegacy, result: &E2eResult<Vec<String>>| {
            format!(
                "[{}.{}] steps[{i}] [{step}], result: {result:?}",
                &self.case_group, &self.case_name
            )
        };
        for (i, step) in steps.iter().enumerate() {
            println!("- Executing step: {step}");
            match step {
                StepLegacy::Log(msg) => println!("LOG: {}", msg),
                StepLegacy::CnosdbRequest { req, auth } => {
                    let client = match auth {
                        Some(a) => {
                            Arc::new(Client::with_auth(a.username.clone(), a.password.clone()))
                        }
                        None => data.client.clone(),
                    };
                    match req {
                        CnosdbRequest::Query {
                            url,
                            sql,
                            resp,
                            sorted,
                        } => {
                            let result_resp = client.api_v1_sql(*url, sql);
                            let fail_message = fail_msg(i, step, &result_resp);
                            match resp {
                                Ok(exp_lines) => {
                                    assert!(result_resp.is_ok(), "{fail_message}");
                                    if let Ok(mut resp_lines) = result_resp {
                                        if *sorted {
                                            resp_lines.sort_unstable();
                                        }
                                        assert_eq!(
                                            resp_lines,
                                            exp_lines.to_vec(),
                                            "{fail_message}"
                                        );
                                    } else {
                                        assert!(result_resp.is_err(), "{fail_message}");
                                    }
                                }
                                Err(exp_err) => {
                                    assert!(
                                        result_resp.is_err(),
                                        "{}",
                                        fail_msg(i, step, &result_resp)
                                    );
                                    assert_eq!(
                                        exp_err,
                                        &result_resp.unwrap_err(),
                                        "{fail_message}"
                                    );
                                }
                            }
                        }
                        CnosdbRequest::Insert { url, sql, resp } => {
                            let result_resp = client.api_v1_write(*url, sql);
                            let fail_message = fail_msg(i, step, &result_resp);
                            match resp {
                                Ok(_) => assert!(result_resp.is_ok(), "{fail_message}"),
                                Err(exp_err) => {
                                    assert!(result_resp.is_err(), "{fail_message}");
                                    assert_eq!(
                                        exp_err,
                                        &result_resp.unwrap_err(),
                                        "{fail_message}"
                                    );
                                }
                            }
                        }
                        CnosdbRequest::Write { url, req, resp } => {
                            let result_resp = client.api_v1_write(*url, req);
                            let fail_message = fail_msg(i, step, &result_resp);
                            match resp {
                                Ok(_) => assert!(result_resp.is_ok(), "{fail_message}"),
                                Err(exp_err) => {
                                    assert!(result_resp.is_err(), "{fail_message}");
                                    assert_eq!(
                                        exp_err,
                                        &result_resp.unwrap_err(),
                                        "{fail_message}"
                                    );
                                }
                            }
                        }
                        CnosdbRequest::Ddl { url, sql, resp } => {
                            let result_resp = client.api_v1_sql(*url, sql);
                            let fail_message = fail_msg(i, step, &result_resp);
                            match resp {
                                Ok(_) => assert!(result_resp.is_ok(), "{fail_message}"),
                                Err(exp_err) => {
                                    assert!(result_resp.is_err(), "{fail_message}");
                                    assert_eq!(
                                        exp_err,
                                        &result_resp.unwrap_err(),
                                        "{fail_message}"
                                    );
                                }
                            }
                        }
                    }
                }
                StepLegacy::RestartDataNode(data_node_index) => {
                    if WAIT_BEFORE_RESTART_SECONDS > 0 {
                        // TODO(zipper): The test sometimes fail if we restart just after a DDL or Insert, why?
                        std::thread::sleep(Duration::from_secs(WAIT_BEFORE_RESTART_SECONDS));
                    }
                    let data_node_def = data.data_node_definitions[*data_node_index].clone();
                    data.restart_one_node(&data_node_def);
                }
                StepLegacy::StartDataNode(data_node_index) => {
                    let data_node_def = data.data_node_definitions[*data_node_index].clone();
                    data.start_one_node(&data_node_def);
                }
                StepLegacy::StopDataNode(data_node_index) => {
                    let data_node_def = data.data_node_definitions[*data_node_index].clone();
                    data.stop_one_node(&data_node_def.config_file_name, false);
                }
                StepLegacy::RestartCluster => {
                    kill_all();
                    run_cluster(
                        &self.test_dir,
                        self.runtime.clone(),
                        &self.cluster_definition,
                        false,
                        false,
                    );
                }
                StepLegacy::Sleep(seconds) => {
                    std::thread::sleep(Duration::from_secs(*seconds));
                }
            }
        }
    }
}

pub struct E2eContext {
    case_group: String,
    case_name: String,
    runtime: Arc<Runtime>,
    cluster_definition: CnosdbClusterDefinition,

    test_dir: PathBuf,
    meta: Option<CnosdbMetaTestHelper>,
    data: Option<CnosdbDataTestHelper>,
    global_variables: HashMap<String, String>,
    context_variables: HashMap<String, String>,
    last_step_variables: HashMap<String, String>,
}

impl E2eContext {
    pub fn case_group(&self) -> &str {
        &self.case_group
    }

    pub fn case_name(&self) -> &str {
        &self.case_name
    }

    pub fn runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    pub fn cluster_definition(&self) -> &CnosdbClusterDefinition {
        &self.cluster_definition
    }

    pub fn test_dir(&self) -> &PathBuf {
        &self.test_dir
    }

    pub fn with_meta(&mut self, meta: Option<CnosdbMetaTestHelper>) {
        self.meta = meta;
    }

    pub fn meta_opt(&self) -> Option<&CnosdbMetaTestHelper> {
        self.meta.as_ref()
    }

    pub fn meta(&self) -> &CnosdbMetaTestHelper {
        match self.meta_opt() {
            Some(d) => d,
            None => panic!("Cnosdb meta server is not in the e2e context"),
        }
    }

    pub fn meta_mut(&mut self) -> &mut CnosdbMetaTestHelper {
        match self.meta.as_mut() {
            Some(d) => d,
            None => panic!("Cnosdb meta server is not in the e2e context"),
        }
    }

    pub fn with_data(&mut self, data: Option<CnosdbDataTestHelper>) {
        self.data = data;
    }

    pub fn data_opt(&self) -> Option<&CnosdbDataTestHelper> {
        self.data.as_ref()
    }

    pub fn data(&self) -> &CnosdbDataTestHelper {
        match self.data_opt() {
            Some(d) => d,
            None => panic!("Cnosdb data server is not in the e2e context"),
        }
    }

    pub fn data_mut(&mut self) -> &mut CnosdbDataTestHelper {
        match self.data.as_mut() {
            Some(d) => d,
            None => panic!("Cnosdb data server is not in the e2e context"),
        }
    }

    pub fn data_dir(&self, i: usize) -> PathBuf {
        let config = match self.data().data_node_configs.get(i) {
            Some(c) => c,
            None => panic!("Data node config of index {i} not found"),
        };
        self.test_dir.join(&config.storage.path)
    }

    pub fn meta_client(&self) -> Arc<Client> {
        self.meta().client.clone()
    }

    pub fn data_client(&self) -> Arc<Client> {
        self.data().client.clone()
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

    pub fn last_step_variables(&self) -> &HashMap<String, String> {
        &self.last_step_variables
    }

    pub fn last_step_variables_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.last_step_variables
    }

    pub fn add_context_variable(&mut self, key: &str, value: &str) {
        self.last_step_variables
            .insert(key.to_string(), value.to_string());
        self.context_variables
            .insert(key.to_string(), value.to_string());
    }
}

impl From<&E2eExecutor> for E2eContext {
    fn from(executor: &E2eExecutor) -> Self {
        E2eContext {
            case_group: executor.case_group.clone(),
            case_name: executor.case_name.clone(),
            runtime: executor.runtime.clone(),
            cluster_definition: executor.cluster_definition.clone(),
            test_dir: executor.test_dir.clone(),
            data: None,
            meta: None,
            global_variables: HashMap::new(),
            context_variables: HashMap::new(),
            last_step_variables: HashMap::new(),
        }
    }
}

// TODO(zipper): Supports: 1. cli execution;
#[derive(Debug)]
#[deprecated]
pub enum StepLegacy {
    Log(String),
    CnosdbRequest {
        req: CnosdbRequest,
        auth: Option<CnosdbAuth>,
    },
    RestartDataNode(usize),
    StartDataNode(usize),
    StopDataNode(usize),
    RestartCluster,
    /// Sleep current thread for a while(in seconds)
    Sleep(u64),
}

impl std::fmt::Display for StepLegacy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StepLegacy::Log(msg) => write!(f, "Log: {msg}"),
            StepLegacy::CnosdbRequest { req, auth } => {
                write!(f, "CnosdbRequest: {req}, ")?;
                match auth {
                    Some(a) => write!(f, ", with auth: '{a}'"),
                    None => write!(f, "no auth"),
                }
            }
            StepLegacy::RestartDataNode(i) => write!(f, "RestartDataNode of data node {i}"),
            StepLegacy::StartDataNode(i) => write!(f, "StartDataNode of data node {i}"),
            StepLegacy::StopDataNode(i) => write!(f, "StopDataNode of data node {i}"),
            StepLegacy::RestartCluster => write!(f, "RestartCluster"),
            StepLegacy::Sleep(d) => write!(f, "Sleep for {d} seconds"),
        }
    }
}

#[derive(Debug)]
#[deprecated]
pub enum CnosdbRequest {
    Query {
        url: &'static str,
        sql: &'static str,
        resp: E2eResult<Vec<&'static str>>,
        sorted: bool,
    },
    Insert {
        url: &'static str,
        sql: &'static str,
        resp: E2eResult<()>,
    },
    Write {
        url: &'static str,
        req: &'static str,
        resp: E2eResult<()>,
    },
    Ddl {
        url: &'static str,
        sql: &'static str,
        resp: E2eResult<()>,
    },
}

impl std::fmt::Display for CnosdbRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CnosdbRequest::Query {
                url,
                sql,
                resp,
                sorted,
            } => {
                write!(
                    f,
                    "Query to '{url}', SQL: '{sql}' => {} {resp:?}",
                    if *sorted { "sorted" } else { "" },
                )
            }
            CnosdbRequest::Insert { url, sql, resp } => {
                write!(f, "INSERT to '{url}', SQL: '{sql}' => {resp:?}")
            }
            CnosdbRequest::Write { url, req, resp } => {
                write!(f, "Write to '{url}', req: '{req}' => {resp:?}")
            }
            CnosdbRequest::Ddl { url, sql, resp } => {
                write!(f, "DDL to '{url}', SQL: '{sql}' => {resp:?}")
            }
        }
    }
}
