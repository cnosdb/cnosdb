#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Runtime;

use crate::cluster_def::{CnosdbClusterDefinition, DataNodeDefinition};
use crate::utils::{
    kill_all, run_cluster, run_singleton, Client, CnosdbDataTestHelper, CnosdbMetaTestHelper,
};
use crate::{E2eError, E2eResult};

const WAIT_BEFORE_RESTART_SECONDS: u64 = 1;

// TODO(zipper): This module also need test.
// TODO(zipper): Test the following scenarios: 1. todo

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

    pub fn execute_steps(&self, steps: &[Step]) {
        println!("Test begin: {}_{}", self.case_group, self.case_name);
        let _ = std::fs::remove_dir_all(&self.test_dir);
        std::fs::create_dir_all(&self.test_dir).unwrap();

        kill_all();

        if self.is_singleton {
            self.execute_steps_in_singleton(steps);
        } else {
            self.execute_steps_in_cluster(steps);
        }
        println!("Test complete: {}.{}", self.case_group, self.case_name);
    }

    fn execute_steps_in_singleton(&self, steps: &[Step]) {
        let mut data = run_singleton(
            &self.test_dir,
            &self.cluster_definition.data_cluster_def[0],
            false,
            true,
        );
        self.execute_steps_inner(&mut None, &mut data, steps);
    }

    fn execute_steps_in_cluster(&self, steps: &[Step]) {
        let (meta, data) = run_cluster(
            &self.test_dir,
            self.runtime.clone(),
            &self.cluster_definition,
            true,
            true,
        );
        let mut data = data.unwrap();
        let mut meta = meta.unwrap();
        self.execute_steps_inner(&mut Some(&mut meta), &mut data, steps);
    }

    fn execute_steps_inner(
        &self,
        meta: &mut Option<&mut CnosdbMetaTestHelper>,
        data: &mut CnosdbDataTestHelper,
        steps: &[Step],
    ) {
        let fail_msg = |i: usize, step: &Step, result: &E2eResult<Vec<String>>| {
            format!(
                "[{}.{}] steps[{i}] [{step}], result: {result:?}",
                &self.case_group, &self.case_name
            )
        };
        for (i, step) in steps.iter().enumerate() {
            println!("- Executing step: {step}");
            match step {
                Step::Log(msg) => println!("LOG: {}", msg),
                Step::CnosdbRequest { req, auth } => {
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
                            regex,
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
                                        if !*regex {
                                            assert_eq!(
                                                resp_lines,
                                                exp_lines.to_vec(),
                                                "{fail_message}"
                                            );
                                        } else {
                                            assert_eq!(
                                                resp_lines.len(),
                                                exp_lines.len(),
                                                "{fail_message}"
                                            );
                                            for (i, resp_line) in resp_lines.iter().enumerate() {
                                                let exp_regex = regex::Regex::new(exp_lines[i])
                                                    .expect("build regex: {fail_message}");
                                                assert!(
                                                    exp_regex.is_match(resp_line),
                                                    "{fail_message}"
                                                );
                                            }
                                        }
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
                                    assert!(
                                        compare_e2e_error(exp_err, &result_resp.unwrap_err()),
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
                                    assert!(
                                        compare_e2e_error(exp_err, &result_resp.unwrap_err()),
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
                                    assert!(
                                        compare_e2e_error(exp_err, &result_resp.unwrap_err()),
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
                                    if matches!(exp_err, E2eError::Ignored) {
                                        return;
                                    }
                                    assert!(result_resp.is_err(), "{fail_message}");
                                    assert!(
                                        compare_e2e_error(exp_err, &result_resp.unwrap_err()),
                                        "{fail_message}"
                                    );
                                }
                            }
                        }
                    }
                }
                Step::RestartDataNode(data_node_index) => {
                    if WAIT_BEFORE_RESTART_SECONDS > 0 {
                        // TODO(zipper): The test sometimes fail if we restart just after a DDL or Insert, why?
                        std::thread::sleep(Duration::from_secs(WAIT_BEFORE_RESTART_SECONDS));
                    }
                    let data_node_def = data.data_node_definitions[*data_node_index].clone();
                    data.restart_one_node(&data_node_def);
                }
                Step::StartDataNode(data_node_index) => {
                    let data_node_def = data.data_node_definitions[*data_node_index].clone();
                    data.start_one_node(&data_node_def);
                }
                Step::StopDataNode(data_node_index) => {
                    let data_node_def = data.data_node_definitions[*data_node_index].clone();
                    data.stop_one_node(&data_node_def.config_file_name, false);
                }
                Step::RestartCluster => {
                    kill_all();
                    run_cluster(
                        &self.test_dir,
                        self.runtime.clone(),
                        &self.cluster_definition,
                        false,
                        false,
                    );
                }
                Step::Sleep(seconds) => {
                    std::thread::sleep(Duration::from_secs(*seconds));
                }
                Step::StopMetaNode(meta_node_index) => {
                    let meta = meta.as_mut().unwrap();
                    let meta_node_def = meta.meta_node_definitions[*meta_node_index].clone();
                    meta.stop_one_node(&meta_node_def.config_file_name, false);
                }
            }
        }
    }
}

// TODO(zipper): Supports: 1. cli execution;
#[derive(Debug)]
pub enum Step {
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
    StopMetaNode(usize),
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Step::Log(msg) => write!(f, "Log: {msg}"),
            Step::CnosdbRequest { req, auth } => {
                write!(f, "CnosdbRequest: {req}, ")?;
                match auth {
                    Some(a) => write!(f, ", with auth: '{a}'"),
                    None => write!(f, "no auth"),
                }
            }
            Step::RestartDataNode(i) => write!(f, "RestartDataNode of data node {i}"),
            Step::StartDataNode(i) => write!(f, "StartDataNode of data node {i}"),
            Step::StopDataNode(i) => write!(f, "StopDataNode of data node {i}"),
            Step::RestartCluster => write!(f, "RestartCluster"),
            Step::Sleep(d) => write!(f, "Sleep for {d} seconds"),
            Step::StopMetaNode(i) => write!(f, "StopMeta of meta node {i}"),
        }
    }
}

#[derive(Debug)]
pub enum CnosdbRequest {
    Query {
        url: &'static str,
        sql: &'static str,
        /// The expected response, and result lines if server returns ok.
        resp: E2eResult<Vec<&'static str>>,
        sorted: bool,
        /// If resp is checked by regex.
        regex: bool,
    },
    Insert {
        url: &'static str,
        sql: &'static str,
        resp: E2eResult<()>,
    },
    Write {
        url: &'static str,
        req: &'static str,
        resp: E2eResult<String>,
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
                regex,
            } => {
                write!(
                    f,
                    "Query to '{url}', SQL: '{sql}' => {} {} {resp:?}",
                    if *sorted { "sorted" } else { "" },
                    if *regex { "regex" } else { "" },
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

#[derive(Debug)]
pub struct CnosdbAuth {
    pub username: String,
    pub password: Option<String>,
}

impl std::fmt::Display for CnosdbAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{username}", username = self.username)?;
        if let Some(password) = &self.password {
            write!(f, ":{password}")?;
        }
        Ok(())
    }
}

fn compare_e2e_error(expected: &E2eError, actual: &E2eError) -> bool {
    match (expected, actual) {
        (E2eError::Connect(msg1), E2eError::Connect(msg2)) => msg1 == msg2,
        (
            E2eError::Http {
                status: status1,
                url: url1,
                req: req1,
                err: err1,
            },
            E2eError::Http {
                status: status2,
                url: url2,
                req: req2,
                err: err2,
            },
        ) => {
            status1 == status2
                && match (url1, url2) {
                    (None, _) => true,
                    (Some(u1), Some(u2)) => u1 == u2,
                    (Some(_), None) => false,
                }
                && match (req1, req2) {
                    (None, _) => true,
                    (Some(r1), Some(r2)) => r1 == r2,
                    (Some(_), None) => false,
                }
                && match (err1, err2) {
                    (None, _) => true,
                    (Some(e1), Some(e2)) => e1 == e2,
                    (Some(_), None) => false,
                }
        }
        (
            E2eError::Api {
                status: status1,
                url: url1,
                req: req1,
                resp: resp1,
            },
            E2eError::Api {
                status: status2,
                url: url2,
                req: req2,
                resp: resp2,
            },
        ) => {
            status1 == status2
                && match (url1, url2) {
                    (None, _) => true,
                    (Some(u1), Some(u2)) => u1 == u2,
                    (Some(_), None) => false,
                }
                && match (req1, req2) {
                    (None, _) => true,
                    (Some(r1), Some(r2)) => r1 == r2,
                    (Some(_), None) => false,
                }
                && match (resp1, resp2) {
                    (None, _) => true,
                    (Some(r1), Some(r2)) => r1 == r2,
                    (Some(_), None) => false,
                }
        }
        _ => false,
    }
}
