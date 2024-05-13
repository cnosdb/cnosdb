use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use super::{CaseFlowControl, CnosdbAuth, E2eContext};
use crate::utils::{kill_all, run_cluster, Client};
use crate::E2eResult;

pub type StepResult = E2eResult<Vec<String>>;
pub type FnStepResult = Box<dyn for<'a> Fn(&'a mut E2eContext) -> CaseFlowControl>;
pub type FnString = Box<dyn for<'a> Fn(&'a E2eContext) -> String>;
pub type FnStringVec = Box<dyn for<'a> Fn(&'a E2eContext) -> Vec<String>>;
pub type FnCommand = Box<dyn for<'a> Fn(&'a E2eContext) -> Vec<Command>>;
pub type FnGeneric<T> = Box<dyn for<'a> Fn(&'a E2eContext) -> T>;
pub type FnVoid = Box<dyn for<'a> Fn(&'a E2eContext)>;
pub type FnAfterRequestSucceed = Box<dyn for<'a> Fn(&'a mut E2eContext, &Vec<String>)>;

pub trait Step: std::fmt::Display {
    fn id(&self) -> usize;

    fn name(&self) -> &str;

    fn execute(&self, context: &mut E2eContext) -> CaseFlowControl;

    fn build_fail_message(&self, context: &E2eContext, result: &StepResult) -> String {
        format!(
            "test[{}.{}] steps[{}-{}] [{self}], result: {result:?}",
            context.case_group(),
            context.case_name(),
            self.id(),
            self.name(),
        )
    }
}

pub enum StrValue {
    Constant(String),
    Function(FnString),
}

impl StrValue {
    pub fn get(&self, context: &E2eContext) -> String {
        match self {
            StrValue::Constant(s) => s.clone(),
            StrValue::Function(f) => f(context),
        }
    }
}

impl std::fmt::Display for StrValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrValue::Constant(c) => write!(f, "{c}"),
            StrValue::Function(_) => write!(f, "fn(&c) -> String"),
        }
    }
}

pub enum StrVecValue {
    Constant(Vec<String>),
    Function(FnStringVec),
}

impl StrVecValue {
    pub fn get(&self, context: &E2eContext) -> Vec<String> {
        match self {
            StrVecValue::Constant(s) => s.clone(),
            StrVecValue::Function(f) => f(context),
        }
    }
}

pub enum GenericValue<T> {
    Constant(T),
    Function(FnGeneric<T>),
}

impl<T: Clone> GenericValue<T> {
    pub fn get(&self, context: &E2eContext) -> T {
        match self {
            Self::Constant(t) => t.clone(),
            Self::Function(f) => f(context),
        }
    }
}

pub struct WrappedStep {
    pub inner: Box<dyn Step>,
    pub before_execute: Option<FnVoid>,
    pub after_execute: Option<FnVoid>,
}

impl WrappedStep {
    pub fn new(
        inner: Box<dyn Step>,
        before_execute: Option<FnVoid>,
        after_execute: Option<FnVoid>,
    ) -> Self {
        Self {
            inner,
            before_execute,
            after_execute,
        }
    }
}

impl Step for WrappedStep {
    fn id(&self) -> usize {
        self.inner.id()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn execute(&self, context: &mut E2eContext) -> CaseFlowControl {
        if let Some(f) = &self.before_execute {
            f(context);
        }
        let result = self.inner.execute(context);
        if let Some(f) = &self.after_execute {
            f(context);
        }
        result
    }
}

impl std::fmt::Display for WrappedStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

pub struct FunctionStep {
    id: usize,
    name: String,
    function: FnStepResult,
}

impl FunctionStep {
    pub fn new(id: usize, name: String, function: FnStepResult) -> Self {
        Self { id, name, function }
    }
}

impl Step for FunctionStep {
    fn id(&self) -> usize {
        self.id
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn execute(&self, context: &mut E2eContext) -> CaseFlowControl {
        (self.function)(context)
    }
}

impl std::fmt::Display for FunctionStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Step(function)[{}-{}]", self.id(), self.name())
    }
}

pub struct LogStep {
    inner: FunctionStep,
}

impl LogStep {
    pub fn new(id: usize, msg: String) -> Self {
        Self {
            inner: FunctionStep::new(
                id,
                "Log".to_string(),
                Box::new(move |_| {
                    println!("LOG: {}", msg);
                    CaseFlowControl::Continue
                }),
            ),
        }
    }

    pub fn with_fn(id: usize, msg: FnString) -> Self {
        Self {
            inner: FunctionStep::new(
                id,
                "Log".to_string(),
                Box::new(move |context| {
                    println!("LOG: {}", msg(context));
                    CaseFlowControl::Continue
                }),
            ),
        }
    }
}

impl Step for LogStep {
    fn id(&self) -> usize {
        self.inner.id()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn execute(&self, context: &mut E2eContext) -> CaseFlowControl {
        self.inner.execute(context)
    }
}

impl std::fmt::Display for LogStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Step(log)[{}-{}]", self.id(), self.name())
    }
}

pub struct RequestStep {
    id: usize,
    name: String,
    req: CnosdbRequest,
    auth: Option<CnosdbAuth>,
    after_request_succeed: Option<FnAfterRequestSucceed>,
}

impl RequestStep {
    pub fn new<Name: ToString>(
        id: usize,
        name: Name,
        req: CnosdbRequest,
        auth: Option<CnosdbAuth>,
        after_request_succeed: Option<FnAfterRequestSucceed>,
    ) -> Self {
        Self {
            id,
            name: name.to_string(),
            req,
            auth,
            after_request_succeed,
        }
    }
}

impl Step for RequestStep {
    fn id(&self) -> usize {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn execute(&self, context: &mut E2eContext) -> CaseFlowControl {
        let client = match &self.auth {
            Some(a) => Arc::new(Client::with_auth(a.username.clone(), a.password.clone())),
            None => context.data_client(),
        };
        match &self.req {
            CnosdbRequest::SqlQuery(s) => s.do_request(context, self, client.as_ref()),
            CnosdbRequest::SqlInsert(s) => s.do_request(context, self, client.as_ref()),
            CnosdbRequest::LineProtocol(l) => l.do_request(context, self, client.as_ref()),
            CnosdbRequest::SqlDdl(s) => s.do_request(context, self, client.as_ref()),
        }
    }
}

impl std::fmt::Display for RequestStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Step(request)[{}-{}]:", self.id(), self.name())?;
        if let Some(auth) = &self.auth {
            write!(f, " auth:({auth}),")?;
        } else {
            write!(f, " auth(None),")?;
        }
        write!(f, " {}", self.req)
    }
}

pub enum CnosdbRequest {
    SqlQuery(SqlQuery),
    SqlInsert(SqlInsert),
    LineProtocol(LineProtocol),
    SqlDdl(SqlDdl),
}

impl std::fmt::Display for CnosdbRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CnosdbRequest::SqlQuery(s) => write!(f, "{s}"),
            CnosdbRequest::SqlInsert(s) => write!(f, "{s}"),
            CnosdbRequest::LineProtocol(s) => write!(f, "{s}"),
            CnosdbRequest::SqlDdl(s) => write!(f, "{s}"),
        }
    }
}

pub struct SqlQuery {
    pub url: StrValue,
    pub sql: StrValue,
    pub resp: E2eResult<StrVecValue>,
    pub sorted: bool,
}

impl SqlQuery {
    pub fn with_str<Url: ToString, Sql: ToString, Line: ToString>(
        url: Url,
        sql: Sql,
        resp: E2eResult<Vec<Line>>,
        sorted: bool,
    ) -> CnosdbRequest {
        CnosdbRequest::SqlQuery(Self {
            url: StrValue::Constant(url.to_string()),
            sql: StrValue::Constant(sql.to_string()),
            resp: resp.map(|v| StrVecValue::Constant(v.iter().map(|l| l.to_string()).collect())),
            sorted,
        })
    }

    pub fn with_fn(
        url: FnString,
        sql: FnString,
        resp: E2eResult<FnStringVec>,
        sorted: bool,
    ) -> CnosdbRequest {
        CnosdbRequest::SqlQuery(Self {
            url: StrValue::Function(url),
            sql: StrValue::Function(sql),
            resp: resp.map(|v| StrVecValue::Function(v)),
            sorted,
        })
    }

    fn do_request(
        &self,
        context: &mut E2eContext,
        request: &RequestStep,
        client: &Client,
    ) -> CaseFlowControl {
        let url = self.url.get(context);
        let sql = self.sql.get(context);
        let resp = self.resp.as_ref().map(|v| v.get(context));
        let result_resp = client.api_v1_sql(url, &sql);
        let fail_message = request.build_fail_message(context, &result_resp);
        match resp {
            Ok(exp_lines) => {
                assert!(result_resp.is_ok(), "{fail_message}");
                if let Ok(mut resp_lines) = result_resp {
                    if self.sorted {
                        resp_lines.sort_unstable();
                    }
                    assert_eq!(resp_lines, exp_lines.to_vec(), "{fail_message}");
                    if let Some(f) = &request.after_request_succeed {
                        f(context, &resp_lines);
                    }
                } else {
                    assert!(result_resp.is_err(), "{fail_message}");
                }
            }
            Err(exp_err) => {
                assert!(
                    result_resp.is_err(),
                    "{}",
                    request.build_fail_message(context, &result_resp)
                );
                assert_eq!(exp_err, &result_resp.unwrap_err(), "{fail_message}");
            }
        }
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for SqlQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SQL Query: url: {}, sql: {}", self.url, self.sql)
    }
}

pub struct SqlInsert {
    pub url: StrValue,
    pub sql: StrValue,
    pub resp: E2eResult<()>,
}

impl SqlInsert {
    pub fn with_str<Url: ToString, Sql: ToString>(
        url: Url,
        sql: Sql,
        resp: E2eResult<()>,
    ) -> CnosdbRequest {
        CnosdbRequest::SqlInsert(Self {
            url: StrValue::Constant(url.to_string()),
            sql: StrValue::Constant(sql.to_string()),
            resp,
        })
    }

    pub fn with_fn(url: FnString, sql: FnString, resp: E2eResult<()>) -> CnosdbRequest {
        CnosdbRequest::SqlInsert(Self {
            url: StrValue::Function(url),
            sql: StrValue::Function(sql),
            resp,
        })
    }

    fn do_request(
        &self,
        context: &mut E2eContext,
        request: &RequestStep,
        client: &Client,
    ) -> CaseFlowControl {
        let url = self.url.get(context);
        let sql = self.sql.get(context);
        let result_resp = client.api_v1_write(url, &sql);
        let fail_message = request.build_fail_message(context, &result_resp);
        match &self.resp {
            Ok(_) => {
                assert!(result_resp.is_ok(), "{fail_message}");
                if let Some(f) = &request.after_request_succeed {
                    f(context, &result_resp.unwrap());
                }
            }
            Err(exp_err) => {
                assert!(result_resp.is_err(), "{fail_message}");
                assert_eq!(exp_err, &result_resp.unwrap_err(), "{fail_message}");
            }
        }
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for SqlInsert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SQL Insert: url: {}, sql: {}", self.url, self.sql)
    }
}

pub struct LineProtocol {
    pub url: StrValue,
    pub req: StrValue,
    pub resp: E2eResult<()>,
}

impl LineProtocol {
    pub fn with_str<Url: ToString, Req: ToString>(
        url: Url,
        req: Req,
        resp: E2eResult<()>,
    ) -> CnosdbRequest {
        CnosdbRequest::LineProtocol(Self {
            url: StrValue::Constant(url.to_string()),
            req: StrValue::Constant(req.to_string()),
            resp,
        })
    }

    pub fn with_fn(url: FnString, req: FnString, resp: E2eResult<()>) -> CnosdbRequest {
        CnosdbRequest::LineProtocol(Self {
            url: StrValue::Function(url),
            req: StrValue::Function(req),
            resp,
        })
    }

    fn do_request(
        &self,
        context: &mut E2eContext,
        request: &RequestStep,
        client: &Client,
    ) -> CaseFlowControl {
        let url = self.url.get(context);
        let req = self.req.get(context);
        let result_resp = client.api_v1_write(url, &req);
        let fail_message = request.build_fail_message(context, &result_resp);
        match &self.resp {
            Ok(_) => {
                assert!(result_resp.is_ok(), "{fail_message}");
                if let Some(f) = &request.after_request_succeed {
                    f(context, &result_resp.unwrap());
                }
            }
            Err(exp_err) => {
                assert!(result_resp.is_err(), "{fail_message}");
                assert_eq!(exp_err, &result_resp.unwrap_err(), "{fail_message}");
            }
        }
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for LineProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Line Protocol: url: {}, req: {}", self.url, self.req)
    }
}

pub struct SqlDdl {
    url: StrValue,
    sql: StrValue,
    resp: E2eResult<()>,
}

impl SqlDdl {
    pub fn with_str<Url: ToString, Sql: ToString>(
        url: Url,
        sql: Sql,
        resp: E2eResult<()>,
    ) -> CnosdbRequest {
        CnosdbRequest::SqlDdl(Self {
            url: StrValue::Constant(url.to_string()),
            sql: StrValue::Constant(sql.to_string()),
            resp,
        })
    }

    pub fn with_fn(url: FnString, sql: FnString, resp: E2eResult<()>) -> CnosdbRequest {
        CnosdbRequest::SqlDdl(Self {
            url: StrValue::Function(url),
            sql: StrValue::Function(sql),
            resp,
        })
    }

    fn do_request(
        &self,
        context: &mut E2eContext,
        request: &RequestStep,
        client: &Client,
    ) -> CaseFlowControl {
        let url = self.url.get(context);
        let sql = self.sql.get(context);
        let result_resp = client.api_v1_sql(url, &sql);
        let fail_message = request.build_fail_message(context, &result_resp);
        match &self.resp {
            Ok(_) => {
                assert!(result_resp.is_ok(), "{fail_message}");
                if let Some(f) = &request.after_request_succeed {
                    f(context, &result_resp.unwrap());
                }
            }
            Err(exp_err) => {
                assert!(result_resp.is_err(), "{fail_message}");
                assert_eq!(exp_err, &result_resp.unwrap_err(), "{fail_message}");
            }
        }
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for SqlDdl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SQL DDL: url: {}, sql: {}", self.url, self.sql)
    }
}

pub struct ControlStep {
    id: usize,
    name: String,
    control: Control,
}

impl ControlStep {
    pub fn new<Name: ToString>(id: usize, name: Name, control: Control) -> Self {
        Self {
            id,
            name: name.to_string(),
            control,
        }
    }
}

impl Step for ControlStep {
    fn id(&self) -> usize {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn execute(&self, context: &mut E2eContext) -> CaseFlowControl {
        self.control.do_control(context);
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for ControlStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Step(Control)[{}-{}]: {}",
            self.id(),
            self.name(),
            self.control
        )
    }
}

pub enum Control {
    RestartDataNode(usize),
    StartDataNode(usize),
    StopDataNode(usize),
    RestartCluster,
    /// Sleep current thread for a while(in seconds)
    Sleep(u64),
}

impl Control {
    const WAIT_BEFORE_RESTART_SECONDS: u64 = 1;

    fn do_control(&self, context: &mut E2eContext) {
        let data = context.data_mut();
        match self {
            Control::RestartDataNode(data_node_index) => {
                if Self::WAIT_BEFORE_RESTART_SECONDS > 0 {
                    // TODO(zipper): The test sometimes fail if we restart just after a DDL or Insert, why?
                    std::thread::sleep(Duration::from_secs(Self::WAIT_BEFORE_RESTART_SECONDS));
                }
                let data_node_def = data.data_node_definitions[*data_node_index].clone();
                data.restart_one_node(&data_node_def);
            }
            Control::StartDataNode(data_node_index) => {
                let data_node_def = data.data_node_definitions[*data_node_index].clone();
                data.start_one_node(&data_node_def);
            }
            Control::StopDataNode(data_node_index) => {
                let data_node_def = data.data_node_definitions[*data_node_index].clone();
                data.stop_one_node(&data_node_def.config_file_name, false);
            }
            Control::RestartCluster => {
                kill_all();
                run_cluster(
                    context.test_dir(),
                    context.runtime(),
                    context.cluster_definition(),
                    false,
                    false,
                );
            }
            Control::Sleep(seconds) => {
                std::thread::sleep(Duration::from_secs(*seconds));
            }
        }
    }
}

impl std::fmt::Display for Control {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Control::RestartDataNode(id) => write!(f, "restart data node: {id}"),
            Control::StartDataNode(id) => write!(f, "start data node: {id}"),
            Control::StopDataNode(id) => write!(f, "stop data node: {id}"),
            Control::RestartCluster => write!(f, "restart cluster"),
            Control::Sleep(sec) => write!(f, "sleep {sec} seconds"),
        }
    }
}
pub struct ShellStep {
    id: usize,
    name: String,
    command: FnGeneric<Command>,
    terminate_on_fail: bool,
    before_execution: Option<FnVoid>,
    after_execute_succeed: Option<FnAfterRequestSucceed>,
}

impl ShellStep {
    pub fn with_fn<Name: ToString>(
        id: usize,
        name: Name,
        command: FnGeneric<Command>,
        terminate_on_fail: bool,
        before_execution: Option<FnVoid>,
        after_execute_succeed: Option<FnAfterRequestSucceed>,
    ) -> Self {
        Self {
            id,
            name: name.to_string(),
            command,
            terminate_on_fail,
            before_execution,
            after_execute_succeed,
        }
    }

    fn command_str(&self, context: &E2eContext) -> String {
        let command = (self.command)(context);
        Self::command_to_string(&command)
    }

    fn command_to_string(command: &Command) -> String {
        let mut buf = command.get_program().to_string_lossy().to_string();
        for arg in command.get_args() {
            buf.push(' ');
            buf.push_str(arg.to_string_lossy().as_ref());
        }
        buf
    }
}

impl Step for ShellStep {
    fn id(&self) -> usize {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn execute(&self, context: &mut E2eContext) -> CaseFlowControl {
        if let Some(f) = &self.before_execution {
            f(context);
        }
        let mut command = (self.command)(context);
        let command_string = Self::command_to_string(&command);
        let output = command
            .output()
            .unwrap_or_else(|e| panic!("failed to execute process '{command_string}': {e}"));
        if output.status.success() {
            if let Some(f) = &self.after_execute_succeed {
                let stdout_utf8 = String::from_utf8_lossy(&output.stdout);
                let lines: Vec<String> = stdout_utf8.lines().map(|l| l.to_string()).collect();
                f(context, &lines);
            }
        } else if self.terminate_on_fail {
            panic!("Execute shell command failed: {command_string}");
        }
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for ShellStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Step(shell)[{}-{}]: fn(&c) -> Command",
            self.id(),
            self.name(),
        )
    }
}
