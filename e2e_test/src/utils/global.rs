#![allow(unused)]

use std::collections::{BTreeMap, HashSet};
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::{Arc, Once};

use parking_lot::Mutex;
use tokio::runtime::Runtime;

use crate::case::E2eExecutor;
use crate::cluster_def::CnosdbClusterDefinition;
use crate::utils::{cargo_build_cnosdb_data, cargo_build_cnosdb_meta, get_workspace_dir};

pub const LOOPBACK_IP: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
pub const WILDCARD_IP: Ipv4Addr = Ipv4Addr::new(0_u8, 0_u8, 0_u8, 0_u8);
pub const MIN_PORT: u16 = 17000;
pub const MAX_PORT: u16 = 18000;

/// The base directory for e2e test.
/// The test directory will be created under this directory: '{E2E_TEST_BASE_DIR}/{case_group}/{case_name}'.
pub const E2E_TEST_BASE_DIR: &str = "/tmp/e2e_test/";

/// Context for an e2e test case.
pub struct E2eContext {
    case_group: Arc<String>,
    case_name: Arc<String>,
    test_dir: Arc<PathBuf>,
    global_context: Arc<E2eGlobalContext>,
}

impl E2eContext {
    pub fn new(case_group: &str, case_name: &str) -> Self {
        static mut GLOBAL_CONTEXT: Option<Arc<E2eGlobalContext>> = None;

        static INIT_TEST: Once = Once::new();
        INIT_TEST.call_once(|| unsafe {
            GLOBAL_CONTEXT = Some(Arc::new(E2eGlobalContext::default()));

            let workspace_dir = get_workspace_dir();
            cargo_build_cnosdb_meta(&workspace_dir);
            cargo_build_cnosdb_data(&workspace_dir);
        });

        #[allow(static_mut_refs)]
        let global_context = unsafe {
            // SAFETY: The static variable is initialized by INIT_TEST.call_once.
            GLOBAL_CONTEXT
                .clone()
                .expect("initialized by INIT_TEST.call_once")
        };

        let case_group = Arc::new(case_group.to_string());
        let case_name = Arc::new(case_name.to_string());
        let test_dir = Arc::new(
            PathBuf::from(E2E_TEST_BASE_DIR)
                .join(case_group.as_ref())
                .join(case_name.as_ref()),
        );

        // Initialize the test directory, remove the old one if exists.
        if test_dir.exists() {
            println!("- Removing test directory: {}", test_dir.display());
            let _ = std::fs::remove_dir_all(test_dir.as_ref());
        }
        println!("- Creating test directory: {}", test_dir.display());
        std::fs::create_dir_all(test_dir.as_ref())
            .map_err(|e| format!("Create test dir '{}' failed: {e}", test_dir.display()))
            .unwrap();

        Self {
            case_group,
            case_name,
            test_dir,
            global_context,
        }
    }

    pub fn next_wildcard_addr(&self) -> IoResult<SocketAddrV4> {
        self.global_context.port_picker.lock().next_wildcard_addr()
    }

    pub fn next_addr(&self, host: Ipv4Addr) -> IoResult<SocketAddrV4> {
        self.global_context.port_picker.lock().next_addr(host)
    }

    /// Build executor by the given cluster definition, listening ports will be reset.
    pub fn build_executor(
        &mut self,
        mut cluster_definition: CnosdbClusterDefinition,
    ) -> E2eExecutor {
        let mut meta_host_ports = Vec::new();
        for def in cluster_definition.meta_cluster_def.iter_mut() {
            def.reset_ports(self);
            meta_host_ports.push(def.host_port);
        }
        let is_singleton_mode = meta_host_ports.is_empty();
        for def in cluster_definition.data_cluster_def.iter_mut() {
            def.reset_ports(self);
            if is_singleton_mode {
                // In singleton mode, we need to reset the meta host port defined by data node.
                for host_port in def.meta_host_ports.iter_mut() {
                    let new_host_port = self.next_addr(*host_port.ip()).expect("get a valid port");
                    *host_port = new_host_port;
                }
            } else {
                def.meta_host_ports = meta_host_ports.clone();
            }
        }
        E2eExecutor::new_cluster(self, Arc::new(cluster_definition))
    }

    pub fn case_group(&self) -> Arc<String> {
        self.case_group.clone()
    }

    pub fn case_name(&self) -> Arc<String> {
        self.case_name.clone()
    }

    pub fn test_dir(&self) -> Arc<PathBuf> {
        self.test_dir.clone()
    }

    pub fn runtime(&self) -> Arc<Runtime> {
        self.global_context.runtime.clone()
    }
}

impl std::fmt::Display for E2eContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "# Case: [{}.{}]", self.case_group, self.case_name)
    }
}

pub struct E2eGlobalContext {
    runtime: Arc<Runtime>,
    port_picker: Arc<Mutex<Ipv4PortPicker>>,
}

impl Default for E2eGlobalContext {
    fn default() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .map_err(|e| format!("Failed to build runtime: {e}"))
            .unwrap();
        Self {
            runtime: Arc::new(runtime),
            port_picker: Arc::new(Mutex::new(Ipv4PortPicker::default())),
        }
    }
}

pub struct Ipv4PortPicker {
    range: RangeInclusive<u16>,

    /// Maps a host IP to the next port number and the set of ports that have been used.
    using: BTreeMap<Ipv4Addr, (u16, HashSet<u16>)>,
}

impl Default for Ipv4PortPicker {
    fn default() -> Self {
        Self {
            range: MIN_PORT..=MAX_PORT,

            using: BTreeMap::new(),
        }
    }
}

impl Ipv4PortPicker {
    pub fn with_range(range: RangeInclusive<u16>) -> Self {
        Self {
            range,

            ..Default::default()
        }
    }

    /// Get the next port for IP 0.0.0.0 .
    pub fn next_wildcard_addr(&mut self) -> IoResult<SocketAddrV4> {
        self.next_addr(WILDCARD_IP)
    }

    /// Get the next port for the given host IP.
    pub fn next_addr(&mut self, host: Ipv4Addr) -> IoResult<SocketAddrV4> {
        let (port_cur, ports_using) = self
            .using
            .entry(host)
            .or_insert((*self.range.start(), HashSet::new()));

        for port in *port_cur..=*self.range.end() {
            if ports_using.contains(&port) {
                continue;
            }
            if Self::try_bind(host, port)? {
                ports_using.insert(port);
                *port_cur = port + 1;
                return Ok(SocketAddrV4::new(host, port));
            }
        }
        for port in *self.range.start()..=*port_cur {
            if ports_using.contains(&port) {
                continue;
            }
            if Self::try_bind(host, port)? {
                ports_using.insert(port);
                *port_cur = port + 1;
                return Ok(SocketAddrV4::new(host, port));
            }
        }
        Err(IoError::new(
            IoErrorKind::Other,
            format!("No available ports in {:?}", self.range),
        ))
    }

    fn try_bind(host: Ipv4Addr, port: u16) -> IoResult<bool> {
        let socket_addr = SocketAddrV4::new(host, port);
        match TcpListener::bind(socket_addr) {
            Ok(_) => Ok(true),
            Err(e) => {
                println!("Bind error: {e:?}");
                match e.kind() {
                    // code=49, message="Can't bind to the address"
                    IoErrorKind::AddrNotAvailable => Err(e),
                    // Could not resolve to any addresses
                    IoErrorKind::InvalidInput => Err(e),
                    // code=48, message="Address already in use"
                    IoErrorKind::AddrInUse => Ok(false),
                    // code=13, message="Permission denied"
                    // code=47, kind=Uncategorized, message="Address family not supported by protocol family"
                    _ => Err(e),
                }
            }
        }
    }
}

mod test {
    use std::net::{Ipv4Addr, TcpListener};

    use super::Ipv4PortPicker;

    #[test]
    fn test() {
        let mut picker = Ipv4PortPicker::default();
        {
            let mut listeners = Vec::new();
            let mut used_addrs = Vec::new();
            for _ in 0..5 {
                let addr = picker.next_wildcard_addr().unwrap();
                println!("Listening on: {addr:?}");
                let listener = TcpListener::bind(addr).unwrap();
                println!("Listened on: {:?}", listener.local_addr().unwrap());
                listeners.push(listener);
                used_addrs.push(addr);
            }
            for addr in used_addrs {
                println!("Trying bind twice on: {:?}", addr);
                let bind_result = TcpListener::bind(addr);
                assert!(bind_result.is_err());
                println!("Expected binding error: {:?}", bind_result.unwrap_err());
            }
        }
        {
            // Try to bind invalid address.
            let addr = picker.next_addr(Ipv4Addr::new(1, 0, 0, 1));
            assert!(addr.is_err());
            let addr = picker.next_addr(Ipv4Addr::new(255, 255, 255, 255));
            assert!(addr.is_err());
        }
    }
}
