use clap::Parser;

#[derive(Clone, Debug, Parser)]
pub struct Opt {
    #[clap(
    long,
    long,
    default_value = "0"
    )]
    pub id: u64,

    #[clap(
    long,
    global = true,
    env = "http_addr",
    default_value = "0.0.0.0:31005"
    )]
    pub http_addr: String,

    #[clap(
    long,
    global = true,
    env = "http_addr",
    default_value = "0.0.0.0:31005"
    )]
    pub rpc_addr: String,

    /// The application specific name of this Raft cluster
    #[clap(
    long,
    env = "RAFT_SNAPSHOT_PATH",
    default_value = "./snapshot"
    )]
    pub snapshot_path: String,

    #[clap(long, env = "RAFT_INSTANCE_PREFIX", default_value = "meta_node")]
    pub instance_prefix: String,

    #[clap(
    long,
    env = "RAFT_JOURNAL_PATH",
    default_value = "./journal"
    )]
    pub journal_path: String,

    #[clap(long, env = "RAFT_SNAPSHOT_PER_EVENTS", default_value = "500")]
    pub snapshot_per_events: u32,

    #[clap(long, env = "META_LOGS_PATH", default_value = "./logs")]
    pub logs_path: String,

    #[clap(long, env = "META_LOGS_LEVEL", default_value = "debug")]
    pub logs_level: String,
}

impl Default for Opt {
    fn default() -> Self {
        <Self as Parser>::parse_from(&Vec::<&'static str>::new())
    }
}
