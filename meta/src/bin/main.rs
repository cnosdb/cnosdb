use clap::Parser;
use config as global_config;
use meta::service::connection::Connections;
use meta::start_raft_node;
use meta::store::Store;
use meta::ExampleTypeConfig;
use openraft::Raft;
use trace::init_global_tracing;

pub type ExampleRaft = Raft<ExampleTypeConfig, Connections, Store>;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Option {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub http_addr: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let global_config = global_config::get_config("../config/config.toml");
    let mut _trace_guard = init_global_tracing(
        &global_config.log.path,
        "meta_server.log",
        &global_config.log.level,
    );
    // Parse the parameters passed by arguments.
    let options = Option::parse();
    start_raft_node(options.id, options.http_addr).await
}
