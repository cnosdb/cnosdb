use clap::Parser;
use meta::service::connection::Connections;
use meta::store::Store;
use meta::ExampleTypeConfig;
use meta::{start_raft_node, store};
use openraft::Raft;
use store::config::Config;
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
    let options = Option::parse();
    let config = Config::default();

    let logs_path = format!("{}/{}", config.logs_path, options.id);
    let _ = init_global_tracing(&logs_path, "meta_server.log", &config.logs_level);

    start_raft_node(options.id, options.http_addr).await
}
