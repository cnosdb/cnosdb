// use clap::Parser;
//
// use serde::Deserialize;
// use serde::Serialize;
//
// #[derive(Clone, Debug, Serialize, Deserialize, Parser)]
// pub struct Config {
//     /// The application specific name of this Raft cluster
//     #[clap(
//         long,
//         env = "RAFT_SNAPSHOT_PATH",
//         default_value = "/tmp/cnosdb/meta/snapshot"
//     )]
//     pub snapshot_path: String,
//
//     #[clap(long, env = "RAFT_INSTANCE_PREFIX", default_value = "match")]
//     pub instance_prefix: String,
//
//     #[clap(
//         long,
//         env = "RAFT_JOURNAL_PATH",
//         default_value = "/tmp/cnosdb/meta/journal"
//     )]
//     pub journal_path: String,
//
//     #[clap(long, env = "RAFT_SNAPSHOT_PER_EVENTS", default_value = "500")]
//     pub snapshot_per_events: u32,
//
//     #[clap(long, env = "META_LOGS_PATH", default_value = "/tmp/cnosdb/meta/logs")]
//     pub logs_path: String,
//
//     #[clap(long, env = "META_LOGS_LEVEL", default_value = "info")]
//     pub logs_level: String,
// }
//
// impl Default for Config {
//     fn default() -> Self {
//         <Self as Parser>::parse_from(&Vec::<&'static str>::new())
//     }
// }
