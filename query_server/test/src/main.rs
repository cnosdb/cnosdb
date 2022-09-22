extern crate core;

use crate::client::CLIENT;
use clap::Parser;
use groups::TestGroups;
use lazy_static::lazy_static;
use std::path::PathBuf;

mod case;
mod client;
mod groups;
mod query;

#[derive(Parser, Debug)]
#[clap(author = "ccx", version = "0.1.0", about, long_about = None)]
pub struct Args {}

fn default_case_path() -> Result<PathBuf, std::io::Error> {
    let mut cases_path = std::env::current_exe()?;

    loop {
        if cases_path.is_dir() {
            let cases_path = cases_path.join("query_server/test/cases");
            if cases_path.exists() {
                return Ok(cases_path);
            }
        }

        if !cases_path.pop() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Not Found Cases Path",
            ));
        }
    }
}

lazy_static! {
    pub static ref CASES_PATH: PathBuf = default_case_path().unwrap();
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_time()
        .enable_io()
        .build()
        .unwrap();
    let mut groups = TestGroups::load(&CASES_PATH);
    let ping = rt.block_on(CLIENT.ping());
    //TODO (ADD startup db)
    if !ping {
        println!("db hasn't setup!");
    }
    rt.block_on(groups.run());
}
