use std::env::current_exe;
use std::path::PathBuf;

use clap::Parser;
use groups::TestGroups;
use lazy_static::lazy_static;
use reqwest::Url;

use crate::client::Client;
use crate::error::{Error, Result};

mod case;
mod client;
mod db_request;
mod db_result;
mod error;
mod groups;

#[derive(Debug, Parser)]
#[command(author, version = "0.1.0", about, long_about = None)]
pub struct Args {
    /// client url
    #[arg(
        short,
        long,
        value_parser,
        default_value = "http://127.0.0.1:8902/api/v1/"
    )]
    pub client_url: Url,

    /// work thread num
    #[arg(short, long, value_parser, default_value = "4")]
    pub thread: usize,

    #[arg(short, long)]
    pub pattern: Option<String>,
}

fn default_case_path() -> Result<PathBuf> {
    let mut cases_path = current_exe().map_err(|_| Error::CasePathNotFound)?;
    loop {
        if cases_path.is_dir() {
            let cases_path = cases_path.join("query_server/test/cases");
            if cases_path.exists() {
                return Ok(cases_path);
            }
        }

        if !cases_path.pop() {
            return Err(Error::CasePathNotFound);
        }
    }
}

lazy_static! {
    pub static ref ARGS: Args = Args::parse();
    pub static ref CLIENT: Client = Client::from_url(ARGS.client_url.clone());
}

fn main() -> Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(ARGS.thread)
        .enable_time()
        .enable_io()
        .build()
        .unwrap();

    let case_path = default_case_path()?;
    let mut groups = TestGroups::load(&case_path, ARGS.pattern.clone())?;

    let ping = rt.block_on(CLIENT.ping());
    //TODO (ADD startup db)
    if !ping {
        println!("db hasn't setup!");
    }

    let fail_cases = rt.block_on(groups.run());
    if fail_cases.is_empty() {
        return Ok(());
    }

    println!("FAIL {} cases FAIL:", fail_cases.len());
    for case in &fail_cases {
        println!("\t{}", case);
    }
    println!();
    Err(Error::CaseFail)
}
