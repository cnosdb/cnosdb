use std::fs::File;
use std::io::Write;

use regex::Regex;
use reqwest::Client;

use super::utils::{get_leader_info, get_metrics};

pub async fn backup(bind: &str, file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let body = get_metrics(&client, bind).await?;
    let (_leader_id, _nodes, leader_addr) = get_leader_info(&body).await?;
    if leader_addr != bind && leader_addr.is_empty() {
        eprintln!("No leader found in the cluster of this bind address.\nPlease enter the correct bind address.");
        return Ok(());
    }
    let dump_url = format!("http://{}/dump", bind);
    let dump_response = client.post(&dump_url).send().await?;
    if !dump_response.status().is_success() {
        return Err(format!("Failed to get dump: {}", dump_response.status()).into());
    }
    let dump_body = dump_response.text().await?;
    let re = Regex::new(r"/([^/]+)/tenants/").unwrap();
    let cluster_name = re
        .captures(&dump_body)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str())
        .ok_or("Cluster name not found in dump response")?;
    let ddl_url = format!("http://{}/dump/sql/ddl/{}", bind, cluster_name);
    let ddl_response = client.post(&ddl_url).send().await?;
    if !ddl_response.status().is_success() {
        return Err(format!("Failed to dump SQL: {}", ddl_response.status()).into());
    }

    let ddl_body = ddl_response.text().await?;
    let mut file = File::create(file)?;
    file.write_all(ddl_body.as_bytes())?;

    Ok(())
}
