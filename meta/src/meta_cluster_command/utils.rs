use std::error::Error;

use reqwest::Client;
use serde_json::{Map, Value};

pub async fn get_metrics(client: &Client, bind: &str) -> Result<Value, Box<dyn Error>> {
    let url = format!("http://{}/metrics", bind);
    let res = client.get(&url).send().await?;
    if res.status().is_success() {
        let body: Value = res.json().await?;
        Ok(body)
    } else {
        Err(format!("Failed to get information from {}: {}", bind, res.status()).into())
    }
}

pub async fn get_leader_info(
    body: &Value,
) -> Result<(u64, Map<String, Value>, String), Box<dyn Error>> {
    let leader_id = body["vote"]["leader_id"]["node_id"]
        .as_u64()
        .ok_or("Leader ID not found")?;
    let nodes = body["membership_config"]["membership"]["nodes"]
        .as_object()
        .ok_or("Nodes not found in metrics")?
        .clone();
    let leader_addr = nodes
        .get(&leader_id.to_string())
        .and_then(|node| node["address"].as_str())
        .unwrap_or("")
        .to_string();
    Ok((leader_id, nodes, leader_addr))
}

pub async fn check_leader(bind: &str, leader_addr: &str) -> Result<(), Box<dyn Error>> {
    if leader_addr != bind {
        if leader_addr.is_empty() {
            eprintln!("No leader found in the cluster of this bind address.\nPlease enter the correct bind address.");
        } else {
            eprintln!(
                "The bind address {} is not the leader. Current bind is {}.",
                bind, leader_addr
            );
        }
        return Err("Not the leader".into());
    }
    Ok(())
}
