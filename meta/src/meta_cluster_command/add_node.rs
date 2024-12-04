use reqwest::Client;
use serde_json::{Map, Value};

use super::utils::{check_leader, get_leader_info, get_metrics};
pub async fn add_node(bind: &str, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let body = get_metrics(&client, bind).await?;
    let (_leader_id, nodes, leader_addr) = get_leader_info(&body).await?;
    check_leader(bind, &leader_addr).await?;
    let add_body = get_metrics(&client, addr).await?;
    let add_node_id = add_body["id"].as_u64().ok_or("Node ID not found")?;
    if nodes.values().any(|v| v["address"] == addr) {
        println!("Node with address {} already exists in the cluster.", addr);
        return Ok(());
    }
    if nodes.contains_key(&add_node_id.to_string()) {
        println!(
            "Node with ID {} already exists in the cluster.",
            add_node_id
        );
        return Ok(());
    }

    let url = format!("http://{}/add-learner", bind);
    let data = serde_json::json!([add_node_id, addr]);

    // Add node to meta service
    let res = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&data)
        .send()
        .await?;

    if !res.status().is_success() {
        return Err(format!(
            "Failed to add node {} to meta service at {}: {}",
            addr,
            bind,
            res.status()
        )
        .into());
    }
    println!(
        "Node {} added successfully to meta service at {}.",
        addr, bind
    );
    update_cluster_membership(&client, bind, &nodes, add_node_id).await?;

    Ok(())
}
async fn update_cluster_membership(
    client: &Client,
    bind: &str,
    nodes: &Map<String, Value>,
    new_node_id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://{}/change-membership", bind);
    let mut node_ids: Vec<u64> = nodes
        .keys()
        .filter_map(|id| id.parse::<u64>().ok())
        .collect();

    node_ids.push(new_node_id);
    let data = serde_json::json!(node_ids);

    let res = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&data)
        .send()
        .await?;

    if res.status().is_success() {
        println!("Cluster membership updated successfully at {}.", bind);
        Ok(())
    } else {
        Err(format!(
            "Failed to update cluster membership at {}: {}",
            bind,
            res.status()
        )
        .into())
    }
}
