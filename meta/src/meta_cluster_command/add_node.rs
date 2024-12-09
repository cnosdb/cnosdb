use reqwest::Client;
use serde_json::Value;

use super::utils::get_metrics;
pub async fn add_node(bind: &str, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let body = get_metrics(&client, bind).await?;
    let nodes = body
        .membership_config
        .membership()
        .nodes()
        .collect::<Vec<_>>();
    let add_body = get_metrics(&client, addr).await?;
    let add_node_id = add_body.id;

    let url = format!("http://{}/add-learner", bind);
    let data = serde_json::json!([add_node_id, addr]);
    // Add node to meta service
    let res = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&data)
        .send()
        .await?;
    let status = res.status();
    let res_body: Value = res.json().await?;
    if !status.is_success() || res_body.get("Err").is_some() {
        return Err(format!(
            "Error adding node {} to meta service at {}: {}",
            addr,
            bind,
            res_body
                .get("Err")
                .unwrap_or(&Value::String(status.to_string()))
        )
        .into());
    }
    let mut node_ids: Vec<u64> = nodes.iter().map(|(id, _)| **id).collect();
    node_ids.push(add_node_id);
    let change_membership_url = format!("http://{}/change-membership", bind);
    let res = client
        .post(&change_membership_url)
        .header("Content-Type", "application/json")
        .json(&node_ids)
        .send()
        .await?;
    let status = res.status();
    let res_body: Value = res.json().await?;
    if status.is_success() {
        if res_body.get("Err").is_some() {
            return Err(format!(
                "Error updating cluster membership at {}: {:?}",
                bind, res_body
            )
            .into());
        }
        println!(
            "Node {} added and cluster membership updated successfully at {}.",
            addr, bind
        );
        Ok(())
    } else {
        Err(format!(
            "Failed to update cluster membership at {}: {}",
            bind, status
        )
        .into())
    }
}
