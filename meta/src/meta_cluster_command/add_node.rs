use serde_json::Value;
use tokio::time::{sleep, Duration};

use super::meta_http_client::HttpClient;
use super::utils::get_metrics;
pub async fn add_node(bind: &str, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = HttpClient::new();
    let body = get_metrics(&http_client, bind).await?;
    let nodes = body
        .membership_config
        .membership()
        .nodes()
        .collect::<Vec<_>>();
    let add_body = get_metrics(&http_client, addr).await?;
    let add_node_id = add_body.id;

    let url = format!("http://{}/add-learner", bind);
    let data = serde_json::json!([add_node_id, addr]);
    let res_body: Value = http_client
        .http_request_method("POST", &url, &data.to_string())
        .await?;

    if res_body.get("Err").is_some() {
        return Err(format!(
            "Error adding node {} to meta service at {}: {}",
            addr,
            bind,
            res_body
                .get("Err")
                .unwrap_or(&Value::String("Unknown error".to_string()))
        )
        .into());
    }
    let mut node_ids: Vec<u64> = nodes.iter().map(|(id, _)| **id).collect();
    node_ids.push(add_node_id);
    let change_membership_url = format!("http://{}/change-membership", bind);

    let mut attempts = 0;
    let max_attempts = 3;
    while attempts < max_attempts {
        let change_res_body: Value = http_client
            .http_request_method(
                "POST",
                &change_membership_url,
                &serde_json::to_string(&node_ids)?,
            )
            .await?;

        if change_res_body.get("Err").is_none() {
            println!(
                "Node {} added and cluster membership updated successfully at {}.",
                addr, bind
            );
            return Ok(());
        }

        attempts += 1;
        if attempts == max_attempts {
            return Err(format!(
                "Error updating cluster membership at {}: {:?}",
                bind, change_res_body
            )
            .into());
        }

        sleep(Duration::from_secs(10)).await;
    }

    Ok(())
}
