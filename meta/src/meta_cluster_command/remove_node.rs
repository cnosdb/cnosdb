use serde_json::Value;

use super::meta_http_client::HttpClient;
use super::utils::get_metrics;

pub async fn remove_node(bind: &str, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = HttpClient::new();
    let body = get_metrics(&http_client, bind).await?;

    let leader_id = body.vote.leader_id.node_id;
    let nodes = body
        .membership_config
        .membership()
        .nodes()
        .collect::<Vec<_>>();
    let node_id_to_remove = nodes
        .iter()
        .find(|(_, v)| v.address == addr)
        .map(|(k, _)| **k)
        .ok_or_else(|| format!("Node with address {} not found in the cluster", addr))?;

    let url = format!("http://{}/change-membership", bind);
    let mut nodes_map = nodes.clone();
    nodes_map.retain(|(id, _)| **id != node_id_to_remove);

    let node_ids: Vec<u64> = nodes_map.iter().map(|(id, _)| **id).collect();
    let data = serde_json::json!(node_ids);
    let res_body: Value = http_client
        .http_request_method("POST", &url, &serde_json::to_string(&data)?)
        .await?;

    if res_body.get("Err").is_some() {
        return Err(format!(
            "Error removing node {} from meta service at {}: {}",
            addr,
            bind,
            res_body
                .get("Err")
                .unwrap_or(&Value::String("Unknown error".to_string()))
        )
        .into());
    }
    println!("Node {} removed successfully", addr);

    if node_id_to_remove == leader_id {
        if let Some(new_leader_candidate) = nodes_map.iter().find_map(|(_, node_info)| {
            let new_addr = &node_info.address;
            if new_addr != addr {
                Some(new_addr.to_string())
            } else {
                None
            }
        }) {
            for _ in 0..100 {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let body = get_metrics(&http_client, &new_leader_candidate).await?;
                let new_leader_id = body.vote.leader_id.node_id;
                if let Some(new_leader_info) =
                    body.membership_config.membership().get_node(&new_leader_id)
                {
                    let new_leader_addr = &new_leader_info.address;
                    if new_leader_id != node_id_to_remove {
                        println!("New leader address: {}", new_leader_addr);
                        return Ok(());
                    }
                }
            }
        } else {
            eprintln!("No other nodes available to query for new leader.");
        }
    }
    Ok(())
}
