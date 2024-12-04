use reqwest::Client;
use serde_json::Value;

use super::utils::{check_leader, get_leader_info, get_metrics};
pub async fn remove_node(bind: &str, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let body = get_metrics(&client, bind).await?;
    let (leader_id, nodes, leader_addr) = get_leader_info(&body).await?;
    check_leader(bind, &leader_addr).await?;

    let node_id_to_remove = nodes
        .iter()
        .find(|(_, v)| v["address"] == addr)
        .and_then(|(k, _)| k.parse::<u64>().ok())
        .ok_or_else(|| format!("Node with address {} not found in the cluster", addr))?;
    // update cluster membership
    let url = format!("http://{}/change-membership", bind);
    let mut nodes_map = nodes.clone();
    nodes_map.remove(&node_id_to_remove.to_string());
    let node_ids: Vec<u64> = nodes_map
        .keys()
        .filter_map(|id| id.parse::<u64>().ok())
        .collect();
    let data = serde_json::json!(node_ids);
    let res = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&data)
        .send()
        .await?;

    if res.status().is_success() {
        let res_body: Value = res.json().await?;
        if res_body.get("Err").is_some() {
            eprintln!("{:?}", res_body);
            return Ok(());
        }
        println!(
            "Node {} removed successfully from meta service at {}",
            addr, bind
        );
        println!("Cluster membership updated successfully at {}", bind);
    } else {
        eprintln!(
            "Failed to update cluster membership at {}: {}",
            bind,
            res.status()
        );
    }

    // check if the removed node is the leader

    if node_id_to_remove == leader_id {
        // select a non-original leader address
        let new_leader_candidate = nodes_map.iter().find_map(|(_node_id, node_info)| {
            let new_addr = node_info["address"].as_str().unwrap_or("");
            if new_addr != addr {
                Some(new_addr.to_string())
            } else {
                None
            }
        });
        if let Some(candidate_addr) = new_leader_candidate {
            for _ in 0..100 {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let body = get_metrics(&client, &candidate_addr).await?;
                if let Some(new_leader_id) = body["vote"]["leader_id"]["node_id"].as_u64() {
                    let new_leader_addr = body["membership_config"]["membership"]["nodes"]
                        [new_leader_id.to_string()]["address"]
                        .as_str()
                        .unwrap_or("");

                    if new_leader_id != node_id_to_remove {
                        println!("New bind and leader address: {}", new_leader_addr);
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
