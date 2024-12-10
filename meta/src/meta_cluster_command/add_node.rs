use openraft::error::{ClientWriteError, RaftError};
use openraft::raft::ClientWriteResponse;
use openraft::RaftMetrics;
use replication::{RaftNodeId, RaftNodeInfo, TypeConfig};
use tokio::time::{sleep, Duration};

use super::meta_http_client::HttpClient;
pub async fn add_node(bind: &str, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = HttpClient::new();
    let url = format!("http://{}/metrics", bind);
    let body: RaftMetrics<RaftNodeId, RaftNodeInfo> =
        http_client.http_request_method("GET", &url, "").await?;
    let nodes = body
        .membership_config
        .membership()
        .nodes()
        .collect::<Vec<_>>();
    let add_node_url = format!("http://{}/metrics", addr);
    let add_body: RaftMetrics<RaftNodeId, RaftNodeInfo> = http_client
        .http_request_method("GET", &add_node_url, "")
        .await?;
    let add_node_id = add_body.id;

    let url = format!("http://{}/add-learner", bind);
    let data = serde_json::json!([add_node_id, addr]);
    let res_body: Result<
        ClientWriteResponse<TypeConfig>,
        RaftError<u64, ClientWriteError<u64, RaftNodeInfo>>,
    > = http_client
        .http_request_method("POST", &url, &data.to_string())
        .await?;
    if let Err(err) = res_body {
        return Err(format!(
            "Error adding node {} to meta service at {}: {}",
            addr, bind, err
        )
        .into());
    }
    let mut node_ids: Vec<u64> = nodes.iter().map(|(id, _)| **id).collect();
    node_ids.push(add_node_id);
    let change_membership_url = format!("http://{}/change-membership", bind);

    let mut attempts = 0;
    let max_attempts = 3;
    while attempts < max_attempts {
        let change_res_body: Result<
            ClientWriteResponse<TypeConfig>,
            RaftError<u64, ClientWriteError<u64, RaftNodeInfo>>,
        > = http_client
            .http_request_method(
                "POST",
                &change_membership_url,
                &serde_json::to_string(&node_ids)?,
            )
            .await?;
        if let Ok(_response) = change_res_body {
            println!(
                "Node {} added and cluster membership updated successfully at {}.",
                addr, bind
            );
            return Ok(());
        } else if let Err(err) = change_res_body {
            return Err(format!(
                "Error adding node {} to meta service at {}: {}",
                addr, bind, err
            )
            .into());
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
