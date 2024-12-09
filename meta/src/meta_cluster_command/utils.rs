use std::error::Error;

use openraft::RaftMetrics;
use replication::{RaftNodeId, RaftNodeInfo};
use reqwest::Client;
use serde_json::from_str;

pub async fn get_metrics(
    client: &Client,
    bind: &str,
) -> Result<RaftMetrics<RaftNodeId, RaftNodeInfo>, Box<dyn Error>> {
    let url = format!("http://{}/metrics", bind);
    let res = client.get(&url).send().await?;
    if res.status().is_success() {
        let body = res.bytes().await?;
        let body_str = std::str::from_utf8(&body)
            .map_err(|err| format!("Failed to convert body to string: {}", err))?;

        match from_str::<RaftMetrics<RaftNodeId, RaftNodeInfo>>(body_str) {
            Ok(metrics) => Ok(metrics),
            Err(err) => Err(format!("Failed to decode RaftMetrics: {}", err).into()),
        }
    } else {
        Err(format!("Failed to get information from {}: {}", bind, res.status()).into())
    }
}
