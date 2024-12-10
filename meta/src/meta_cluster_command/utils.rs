use std::error::Error;
use std::fs::File;
use std::io::Write;

use openraft::RaftMetrics;
use replication::{RaftNodeId, RaftNodeInfo};
use reqwest::Client;

use super::meta_http_client::HttpClient;
pub async fn get_metrics(
    http_client: &HttpClient,
    bind: &str,
) -> Result<RaftMetrics<RaftNodeId, RaftNodeInfo>, Box<dyn Error>> {
    let url = format!("http://{}/metrics", bind);
    let res: RaftMetrics<RaftNodeId, RaftNodeInfo> =
        http_client.http_request_method("GET", &url, "").await?;
    Ok(res)
}

pub async fn http_save_to_file(url: &str, file: &str) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let response = client.post(url).send().await?;
    let status = response.status();

    let response_text = response.text().await?;
    // println!("Response body: {}", response_text);

    if !status.is_success() {
        return Err(format!("Request to {} failed with status: {}", url, status).into());
    }

    if response_text.contains("Err") {
        return Err(format!("Error in response from {}: {}", url, response_text).into());
    }

    let mut file = File::create(file)?;
    file.write_all(response_text.as_bytes())?;
    file.flush()?;

    Ok(())
}
