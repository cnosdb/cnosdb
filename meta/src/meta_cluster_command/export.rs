use std::fs::File;
use std::io::Write;

use reqwest::Client;

use super::utils::{get_leader_info, get_metrics};
pub async fn export(bind: &str, file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let body = get_metrics(&client, bind).await?;
    let (_leader_id, _nodes, leader_addr) = get_leader_info(&body).await?;
    if leader_addr != bind && leader_addr.is_empty() {
        eprintln!("No leader found in the cluster of this bind address.\nPlease enter the correct bind address.");
        return Ok(());
    }
    let url = format!("http://{}/dump", bind);
    let response = client.post(&url).send().await?;
    if !response.status().is_success() {
        return Err(format!("Failed to export data: {}", response.status()).into());
    }
    let response_body = response.text().await?;
    let mut file = File::create(file)?;
    file.write_all(response_body.as_bytes())?;

    Ok(())
}
