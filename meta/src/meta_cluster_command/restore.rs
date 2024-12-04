use std::fs::File;
use std::io::Read;
use std::time::Duration;

use reqwest::Client;

use super::utils::{check_leader, get_leader_info, get_metrics};
pub async fn restore(bind: &str, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(600))
        .build()?;

    let body = get_metrics(&client, bind).await?;
    let (_leader_id, _nodes, leader_addr) = get_leader_info(&body).await?;
    check_leader(bind, &leader_addr).await?;
    let url = format!("http://{}/restore", bind);
    let mut file = File::open(file_path)?;
    let mut file_content = Vec::new();
    file.read_to_end(&mut file_content)?;
    let response = client.post(&url).body(file_content).send().await?;

    if !response.status().is_success() {
        return Err(format!("Failed to restore data: {}", response.status()).into());
    }

    let response_body = response.text().await?;
    println!("{}", response_body);

    Ok(())
}
