use std::fs::File;
use std::io::Read;
use std::time::Duration;

use reqwest::Client;
use serde_json::Value;
pub async fn restore(bind: &str, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(600))
        .build()?;

    let url = format!("http://{}/restore", bind);
    let mut file = File::open(file_path)?;
    let mut file_content = Vec::new();
    file.read_to_end(&mut file_content)?;
    let response = client.post(&url).body(file_content).send().await?;

    if !response.status().is_success() {
        return Err(format!("Failed to restore data: {}", response.status()).into());
    }

    let response_body = response.text().await?;
    let response_json: Value = serde_json::from_str(&response_body)
        .unwrap_or_else(|_| Value::String(response_body.clone()));

    if response_json.get("Err").is_some() {
        return Err(format!("Error in restore response: {:?}", response_json).into());
    }
    println!("{}", response_body);

    Ok(())
}
