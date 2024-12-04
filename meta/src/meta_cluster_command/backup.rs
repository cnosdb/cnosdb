use std::fs::File;
use std::io::Write;

use reqwest::Client;
use serde_json::Value;
pub async fn backup(
    bind: &str,
    cluster_name: &str,
    file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let ddl_url = format!("http://{}/dump/sql/ddl/{}", bind, cluster_name);
    let ddl_response = client.post(&ddl_url).send().await?;
    let status = ddl_response.status();
    let ddl_text = ddl_response.text().await?;
    let ddl_body: Value =
        serde_json::from_str(&ddl_text).unwrap_or_else(|_| Value::String(ddl_text.clone()));
    if !status.is_success() {
        return Err(format!("Failed to dump SQL: {}", status).into());
    }
    if let Some(err) = ddl_body.get("Err") {
        return Err(format!("Error in dump SQL response: {:?}", err).into());
    }
    let mut file = File::create(file)?;
    file.write_all(ddl_text.as_bytes())?;

    Ok(())
}
