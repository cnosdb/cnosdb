use std::fs::File;
use std::io::Write;

use reqwest::Client;

pub async fn export(
    bind: &str,
    cluster_name: &str,
    file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let url = format!("http://{}/dump", bind);
    let response = client.post(&url).send().await?;
    let status = response.status();
    let response_text = response.text().await?;

    if !status.is_success() {
        return Err(format!("Failed to dump SQL: {}", status).into());
    }

    let lines: Vec<&str> = response_text.lines().collect();
    let mut filtered_lines = Vec::new();

    if !lines.is_empty() {
        filtered_lines.push(lines[0]);
    }

    for line in &lines[1..lines.len() - 1] {
        if line.contains(cluster_name) {
            filtered_lines.push(line);
        }
    }

    if lines.len() > 1 {
        filtered_lines.push(lines[lines.len() - 1]);
    }

    let mut file = File::create(file)?;
    for line in filtered_lines {
        writeln!(file, "{}", line)?;
    }
    Ok(())
}
