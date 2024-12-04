use reqwest::Client;

pub async fn meta_init(bind: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let url = format!("http://{}/is_initialized", bind);
    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.text().await {
                    Ok(raw_body) => {
                        if raw_body.contains("true") {
                            println!("Cluster is already initialized at {}", bind);
                            return Ok(());
                        }
                    }
                    Err(err) => {
                        eprintln!("Failed to read response body: {}", err);
                        return Ok(());
                    }
                }
            } else {
                eprintln!("Unexpected response status: {}", response.status());
                return Ok(());
            }
        }
        Err(err) => {
            eprintln!("Failed to check cluster status: {}", err);
            return Ok(());
        }
    }
    let client = Client::new();
    let url = format!("http://{}/init", bind);
    let res = client.post(&url).json(&serde_json::json!({})).send().await;

    match res {
        Ok(response) => {
            if response.status().is_success() {
                println!("Cluster initialized successfully at {}", bind);
            } else {
                eprintln!(
                    "Failed to initialize cluster at {}: {}",
                    bind,
                    response.status()
                );
            }
        }
        Err(err) => {
            eprintln!("Failed to send request: {}", err);
        }
    }

    Ok(())
}
