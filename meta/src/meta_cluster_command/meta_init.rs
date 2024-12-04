use reqwest::Client;
use serde_json::Value;
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
            let status = response.status();
            let res_body: Value = response
                .json()
                .await
                .unwrap_or_else(|_| Value::String(status.to_string()));
            if status.is_success() {
                if res_body.get("Err").is_some() {
                    eprintln!("Error initializing cluster at {}: {:?}", bind, res_body);
                    return Err(
                        format!("Error initializing cluster at {}: {:?}", bind, res_body).into(),
                    );
                }
                println!("Cluster initialized successfully at {}", bind);
            } else {
                eprintln!("Failed to initialize cluster at {}: {}", bind, status);
            }
        }
        Err(err) => {
            eprintln!("Failed to send request: {}", err);
        }
    }

    Ok(())
}
