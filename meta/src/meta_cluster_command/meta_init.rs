use serde_json::Value;

use super::meta_http_client::HttpClient;

pub async fn meta_init(bind: &str) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = HttpClient::new();
    let url = format!("http://{}/is_initialized", bind);
    let response_text: String = http_client.http_request_method("GET", &url, "").await?;

    if response_text.contains("\"initialized\": true") {
        println!("Cluster is already initialized at {}", bind);
        return Ok(());
    }

    if response_text.contains("\"initialized\": false") {
        let url = format!("http://{}/init", bind);
        let res_body: Value = http_client.http_request_method("POST", &url, "").await?;

        if res_body.get("Err").is_some() {
            return Err(format!("Error initializing cluster at {}: {:?}", bind, res_body).into());
        }

        println!("Cluster initialized successfully at {}", bind);
        return Ok(());
    }

    Err(format!("Unexpected response body: {}", response_text).into())
}
