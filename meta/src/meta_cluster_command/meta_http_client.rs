use std::error::Error;

use reqwest::Client;
use serde::de::DeserializeOwned;
pub struct HttpClient {
    client: Client,
}

impl HttpClient {
    pub fn new() -> Self {
        HttpClient {
            client: Client::new(),
        }
    }

    pub async fn http_request_method<T>(
        &self,
        method: &str,
        url: &str,
        data: &str,
    ) -> Result<T, Box<dyn Error>>
    where
        T: DeserializeOwned,
    {
        let request = match method.to_uppercase().as_str() {
            "POST" => self
                .client
                .post(url)
                .header("Content-Type", "application/json")
                .body(data.to_string()),
            "GET" => self.client.get(url),
            _ => return Err(format!("Unsupported HTTP method: {}", method).into()),
        };

        let res = request
            .send()
            .await
            .map_err(|e| format!("Request error: {}", e))?;

        if !res.status().is_success() {
            return Err(format!("Request failed with status: {}", res.status()).into());
        }

        let rsp = res
            .text()
            .await
            .map_err(|e| format!("Failed to read response body: {}", e))?;

        // println!("Response body: {}", rsp);

        let result = serde_json::from_str::<T>(&rsp)
            .map_err(|e| format!("Failed to deserialize response: {}, body: {}", e, rsp))?;

        Ok(result)
    }
}
impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}
