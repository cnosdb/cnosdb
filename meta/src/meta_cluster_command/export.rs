use super::utils::http_save_to_file;

pub async fn export(bind: &str, file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://{}/dump", bind);
    http_save_to_file(&url, file).await
}
