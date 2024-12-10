use super::utils::http_save_to_file;

pub async fn backup(
    bind: &str,
    cluster_name: &str,
    file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let ddl_url = format!("http://{}/dump/sql/ddl/{}", bind, cluster_name);
    http_save_to_file(&ddl_url, file).await
}
