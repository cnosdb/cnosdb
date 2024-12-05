use reqwest::Client;

use super::utils::get_metrics;

pub async fn show_nodes(bind: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let body = get_metrics(&client, bind).await?;
    let nodes = body["membership_config"]["membership"]["nodes"].clone();
    if let Some(nodes) = nodes.as_object() {
        println!("Node ID  Address         State     Term  Last_Log_index  Last_Applied  Leader  Members");
        let current_leader = body["current_leader"].as_u64().unwrap_or(0);

        let configs = body["membership_config"]["membership"]["configs"].clone();
        let empty_vec = Vec::new();
        let members: Vec<u64> = configs
            .as_array()
            .map(|array| {
                array
                    .iter()
                    .flat_map(|config| config.as_array().unwrap_or(&empty_vec))
                    .filter_map(|id| id.as_u64())
                    .collect()
            })
            .unwrap_or_default();

        for (node_id, node_info) in nodes {
            let address = node_info["address"].as_str().unwrap_or("-");
            let state = if current_leader == node_id.parse().unwrap_or(0) {
                "Leader"
            } else {
                "Follower"
            };
            let term = body["current_term"].as_u64().unwrap_or(0);
            let last_log = body["last_log_index"].as_u64().unwrap_or(0);
            let last_applied = body["last_applied"]["index"].as_u64().unwrap_or(0);
            let leader = body["current_leader"].as_u64().unwrap_or(0);

            println!(
                "{:<8} {:<15} {:<9} {:<6} {:<16} {:<12} {:<7} {:?}",
                node_id, address, state, term, last_log, last_applied, leader, members
            );
        }
    } else {
        println!("No nodes found in the cluster.");
    }

    Ok(())
}
