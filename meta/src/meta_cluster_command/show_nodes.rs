use super::meta_http_client::HttpClient;
use super::utils::get_metrics;

pub async fn show_nodes(bind: &str) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = HttpClient::new();
    let body = get_metrics(&http_client, bind).await?;

    let nodes = body
        .membership_config
        .membership()
        .nodes()
        .collect::<Vec<_>>();
    let term = body.current_term;
    let last_log_index = body.last_log_index.unwrap_or(0);
    let last_applied = body.last_applied.map(|log_id| log_id.index).unwrap_or(0);
    let leader = body.current_leader.unwrap_or(0);
    let members = body.membership_config.membership().get_joint_config();

    println!(
        "Node ID  Address         State     Term  Last_Log_index  Last_Applied  Leader  Members"
    );

    let members_str = members
        .iter()
        .map(|set| {
            let ids = set
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{}]", ids)
        })
        .collect::<Vec<_>>()
        .join(", ");

    for (node_id, node_info) in nodes {
        let address = &node_info.address;
        let state = if Some(*node_id) == body.current_leader {
            "Leader"
        } else {
            "Follower"
        };
        println!(
            "{:<8} {:<15} {:<9} {:<6} {:<16} {:<12} {:<7} {}",
            node_id, address, state, term, last_log_index, last_applied, leader, members_str
        );
    }
    Ok(())
}
