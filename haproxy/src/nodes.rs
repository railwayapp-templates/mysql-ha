use anyhow::{anyhow, Result};

pub struct MySqlNode {
    pub name: String,
    pub host: String,
    pub mysql_port: u16,
}

/// Parse the MYSQL_NODES env var.
///
/// Format: "hostname:port,hostname:port,..."
/// Example: "mysql-1.railway.internal:3306,mysql-2.railway.internal:3306"
pub fn parse_nodes(mysql_nodes: &str) -> Result<Vec<MySqlNode>> {
    mysql_nodes
        .split(',')
        .map(|entry| {
            let entry = entry.trim();
            let parts: Vec<&str> = entry.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(anyhow!(
                    "invalid node format: '{}'. Expected hostname:port",
                    entry
                ));
            }
            let host = parts[0].to_string();
            let mysql_port = parts[1]
                .parse::<u16>()
                .map_err(|_| anyhow!("invalid port in '{}': {}", entry, parts[1]))?;
            let name = host.split('.').next().unwrap_or(&host).to_string();

            Ok(MySqlNode { name, host, mysql_port })
        })
        .collect()
}
