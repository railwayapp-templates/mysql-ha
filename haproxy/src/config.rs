use anyhow::{Context, Result};
use common::ConfigExt;

pub struct Config {
    /// Comma-separated "hostname:port" list of MySQL backends.
    /// Example: "mysql-1.railway.internal:3306,mysql-2.railway.internal:3306"
    pub mysql_nodes: String,
    /// Port where mysql-wrapper's health server listens on each backend node.
    pub health_port: u16,
    pub mysql_port: u16,
    pub max_conn: String,
    pub timeout_connect: String,
    pub timeout_client: String,
    pub timeout_server: String,
    pub timeout_check: String,
    pub check_interval: String,
    pub check_fastinter: String,
    pub check_downinter: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let mysql_nodes = String::env_required("MYSQL_NODES").context(
            "MYSQL_NODES is required.\n\
             Format: hostname:port,...\n\
             Example: mysql-1.railway.internal:3306,mysql-2.railway.internal:3306",
        )?;

        Ok(Self {
            mysql_nodes,
            health_port: u16::env_parse("HEALTH_CHECK_PORT", 8080),
            mysql_port: u16::env_parse("MYSQL_PORT", 3306),
            max_conn: String::env_or("HAPROXY_MAX_CONN", "10000"),
            timeout_connect: String::env_or("HAPROXY_TIMEOUT_CONNECT", "10s"),
            // Idle sessions are mysqld's to close (wait_timeout), not the
            // proxy's — keep these above it.
            timeout_client: String::env_or("HAPROXY_TIMEOUT_CLIENT", "1d"),
            timeout_server: String::env_or("HAPROXY_TIMEOUT_SERVER", "1d"),
            timeout_check: String::env_or("HAPROXY_TIMEOUT_CHECK", "3s"),
            check_interval: String::env_or("HAPROXY_CHECK_INTERVAL", "3s"),
            check_fastinter: String::env_or("HAPROXY_CHECK_FASTINTER", "500ms"),
            check_downinter: String::env_or("HAPROXY_CHECK_DOWNINTER", "500ms"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// G11 pinned the idle-session timeouts to 1d (above mysqld's
    /// wait_timeout) — the test fixture in template.rs still uses 30m, so
    /// nothing unit-level notices if the production default regresses to
    /// the old 30m. This reads the real `from_env` defaults.
    #[test]
    fn production_defaults_keep_idle_sessions_at_1d() {
        for var in [
            "HAPROXY_TIMEOUT_CONNECT",
            "HAPROXY_TIMEOUT_CLIENT",
            "HAPROXY_TIMEOUT_SERVER",
            "HAPROXY_TIMEOUT_CHECK",
        ] {
            std::env::remove_var(var);
        }
        std::env::set_var("MYSQL_NODES", "mysql-1.railway.internal:3306");
        let config = Config::from_env().expect("from_env with only MYSQL_NODES set");
        std::env::remove_var("MYSQL_NODES");
        assert_eq!(config.timeout_client, "1d");
        assert_eq!(config.timeout_server, "1d");
        assert_eq!(config.timeout_connect, "10s");
        assert_eq!(config.timeout_check, "3s");
    }
}
