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
    /// Basic-auth account for the stats page when reached over the network.
    /// Loopback (the in-container monitor and the container healthcheck)
    /// never authenticates. `None` means no credential is available, and
    /// remote access to the stats page is denied outright.
    pub stats_auth: Option<StatsAuth>,
}

/// Credential guarding the stats page for non-loopback clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatsAuth {
    pub user: String,
    pub password: String,
}

/// Resolve the stats credential: `HAPROXY_STATS_USER` / `HAPROXY_STATS_PASSWORD`
/// when set, else the database account the edge already carries
/// (`MYSQLUSER` / `MYSQLPASSWORD`, stamped by the template). An empty
/// password yields `None`. Same shape as postgres-ha's edge (`PGUSER` /
/// `PGPASSWORD` there).
pub(crate) fn resolve_stats_auth(
    stats_user: Option<String>,
    stats_password: Option<String>,
    mysql_user: Option<String>,
    mysql_password: Option<String>,
) -> Option<StatsAuth> {
    let non_empty = |v: Option<String>| v.filter(|s| !s.trim().is_empty());
    let password = non_empty(stats_password).or_else(|| non_empty(mysql_password))?;
    let user = non_empty(stats_user)
        .or_else(|| non_empty(mysql_user))
        .unwrap_or_else(|| "root".to_string());
    Some(StatsAuth { user, password })
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
            stats_auth: resolve_stats_auth(
                std::env::var("HAPROXY_STATS_USER").ok(),
                std::env::var("HAPROXY_STATS_PASSWORD").ok(),
                std::env::var("MYSQLUSER").ok(),
                std::env::var("MYSQLPASSWORD").ok(),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Process env is global and `cargo test` runs tests on parallel
    /// threads: every test that reads or mutates env vars must hold this
    /// for its whole body, so env tests can never race each other.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// G11 pinned the idle-session timeouts to 1d (above mysqld's
    /// wait_timeout) — the test fixture in template.rs still uses 30m, so
    /// nothing unit-level notices if the production default regresses to
    /// the old 30m. This reads the real `from_env` defaults.
    #[test]
    fn production_defaults_keep_idle_sessions_at_1d() {
        let _env = ENV_LOCK.lock().unwrap();
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

    fn s(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    #[test]
    fn stats_auth_prefers_explicit_over_database_account() {
        let auth = resolve_stats_auth(s("ops"), s("secret"), s("root"), s("dbpass")).unwrap();
        assert_eq!(
            auth,
            StatsAuth {
                user: "ops".into(),
                password: "secret".into()
            }
        );
    }

    #[test]
    fn stats_auth_falls_back_to_the_database_account_the_edge_carries() {
        let auth = resolve_stats_auth(None, None, s("root"), s("dbpass")).unwrap();
        assert_eq!(
            auth,
            StatsAuth {
                user: "root".into(),
                password: "dbpass".into()
            }
        );
    }

    #[test]
    fn stats_auth_defaults_user_to_root_when_only_a_password_is_known() {
        let auth = resolve_stats_auth(None, s("secret"), None, None).unwrap();
        assert_eq!(auth.user, "root");
        let auth = resolve_stats_auth(None, None, None, s("dbpass")).unwrap();
        assert_eq!(auth.user, "root");
    }

    #[test]
    fn stats_auth_is_none_without_a_password() {
        assert!(resolve_stats_auth(s("ops"), None, s("root"), None).is_none());
        assert!(resolve_stats_auth(None, s("  "), None, s("")).is_none());
    }

    /// The template stamps MYSQLUSER/MYSQLPASSWORD on the edge (the Data UI
    /// variables); the real `from_env` must pick them up as the fallback.
    #[test]
    fn from_env_falls_back_to_mysqluser_and_mysqlpassword() {
        let _env = ENV_LOCK.lock().unwrap();
        for var in ["HAPROXY_STATS_USER", "HAPROXY_STATS_PASSWORD"] {
            std::env::remove_var(var);
        }
        std::env::set_var("MYSQL_NODES", "mysql-1.railway.internal:3306");
        std::env::set_var("MYSQLUSER", "root");
        std::env::set_var("MYSQLPASSWORD", "edge-carries-this");
        let config = Config::from_env().expect("from_env");
        for var in ["MYSQL_NODES", "MYSQLUSER", "MYSQLPASSWORD"] {
            std::env::remove_var(var);
        }
        assert_eq!(
            config.stats_auth,
            Some(StatsAuth {
                user: "root".into(),
                password: "edge-carries-this".into()
            })
        );
    }
}
