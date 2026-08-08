//! Configuration for the MySQL Group Replication node wrapper.
//!
//! Two modes, decided by GR_SEEDS:
//!   - unset  → standalone passthrough: mysqld runs with no Group Replication
//!     config at all; /health is a real liveness probe and /role answers 200
//!     while mysqld is alive (there is nothing to fence against).
//!   - set    → HA mode: my.cnf is rendered with Group Replication
//!     (single-primary, MYSQL communication stack), and the orchestrator
//!     decides bootstrap-vs-join. GR_REPLICATION_PASSWORD is required.

use anyhow::{bail, Context, Result};
use common::{ConfigExt, RailwayEnv};

pub struct Config {
    /// MySQL root password. Required — passed through to the upstream
    /// `docker-entrypoint.sh` via the environment (not re-forwarded as a CLI
    /// arg), which is what actually initializes the root account. The wrapper
    /// also uses it for its own local socket connection.
    pub mysql_root_password: String,
    pub mysql_port: u16,
    /// Group Replication server_id. Every node needs a distinct one; when
    /// unset it is derived from the node's private domain (FNV-1a, never 0).
    pub server_id: Option<u32>,
    /// Declarative HA switch (default true). The template stamps
    /// GR_ENABLED=true as its `haActiveVariable`; the revert flow strips it
    /// together with GR_SEEDS — either alone is enough to boot standalone.
    pub gr_enabled_flag: bool,
    /// Comma-separated "host:port" list of ALL group members (self included),
    /// in template order — the first entry is the bootstrap candidate. Ports
    /// are the SQL port (3306): the group uses the MYSQL communication stack,
    /// not a separate XCom port.
    pub gr_seeds: Option<String>,
    /// Group Replication group_name (a UUID). Resolution order at runtime:
    /// this env var > the marker persisted on the volume > a UUIDv5 derived
    /// from the Railway environment id (then persisted). The group name is
    /// baked into every group transaction's GTID, so once a group has run it
    /// must never change — hence the persisted marker taking precedence over
    /// re-derivation.
    pub gr_group_name: Option<String>,
    /// Password for the `gr_recovery` user (distributed recovery / clone).
    /// Required in HA mode.
    pub gr_replication_password: Option<String>,
    pub health_port: u16,
    /// This node's private Railway hostname.
    pub private_domain: String,
    /// Unix socket the wrapper uses for its own root connection.
    pub socket_path: String,
    /// MySQL datadir — the Railway volume mount. Markers (e.g. the persisted
    /// group name) live here so they survive redeploys, but are only written
    /// after mysqld is up: `mysqld --initialize` refuses a non-empty datadir.
    pub data_dir: String,
    /// Directory the rendered Group Replication config is written into.
    /// `/etc/my.cnf` in the official image `!includedir`s it.
    pub conf_dir: String,
    /// Timeout for a single peer /gr/state query.
    pub peer_query_timeout_ms: u64,
    /// How long the bootstrap decision must hold stable before the candidate
    /// actually bootstraps a brand-new group.
    pub bootstrap_dwell_seconds: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let mysql_root_password = String::env_required("MYSQL_ROOT_PASSWORD")
            .context("MYSQL_ROOT_PASSWORD must be set")?;

        let config = Self {
            mysql_root_password,
            mysql_port: u16::env_parse("MYSQL_PORT", 3306),
            server_id: non_empty(std::env::var("SERVER_ID").ok()).and_then(|v| v.parse().ok()),
            // Only a literal "false" disables — absence means "follow GR_SEEDS".
            gr_enabled_flag: std::env::var("GR_ENABLED").map_or(true, |v| v != "false"),
            gr_seeds: non_empty(std::env::var("GR_SEEDS").ok()),
            gr_group_name: non_empty(std::env::var("GR_GROUP_NAME").ok()),
            gr_replication_password: non_empty(std::env::var("GR_REPLICATION_PASSWORD").ok()),
            health_port: u16::env_parse("HEALTH_PORT", 8080),
            private_domain: RailwayEnv::private_domain(),
            socket_path: String::env_or("MYSQL_SOCKET", "/var/run/mysqld/mysqld.sock"),
            data_dir: non_empty(std::env::var("DATA_DIR").ok())
                .or_else(|| non_empty(std::env::var("RAILWAY_VOLUME_MOUNT_PATH").ok()))
                .unwrap_or_else(|| "/var/lib/mysql".to_string()),
            conf_dir: String::env_or("MYSQL_CONF_DIR", "/etc/mysql/conf.d"),
            peer_query_timeout_ms: u64::env_parse("PEER_QUERY_TIMEOUT_MS", 2000),
            bootstrap_dwell_seconds: u64::env_parse("BOOTSTRAP_DWELL_SECONDS", 15),
        };

        if config.gr_enabled() && config.gr_replication_password.is_none() {
            bail!("GR_REPLICATION_PASSWORD must be set when GR_SEEDS is set");
        }

        Ok(config)
    }

    pub fn gr_enabled(&self) -> bool {
        self.gr_enabled_flag && self.gr_seeds.is_some()
    }

    /// The bare hostnames from GR_SEEDS, in declared order.
    pub fn seed_hosts(&self) -> Vec<String> {
        self.gr_seeds
            .as_deref()
            .map(|s| {
                s.split(',')
                    .map(|entry| entry.trim().split(':').next().unwrap_or("").to_string())
                    .filter(|h| !h.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Peer hostnames — every declared seed except this node.
    pub fn peer_hosts(&self) -> Vec<String> {
        self.seed_hosts()
            .into_iter()
            .filter(|h| !h.eq_ignore_ascii_case(&self.private_domain))
            .collect()
    }

    /// Deterministic bootstrap candidacy: the first host in the declared seed
    /// list. Same convention as postgres-ha's etcd (alphabetically-first
    /// bootstrap leader) — a name-order pact, not an election. Only matters
    /// when no live group exists; the peer-query guard makes it safe.
    pub fn is_bootstrap_candidate(&self) -> bool {
        self.seed_hosts()
            .first()
            .is_some_and(|h| h.eq_ignore_ascii_case(&self.private_domain))
    }
}

/// `env::var` returns `Ok("")` for a variable that is set-but-empty, which is
/// meant the same as unset for every optional field here.
fn non_empty(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Mutex;

    /// Env vars are process-global and cargo runs tests in parallel threads —
    /// every test that reads or writes the environment holds this.
    pub(crate) static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env() {
        for key in [
            "MYSQL_ROOT_PASSWORD",
            "MYSQL_PORT",
            "SERVER_ID",
            "GR_ENABLED",
            "GR_SEEDS",
            "GR_GROUP_NAME",
            "GR_REPLICATION_PASSWORD",
            "HEALTH_PORT",
            "MYSQL_SOCKET",
            "DATA_DIR",
            "RAILWAY_VOLUME_MOUNT_PATH",
            "MYSQL_CONF_DIR",
            "RAILWAY_PRIVATE_DOMAIN",
        ] {
            env::remove_var(key);
        }
    }

    fn base_env() {
        clear_env();
        env::set_var("MYSQL_ROOT_PASSWORD", "pw");
    }

    #[test]
    fn requires_root_password() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        let err = Config::from_env().err().expect("should fail without a password");
        assert!(err.to_string().contains("MYSQL_ROOT_PASSWORD"));
    }

    #[test]
    fn defaults_are_applied_when_optional_vars_are_absent() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        let config = Config::from_env().unwrap();
        assert_eq!(config.mysql_port, 3306);
        assert_eq!(config.health_port, 8080);
        assert_eq!(config.socket_path, "/var/run/mysqld/mysqld.sock");
        assert_eq!(config.data_dir, "/var/lib/mysql");
        assert_eq!(config.conf_dir, "/etc/mysql/conf.d");
        assert!(config.server_id.is_none());
        assert!(!config.gr_enabled());
        assert!(config.seed_hosts().is_empty());
        assert!(!config.is_bootstrap_candidate());
    }

    #[test]
    fn ha_mode_requires_replication_password() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        env::set_var("GR_SEEDS", "mysql-1.railway.internal:3306");
        let err = Config::from_env().err().expect("should fail");
        assert!(err.to_string().contains("GR_REPLICATION_PASSWORD"));
    }

    #[test]
    fn seed_parsing_and_candidacy() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        env::set_var("RAILWAY_PRIVATE_DOMAIN", "MySQL-1.railway.internal");
        env::set_var(
            "GR_SEEDS",
            "mysql-1.railway.internal:3306, mysql-2.railway.internal:3306,mysql-3.railway.internal:3306",
        );
        env::set_var("GR_REPLICATION_PASSWORD", "rp");

        let config = Config::from_env().unwrap();
        assert!(config.gr_enabled());
        assert_eq!(
            config.seed_hosts(),
            vec![
                "mysql-1.railway.internal",
                "mysql-2.railway.internal",
                "mysql-3.railway.internal"
            ]
        );
        // Candidacy is case-insensitive: Railway service names are
        // capitalized but DNS hostnames come back lowercased.
        assert!(config.is_bootstrap_candidate());
        assert_eq!(
            config.peer_hosts(),
            vec!["mysql-2.railway.internal", "mysql-3.railway.internal"]
        );
    }

    #[test]
    fn gr_enabled_false_forces_standalone_even_with_seeds() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        env::set_var("GR_SEEDS", "mysql-1.railway.internal:3306");
        env::set_var("GR_REPLICATION_PASSWORD", "rp");
        env::set_var("GR_ENABLED", "false");
        let config = Config::from_env().unwrap();
        assert!(!config.gr_enabled());

        // Anything other than the literal "false" keeps HA on.
        env::set_var("GR_ENABLED", "true");
        assert!(Config::from_env().unwrap().gr_enabled());
        env::remove_var("GR_ENABLED");
        assert!(Config::from_env().unwrap().gr_enabled());
    }

    #[test]
    fn non_first_seed_is_not_candidate() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        env::set_var("RAILWAY_PRIVATE_DOMAIN", "mysql-2.railway.internal");
        env::set_var(
            "GR_SEEDS",
            "mysql-1.railway.internal:3306,mysql-2.railway.internal:3306",
        );
        env::set_var("GR_REPLICATION_PASSWORD", "rp");

        let config = Config::from_env().unwrap();
        assert!(!config.is_bootstrap_candidate());
    }

    #[test]
    fn volume_mount_path_is_the_default_datadir() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        env::set_var("RAILWAY_VOLUME_MOUNT_PATH", "/var/lib/mysql");
        let config = Config::from_env().unwrap();
        assert_eq!(config.data_dir, "/var/lib/mysql");
        // Explicit DATA_DIR wins over the volume path.
        env::set_var("DATA_DIR", "/custom");
        let config = Config::from_env().unwrap();
        assert_eq!(config.data_dir, "/custom");
    }
}
