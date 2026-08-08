//! Configuration for the MySQL Group Replication node wrapper.
//!
//! v0 scope: this only parses and validates the environment. None of these
//! fields are wired into a rendered my.cnf yet — see the TODOs in main.rs.

use anyhow::{Context, Result};
use common::{ConfigExt, RailwayEnv};

#[allow(dead_code)] // TODO: consumed once my.cnf rendering / bootstrap guard land (see main.rs)
pub struct Config {
    /// MySQL root password. Required — passed through to the upstream
    /// `docker-entrypoint.sh` via the environment (not re-forwarded as a CLI
    /// arg), which is what actually initializes the root account.
    pub mysql_root_password: String,
    pub mysql_port: u16,
    /// Group Replication server_id. Optional for now: a real deployment must
    /// give every node a distinct id, but nothing derives or defaults it yet.
    pub server_id: Option<u32>,
    /// Comma-separated "host:33061" list of Group Replication seed peers.
    /// Example: "mysql-1.railway.internal:33061,mysql-2.railway.internal:33061"
    pub gr_seeds: Option<String>,
    /// Group Replication group_name (a UUID string). TODO: generate one and
    /// persist it to the volume on first boot when unset, instead of leaving
    /// the group nameless.
    pub gr_group_name: Option<String>,
    pub health_port: u16,
    /// This node's private Railway hostname.
    pub private_domain: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let mysql_root_password = String::env_required("MYSQL_ROOT_PASSWORD")
            .context("MYSQL_ROOT_PASSWORD must be set")?;

        Ok(Self {
            mysql_root_password,
            mysql_port: u16::env_parse("MYSQL_PORT", 3306),
            server_id: non_empty(std::env::var("SERVER_ID").ok()).and_then(|v| v.parse().ok()),
            gr_seeds: non_empty(std::env::var("GR_SEEDS").ok()),
            gr_group_name: non_empty(std::env::var("GR_GROUP_NAME").ok()),
            health_port: u16::env_parse("HEALTH_PORT", 8080),
            private_domain: RailwayEnv::private_domain(),
        })
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
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env() {
        for key in [
            "MYSQL_ROOT_PASSWORD",
            "MYSQL_PORT",
            "SERVER_ID",
            "GR_SEEDS",
            "GR_GROUP_NAME",
            "HEALTH_PORT",
        ] {
            env::remove_var(key);
        }
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
        clear_env();
        env::set_var("MYSQL_ROOT_PASSWORD", "pw");
        let config = Config::from_env().unwrap();
        assert_eq!(config.mysql_port, 3306);
        assert_eq!(config.health_port, 8080);
        assert!(config.server_id.is_none());
        assert!(config.gr_seeds.is_none());
        assert!(config.gr_group_name.is_none());
    }

    #[test]
    fn explicit_values_override_defaults() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("MYSQL_ROOT_PASSWORD", "pw");
        env::set_var("MYSQL_PORT", "3307");
        env::set_var("HEALTH_PORT", "9090");
        env::set_var("SERVER_ID", "42");
        env::set_var(
            "GR_SEEDS",
            "mysql-1.railway.internal:33061,mysql-2.railway.internal:33061",
        );
        env::set_var("GR_GROUP_NAME", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

        let config = Config::from_env().unwrap();
        assert_eq!(config.mysql_port, 3307);
        assert_eq!(config.health_port, 9090);
        assert_eq!(config.server_id, Some(42));
        assert_eq!(
            config.gr_seeds.as_deref(),
            Some("mysql-1.railway.internal:33061,mysql-2.railway.internal:33061")
        );
        assert_eq!(
            config.gr_group_name.as_deref(),
            Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        );
    }

    #[test]
    fn empty_optional_vars_are_treated_as_unset() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("MYSQL_ROOT_PASSWORD", "pw");
        env::set_var("SERVER_ID", "");
        env::set_var("GR_SEEDS", "");
        env::set_var("GR_GROUP_NAME", "");

        let config = Config::from_env().unwrap();
        assert!(config.server_id.is_none());
        assert!(config.gr_seeds.is_none());
        assert!(config.gr_group_name.is_none());
    }

    #[test]
    fn an_unparseable_server_id_is_treated_as_unset() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("MYSQL_ROOT_PASSWORD", "pw");
        env::set_var("SERVER_ID", "not-a-number");

        let config = Config::from_env().unwrap();
        assert!(config.server_id.is_none());
    }
}
