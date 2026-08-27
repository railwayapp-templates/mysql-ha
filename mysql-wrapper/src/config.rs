//! Configuration for the MySQL Group Replication node wrapper.
//!
//! Two modes, decided by GR_SEEDS:
//!   - unset  → standalone passthrough: mysqld runs with no Group Replication
//!     config at all; /health is a real liveness probe and /role answers 200
//!     while mysqld is alive (there is nothing to fence against).
//!   - set    → HA mode: my.cnf is rendered with Group Replication
//!     (single-primary, MYSQL communication stack), and the orchestrator
//!     decides bootstrap-vs-join. GR_REPLICATION_PASSWORD is required.
//!
//! A third, orthogonal, standalone-only concern layers on top: point-in-time
//! recovery (see pitr.rs, archiver.rs, restore.rs). Two independent gates,
//! each all-or-nothing with its own sub-vars:
//!   - BINLOG_ARCHIVE_BUCKET set     → continuous binlog archiving.
//!   - BINLOG_RECOVER_FROM_BUCKET +
//!     MYSQL_RECOVERY_TARGET_TIME set → restore-on-boot to that instant.
//!
//! Both are refused (logged, not fatal) whenever GR_SEEDS is also set — see
//! main.rs — because this version's archiver/restore paths assume they own
//! the only mysqld in the picture.

use crate::pitr::S3Location;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
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
    /// in template order — the declared order is the seed-order tie-break for
    /// bootstrap (candidacy itself is GTID-driven, see gr::decide). Ports
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
    /// Override for the cgroup-derived innodb_buffer_pool_size, in MB
    /// (INNODB_BUFFER_POOL_MB) — redis-ha's MAXMEMORY_MB analog.
    pub innodb_buffer_pool_mb: Option<u64>,
    /// Override for the cgroup-derived max_connections
    /// (MYSQL_MAX_CONNECTIONS).
    pub mysql_max_connections: Option<u64>,
    /// Overall bound on the pre-shutdown primary handoff, milliseconds
    /// (see demote_on_shutdown.rs).
    pub demote_timeout_ms: u64,
    /// How long a declared peer's NAME must be authoritatively gone
    /// (continuous NXDOMAIN) before the bootstrap guard stops waiting on it.
    /// Long on purpose: a redeploy passes through a no-container NXDOMAIN
    /// window, and waiving a peer that was merely mid-redeploy could
    /// bootstrap past the most advanced dataset. See gr::GoneTracker.
    pub peer_gone_dwell_seconds: u64,
    /// Consecutive supervised mysqld starts that failed to reach
    /// accepting-connections before the boot-loop self-heal arms
    /// (BOOT_LOOP_THRESHOLD). Counted in a volume marker — every failed
    /// start exits the container, so the count must survive restarts. See
    /// self_heal.rs for the gates the heal itself sits behind.
    pub boot_loop_threshold: u32,
    /// Per-boot budget for mysqld to reach accepting-connections
    /// (BOOT_READY_BUDGET_SECONDS). Only enforced while a peer answers
    /// /role 200: with the group healthy, a member grinding past this is
    /// restarted so the attempt is counted; with no healthy group anywhere
    /// a slow crash recovery is left to finish, however long it takes — it
    /// may be recovering the best surviving copy.
    pub boot_ready_budget_seconds: u64,
    /// How long a member may sit in ERROR, or in RECOVERING without
    /// observable progress, before the stuck-member self-heal arms
    /// (STUCK_MEMBER_DWELL_SECONDS). Progress — GTIDs applying, recovery
    /// streaming, a clone advancing — resets the clock: a slow member is
    /// not a stuck one.
    pub stuck_member_dwell_seconds: u64,
    /// Self-heal attempts (datadir discard or reclone) before the node
    /// stops healing and stays up for inspection (SELF_HEAL_ATTEMPT_CAP).
    /// Persisted on the volume; reset after an hour of continuous ONLINE.
    pub self_heal_attempt_cap: u32,
    /// Base of the exponential backoff between self-heal attempts, seconds
    /// (SELF_HEAL_BACKOFF_BASE_SECONDS): attempt N+1 waits base * 2^(N-1).
    /// A clone is heavy on the donor — repeated attempts must space out.
    pub self_heal_backoff_base_seconds: u64,
    /// TEST-ONLY fault injection, default 0/off
    /// (RAILWAY_TEST_ADOPTION_DETECTION_DELAY_MS): artificially widens the
    /// gap between "mysqld is answering" and orchestrate's adoption
    /// detection completing, so an e2e test can deterministically land a
    /// peer's bootstrap query inside that window instead of racing a
    /// naturally sub-second gap. Never set outside test harnesses — it only
    /// makes this node's own /gr/state block longer before answering
    /// (adoption_checked; see health_server::AppState), never weakens a
    /// check.
    pub test_adoption_detection_delay_ms: u64,

    // --- PITR: archive gate (BINLOG_ARCHIVE_*) ---
    pub binlog_archive_bucket: Option<String>,
    pub binlog_archive_key: Option<String>,
    pub binlog_archive_secret: Option<String>,
    pub binlog_archive_region: Option<String>,
    pub binlog_archive_endpoint: Option<String>,
    pub binlog_archive_path: String,
    /// How often a fresh full backup is taken once one already exists
    /// (BINLOG_FULL_BACKUP_INTERVAL_SECONDS).
    pub binlog_full_backup_interval_seconds: u64,
    /// `FLUSH BINARY LOGS` cadence — bounds the recovery point objective, the
    /// same role `archive_timeout` plays for a WAL archive
    /// (BINLOG_ROTATE_INTERVAL_SECONDS).
    pub binlog_rotate_interval_seconds: u64,
    /// How far back the archive stays restorable, in days
    /// (BINLOG_RETENTION_DAYS). `None` — the default — means the archive is
    /// never expired and grows without bound, which is what every service had
    /// before retention existed.
    ///
    /// Deliberately opt-in: a default horizon would turn an image bump into a
    /// destructive change on every existing PITR service. Setting it is the
    /// platform's explicit, visible decision (the mysql-pitr template stamps
    /// it), the same way `WAL_BACKUP_RETENTION_FULL` is for postgres-pitr.
    ///
    /// The horizon is the promise ("restorable to any point in the last N
    /// days"), not the whole rule: `pitr::MIN_ACTIVE_FULLS_KEPT` fulls survive
    /// regardless of age, so a long archiver outage can never expire the
    /// service into being unrestorable.
    pub binlog_retention_days: Option<u64>,
    /// Log what retention WOULD delete, and delete nothing
    /// (BINLOG_RETENTION_DRY_RUN). For validating a horizon against a real
    /// bucket before letting it act.
    pub binlog_retention_dry_run: bool,
    /// TEST-ONLY override for `pitr::RETENTION_MIN_OBJECT_AGE_SECONDS`,
    /// defaulting to it (RAILWAY_TEST_RETENTION_MIN_OBJECT_AGE_SECONDS).
    ///
    /// Unlike RAILWAY_TEST_ADOPTION_DETECTION_DELAY_MS, this one CAN weaken a
    /// safety rail, so it is spelled out: the hour-long floor exists so a
    /// just-uploaded object is never expired, and an e2e test cannot forge an
    /// old `LastModified` (S3 stamps it on write) — without this knob the
    /// only way to prove retention actually deletes is to wait out that hour,
    /// which no test harness run can do.
    ///
    /// Never set it outside a test workspace. Production keeps the default
    /// purely by not setting it, the same way the horizon itself is opt-in.
    pub test_retention_min_object_age_seconds: i64,

    // --- PITR: restore gate (BINLOG_RECOVER_FROM_* + MYSQL_RECOVERY_TARGET_TIME) ---
    pub binlog_recover_from_bucket: Option<String>,
    pub binlog_recover_from_key: Option<String>,
    pub binlog_recover_from_secret: Option<String>,
    pub binlog_recover_from_region: Option<String>,
    pub binlog_recover_from_endpoint: Option<String>,
    pub binlog_recover_from_path: String,
    /// Parsed once at startup so a malformed value fails boot immediately
    /// with a clear error rather than deep inside the restore path.
    pub mysql_recovery_target_time: Option<DateTime<Utc>>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let mysql_root_password = String::env_required("MYSQL_ROOT_PASSWORD")
            .context("MYSQL_ROOT_PASSWORD must be set")?;

        let mysql_recovery_target_time =
            non_empty(std::env::var("MYSQL_RECOVERY_TARGET_TIME").ok())
                .map(|raw| crate::pitr::parse_target_time(&raw))
                .transpose()?;

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
            innodb_buffer_pool_mb: non_empty(std::env::var("INNODB_BUFFER_POOL_MB").ok())
                .and_then(|v| v.parse().ok()),
            mysql_max_connections: non_empty(std::env::var("MYSQL_MAX_CONNECTIONS").ok())
                .and_then(|v| v.parse().ok()),
            demote_timeout_ms: u64::env_parse("DEMOTE_TIMEOUT_MS", 20_000),
            peer_gone_dwell_seconds: u64::env_parse("PEER_GONE_DWELL_SECONDS", 1800),
            boot_loop_threshold: u32::env_parse("BOOT_LOOP_THRESHOLD", 3),
            boot_ready_budget_seconds: u64::env_parse("BOOT_READY_BUDGET_SECONDS", 900),
            stuck_member_dwell_seconds: u64::env_parse("STUCK_MEMBER_DWELL_SECONDS", 900),
            self_heal_attempt_cap: u32::env_parse("SELF_HEAL_ATTEMPT_CAP", 5),
            self_heal_backoff_base_seconds: u64::env_parse("SELF_HEAL_BACKOFF_BASE_SECONDS", 60),
            test_adoption_detection_delay_ms: u64::env_parse(
                "RAILWAY_TEST_ADOPTION_DETECTION_DELAY_MS",
                0,
            ),

            binlog_archive_bucket: non_empty(std::env::var("BINLOG_ARCHIVE_BUCKET").ok()),
            binlog_archive_key: non_empty(std::env::var("BINLOG_ARCHIVE_KEY").ok()),
            binlog_archive_secret: non_empty(std::env::var("BINLOG_ARCHIVE_SECRET").ok()),
            binlog_archive_region: non_empty(std::env::var("BINLOG_ARCHIVE_REGION").ok()),
            binlog_archive_endpoint: non_empty(std::env::var("BINLOG_ARCHIVE_ENDPOINT").ok()),
            binlog_archive_path: String::env_or("BINLOG_ARCHIVE_PATH", "/binlog"),
            binlog_full_backup_interval_seconds: u64::env_parse(
                "BINLOG_FULL_BACKUP_INTERVAL_SECONDS",
                86_400,
            ),
            binlog_rotate_interval_seconds: u64::env_parse("BINLOG_ROTATE_INTERVAL_SECONDS", 60),
            // Absent, empty, unparseable, or 0 all mean "no retention". A
            // malformed value must not silently become an aggressive horizon,
            // and 0 must not mean "expire everything".
            binlog_retention_days: non_empty(std::env::var("BINLOG_RETENTION_DAYS").ok())
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|d| *d > 0),
            binlog_retention_dry_run: bool::env_bool("BINLOG_RETENTION_DRY_RUN", false),
            test_retention_min_object_age_seconds: i64::env_parse(
                "RAILWAY_TEST_RETENTION_MIN_OBJECT_AGE_SECONDS",
                crate::pitr::RETENTION_MIN_OBJECT_AGE_SECONDS,
            ),

            binlog_recover_from_bucket: non_empty(std::env::var("BINLOG_RECOVER_FROM_BUCKET").ok()),
            binlog_recover_from_key: non_empty(std::env::var("BINLOG_RECOVER_FROM_KEY").ok()),
            binlog_recover_from_secret: non_empty(std::env::var("BINLOG_RECOVER_FROM_SECRET").ok()),
            binlog_recover_from_region: non_empty(std::env::var("BINLOG_RECOVER_FROM_REGION").ok()),
            binlog_recover_from_endpoint: non_empty(
                std::env::var("BINLOG_RECOVER_FROM_ENDPOINT").ok(),
            ),
            binlog_recover_from_path: String::env_or("BINLOG_RECOVER_FROM_PATH", "/binlog"),
            mysql_recovery_target_time,
        };

        if config.gr_enabled() && config.gr_replication_password.is_none() {
            bail!("GR_REPLICATION_PASSWORD must be set when GR_SEEDS is set");
        }

        if config.binlog_archive_bucket.is_some()
            && (config.binlog_archive_key.is_none()
                || config.binlog_archive_secret.is_none()
                || config.binlog_archive_region.is_none()
                || config.binlog_archive_endpoint.is_none())
        {
            bail!(
                "BINLOG_ARCHIVE_KEY, BINLOG_ARCHIVE_SECRET, BINLOG_ARCHIVE_REGION, and \
                 BINLOG_ARCHIVE_ENDPOINT must all be set when BINLOG_ARCHIVE_BUCKET is set"
            );
        }

        if config.binlog_recover_from_bucket.is_some()
            != config.mysql_recovery_target_time.is_some()
        {
            bail!("BINLOG_RECOVER_FROM_BUCKET and MYSQL_RECOVERY_TARGET_TIME must be set together");
        }
        if config.binlog_recover_from_bucket.is_some()
            && (config.binlog_recover_from_key.is_none()
                || config.binlog_recover_from_secret.is_none()
                || config.binlog_recover_from_region.is_none()
                || config.binlog_recover_from_endpoint.is_none())
        {
            bail!(
                "BINLOG_RECOVER_FROM_KEY, BINLOG_RECOVER_FROM_SECRET, BINLOG_RECOVER_FROM_REGION, \
                 and BINLOG_RECOVER_FROM_ENDPOINT must all be set when \
                 BINLOG_RECOVER_FROM_BUCKET is set"
            );
        }

        Ok(config)
    }

    pub fn gr_enabled(&self) -> bool {
        self.gr_enabled_flag && self.gr_seeds.is_some()
    }

    /// The archive gate: BINLOG_ARCHIVE_BUCKET (and its required siblings,
    /// already validated in `from_env`) are set.
    pub fn archive_enabled(&self) -> bool {
        self.binlog_archive_bucket.is_some()
    }

    /// The restore gate: BINLOG_RECOVER_FROM_BUCKET + MYSQL_RECOVERY_TARGET_TIME
    /// (and the bucket's required siblings) are set.
    pub fn restore_enabled(&self) -> bool {
        self.binlog_recover_from_bucket.is_some()
    }

    /// The parsed restore target — only meaningful (and only ever `Some`)
    /// when `restore_enabled()`.
    pub fn recovery_target_time(&self) -> Option<DateTime<Utc>> {
        self.mysql_recovery_target_time
    }

    /// Where the archiver ships to, built from the `BINLOG_ARCHIVE_*` family.
    /// `None` unless `archive_enabled()`.
    pub fn archive_s3_location(&self) -> Option<S3Location> {
        Some(S3Location {
            bucket: self.binlog_archive_bucket.clone()?,
            access_key: self.binlog_archive_key.clone()?,
            secret_key: self.binlog_archive_secret.clone()?,
            region: self.binlog_archive_region.clone()?,
            endpoint: self.binlog_archive_endpoint.clone()?,
            path: self.binlog_archive_path.clone(),
        })
    }

    /// Where restore reads from, built from the `BINLOG_RECOVER_FROM_*`
    /// family. `None` unless `restore_enabled()`.
    pub fn restore_s3_location(&self) -> Option<S3Location> {
        Some(S3Location {
            bucket: self.binlog_recover_from_bucket.clone()?,
            access_key: self.binlog_recover_from_key.clone()?,
            secret_key: self.binlog_recover_from_secret.clone()?,
            region: self.binlog_recover_from_region.clone()?,
            endpoint: self.binlog_recover_from_endpoint.clone()?,
            path: self.binlog_recover_from_path.clone(),
        })
    }

    /// The image's own uninitialized-instance test (mirrors main.rs's HA-mode
    /// `fresh_datadir` check and docker-entrypoint.sh's own): no `mysql`
    /// system schema directory yet.
    pub fn datadir_is_initialized(&self) -> bool {
        std::path::Path::new(&self.data_dir).join("mysql").is_dir()
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

    /// Whether this node is the FIRST host in the declared seed list — the
    /// node that wins the seed-order tie-break on a first deploy, where every
    /// dataset is empty. Logging/diagnostics only: bootstrap candidacy itself
    /// is dynamic (GTID-driven, see gr::decide), because a fixed candidate
    /// deadlocks the group whenever it is behind or permanently gone.
    pub fn is_bootstrap_candidate(&self) -> bool {
        self.my_seed_rank() == Some(0)
    }

    /// This host's 0-based position in the declared seed order — the final
    /// tie-break when two nodes hold identical GTID sets. None when this node
    /// isn't in its own seed list (misconfiguration); callers treat that as
    /// lowest priority, so a correctly-listed peer always wins the tie.
    pub fn my_seed_rank(&self) -> Option<usize> {
        self.seed_rank(&self.private_domain)
    }

    /// A host's 0-based position in the declared seed order.
    pub fn seed_rank(&self, host: &str) -> Option<usize> {
        self.seed_hosts()
            .iter()
            .position(|h| h.eq_ignore_ascii_case(host))
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
            "BOOT_LOOP_THRESHOLD",
            "BOOT_READY_BUDGET_SECONDS",
            "STUCK_MEMBER_DWELL_SECONDS",
            "SELF_HEAL_ATTEMPT_CAP",
            "SELF_HEAL_BACKOFF_BASE_SECONDS",
            "BINLOG_ARCHIVE_BUCKET",
            "BINLOG_ARCHIVE_KEY",
            "BINLOG_ARCHIVE_SECRET",
            "BINLOG_ARCHIVE_REGION",
            "BINLOG_ARCHIVE_ENDPOINT",
            "BINLOG_ARCHIVE_PATH",
            "BINLOG_FULL_BACKUP_INTERVAL_SECONDS",
            "BINLOG_ROTATE_INTERVAL_SECONDS",
            "BINLOG_RECOVER_FROM_BUCKET",
            "BINLOG_RECOVER_FROM_KEY",
            "BINLOG_RECOVER_FROM_SECRET",
            "BINLOG_RECOVER_FROM_REGION",
            "BINLOG_RECOVER_FROM_ENDPOINT",
            "BINLOG_RECOVER_FROM_PATH",
            "MYSQL_RECOVERY_TARGET_TIME",
        ] {
            env::remove_var(key);
        }
    }

    fn set_archive_env() {
        env::set_var("BINLOG_ARCHIVE_BUCKET", "my-bucket");
        env::set_var("BINLOG_ARCHIVE_KEY", "ak");
        env::set_var("BINLOG_ARCHIVE_SECRET", "sk");
        env::set_var("BINLOG_ARCHIVE_REGION", "auto");
        env::set_var("BINLOG_ARCHIVE_ENDPOINT", "https://s3.example.com");
    }

    fn set_restore_env() {
        env::set_var("BINLOG_RECOVER_FROM_BUCKET", "my-bucket");
        env::set_var("BINLOG_RECOVER_FROM_KEY", "ak");
        env::set_var("BINLOG_RECOVER_FROM_SECRET", "sk");
        env::set_var("BINLOG_RECOVER_FROM_REGION", "auto");
        env::set_var("BINLOG_RECOVER_FROM_ENDPOINT", "https://s3.example.com");
        env::set_var("MYSQL_RECOVERY_TARGET_TIME", "2026-08-13T14:00:00.000Z");
    }

    fn base_env() {
        clear_env();
        env::set_var("MYSQL_ROOT_PASSWORD", "pw");
    }

    #[test]
    fn requires_root_password() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        let err = Config::from_env()
            .err()
            .expect("should fail without a password");
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
        assert_eq!(config.boot_loop_threshold, 3);
        assert_eq!(config.boot_ready_budget_seconds, 900);
        assert_eq!(config.stuck_member_dwell_seconds, 900);
        assert_eq!(config.self_heal_attempt_cap, 5);
        assert_eq!(config.self_heal_backoff_base_seconds, 60);
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
        assert_eq!(config.my_seed_rank(), Some(0));
        assert_eq!(config.seed_rank("mysql-3.railway.internal"), Some(2));
        assert_eq!(config.seed_rank("not-in-the-list.railway.internal"), None);
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

    #[test]
    fn archive_disabled_by_default() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        let config = Config::from_env().unwrap();
        assert!(!config.archive_enabled());
        assert!(config.archive_s3_location().is_none());
        assert_eq!(config.binlog_archive_path, "/binlog");
        assert_eq!(config.binlog_full_backup_interval_seconds, 86_400);
        assert_eq!(config.binlog_rotate_interval_seconds, 60);
        // Retention is opt-in: absent means the archive is never expired,
        // which is the behavior every service had before it existed.
        assert_eq!(config.binlog_retention_days, None);
        assert!(!config.binlog_retention_dry_run);
    }

    #[test]
    fn retention_horizon_parses_and_refuses_meaningless_values() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        for (raw, expected) in [
            ("7", Some(7u64)),
            // 0 must mean "no retention", never "expire everything".
            ("0", None),
            // Malformed input must not silently become an aggressive horizon.
            ("", None),
            ("   ", None),
            ("seven", None),
            ("-3", None),
            ("3.5", None),
        ] {
            base_env();
            env::set_var("BINLOG_RETENTION_DAYS", raw);
            let config = Config::from_env().unwrap();
            assert_eq!(
                config.binlog_retention_days, expected,
                "BINLOG_RETENTION_DAYS={raw:?} should parse to {expected:?}"
            );
            env::remove_var("BINLOG_RETENTION_DAYS");
        }
    }

    #[test]
    fn retention_dry_run_is_opt_in() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        env::set_var("BINLOG_RETENTION_DAYS", "14");
        let config = Config::from_env().unwrap();
        assert_eq!(config.binlog_retention_days, Some(14));
        assert!(!config.binlog_retention_dry_run);

        env::set_var("BINLOG_RETENTION_DRY_RUN", "true");
        let config = Config::from_env().unwrap();
        assert!(config.binlog_retention_dry_run);
        env::remove_var("BINLOG_RETENTION_DRY_RUN");
        env::remove_var("BINLOG_RETENTION_DAYS");
    }

    #[test]
    fn archive_enabled_requires_every_sibling_var() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        env::set_var("BINLOG_ARCHIVE_BUCKET", "my-bucket");
        let err = Config::from_env().err().expect("should fail");
        assert!(err.to_string().contains("BINLOG_ARCHIVE_KEY"));

        set_archive_env();
        let config = Config::from_env().unwrap();
        assert!(config.archive_enabled());
        let loc = config.archive_s3_location().unwrap();
        assert_eq!(loc.bucket, "my-bucket");
        assert_eq!(loc.access_key, "ak");
        assert_eq!(loc.secret_key, "sk");
        assert_eq!(loc.region, "auto");
        assert_eq!(loc.endpoint, "https://s3.example.com");
        assert_eq!(loc.path, "/binlog");
    }

    #[test]
    fn archive_path_and_intervals_are_overridable() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        set_archive_env();
        env::set_var("BINLOG_ARCHIVE_PATH", "/custom-path");
        env::set_var("BINLOG_FULL_BACKUP_INTERVAL_SECONDS", "3600");
        env::set_var("BINLOG_ROTATE_INTERVAL_SECONDS", "30");
        let config = Config::from_env().unwrap();
        assert_eq!(config.archive_s3_location().unwrap().path, "/custom-path");
        assert_eq!(config.binlog_full_backup_interval_seconds, 3600);
        assert_eq!(config.binlog_rotate_interval_seconds, 30);
    }

    #[test]
    fn restore_disabled_by_default() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        let config = Config::from_env().unwrap();
        assert!(!config.restore_enabled());
        assert!(config.restore_s3_location().is_none());
        assert!(config.recovery_target_time().is_none());
    }

    #[test]
    fn restore_requires_bucket_and_target_time_together() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        env::set_var("BINLOG_RECOVER_FROM_BUCKET", "my-bucket");
        env::set_var("BINLOG_RECOVER_FROM_KEY", "ak");
        env::set_var("BINLOG_RECOVER_FROM_SECRET", "sk");
        env::set_var("BINLOG_RECOVER_FROM_REGION", "auto");
        env::set_var("BINLOG_RECOVER_FROM_ENDPOINT", "https://s3.example.com");
        // Bucket set without the target time.
        let err = Config::from_env().err().expect("should fail");
        assert!(err.to_string().contains("MYSQL_RECOVERY_TARGET_TIME"));

        // Target time set without the bucket.
        base_env();
        env::set_var("MYSQL_RECOVERY_TARGET_TIME", "2026-08-13T14:00:00.000Z");
        let err = Config::from_env().err().expect("should fail");
        assert!(err.to_string().contains("MYSQL_RECOVERY_TARGET_TIME"));
    }

    #[test]
    fn restore_enabled_requires_every_sibling_var() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        env::set_var("BINLOG_RECOVER_FROM_BUCKET", "my-bucket");
        env::set_var("MYSQL_RECOVERY_TARGET_TIME", "2026-08-13T14:00:00.000Z");
        let err = Config::from_env().err().expect("should fail");
        assert!(err.to_string().contains("BINLOG_RECOVER_FROM_KEY"));

        set_restore_env();
        let config = Config::from_env().unwrap();
        assert!(config.restore_enabled());
        let loc = config.restore_s3_location().unwrap();
        assert_eq!(loc.bucket, "my-bucket");
        assert_eq!(
            config.recovery_target_time().unwrap(),
            crate::pitr::parse_target_time("2026-08-13T14:00:00.000Z").unwrap()
        );
    }

    #[test]
    fn restore_target_time_must_parse() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        set_restore_env();
        env::set_var("MYSQL_RECOVERY_TARGET_TIME", "not-a-timestamp");
        let err = Config::from_env().err().expect("should fail");
        assert!(err.to_string().contains("MYSQL_RECOVERY_TARGET_TIME"));
    }

    #[test]
    fn datadir_initialized_check_matches_the_mysql_system_dir() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        base_env();
        let dir = std::env::temp_dir().join(format!(
            "mysql-wrapper-config-test-datadir-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        env::set_var("DATA_DIR", dir.to_string_lossy().to_string());
        let config = Config::from_env().unwrap();
        assert!(!config.datadir_is_initialized());
        std::fs::create_dir_all(dir.join("mysql")).unwrap();
        assert!(config.datadir_is_initialized());
        std::fs::remove_dir_all(&dir).ok();
    }
}
