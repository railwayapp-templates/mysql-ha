//! Renders the Group Replication my.cnf fragment.
//!
//! Written to `{conf_dir}/zz-railway-gr.cnf` before mysqld is spawned —
//! `/etc/my.cnf` in the official image `!includedir`s `/etc/mysql/conf.d`,
//! and the `zz-` prefix sorts it last so it wins over anything the base
//! image ships. Railway's standalone template passes its overrides as CLI
//! flags instead; the HA template's start command carries no flags, so this
//! file is the single source of MySQL configuration in HA mode.
//!
//! Notable choices:
//!   - `group_replication_communication_stack = MYSQL`: group traffic runs
//!     over the normal SQL port with regular account auth (the gr_recovery
//!     user), instead of a separate XCom port with an IP allowlist. One port,
//!     no allowlist guesswork on Railway's IPv6 ULA private network.
//!   - `group_replication_start_on_boot = OFF`: joining/bootstrapping is the
//!     orchestrator's decision, made after the peer-query guard — never
//!     mysqld's. `start_on_boot=ON` is the documented way to accidentally
//!     bootstrap a second, competing group after a partition heals.
//!   - `super_read_only = ON` at boot: a node that just started is not
//!     writable until Group Replication promotes it. Group Replication flips
//!     read_only off on the elected primary automatically.
//!   - `gtid_mode = ON` directly at startup: safe here even for adopted
//!     standalone volumes, because Railway's standalone template runs with
//!     `--disable-log-bin` — there is no anonymous-transaction binlog history
//!     to migrate through the staged gtid_mode dance.

use crate::config::Config;
use anyhow::{Context, Result};
use std::path::Path;

pub const CONF_FILE_NAME: &str = "zz-railway-gr.cnf";

/// Derive a stable, non-zero server_id from the node's private hostname
/// (FNV-1a 32-bit). Every group member must have a distinct id; hostnames
/// are unique per service on Railway's private network.
pub fn derive_server_id(private_domain: &str) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in private_domain.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    // server_id 0 means "replication disabled"; never emit it.
    if hash == 0 {
        1
    } else {
        hash
    }
}

/// innodb_buffer_pool_size from the container's cgroup memory limit:
/// 50% of the limit, floored at 128MB. Falls back to 512MB when no limit is
/// readable (local runs). The standalone template hardcodes 1G regardless of
/// container size, which OOMs small containers and wastes big ones.
/// `override_mb` (INNODB_BUFFER_POOL_MB) wins over the computation when set —
/// the same escape hatch redis-ha's MAXMEMORY_MB provides.
pub fn buffer_pool_bytes(cgroup_limit_bytes: Option<u64>, override_mb: Option<u64>) -> u64 {
    const MIN: u64 = 128 * 1024 * 1024;
    const FALLBACK: u64 = 512 * 1024 * 1024;
    if let Some(mb) = override_mb {
        return (mb * 1024 * 1024).max(MIN);
    }
    match cgroup_limit_bytes {
        Some(limit) => (limit / 2).max(MIN),
        None => FALLBACK,
    }
}

/// max_connections from the container's cgroup memory limit: the image
/// default (151) is low for pooled applications behind the edge, but each
/// connection costs per-session buffers, so the ceiling must scale with the
/// container. limit/8MB, clamped to [151, 1000]; image default when no limit
/// is readable. MYSQL_MAX_CONNECTIONS overrides.
pub fn max_connections(cgroup_limit_bytes: Option<u64>, override_conns: Option<u64>) -> u64 {
    const IMAGE_DEFAULT: u64 = 151;
    const CEILING: u64 = 1000;
    if let Some(n) = override_conns {
        return n.max(1);
    }
    match cgroup_limit_bytes {
        Some(limit) => (limit / (8 * 1024 * 1024)).clamp(IMAGE_DEFAULT, CEILING),
        None => IMAGE_DEFAULT,
    }
}

/// Read the container memory limit from cgroup v2, then v1. `None` when
/// unlimited or unreadable.
pub fn read_cgroup_memory_limit() -> Option<u64> {
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        if let Ok(raw) = std::fs::read_to_string(path) {
            let raw = raw.trim();
            if raw == "max" {
                return None;
            }
            if let Ok(bytes) = raw.parse::<u64>() {
                // cgroup v1 reports a huge sentinel (~9.2e18) for "unlimited".
                if bytes > (1 << 60) {
                    return None;
                }
                return Some(bytes);
            }
        }
    }
    None
}

pub struct GrConfInput<'a> {
    pub server_id: u32,
    pub group_name: &'a str,
    pub gr_seeds: &'a str,
    pub private_domain: &'a str,
    pub mysql_port: u16,
    pub buffer_pool_bytes: u64,
    pub max_connections: u64,
}

pub fn render_gr_conf(input: &GrConfInput) -> String {
    format!(
        r#"# Rendered by mysql-wrapper — do not edit; changes are overwritten on boot.
[mysqld]
server_id = {server_id}
report_host = {private_domain}
report_port = {mysql_port}

# Group Replication prerequisites
gtid_mode = ON
enforce_gtid_consistency = ON
# The standalone template disables both of these; GR needs them.
log_bin = binlog
performance_schema = ON

innodb_buffer_pool_size = {buffer_pool}
max_connections = {max_connections}

# Binlogs are mandatory for Group Replication and the default 30-day expiry
# can fill a small volume long before it triggers — and a disk-full member
# keeps reporting ONLINE while it silently stalls the whole group. Three
# days is plenty for recovery: a rejoining member whose gap outruns the
# retained binlogs falls back to a clone (the plugin is loaded below).
binlog_expire_logs_seconds = 259200

# Repeated aborted connections from one host (a crashing client in a tight
# loop, an aggressive prober) would otherwise trip max_connect_errors and
# block that host until flush-hosts — the classic "Host is blocked" page.
# Disabling the host cache removes the blocking behavior entirely without
# changing grant semantics for adopted volumes (unlike skip_name_resolve,
# which breaks hostname-based grants).
host_cache_size = 0

plugin-load-add = group_replication.so
plugin-load-add = mysql_clone.so

# super_read_only is deliberately NOT set here: docker-entrypoint's
# first-boot initialization runs its setup SQL against a temp server that
# reads this file, and would fail read-only. The orchestrator sets
# super_read_only=ON the moment the final server answers, before any
# join/bootstrap decision; Group Replication lifts it on the elected primary.

# Every group_replication_* variable is loose-prefixed: `mysqld --initialize`
# ignores plugin-load-add and would otherwise abort on "unknown variable"
# before the plugin exists. loose- turns that into a warning during init and
# applies the values once the plugin loads at real boot.
loose-group_replication_group_name = {group_name}
loose-group_replication_start_on_boot = OFF
loose-group_replication_single_primary_mode = ON
loose-group_replication_enforce_update_everywhere_checks = OFF
loose-group_replication_communication_stack = MYSQL
loose-group_replication_local_address = {private_domain}:{mysql_port}
loose-group_replication_group_seeds = {gr_seeds}
loose-group_replication_paxos_single_leader = ON
# caching_sha2_password without client-side TLS certs needs RSA key exchange
# on the recovery channel.
loose-group_replication_recovery_get_public_key = ON

# Deliberately left at their defaults — each was weighed, not forgotten:
#   group_replication_member_expel_timeout (5s): raising it makes transient
#     blips survivable but delays crash failover by the same amount; the
#     expelled-healthy-member case is already covered by autorejoin.
#   group_replication_unreachable_majority_timeout (0 = block): a timeout
#     unblocks clients hung on a minority primary, but parks the member in
#     ERROR where rejoining waits on autorejoin's fixed 5-minute cycle; the
#     /role fence already pulls a minority primary out of routing in one
#     probe interval.
#   group_replication_transaction_size_limit (~143MB): transactions above it
#     are refused, which is the safe behavior — raising it invites
#     group-wide stalls (flow control) on bulk imports.
#   auto_increment_increment: single-primary Group Replication pins it to 1
#     (offset 2) by itself; only multi-primary uses the 7-step spacing.
"#,
        server_id = input.server_id,
        private_domain = input.private_domain,
        mysql_port = input.mysql_port,
        buffer_pool = input.buffer_pool_bytes,
        max_connections = input.max_connections,
        group_name = input.group_name,
        gr_seeds = input.gr_seeds,
    )
}

/// Write the rendered config into the include dir. Must run before mysqld is
/// spawned — every phase of docker-entrypoint.sh (initialize, temp server,
/// final exec) reads it.
pub fn write_gr_conf(config: &Config, group_name: &str, server_id: u32) -> Result<()> {
    let seeds = config
        .gr_seeds
        .as_deref()
        .expect("write_gr_conf is only called in HA mode");
    let cgroup_limit = read_cgroup_memory_limit();
    let content = render_gr_conf(&GrConfInput {
        server_id,
        group_name,
        gr_seeds: seeds,
        private_domain: &config.private_domain,
        mysql_port: config.mysql_port,
        buffer_pool_bytes: buffer_pool_bytes(cgroup_limit, config.innodb_buffer_pool_mb),
        max_connections: max_connections(cgroup_limit, config.mysql_max_connections),
    });

    let path = Path::new(&config.conf_dir).join(CONF_FILE_NAME);
    std::fs::create_dir_all(&config.conf_dir)
        .with_context(|| format!("creating {}", config.conf_dir))?;
    std::fs::write(&path, content).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

pub const ARCHIVE_CONF_FILE_NAME: &str = "zz-railway-pitr-archive.cnf";

/// Standalone + `BINLOG_ARCHIVE_BUCKET` set: the plain standalone rendering
/// (no file at all — see main.rs) leaves the binlog off, same as the
/// upstream image. Archiving needs it on; this is the ONLY difference from
/// that plain rendering. Written to its own file, separate from
/// `CONF_FILE_NAME`, since the two are mutually exclusive (GR mode never
/// reaches this path — see main.rs's standalone-only gate).
pub struct StandaloneArchiveConfInput {
    pub server_id: u32,
}

pub fn render_standalone_archive_conf(input: &StandaloneArchiveConfInput) -> String {
    format!(
        r#"# Rendered by mysql-wrapper — do not edit; changes are overwritten on boot.
# Standalone mode with BINLOG_ARCHIVE_BUCKET set: binlog archiving needs the
# binlog on, which the plain standalone rendering (no file at all) otherwise
# leaves off, same as the upstream image — see main.rs.
[mysqld]
server_id = {server_id}

log_bin = binlog
binlog_format = ROW
sync_binlog = 1

# The archiver reclaims a closed binlog itself once it has confirmed the
# upload (see archiver.rs's PURGE BINARY LOGS TO) — and NOTHING else may:
# auto-expiry is disabled outright, because mysqld's own purge cannot know
# what has shipped, so any nonzero expiry can reclaim a not-yet-uploaded
# binlog and punch a silent, permanent hole into the archive lineage. A
# wedged archiver therefore grows the volume LOUDLY (disk pressure is
# monitored and mysqld blocks writes on a full disk) instead of quietly
# corrupting point-in-time recovery — the failure a backup system must never
# trade for disk space.
binlog_expire_logs_seconds = 0
"#,
        server_id = input.server_id,
    )
}

/// Write the standalone archive config fragment. Must run before mysqld is
/// spawned, same requirement as `write_gr_conf`. Never called when GR_SEEDS
/// is set — main.rs gates archiving to standalone only.
pub fn write_standalone_archive_conf(config: &Config, server_id: u32) -> Result<()> {
    let content = render_standalone_archive_conf(&StandaloneArchiveConfInput { server_id });
    let path = Path::new(&config.conf_dir).join(ARCHIVE_CONF_FILE_NAME);
    std::fs::create_dir_all(&config.conf_dir)
        .with_context(|| format!("creating {}", config.conf_dir))?;
    std::fs::write(&path, content).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> String {
        render_gr_conf(&GrConfInput {
            server_id: 42,
            group_name: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            gr_seeds: "mysql-1.railway.internal:3306,mysql-2.railway.internal:3306",
            private_domain: "mysql-1.railway.internal",
            mysql_port: 3306,
            buffer_pool_bytes: 536870912,
            max_connections: 500,
        })
    }

    #[test]
    fn renders_the_load_bearing_directives() {
        let conf = sample();
        for directive in [
            "server_id = 42",
            "gtid_mode = ON",
            "enforce_gtid_consistency = ON",
            "log_bin = binlog",
            "performance_schema = ON",
            "loose-group_replication_start_on_boot = OFF",
            "loose-group_replication_single_primary_mode = ON",
            "loose-group_replication_communication_stack = MYSQL",
            "loose-group_replication_group_name = aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "loose-group_replication_local_address = mysql-1.railway.internal:3306",
            "loose-group_replication_group_seeds = mysql-1.railway.internal:3306,mysql-2.railway.internal:3306",
            "plugin-load-add = group_replication.so",
            "plugin-load-add = mysql_clone.so",
            "innodb_buffer_pool_size = 536870912",
            "max_connections = 500",
            "binlog_expire_logs_seconds = 259200",
            "host_cache_size = 0",
            "report_host = mysql-1.railway.internal",
        ] {
            assert!(conf.contains(directive), "missing directive: {directive}");
        }
    }

    #[test]
    fn server_id_is_stable_distinct_and_nonzero() {
        let a = derive_server_id("mysql-1.railway.internal");
        let b = derive_server_id("mysql-2.railway.internal");
        assert_eq!(a, derive_server_id("mysql-1.railway.internal"));
        assert_ne!(a, 0);
        assert_ne!(a, b);
    }

    #[test]
    fn buffer_pool_sizing_clamps() {
        // 50% of the limit…
        assert_eq!(
            buffer_pool_bytes(Some(2 * 1024 * 1024 * 1024), None),
            1024 * 1024 * 1024
        );
        // …floored at 128MB…
        assert_eq!(
            buffer_pool_bytes(Some(100 * 1024 * 1024), None),
            128 * 1024 * 1024
        );
        // …with a 512MB fallback when unlimited.
        assert_eq!(buffer_pool_bytes(None, None), 512 * 1024 * 1024);
        // The override wins over any limit, still floored.
        assert_eq!(
            buffer_pool_bytes(Some(8 * 1024 * 1024 * 1024), Some(256)),
            256 * 1024 * 1024
        );
        assert_eq!(buffer_pool_bytes(None, Some(1)), 128 * 1024 * 1024);
    }

    #[test]
    fn max_connections_scales_with_the_container() {
        // limit/8MB, clamped to [151, 1000]; image default when unknown.
        assert_eq!(max_connections(Some(1024 * 1024 * 1024), None), 151);
        assert_eq!(max_connections(Some(4 * 1024 * 1024 * 1024), None), 512);
        assert_eq!(max_connections(Some(32 * 1024 * 1024 * 1024), None), 1000);
        assert_eq!(max_connections(None, None), 151);
        // The override wins, unclamped above but never zero.
        assert_eq!(max_connections(Some(1024 * 1024 * 1024), Some(2000)), 2000);
        assert_eq!(max_connections(None, Some(0)), 1);
    }

    #[test]
    fn standalone_archive_conf_renders_the_load_bearing_directives() {
        let conf = render_standalone_archive_conf(&StandaloneArchiveConfInput { server_id: 1 });
        for directive in [
            "server_id = 1",
            "log_bin = binlog",
            "binlog_format = ROW",
            "sync_binlog = 1",
            // Auto-expiry DISABLED on an archiving node: mysqld's own purge
            // cannot know what has shipped, so any nonzero expiry can reclaim
            // a not-yet-uploaded binlog and punch a silent, permanent hole
            // into the archive lineage. Only the archiver purges (uploaded
            // files only) — see archiver.rs.
            "binlog_expire_logs_seconds = 0",
        ] {
            assert!(conf.contains(directive), "missing directive: {directive}");
        }
        // Never renders any Group Replication directive — this file only
        // ever ships in standalone mode.
        assert!(!conf.contains("group_replication"));
    }
}
