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
pub fn buffer_pool_bytes(cgroup_limit_bytes: Option<u64>) -> u64 {
    const MIN: u64 = 128 * 1024 * 1024;
    const FALLBACK: u64 = 512 * 1024 * 1024;
    match cgroup_limit_bytes {
        Some(limit) => (limit / 2).max(MIN),
        None => FALLBACK,
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

plugin-load-add = group_replication.so
plugin-load-add = mysql_clone.so

# Not writable until Group Replication decides this node's role. GR lifts
# read_only on the elected primary automatically.
super_read_only = ON

group_replication_group_name = {group_name}
group_replication_start_on_boot = OFF
group_replication_single_primary_mode = ON
group_replication_enforce_update_everywhere_checks = OFF
group_replication_communication_stack = MYSQL
group_replication_local_address = {private_domain}:{mysql_port}
group_replication_group_seeds = {gr_seeds}
group_replication_paxos_single_leader = ON
# caching_sha2_password without client-side TLS certs needs RSA key exchange
# on the recovery channel.
group_replication_recovery_get_public_key = ON
"#,
        server_id = input.server_id,
        private_domain = input.private_domain,
        mysql_port = input.mysql_port,
        buffer_pool = input.buffer_pool_bytes,
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
    let content = render_gr_conf(&GrConfInput {
        server_id,
        group_name,
        gr_seeds: seeds,
        private_domain: &config.private_domain,
        mysql_port: config.mysql_port,
        buffer_pool_bytes: buffer_pool_bytes(read_cgroup_memory_limit()),
    });

    let path = Path::new(&config.conf_dir).join(CONF_FILE_NAME);
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
            "super_read_only = ON",
            "group_replication_start_on_boot = OFF",
            "group_replication_single_primary_mode = ON",
            "group_replication_communication_stack = MYSQL",
            "group_replication_group_name = aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "group_replication_local_address = mysql-1.railway.internal:3306",
            "group_replication_group_seeds = mysql-1.railway.internal:3306,mysql-2.railway.internal:3306",
            "plugin-load-add = group_replication.so",
            "plugin-load-add = mysql_clone.so",
            "innodb_buffer_pool_size = 536870912",
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
            buffer_pool_bytes(Some(2 * 1024 * 1024 * 1024)),
            1024 * 1024 * 1024
        );
        // …floored at 128MB…
        assert_eq!(buffer_pool_bytes(Some(100 * 1024 * 1024)), 128 * 1024 * 1024);
        // …with a 512MB fallback when unlimited.
        assert_eq!(buffer_pool_bytes(None), 512 * 1024 * 1024);
    }
}
