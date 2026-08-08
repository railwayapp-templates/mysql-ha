//! Entrypoint for the MySQL Group Replication node container.
//!
//! *** WIP SCAFFOLD — v0 scope. ***
//!
//! Today this wrapper only does three things:
//!   1. Parse and validate configuration from environment variables.
//!   2. Run an HTTP health server that fails closed (503 on both /health and
//!      /role) until the real Group Replication checks are implemented.
//!   3. Supervise a single `docker-entrypoint.sh mysqld` child process,
//!      passing through any CLI args, exiting with its exit code if it dies.
//!
//! None of the actual Group Replication behavior — my.cnf rendering, the
//! bootstrap guard, total-outage recovery — exists yet. See the TODOs below
//! and in `health_server.rs`. This mirrors redis-ha's redis-sentinel crate
//! structure (config / health_server / process_manager / main) so the real
//! implementation can slot in module-by-module.

mod config;
mod health_server;
mod process_manager;

use anyhow::Result;
use common::{init_logging, Telemetry, TelemetryEvent};
use config::Config;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = init_logging("mysql-wrapper");

    let config = Config::from_env()?;
    let telemetry = Telemetry::from_env("mysql-ha");

    info!(
        mysql_port = config.mysql_port,
        health_port = config.health_port,
        server_id = ?config.server_id,
        gr_group_name = ?config.gr_group_name,
        gr_seeds = ?config.gr_seeds,
        "starting mysql-wrapper"
    );

    // TODO(my.cnf rendering): before starting mysqld, render a my.cnf (or an
    // equivalent --defaults-extra-file) carrying:
    //   - group_replication_group_name = config.gr_group_name (generate one
    //     and persist it to the volume on first boot when unset).
    //   - group_replication_start_on_boot = OFF — GR must be joined/started
    //     explicitly, after the bootstrap guard below decides this node's
    //     role, never automatically as part of mysqld startup. Starting on
    //     boot is exactly how a booting node could bootstrap a competing
    //     group instead of rejoining a live one.
    //   - gtid_mode = ON, enforce_gtid_consistency = ON — required by GR.
    //   - performance_schema = ON — required to read
    //     replication_group_members for the /role check. Railway's
    //     standalone mysql template runs with performance_schema=0, so a
    //     standalone → HA conversion has to flip this.
    //   - binlog re-enabled — the standalone template runs
    //     --disable-log-bin, but GR replicates via the binary log.
    //   - innodb_buffer_pool_size sized from the container's actual memory
    //     limit, not the standalone template's fixed 1G.
    // See README.md's "Conversion notes" section for the full standalone→HA
    // config diff this needs to bridge.

    // TODO(bootstrap guard): before issuing `START GROUP_REPLICATION`, query
    // the declared peers in config.gr_seeds for an already-ONLINE group. Only
    // bootstrap a brand new group
    // (`group_replication_bootstrap_group=ON` + START GROUP_REPLICATION) when
    // no peer answers with a live group; otherwise join the existing one via
    // a plain START GROUP_REPLICATION. Without this guard, a node booting
    // after a network partition heals could start a second, competing group
    // instead of rejoining the real one — the GR analogue of redis-ha's
    // peer-Sentinel boot query.

    // TODO(total-outage recovery): if every declared peer is unreachable
    // (not just this node), nothing here may unilaterally bootstrap a new
    // group — that would silently pick a potentially stale dataset as the
    // new source of truth. Recovery should exchange each candidate's
    // executed-GTID set via their /health servers (once that endpoint reports
    // it), let the node with the most-advanced set bootstrap after a dwell
    // period so the rest of the group can catch up and join, and cap the
    // number of automatic attempts so a flapping network can't repeatedly
    // re-elect different nodes.

    tokio::spawn(health_server::run_health_server(config.health_port));

    telemetry.send(TelemetryEvent::NodeStarted {
        node: config.private_domain.clone(),
        // TODO: "unknown" is a placeholder — the real role ("primary" /
        // "secondary") is only knowable once the bootstrap guard and GR join
        // logic above exist.
        role: "unknown".to_string(),
    });

    // TODO: this execs mysqld with no rendered GR config at all yet — it
    // currently boots as a plain standalone mysqld. Wire in the my.cnf
    // rendering TODO above before this is anything more than a supervision
    // skeleton. MYSQL_ROOT_PASSWORD and friends reach docker-entrypoint.sh
    // through the inherited process environment, not as CLI args.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let child = process_manager::spawn_mysqld(&args).await?;

    process_manager::supervise(child).await
}
