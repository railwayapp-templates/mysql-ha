//! Entrypoint for the MySQL Group Replication node container.
//!
//! Boot sequence (HA mode, GR_SEEDS set):
//!   1. Parse config; resolve the group name (env > volume marker > derived
//!      from the Railway environment id).
//!   2. Render the Group Replication my.cnf fragment into /etc/mysql/conf.d —
//!      BEFORE mysqld spawns, so initialization sees it too.
//!   3. Start the health server (/health, /role, /gr/state) — fail-closed
//!      until mysqld answers.
//!   4. Spawn `docker-entrypoint.sh mysqld` (args passed through) and
//!      supervise it: the container lives and dies with mysqld.
//!   5. In the background, run the orchestrator: wait for mysqld, then join
//!      the existing group if any peer reports one, else — only on the
//!      declared bootstrap candidate, only with every peer answering, only
//!      with every peer's GTID set a subset of ours, and only after a dwell —
//!      bootstrap a new group.
//!
//! Standalone mode (GR_SEEDS unset): no GR config is rendered and no
//! orchestration runs — mysqld boots exactly as the upstream image would,
//! with /health as a real liveness probe and /role answering 200 while
//! mysqld is alive. This is the state a reverted (HA → standalone) service
//! runs in while it still uses this image.

mod config;
mod gr;
mod health_server;
mod mysql_conf;
mod peers;
mod process_manager;
mod sql;

use anyhow::Result;
use common::{init_logging, Telemetry, TelemetryEvent};
use config::Config;
use health_server::AppState;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = init_logging("mysql-wrapper");

    let config = Arc::new(Config::from_env()?);
    let telemetry = Arc::new(Telemetry::from_env("mysql-ha"));

    info!(
        mysql_port = config.mysql_port,
        health_port = config.health_port,
        gr_enabled = config.gr_enabled(),
        is_bootstrap_candidate = config.is_bootstrap_candidate(),
        "starting mysql-wrapper"
    );

    let sql = sql::Sql::connect_root_over_socket(&config.socket_path, &config.mysql_root_password);

    if config.gr_enabled() {
        let group_name = gr::resolve_group_name(&config);
        let server_id = config
            .server_id
            .unwrap_or_else(|| mysql_conf::derive_server_id(&config.private_domain));
        info!(group_name = %group_name, server_id, "rendering group replication config");
        mysql_conf::write_gr_conf(&config, &group_name, server_id)?;

        // Same uninitialized-instance test docker-entrypoint.sh uses: no
        // `mysql` system schema directory in the datadir. Checked BEFORE the
        // entrypoint spawns — it decides whether this boot's local GTID
        // history is init noise that must be purged (see gr::orchestrate).
        let fresh_datadir = !std::path::Path::new(&config.data_dir).join("mysql").is_dir();

        tokio::spawn(health_server::run_health_server_supervised(
            config.health_port,
            Arc::new(AppState {
                sql: sql.clone(),
                standalone: false,
                data_dir: config.data_dir.clone(),
            }),
            telemetry.clone(),
        ));

        tokio::spawn(gr::orchestrate(
            config.clone(),
            sql.clone(),
            telemetry.clone(),
            group_name,
            fresh_datadir,
        ));
    } else {
        info!("GR_SEEDS not set — standalone passthrough mode");
        tokio::spawn(health_server::run_health_server_supervised(
            config.health_port,
            Arc::new(AppState {
                sql: sql.clone(),
                standalone: true,
                data_dir: config.data_dir.clone(),
            }),
            telemetry.clone(),
        ));
        telemetry.send(TelemetryEvent::NodeStarted {
            node: config.private_domain.clone(),
            role: "standalone".to_string(),
        });
    }

    // MYSQL_ROOT_PASSWORD and friends reach docker-entrypoint.sh through the
    // inherited process environment, not as CLI args.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let child = process_manager::spawn_mysqld(&args).await?;

    process_manager::supervise(child).await
}
