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
//!
//! Point-in-time recovery (standalone mode only, this version — see
//! pitr.rs/archiver.rs/restore.rs): two independent env-gated concerns layer
//! on top of the standalone path.
//!   - BINLOG_ARCHIVE_BUCKET set: the standalone conf enables the binlog
//!     (instead of the plain no-binlog rendering) and an archiver task ships
//!     full backups + binlogs to an S3-compatible bucket.
//!   - BINLOG_RECOVER_FROM_BUCKET + MYSQL_RECOVERY_TARGET_TIME set, on an
//!     uninitialized (fresh) datadir: restore-on-boot loads the newest
//!     qualifying full backup and replays binlogs up to the target instant
//!     before mysqld ever starts serving.
//!
//! Both are refused (warned, not fatal) whenever GR_SEEDS is also set — this
//! version's archiver/restore paths are standalone-only and must never
//! touch the Group Replication path.

mod archiver;
mod config;
mod demote_on_shutdown;
mod dns_probe;
mod gr;
mod health_server;
mod mysql_conf;
mod password_pin;
mod peers;
mod pitr;
mod process_manager;
mod restore;
mod s3;
mod self_heal;
mod sql;
mod volume_lock;

use anyhow::{bail, Result};
use common::{init_logging, Telemetry, TelemetryEvent};
use config::Config;
use health_server::AppState;
use std::sync::Arc;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = init_logging("mysql-wrapper");

    let config = Arc::new(Config::from_env()?);
    let telemetry = Arc::new(Telemetry::from_env("mysql-ha"));

    // At most one container runs against this dataset at a time: wait for a
    // previous container's supervisor to release the volume before anything
    // below (the password pin, GR config, mysqld) touches the data directory
    // (see volume_lock for the overlap rationale). Fail-stop on timeout —
    // the restart policy retries the boot.
    volume_lock::acquire_volume_runtime_lock(&config.data_dir)?;

    // A PITR restore that crashed mid-way leaves the datadir in an unknown,
    // partially-loaded state. Everything in that datadir is derived from the
    // recovery bucket by construction (a restore only ever runs on an
    // uninitialized volume), so when this boot can restore again — recover
    // vars present, standalone — the partial state is wiped here and the
    // restore below re-runs from scratch: a deterministic retry, the same
    // convergent posture as any interrupted init. Only when there is nothing
    // to retry from (recover vars removed, or GR_SEEDS set) does the boot
    // refuse: serving partial data silently, or initializing an empty server
    // that masquerades as data loss, are both worse than stopping.
    // Checked unconditionally (not just when restore_enabled()): the marker
    // on the volume stays the source of truth even if the env vars changed
    // after the crash. See restore.rs.
    if restore::crashed_mid_restore(&config.data_dir) {
        if config.restore_enabled() && config.gr_seeds.is_none() {
            warn!(
                "a previous point-in-time restore did not complete; wiping the \
                 partially-restored data directory (derived state only) and retrying the \
                 restore from scratch"
            );
            restore::reset_partial_restore(&config.data_dir)?;
        } else {
            bail!(
                "a previous point-in-time restore did not complete (crashed mid-restore) and \
                 this boot has no standalone BINLOG_RECOVER_FROM_* configuration to retry it \
                 with; the data directory is in an unknown state and refuses to boot — re-add \
                 the recovery configuration, or restore onto a fresh volume"
            );
        }
    }

    info!(
        mysql_port = config.mysql_port,
        health_port = config.health_port,
        gr_enabled = config.gr_enabled(),
        is_bootstrap_candidate = config.is_bootstrap_candidate(),
        "starting mysql-wrapper"
    );

    // Boot accounting and, past the boot-loop threshold, the wedged-datadir
    // self-heal (see self_heal.rs). Must run before ANYTHING reads the
    // datadir — the password pin, the group-name marker and the fresh-datadir
    // test below all change meaning when the heal discards the datadir.
    let preboot = if config.gr_enabled() {
        Some(self_heal::preboot(&config, &telemetry).await)
    } else {
        None
    };

    // The pool starts on the pinned password when one exists and disagrees
    // with the environment — a drifted MYSQL_ROOT_PASSWORD edit must not lock
    // the wrapper out of its own mysqld (see password_pin.rs). The resolver
    // task then proves the active password against the live server, swaps the
    // pool if needed, and refreshes the pin.
    let boot_pin = password_pin::read_pin(&config.data_dir);
    let boot_password =
        password_pin::initial_password(&config.mysql_root_password, boot_pin.as_deref());
    let sql = sql::Sql::connect_root_over_socket(&config.socket_path, &boot_password);
    tokio::spawn(password_pin::resolve_and_apply(
        config.clone(),
        sql.clone(),
        telemetry.clone(),
    ));

    let mut boot_note = None;
    if config.gr_enabled() {
        // PITR archiving is standalone-only in this version — the archiver
        // and the GR orchestrator have never been exercised together, and
        // this version doesn't attempt it. Warn and fall through to the
        // GR path completely unchanged rather than silently ignoring the
        // variable or refusing to boot.
        if config.archive_enabled() {
            warn!(
                "BINLOG_ARCHIVE_BUCKET is set but GR_SEEDS is also set; binlog archiving is \
                 standalone-only for now — continuing without archiving"
            );
        }
        if config.restore_enabled() {
            warn!(
                "BINLOG_RECOVER_FROM_BUCKET/MYSQL_RECOVERY_TARGET_TIME are set but GR_SEEDS is \
                 also set; point-in-time restore is standalone-only for now — skipping restore"
            );
        }

        let preboot = preboot.expect("preboot runs whenever gr is enabled");
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
        let fresh_datadir = !config.datadir_is_initialized();

        tokio::spawn(health_server::run_health_server_supervised(
            config.health_port,
            Arc::new(AppState {
                sql: sql.clone(),
                standalone: false,
                data_dir: config.data_dir.clone(),
            }),
            telemetry.clone(),
        ));

        boot_note = Some(self_heal::PlannedShutdownNote {
            data_dir: config.data_dir.clone(),
            ready: preboot.ready.clone(),
        });
        tokio::spawn(self_heal::boot_watch(
            config.clone(),
            sql.clone(),
            telemetry.clone(),
            preboot,
        ));

        // Shared with orchestrate: the stuck-member watchdog raises it while
        // it drives a stop-plugin-then-clone heal, so the join loop can't
        // restart the plugin mid-clone.
        let healing = Arc::new(std::sync::atomic::AtomicBool::new(false));
        tokio::spawn(self_heal::stuck_watch(
            config.clone(),
            sql.clone(),
            telemetry.clone(),
            healing.clone(),
        ));

        tokio::spawn(gr::orchestrate(
            config.clone(),
            sql.clone(),
            telemetry.clone(),
            group_name,
            fresh_datadir,
            healing,
        ));
    } else {
        // Restore-on-boot runs before anything else in standalone mode: on
        // success it leaves an already-initialized datadir behind, which the
        // conf rendering and the final mysqld spawn below both need to see.
        // Gated on GR_SEEDS itself (not gr_enabled()) so a GR_ENABLED=false
        // revert-to-standalone with GR_SEEDS still present gets the same
        // standalone-only refusal as the GR arm above, instead of silently
        // restoring under a half-reverted config.
        if config.restore_enabled() {
            if config.gr_seeds.is_some() {
                warn!(
                    "BINLOG_RECOVER_FROM_BUCKET/MYSQL_RECOVERY_TARGET_TIME are set but GR_SEEDS \
                     is also set; point-in-time restore is standalone-only for now — skipping \
                     restore"
                );
            } else if config.datadir_is_initialized() {
                info!(
                    "data directory is already initialized; skipping point-in-time restore \
                     (idempotent restart)"
                );
            } else {
                restore::run(&config).await?;
            }
        }

        let archiving = config.archive_enabled() && config.gr_seeds.is_none();
        if config.archive_enabled() && config.gr_seeds.is_some() {
            warn!(
                "BINLOG_ARCHIVE_BUCKET is set but GR_SEEDS is also set; binlog archiving is \
                 standalone-only for now — continuing without archiving"
            );
        }
        if archiving {
            // No SERVER_ID override in standalone mode today — 1 is the
            // conventional single-server default (see the env contract).
            let server_id = config.server_id.unwrap_or(1);
            info!(server_id, "rendering standalone PITR archive config");
            mysql_conf::write_standalone_archive_conf(&config, server_id)?;
        }

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

        if archiving {
            tokio::spawn(archiver::run(config.clone(), sql.clone(), telemetry.clone()));
        }
    }

    // MYSQL_ROOT_PASSWORD and friends reach docker-entrypoint.sh through the
    // inherited process environment, not as CLI args.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let child = process_manager::spawn_mysqld(&args).await?;

    // HA mode: hand the primary role off before mysqld is signaled, so a
    // planned shutdown is a switchover, not a detection-timeout failover.
    let demote = config.gr_enabled().then(|| demote_on_shutdown::DemoteCtx {
        sql: sql.clone(),
        deadline_ms: config.demote_timeout_ms,
    });

    process_manager::supervise(child, demote, boot_note).await
}
