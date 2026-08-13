//! Process supervision for the mysqld subprocess.
//!
//! Adapted from redis-ha's `process_manager::supervise`, which supervises two
//! colocated processes (redis-server + redis-sentinel). Group Replication
//! runs inside mysqld itself — there is no second process to colocate — so
//! this is the single-child form: spawn, forward signals, and exit the
//! container with the child's own exit code if it dies, letting Railway's
//! restart policy handle recovery.

use anyhow::{Context, Result};
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use tokio::process::{Child, Command};
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info, warn};

/// Spawn the upstream entrypoint, passing through any CLI args this process
/// was itself invoked with — mirrors `docker-entrypoint.sh mysqld [args...]`.
///
/// In HA mode the Group Replication my.cnf fragment is already on disk by
/// the time this runs — main.rs renders it (mysql_conf::write_gr_conf)
/// before spawning, so every phase of docker-entrypoint.sh reads it.
pub async fn spawn_mysqld(args: &[String]) -> Result<Child> {
    info!(?args, "starting docker-entrypoint.sh mysqld");

    Command::new("docker-entrypoint.sh")
        .arg("mysqld")
        .args(args)
        .kill_on_drop(false)
        .spawn()
        .context("failed to spawn docker-entrypoint.sh mysqld")
}

/// Supervise the mysqld child: forward SIGTERM/SIGINT and wait for a graceful
/// exit, or exit the container immediately (with the child's own code) if it
/// dies on its own.
///
/// `demote` — present in HA mode only: hand the primary role off through the
/// group BEFORE mysqld is signaled, so a planned shutdown is a consensual
/// switchover rather than a detection-timeout failover (see
/// demote_on_shutdown.rs). Runs while mysqld is still healthy; a failure
/// there never blocks the shutdown.
///
/// `boot_note` — present in HA mode only: a signaled shutdown before mysqld
/// ever accepted connections is an interrupted boot, not a failed one — the
/// note un-counts it so redeploy bursts can't read as a boot loop (see
/// self_heal.rs).
pub async fn supervise(
    mut child: Child,
    demote: Option<crate::demote_on_shutdown::DemoteCtx>,
    boot_note: Option<crate::self_heal::PlannedShutdownNote>,
) -> Result<()> {
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    let pid = child.id().map(|id| Pid::from_raw(id as i32));

    // Every arm below ends the process — the "loop" runs at most once.
    #[allow(clippy::never_loop)]
    loop {
        tokio::select! {
            status = child.wait() => {
                let code = match status {
                    Ok(s) => {
                        error!(code = s.code(), "mysqld exited unexpectedly");
                        s.code().unwrap_or(1)
                    }
                    Err(e) => {
                        error!(error = %e, "mysqld wait error");
                        1
                    }
                };
                // An unasked exit must never look like success: with an
                // ON_FAILURE restart policy, propagating mysqld's clean 0
                // here leaves the container "exited (0)" and the database
                // down until a human redeploys. Every deliberate stop goes
                // through the signal branches below, which are the only
                // paths allowed to exit 0.
                std::process::exit(if code == 0 { 1 } else { code });
            }

            _ = sigterm.recv() => {
                info!("received SIGTERM, shutting down");
                if let Some(note) = &boot_note {
                    note.note_planned_shutdown();
                }
                if let Some(ctx) = &demote {
                    crate::demote_on_shutdown::demote_if_primary(ctx).await;
                }
                graceful_shutdown(pid, &mut child).await;
                std::process::exit(0);
            }

            _ = sigint.recv() => {
                info!("received SIGINT, shutting down");
                if let Some(note) = &boot_note {
                    note.note_planned_shutdown();
                }
                if let Some(ctx) = &demote {
                    crate::demote_on_shutdown::demote_if_primary(ctx).await;
                }
                graceful_shutdown(pid, &mut child).await;
                std::process::exit(0);
            }
        }
    }
}

async fn graceful_shutdown(pid: Option<Pid>, child: &mut Child) {
    if let Some(pid) = pid {
        info!("sending SIGTERM to mysqld");
        let _ = signal::kill(pid, Signal::SIGTERM);
        tokio::select! {
            _ = child.wait() => {}
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                warn!("mysqld did not exit in time, killing");
                let _ = child.kill().await;
            }
        }
    }
}
