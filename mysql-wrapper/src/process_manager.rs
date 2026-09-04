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
use std::path::Path;
use tokio::process::{Child, Command};
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info, warn};

/// Remove mysqld's unix-socket lock files (`<socket>.lock`, and the X
/// plugin's `mysqlx.sock.lock` beside it) left behind by a previous mysqld
/// in THIS container's filesystem — a crash, a killed restore-phase server,
/// or a restart-policy restart, none of which clean `/var/run/mysqld` up.
///
/// mysqld itself only reclaims such a lock when the PID inside is dead. On a
/// container restart PIDs start over from 1, so a low PID recorded by the
/// previous life is routinely alive again as one of this wrapper's own
/// threads or the entrypoint's subshells — and mysqld then refuses to start
/// ("Another process with pid N is using unix socket file") for as long as
/// the restart loop keeps re-creating that coincidence. Called once at boot,
/// after the volume lock proved no other container runs against this data:
/// at that point no mysqld of ours exists yet, so any lock file is stale by
/// construction. Only a lock whose PID is a LIVE `mysqld` is kept — that is
/// not a situation this wrapper can produce, and it must not delete another
/// engine's lock if it ever were.
pub fn clear_stale_socket_locks(socket_path: &str) {
    let socket = Path::new(socket_path);
    let mut candidates =
        vec![
            socket.with_extension(match socket.extension().and_then(|e| e.to_str()) {
                Some(ext) => format!("{ext}.lock"),
                None => "lock".to_string(),
            }),
        ];
    if let Some(dir) = socket.parent() {
        candidates.push(dir.join("mysqlx.sock.lock"));
    }
    for lock in candidates {
        let Ok(contents) = std::fs::read_to_string(&lock) else {
            continue;
        };
        let pid = contents.trim().parse::<u32>().ok();
        if let Some(pid) = pid {
            let comm = std::fs::read_to_string(format!("/proc/{pid}/comm")).unwrap_or_default();
            if comm.trim() == "mysqld" {
                warn!(
                    lock = %lock.display(),
                    pid,
                    "socket lock names a live mysqld; leaving it in place"
                );
                continue;
            }
        }
        match std::fs::remove_file(&lock) {
            Ok(()) => info!(
                lock = %lock.display(),
                stale_pid = ?pid,
                "removed a stale mysqld socket lock left by a previous container life"
            ),
            Err(e) => {
                warn!(error = %e, lock = %lock.display(), "could not remove a stale socket lock")
            }
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mysql-wrapper-socket-lock-test-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn stale_locks_are_removed_whether_or_not_their_pid_is_alive() {
        let dir = temp_dir("stale");
        let socket = dir.join("mysqld.sock");
        let main_lock = dir.join("mysqld.sock.lock");
        let x_lock = dir.join("mysqlx.sock.lock");
        // A PID that is alive right now (ours) but is not a mysqld — the
        // restart-coincidence case — and a dead one.
        std::fs::write(&main_lock, format!("{}\n", std::process::id())).unwrap();
        std::fs::write(&x_lock, "999999999\n").unwrap();
        clear_stale_socket_locks(socket.to_str().unwrap());
        assert!(
            !main_lock.exists(),
            "live-but-not-mysqld pid must not keep the lock"
        );
        assert!(!x_lock.exists(), "dead pid lock must go");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_locks_are_a_no_op() {
        let dir = temp_dir("none");
        clear_stale_socket_locks(dir.join("mysqld.sock").to_str().unwrap());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn garbage_lock_contents_are_removed_too() {
        let dir = temp_dir("garbage");
        let socket = dir.join("mysqld.sock");
        let main_lock = dir.join("mysqld.sock.lock");
        std::fs::write(&main_lock, "not a pid").unwrap();
        clear_stale_socket_locks(socket.to_str().unwrap());
        assert!(!main_lock.exists());
        std::fs::remove_dir_all(&dir).ok();
    }
}
