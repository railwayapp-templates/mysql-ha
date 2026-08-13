//! Restore-on-boot to an arbitrary timestamp — standalone mode only (see
//! main.rs's gate: this is never invoked while GR_SEEDS is set).
//!
//! Only runs against an UNINITIALIZED datadir (a fresh volume) — main.rs
//! checks `Config::datadir_is_initialized()` before calling `run` at all, so
//! a redeploy/restart of an already-restored service is a no-op here, not a
//! repeat restore.
//!
//! Sequence:
//!   1. Discover every full backup across every `server-*/full/` lineage in
//!      the bucket, pick the newest one at or before the recovery target
//!      (`pitr::newest_qualifying_full`).
//!   2. Spawn a restore-phase mysqld the same way any other boot would
//!      (`process_manager::spawn_mysqld` — docker-entrypoint.sh runs its
//!      normal first-boot init against the empty datadir). As soon as the
//!      datadir takes its first write, persist an in-progress marker: a
//!      crash any time after this point must fail loud on the next boot
//!      instead of half-serving (see `crashed_mid_restore`, checked by
//!      main.rs before anything else runs).
//!   3. Once the FINAL server (not docker-entrypoint's own init-temp
//!      instance) is reachable, best-effort attempt to disable its network
//!      listener (`SET GLOBAL skip_networking = ON`) for defense in depth —
//!      verified read-only at runtime on the bundled 8.4 series (MySQL never
//!      shipped the dynamic form some release notes describe), so this
//!      logs a warning and moves on rather than failing the restore over it.
//!      The real reason this is safe either way: no health server is up yet
//!      and nothing routes to a boot this fresh regardless.
//!   4. Load the selected full backup (`gunzip -c | mysql`, streamed
//!      straight from the bucket — nothing stages the whole dump on disk).
//!   5. Replay the lineage's binlogs from the full's own coordinate up to
//!      the target time, stopping at the first sequence gap.
//!   6. Shut the restore-phase mysqld down cleanly and return — main.rs's
//!      normal flow starts the real, fully-networked, long-lived instance
//!      right after this, against the now-restored (and already
//!      initialized) datadir.

use crate::config::Config;
use crate::pitr::{self, FullBackupMeta, FullBackupRef, S3Location};
use crate::process_manager;
use crate::s3::S3Client;
use crate::sql::Sql;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tracing::{info, warn};

const RESTORE_STATE_FILE: &str = ".pitr_restore_state.json";
const SCRATCH_DIR: &str = ".pitr_restore_binlogs";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoreStatus {
    InProgress,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreMarker {
    pub status: RestoreStatus,
    pub target_time: String,
    pub updated_at: String,
}

fn restore_marker_path(data_dir: &str) -> PathBuf {
    Path::new(data_dir).join(RESTORE_STATE_FILE)
}

pub fn read_restore_marker(data_dir: &str) -> Option<RestoreMarker> {
    let content = std::fs::read_to_string(restore_marker_path(data_dir)).ok()?;
    serde_json::from_str(&content).ok()
}

/// True when a previous restore attempt marked itself in-progress and never
/// reached completion — the datadir is in an unknown, partially-loaded
/// state. Checked by main.rs before anything else on EVERY boot (not just
/// when `restore_enabled()`), because the recover env vars themselves may
/// have been removed after the crash.
pub fn crashed_mid_restore(data_dir: &str) -> bool {
    matches!(
        read_restore_marker(data_dir),
        Some(RestoreMarker {
            status: RestoreStatus::InProgress,
            ..
        })
    )
}

/// Reset a datadir left behind by a crashed mid-restore attempt so the
/// restore can re-run from scratch on this boot. Everything in the datadir
/// is derived state by construction — a restore only ever runs on an
/// uninitialized volume, so nothing in it was authoritative; deleting it
/// and re-deriving from the bucket is a deterministic retry, not data loss.
/// The one live file is the runtime volume lock, held by THIS boot — it
/// survives the sweep.
pub fn reset_partial_restore(data_dir: &str) -> Result<()> {
    let keep = std::ffi::OsStr::new(crate::volume_lock::RUNTIME_LOCK_FILE);
    for entry in std::fs::read_dir(data_dir)
        .with_context(|| format!("listing {data_dir} to reset a partial restore"))?
    {
        let entry = entry.with_context(|| format!("listing {data_dir}"))?;
        if entry.file_name() == keep {
            continue;
        }
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("stat {}", path.display()))?;
        let removed = if file_type.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        removed.with_context(|| format!("removing {}", path.display()))?;
    }
    Ok(())
}

fn write_restore_marker(data_dir: &str, status: RestoreStatus, target: DateTime<Utc>) -> Result<()> {
    let marker = RestoreMarker {
        status,
        target_time: pitr::format_rfc3339_millis(target),
        updated_at: pitr::format_rfc3339_millis(Utc::now()),
    };
    let path = restore_marker_path(data_dir);
    let json = serde_json::to_string(&marker).context("serializing the PITR restore marker")?;
    std::fs::write(&path, json).with_context(|| format!("writing {}", path.display()))
}

/// Run the whole restore end-to-end. Only ever called by main.rs when
/// `Config::restore_enabled()` and the datadir is still uninitialized.
pub async fn run(config: &Config) -> Result<()> {
    let data_dir = config.data_dir.clone();
    let target = config
        .recovery_target_time()
        .context("restore::run called without a parsed MYSQL_RECOVERY_TARGET_TIME")?;
    info!(target = %pitr::format_rfc3339_millis(target), "starting point-in-time restore");

    let location = config
        .restore_s3_location()
        .expect("restore::run is only called when Config::restore_enabled()");
    let s3 = S3Client::new(&location)
        .await
        .context("building the PITR restore S3 client")?;

    let fulls = discover_fulls(&s3, &location)
        .await
        .context("discovering full backups in the bucket")?;
    let full = pitr::newest_qualifying_full(&fulls, target)
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "no full backup found at or before target time {} under the configured bucket/path",
                pitr::format_rfc3339_millis(target)
            )
        })?;
    info!(
        server_uuid = %full.server_uuid,
        dump_key = %full.dump_key,
        taken_at = %pitr::format_rfc3339_millis(full.meta.taken_at),
        "selected full backup for restore"
    );

    // Same invocation as any other boot — docker-entrypoint.sh sees the
    // empty datadir and runs its normal first-boot init.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut child = process_manager::spawn_mysqld(&args)
        .await
        .context("spawning the restore-phase mysqld")?;

    write_marker_once_datadir_exists(&data_dir, &mut child, target).await?;

    let sql = Sql::connect_root_over_socket(&config.socket_path, &config.mysql_root_password);
    wait_for_ready_or_exit(&mut child, &sql).await?;
    info!("restore-phase mysqld is ready");

    if let Err(e) = sql.set_global_skip_networking(true).await {
        warn!(error = %e, "could not disable networking for the restore phase; continuing (nothing external can reach a boot this fresh regardless — no health server is up yet)");
    }

    load_full_backup(&s3, &full, config)
        .await
        .context("loading the full backup")?;
    info!("full backup loaded");

    replay_binlogs(&s3, &location, &full, target, config)
        .await
        .context("replaying binlogs")?;
    info!("binlog replay complete");

    let _ = sql.shutdown_server().await;
    match child.wait().await {
        Ok(status) if !status.success() => {
            warn!(?status, "restore-phase mysqld exited non-zero on shutdown (harmless — the data was already loaded)");
        }
        Ok(_) => {}
        Err(e) => warn!(error = %e, "error waiting for the restore-phase mysqld to exit"),
    }

    write_restore_marker(&data_dir, RestoreStatus::Completed, target)?;
    info!("point-in-time restore completed; the normal boot flow starts mysqld in serving mode next");
    Ok(())
}

/// Poll until the datadir takes its first write (docker-entrypoint's
/// `mysqld --initialize` is under way), then immediately persist the
/// in-progress marker — as early as it is SAFE to write anything, since
/// `mysqld --initialize` itself requires the datadir to still be empty at
/// the instant it starts (see self_heal.rs's own `INIT_TOLERATED_ENTRIES`
/// for the same hazard in the HA boot-loop heal). Bails immediately if the
/// child exits before ever writing anything.
async fn write_marker_once_datadir_exists(
    data_dir: &str,
    child: &mut Child,
    target: DateTime<Utc>,
) -> Result<()> {
    loop {
        tokio::select! {
            status = child.wait() => {
                let status = status.context("waiting for the restore-phase mysqld")?;
                anyhow::bail!("restore-phase mysqld exited during initialization (status {status})");
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let non_empty = std::fs::read_dir(data_dir)
                    .map(|mut d| d.next().is_some())
                    .unwrap_or(false);
                if non_empty {
                    write_restore_marker(data_dir, RestoreStatus::InProgress, target)?;
                    return Ok(());
                }
            }
        }
    }
}

/// Wait for the FINAL mysqld (not docker-entrypoint's own transient init
/// server) the same way gr.rs's orchestrator does, or bail if the child
/// exits first.
async fn wait_for_ready_or_exit(child: &mut Child, sql: &Sql) -> Result<()> {
    loop {
        tokio::select! {
            status = child.wait() => {
                let status = status.context("waiting for the restore-phase mysqld")?;
                anyhow::bail!("restore-phase mysqld exited before it started accepting connections (status {status})");
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                if let Ok(false) = sql.is_init_temp_server().await {
                    return Ok(());
                }
            }
        }
    }
}

/// Every full backup across every lineage in the bucket, parsed from its
/// `meta.json` sidecar. Best-effort per entry: a corrupt/unreadable meta
/// just drops that one candidate (logged) rather than failing the whole
/// discovery — one bad object must not block recovering from every other
/// good one.
async fn discover_fulls(s3: &S3Client, location: &S3Location) -> Result<Vec<FullBackupRef>> {
    let base = pitr::base_prefix(location);
    let keys = s3
        .list_keys_with_prefix(&base)
        .await
        .context("listing the PITR archive bucket")?;

    let mut fulls = Vec::new();
    for key in keys {
        if !key.ends_with(".meta.json") || !key.contains("/full/") {
            continue;
        }
        let Some(server_uuid) = pitr::server_uuid_from_key(location, &key) else {
            continue;
        };
        let bytes = match s3.get_object_bytes(&key).await {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, %key, "could not read a full-backup meta.json; skipping it");
                continue;
            }
        };
        let meta: FullBackupMeta = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, %key, "could not parse a full-backup meta.json; skipping it");
                continue;
            }
        };
        let Some(stem) = key.strip_suffix(".meta.json") else {
            continue;
        };
        fulls.push(FullBackupRef {
            server_uuid,
            dump_key: format!("{stem}.sql.gz"),
            meta,
        });
    }
    Ok(fulls)
}

/// `gunzip -c | mysql`, streamed straight from the bucket through both
/// subprocesses — nothing buffers the whole (potentially huge) dump.
async fn load_full_backup(s3: &S3Client, full: &FullBackupRef, config: &Config) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let s3_reader = s3
        .get_object_async_read(&full.dump_key)
        .await
        .context("GET the full backup dump")?;

    let mut gunzip = Command::new("gunzip")
        .arg("-c")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning gunzip")?;
    let mut mysql = Command::new("mysql")
        .arg(format!("--socket={}", config.socket_path))
        .arg("-uroot")
        .env("MYSQL_PWD", &config.mysql_root_password)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning mysql")?;

    let mut gunzip_stdin = gunzip.stdin.take().context("gunzip stdin was not piped")?;
    let gunzip_stdout = gunzip.stdout.take().context("gunzip stdout was not piped")?;
    let mut mysql_stdin = mysql.stdin.take().context("mysql stdin was not piped")?;

    let relay_in = tokio::spawn(async move {
        let mut reader = s3_reader;
        tokio::io::copy(&mut reader, &mut gunzip_stdin).await?;
        gunzip_stdin.shutdown().await
    });
    let relay_out = tokio::spawn(async move {
        let mut reader = gunzip_stdout;
        tokio::io::copy(&mut reader, &mut mysql_stdin).await?;
        mysql_stdin.shutdown().await
    });

    let (in_result, out_result) = tokio::join!(relay_in, relay_out);
    in_result
        .context("relay task panicked")?
        .context("streaming the dump from S3 into gunzip")?;
    out_result
        .context("relay task panicked")?
        .context("streaming gunzip's output into mysql")?;

    let gunzip_status = gunzip.wait().await.context("waiting for gunzip")?;
    let mysql_status = mysql.wait().await.context("waiting for mysql")?;
    if !gunzip_status.success() {
        anyhow::bail!("gunzip exited with {gunzip_status}");
    }
    if !mysql_status.success() {
        anyhow::bail!("mysql (loading the full backup) exited with {mysql_status}");
    }
    Ok(())
}

/// Download the lineage's binlogs from the full's own coordinate up to the
/// first sequence gap, then replay them with `mysqlbinlog | mysql`: the
/// first file gets `--start-position`, every file gets the shared
/// `--stop-datetime`. `mysqlbinlog` needs real files on disk (it doesn't
/// support multiple stdin streams), so these ARE staged locally, in a
/// scratch directory removed once replay finishes.
async fn replay_binlogs(
    s3: &S3Client,
    location: &S3Location,
    full: &FullBackupRef,
    target: DateTime<Utc>,
    config: &Config,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let prefix = pitr::binlog_prefix(location, &full.server_uuid);
    let keys = s3
        .list_keys_with_prefix(&prefix)
        .await
        .context("listing the lineage's binlogs")?;
    let names: Vec<String> = keys
        .iter()
        .filter_map(|k| k.rsplit('/').next().map(str::to_string))
        .collect();

    let to_replay = pitr::binlogs_to_replay(names, &full.meta.binlog_file);
    if to_replay.is_empty() {
        info!(
            start_file = %full.meta.binlog_file,
            "no binlogs to replay beyond the full backup (none were shipped yet, or the \
             lineage's own coordinate file is missing from the archive)"
        );
        return Ok(());
    }
    info!(files = ?to_replay, start_position = full.meta.binlog_pos, "replaying binlogs");

    let scratch = Path::new(&config.data_dir).join(SCRATCH_DIR);
    std::fs::create_dir_all(&scratch)
        .with_context(|| format!("creating {}", scratch.display()))?;
    let mut local_paths = Vec::new();
    for name in &to_replay {
        let key = pitr::binlog_key(location, &full.server_uuid, name);
        let local = scratch.join(name);
        s3.download_to_file(&key, &local)
            .await
            .with_context(|| format!("downloading {key}"))?;
        local_paths.push(local);
    }

    let stop_dt = target.format("%Y-%m-%d %H:%M:%S").to_string();
    let mut mysqlbinlog_cmd = Command::new("mysqlbinlog");
    mysqlbinlog_cmd
        .arg(format!("--start-position={}", full.meta.binlog_pos))
        .arg(format!("--stop-datetime={stop_dt}"));
    for path in &local_paths {
        mysqlbinlog_cmd.arg(path);
    }
    let mut mysqlbinlog = mysqlbinlog_cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning mysqlbinlog")?;
    let mut mysql = Command::new("mysql")
        .arg(format!("--socket={}", config.socket_path))
        .arg("-uroot")
        .env("MYSQL_PWD", &config.mysql_root_password)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning mysql")?;

    let binlog_stdout = mysqlbinlog
        .stdout
        .take()
        .context("mysqlbinlog stdout was not piped")?;
    let mut mysql_stdin = mysql.stdin.take().context("mysql stdin was not piped")?;
    let relay = tokio::spawn(async move {
        let mut reader = binlog_stdout;
        tokio::io::copy(&mut reader, &mut mysql_stdin).await?;
        mysql_stdin.shutdown().await
    });

    let relay_result = relay.await;
    let binlog_status = mysqlbinlog.wait().await.context("waiting for mysqlbinlog")?;
    let mysql_status = mysql.wait().await.context("waiting for mysql")?;
    let _ = std::fs::remove_dir_all(&scratch);

    relay_result
        .context("relay task panicked")?
        .context("streaming mysqlbinlog's output into mysql")?;
    if !binlog_status.success() {
        anyhow::bail!("mysqlbinlog exited with {binlog_status}");
    }
    if !mysql_status.success() {
        anyhow::bail!("mysql (replaying binlogs) exited with {mysql_status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "mysql-wrapper-restore-test-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    fn t() -> DateTime<Utc> {
        pitr::parse_target_time("2026-08-13T14:00:00.000Z").unwrap()
    }

    #[test]
    fn no_marker_is_not_a_crash() {
        let dir = temp_dir("none");
        assert!(read_restore_marker(&dir).is_none());
        assert!(!crashed_mid_restore(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn in_progress_marker_reads_as_crashed() {
        let dir = temp_dir("in-progress");
        write_restore_marker(&dir, RestoreStatus::InProgress, t()).unwrap();
        assert!(crashed_mid_restore(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn completed_marker_is_not_a_crash() {
        let dir = temp_dir("completed");
        write_restore_marker(&dir, RestoreStatus::InProgress, t()).unwrap();
        write_restore_marker(&dir, RestoreStatus::Completed, t()).unwrap();
        assert!(!crashed_mid_restore(&dir));
        let marker = read_restore_marker(&dir).unwrap();
        assert_eq!(marker.status, RestoreStatus::Completed);
        assert_eq!(marker.target_time, "2026-08-13T14:00:00.000Z");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reset_partial_restore_wipes_everything_but_the_runtime_lock() {
        let dir = temp_dir("reset");
        write_restore_marker(&dir, RestoreStatus::InProgress, t()).unwrap();
        std::fs::create_dir_all(Path::new(&dir).join("mysql")).unwrap();
        std::fs::write(Path::new(&dir).join("mysql").join("ibdata1"), "junk").unwrap();
        std::fs::write(Path::new(&dir).join("binlog.000001"), "junk").unwrap();
        let lock = Path::new(&dir).join(crate::volume_lock::RUNTIME_LOCK_FILE);
        std::fs::write(&lock, "held-by-this-boot").unwrap();

        reset_partial_restore(&dir).unwrap();

        assert!(!crashed_mid_restore(&dir), "marker must be gone");
        assert!(!Path::new(&dir).join("mysql").exists(), "partial datadir must be gone");
        assert!(!Path::new(&dir).join("binlog.000001").exists());
        assert_eq!(
            std::fs::read_to_string(&lock).unwrap(),
            "held-by-this-boot",
            "the held runtime lock must survive the sweep"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn garbage_marker_file_degrades_to_absent() {
        let dir = temp_dir("garbage");
        std::fs::write(restore_marker_path(&dir), "not json").unwrap();
        assert!(read_restore_marker(&dir).is_none());
        assert!(!crashed_mid_restore(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }
}
