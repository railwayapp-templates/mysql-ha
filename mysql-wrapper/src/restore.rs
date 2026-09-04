//! Restore-on-boot to an arbitrary timestamp — standalone mode only (see
//! main.rs's gate: this is never invoked while GR_SEEDS is set; a restore
//! produces a new standalone server, whatever archived into the bucket).
//!
//! The archive it reads may be either kind pitr.rs describes: one
//! standalone server's independent lineages, or one Group Replication
//! group's shared history archived by whichever member was primary. The
//! full's meta says which (`gtid_purged`), and step 5 below branches on it.
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
//!   5. Replay binlogs up to the target time.
//!      - Independent history (anonymous transactions): the full's own
//!        lineage from its recorded coordinate. A sequence gap with binlogs
//!        still present past it FAILS the restore loudly (see
//!        replay_binlogs) — replaying short of the target and reporting
//!        success would silently lose everything after the hole.
//!      - Shared history (GTIDs): the full's lineage from its coordinate,
//!        then EVERY other lineage from its first archived file (see
//!        replay_shared_history). The restore-phase mysqld runs with
//!        gtid_mode=ON_PERMISSIVE so the dump's GTID set loads and the
//!        server skips each GTID it already holds, whichever lineage
//!        delivers it — that is what makes a failed-over primary's
//!        never-uploaded tail recoverable from the next primary's lineage.
//!        Completeness is then proven on the result, not assumed from the
//!        file names: a hole in any UUID's `gtid_executed` interval set, or
//!        a dump transaction missing from it, FAILS the restore loudly.
//!      Either way the ACHIEVED recovery point is verified against the
//!      target (see verify_achieved_point): mysqlbinlog exits 0 when the
//!      logs simply end before --stop-datetime, so an archive that stopped
//!      shipping hours before the target would otherwise "succeed" silently;
//!      more than a rotation-bounded lag behind the target fails as loudly
//!      as the gap check, and the point reached is recorded in the marker.
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
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tracing::{error, info, warn};

const RESTORE_STATE_FILE: &str = ".pitr_restore_state.json";
const SCRATCH_DIR: &str = ".pitr_restore_binlogs";

/// Extra mysqld flags for the restore-phase server when the archive is one
/// shared GTID history (see the module doc). The dump's
/// `SET @@GLOBAL.GTID_PURGED` refuses to load under gtid_mode=OFF, replaying
/// another lineage relies on the server skipping GTIDs it already holds, and
/// any anonymous-transaction binlogs from before a standalone→HA conversion
/// still have to apply — ON_PERMISSIVE is the one mode that accepts all
/// three. enforce_gtid_consistency=ON is what any gtid_mode above
/// OFF_PERMISSIVE requires. Restore-phase only: the serving mysqld main.rs
/// starts afterwards is spawned with the service's own args.
const SHARED_HISTORY_RESTORE_ARGS: [&str; 2] =
    ["--gtid-mode=ON_PERMISSIVE", "--enforce-gtid-consistency=ON"];

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
    /// The recovery point the restore actually reached (Completed markers
    /// only) — at most `achieved_lag_bound_seconds` behind `target_time`,
    /// verified before the marker is written (see `verify_achieved_point`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub achieved_time: Option<String>,
    pub updated_at: String,
}

fn restore_marker_path(data_dir: &str) -> PathBuf {
    Path::new(data_dir).join(RESTORE_STATE_FILE)
}

/// The three states the marker FILE can be in — `Absent` and
/// present-but-unparseable mean opposite things for the crash check below,
/// so they must never collapse into one.
enum MarkerFile {
    Absent,
    Unparseable,
    Present(RestoreMarker),
}

fn read_marker_file(data_dir: &str) -> MarkerFile {
    let path = restore_marker_path(data_dir);
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return MarkerFile::Absent,
        // A file that exists but cannot even be read is as untrustworthy as
        // one that doesn't parse.
        Err(_) => return MarkerFile::Unparseable,
    };
    match serde_json::from_str(&content) {
        Ok(marker) => MarkerFile::Present(marker),
        Err(_) => MarkerFile::Unparseable,
    }
}

/// Test-facing view of the marker. Production reads go through
/// `read_marker_file`/`crashed_mid_restore`, which must keep unparseable
/// distinct from absent — this collapses both to None.
#[cfg(test)]
fn read_restore_marker(data_dir: &str) -> Option<RestoreMarker> {
    match read_marker_file(data_dir) {
        MarkerFile::Present(marker) => Some(marker),
        MarkerFile::Absent | MarkerFile::Unparseable => None,
    }
}

/// True when a previous restore attempt marked itself in-progress and never
/// reached completion — the datadir is in an unknown, partially-loaded
/// state. Checked by main.rs before anything else on EVERY boot (not just
/// when `restore_enabled()`), because the recover env vars themselves may
/// have been removed after the crash.
pub fn crashed_mid_restore(data_dir: &str) -> bool {
    match read_marker_file(data_dir) {
        MarkerFile::Absent => false,
        MarkerFile::Present(marker) => marker.status == RestoreStatus::InProgress,
        // A marker that EXISTS but cannot be parsed is a torn write or disk
        // decay on the very file that records whether a restore completed —
        // never a fresh volume (nothing else writes this path). Defaulting
        // open here would boot a vanilla mysqld on a possibly half-restored
        // datadir; treat it as in-progress instead (fail closed, the same
        // rationale as self_heal::read_ledger) and let main.rs's existing
        // wipe-and-retry / refuse logic take it from there.
        MarkerFile::Unparseable => {
            warn!(
                "PITR restore marker is present but unparseable; treating it as a crashed \
                 mid-restore (fail closed)"
            );
            true
        }
    }
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

fn write_restore_marker(
    data_dir: &str,
    status: RestoreStatus,
    target: DateTime<Utc>,
    achieved: Option<DateTime<Utc>>,
) -> Result<()> {
    use std::io::Write;

    let marker = RestoreMarker {
        status,
        target_time: pitr::format_rfc3339_millis(target),
        achieved_time: achieved.map(pitr::format_rfc3339_millis),
        updated_at: pitr::format_rfc3339_millis(Utc::now()),
    };
    let json = serde_json::to_string(&marker).context("serializing the PITR restore marker")?;
    // Publish atomically — tmp in the same dir + fsync + rename, the same
    // pattern as password_pin::write_pin: the reader deliberately fails
    // closed on a present-but-unparseable marker (see crashed_mid_restore),
    // so a torn in-place write here would either fabricate a crashed
    // restore out of a completed one or, worse, tear the very InProgress
    // record the crash check depends on.
    let path = restore_marker_path(data_dir);
    let tmp = Path::new(data_dir).join(format!("{RESTORE_STATE_FILE}.tmp"));
    let mut file =
        std::fs::File::create(&tmp).with_context(|| format!("opening {}", tmp.display()))?;
    file.write_all(json.as_bytes())
        .and_then(|()| file.sync_all())
        .with_context(|| format!("writing {}", tmp.display()))?;
    drop(file);
    std::fs::rename(&tmp, &path).with_context(|| format!("renaming {} into place", path.display()))
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
        gtid_purged = ?full.meta.gtid_purged,
        "selected full backup for restore"
    );

    // One GTID full anywhere, or the marker a group primary writes when it
    // starts archiving, marks the archive as one shared history (the same
    // test retention applies — pitr::archive_shares_history plus the
    // marker): a lineage that never dumped has nothing else to declare
    // itself with, and erring this way only ever replays MORE — which the
    // server dedups. The marker read fails the restore rather than guessing:
    // misreading a shared history as independent would replay one lineage
    // under gtid_mode=OFF and refuse its GTID binlogs half-way.
    let shared_history = fulls.iter().any(|f| f.meta.gtid_purged.is_some())
        || s3
            .exists(&pitr::shared_history_marker_key(&location))
            .await
            .context("checking the archive for the shared-history marker")?;

    // Same invocation as any other boot — docker-entrypoint.sh sees the
    // empty datadir and runs its normal first-boot init — plus, for a
    // shared history, the GTID flags the replay depends on.
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if shared_history {
        args.extend(SHARED_HISTORY_RESTORE_ARGS.iter().map(|a| a.to_string()));
        info!(
            "the archive is one shared GTID history (Group Replication); the restore-phase \
             mysqld runs with gtid_mode=ON_PERMISSIVE and every lineage is replayed"
        );
    }
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

    let achieved = if shared_history {
        replay_shared_history(&s3, &location, &fulls, &full, target, config, &sql).await
    } else {
        replay_binlogs(&s3, &location, &fulls, &full, target, config).await
    }
    .context("replaying binlogs")?;
    info!(
        achieved = %pitr::format_rfc3339_millis(achieved),
        target = %pitr::format_rfc3339_millis(target),
        "binlog replay complete"
    );

    let _ = sql.shutdown_server().await;
    match child.wait().await {
        Ok(status) if !status.success() => {
            warn!(?status, "restore-phase mysqld exited non-zero on shutdown (harmless — the data was already loaded)");
        }
        Ok(_) => {}
        Err(e) => warn!(error = %e, "error waiting for the restore-phase mysqld to exit"),
    }

    write_restore_marker(&data_dir, RestoreStatus::Completed, target, Some(achieved))?;
    info!(
        achieved = %pitr::format_rfc3339_millis(achieved),
        "point-in-time restore completed; the normal boot flow starts mysqld in serving mode next"
    );
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
                    write_restore_marker(data_dir, RestoreStatus::InProgress, target, None)?;
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
    let gunzip_stdout = gunzip
        .stdout
        .take()
        .context("gunzip stdout was not piped")?;
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
///
/// Returns the ACHIEVED recovery point, verified against the target: with an
/// empty run it is the full's own `taken_at`; otherwise the last event
/// timestamp of the last replayed binlog (capped at the target — everything
/// past `--stop-datetime` was deliberately not applied). `mysqlbinlog` exits
/// 0 when the logs simply end before `--stop-datetime`, so exit codes alone
/// would report success for a restore that silently stopped hours short of
/// the request — `verify_achieved_point` is what closes that hole.
async fn replay_binlogs(
    s3: &S3Client,
    location: &S3Location,
    fulls: &[FullBackupRef],
    full: &FullBackupRef,
    target: DateTime<Utc>,
    config: &Config,
) -> Result<DateTime<Utc>> {
    let prefix = pitr::binlog_prefix(location, &full.server_uuid);
    let keys = s3
        .list_keys_with_prefix(&prefix)
        .await
        .context("listing the lineage's binlogs")?;
    let names: Vec<String> = keys
        .iter()
        .filter_map(|k| k.rsplit('/').next().map(str::to_string))
        .collect();

    let plan = pitr::binlogs_to_replay(names, &full.meta.binlog_file);
    if let Some(gap) = &plan.gap {
        // Binlogs exist PAST a hole in the lineage: replaying up to the hole
        // and stopping would serve a database silently missing everything
        // after it while reporting success — worse than failing. Refuse, name
        // the gap, and leave the datadir marked mid-restore (fail-closed, the
        // same posture as every other unrecoverable restore state).
        error!(
            after = %gap.after,
            next_present = %gap.next_present,
            start_file = %full.meta.binlog_file,
            "binlog lineage has a gap: a binlog is missing from the archive while later \
             binlogs exist past it — the requested point-in-time target cannot be reached, \
             and replaying short of it would silently lose the data after the gap"
        );
        anyhow::bail!(
            "binlog lineage gap: no binlog follows {:?} but {:?} exists past the hole — \
             the archive is missing at least one binlog (expired, deleted, or lost before \
             upload), so a restore to the requested target is impossible; pick a target \
             at or before the gap, or restore from another full backup \
             (other discovered full backups: {})",
            if gap.after.is_empty() {
                full.meta.binlog_file.as_str()
            } else {
                gap.after.as_str()
            },
            gap.next_present,
            pitr::describe_fallback_fulls(fulls, full),
        );
    }
    let to_replay = plan.run;
    if to_replay.is_empty() {
        info!(
            start_file = %full.meta.binlog_file,
            "no binlogs to replay beyond the full backup (none were shipped yet, or \
             everything after the dump coordinate is still in the active binlog)"
        );
        // With nothing to replay, the dump itself is the whole restore — the
        // achieved point is the instant it was taken, and it must still sit
        // within the rotation bound of the target: an old full with no
        // shipped binlogs behind it can be hours short of the request.
        let achieved = full.meta.taken_at;
        verify_achieved_point(achieved, target, config, fulls, full)?;
        return Ok(achieved);
    }
    info!(files = ?to_replay, start_position = full.meta.binlog_pos, "replaying binlogs");

    let scratch = Path::new(&config.data_dir).join(SCRATCH_DIR);
    std::fs::create_dir_all(&scratch).with_context(|| format!("creating {}", scratch.display()))?;
    let mut local_paths = Vec::new();
    for name in &to_replay {
        let key = pitr::binlog_key(location, &full.server_uuid, name);
        let local = scratch.join(name);
        s3.download_to_file(&key, &local)
            .await
            .with_context(|| format!("downloading {key}"))?;
        local_paths.push(local);
    }

    // The achieved-point pass below reads the last staged file, so it must
    // run before the scratch dir is removed — and the dir must be removed on
    // the failure paths too, hence the inner-result shape.
    let result = replay_downloaded(&local_paths, Some(full.meta.binlog_pos), target, config).await;
    let _ = std::fs::remove_dir_all(&scratch);
    let achieved = result?;
    verify_achieved_point(achieved, target, config, fulls, full)?;
    Ok(achieved)
}

/// One lineage's contribution to a shared-history replay.
struct LineageRun {
    server_uuid: String,
    /// Gap-free, in order.
    files: Vec<String>,
    /// `--start-position` for the first file — only the selected full's own
    /// lineage has one (its recorded dump coordinate); every other lineage
    /// replays from the start of its first archived file and lets the server
    /// skip what the dump already holds.
    start_position: Option<u64>,
}

fn note_lineage_gap(server_uuid: &str, plan: &pitr::BinlogReplayPlan) {
    if let Some(gap) = &plan.gap {
        warn!(
            lineage = %server_uuid,
            after = %gap.after,
            next_present = %gap.next_present,
            "lineage has a sequence gap; replaying up to it — another lineage may carry the \
             missing transactions, and the GTID completeness check after replay decides"
        );
    }
}

/// The shared-history replay (module doc, step 5): the selected full's own
/// lineage from its recorded coordinate, then every other lineage's gap-free
/// run from its first archived file, each cut at `--stop-datetime`. Order is
/// immaterial to the result — every lineage is an in-order stream of the same
/// group history and the server applies each GTID once, whichever stream
/// delivers it first — so the full's lineage only goes first because it
/// continues the dump exactly. A sequence gap inside one lineage is not
/// fatal here: another member's stream may carry those transactions (the
/// reason for archiving from whichever member is primary), so it is logged
/// and the verdict left to the completeness check at the end — the restored
/// server's `gtid_executed` must have no hole in any UUID, and must contain
/// everything the dump declared. Either failing is the loud, fail-closed
/// refusal: the archive lost a transaction on every lineage that held it.
async fn replay_shared_history(
    s3: &S3Client,
    location: &S3Location,
    fulls: &[FullBackupRef],
    full: &FullBackupRef,
    target: DateTime<Utc>,
    config: &Config,
    sql: &Sql,
) -> Result<DateTime<Utc>> {
    let keys = s3
        .list_keys_with_prefix(&pitr::base_prefix(location))
        .await
        .context("listing the archive's binlogs across lineages")?;
    let mut by_lineage: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for key in &keys {
        if !key.contains("/binlog/") {
            continue;
        }
        let Some(uuid) = pitr::server_uuid_from_key(location, key) else {
            continue;
        };
        if let Some(name) = key.rsplit('/').next() {
            by_lineage.entry(uuid).or_default().push(name.to_string());
        }
    }

    let mut runs: Vec<LineageRun> = Vec::new();
    let own = by_lineage.remove(&full.server_uuid).unwrap_or_default();
    let own_plan = pitr::binlogs_to_replay(own, &full.meta.binlog_file);
    note_lineage_gap(&full.server_uuid, &own_plan);
    runs.push(LineageRun {
        server_uuid: full.server_uuid.clone(),
        files: own_plan.run,
        start_position: Some(full.meta.binlog_pos),
    });
    for (uuid, mut names) in by_lineage {
        names.sort_by(|a, b| pitr::binlog_name_cmp(a, b));
        let Some(first) = names.first().cloned() else {
            continue;
        };
        let plan = pitr::binlogs_to_replay(names, &first);
        note_lineage_gap(&uuid, &plan);
        if plan.run.is_empty() {
            continue;
        }
        runs.push(LineageRun {
            server_uuid: uuid,
            files: plan.run,
            start_position: None,
        });
    }
    info!(
        lineages = runs.len(),
        files = runs.iter().map(|r| r.files.len()).sum::<usize>(),
        "replaying the shared history across lineages"
    );

    let scratch = Path::new(&config.data_dir).join(SCRATCH_DIR);
    let mut achieved = full.meta.taken_at;
    let replay: Result<()> = async {
        for run in &runs {
            if run.files.is_empty() {
                continue;
            }
            // One directory per lineage, removed as soon as that lineage has
            // replayed, so the staged copy on the volume never exceeds one
            // lineage's worth of binlogs.
            let dir = scratch.join(format!("server-{}", run.server_uuid));
            std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
            let mut local_paths = Vec::new();
            for name in &run.files {
                let key = pitr::binlog_key(location, &run.server_uuid, name);
                let local = dir.join(name);
                s3.download_to_file(&key, &local)
                    .await
                    .with_context(|| format!("downloading {key}"))?;
                local_paths.push(local);
            }
            info!(
                lineage = %run.server_uuid,
                files = ?run.files,
                start_position = ?run.start_position,
                "replaying lineage"
            );
            let reached = replay_downloaded(&local_paths, run.start_position, target, config)
                .await
                .with_context(|| format!("replaying lineage {}", run.server_uuid))?;
            achieved = achieved.max(reached);
            let _ = std::fs::remove_dir_all(&dir);
        }
        Ok(())
    }
    .await;
    let _ = std::fs::remove_dir_all(&scratch);
    replay?;

    // Completeness, proven on the result. Under Group Replication every
    // group transaction takes the group's UUID and the next number, so a
    // hole in the restored gtid_executed is exactly a transaction no lineage
    // delivered — the shared-history form of the single-lineage gap check,
    // and the reason a per-lineage gap above was not fatal on its own.
    let executed = sql
        .executed_gtid_set()
        .await
        .context("reading gtid_executed from the restored server")?;
    let holes = pitr::gtid_set_holes(&executed);
    if !holes.is_empty() {
        let listed = holes
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        error!(
            holes = %listed,
            gtid_executed = %executed,
            "restored GTID history has holes: the archive is missing the binlogs that carried \
             these transactions on every lineage that held them — the requested point-in-time \
             target cannot be reached, and serving the result would silently lose them"
        );
        anyhow::bail!(
            "gtid history has holes: {listed} — the archive is missing at least one binlog \
             (expired, deleted, or lost before upload) on every lineage that carried these \
             transactions, so a restore to the requested target is impossible; pick an earlier \
             target, or restore from another full backup (other discovered full backups: {})",
            pitr::describe_fallback_fulls(fulls, full),
        );
    }
    if let Some(purged) = full.meta.gtid_purged.as_deref().filter(|p| !p.is_empty()) {
        // gtid_compare(mine, peer) -> (peer ⊆ mine, mine ⊆ peer).
        let (dump_within_result, _) = sql
            .gtid_compare(&executed, purged)
            .await
            .context("checking the dump's GTID set against the restored server")?;
        if !dump_within_result {
            anyhow::bail!(
                "the restored server lacks transactions the full backup declared it contains \
                 (dump GTID set {purged}, restored gtid_executed {executed}) — the dump did not \
                 load completely"
            );
        }
    }
    info!(
        gtid_executed = %executed,
        "restored GTID history is contiguous and contains the full backup's set"
    );

    verify_achieved_point(achieved, target, config, fulls, full)?;
    Ok(achieved)
}

/// The `mysqlbinlog | mysql` replay over the already-staged files, followed
/// by the local achieved-point pass over the last of them. Split out of
/// `replay_binlogs` so the caller can clean the scratch directory up on
/// every path.
async fn replay_downloaded(
    local_paths: &[PathBuf],
    start_position: Option<u64>,
    target: DateTime<Utc>,
    config: &Config,
) -> Result<DateTime<Utc>> {
    use tokio::io::AsyncWriteExt;

    let stop_dt = target.format("%Y-%m-%d %H:%M:%S").to_string();
    let mut mysqlbinlog_cmd = Command::new("mysqlbinlog");
    if let Some(pos) = start_position {
        mysqlbinlog_cmd.arg(format!("--start-position={pos}"));
    }
    mysqlbinlog_cmd
        .arg(format!("--stop-datetime={stop_dt}"))
        // mysqlbinlog interprets --stop-datetime in ITS local time zone, and
        // stop_dt above is the UTC target formatted without one: pin TZ so
        // the replay's cut-off and the achieved-point pass below (which pins
        // TZ the same way) agree with the UTC target on any container TZ.
        .env("TZ", "UTC");
    for path in local_paths {
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
    let binlog_status = mysqlbinlog
        .wait()
        .await
        .context("waiting for mysqlbinlog")?;
    let mysql_status = mysql.wait().await.context("waiting for mysql")?;

    relay_result
        .context("relay task panicked")?
        .context("streaming mysqlbinlog's output into mysql")?;
    if !binlog_status.success() {
        anyhow::bail!("mysqlbinlog exited with {binlog_status}");
    }
    if !mysql_status.success() {
        anyhow::bail!("mysql (replaying binlogs) exited with {mysql_status}");
    }

    // How far the archive's history actually extends: the last event of the
    // last replayed binlog. Capped at the target — events past the
    // --stop-datetime were deliberately not applied, so a tail that runs
    // beyond the target means the target itself was reached exactly.
    let last_local = local_paths
        .last()
        .expect("replay_downloaded is only called with a non-empty run");
    let last_event = last_binlog_event_time(last_local).await.with_context(|| {
        format!(
            "reading the achieved recovery point from {}",
            last_local.display()
        )
    })?;
    Ok(last_event.min(target))
}

/// The timestamp of the LAST event in a staged binlog file, via a local
/// `mysqlbinlog` pass (`--base64-output=decode-rows` suppresses the base64
/// event bodies, leaving the headers). Every event prints a
/// `#YYMMDD HH:MM:SS server id N ...` header — including the trailing
/// Rotate/Stop event mysqld writes when it closes the file — so the last
/// header marks the archive's coverage even when the tail carries no user
/// transactions. `SET TIMESTAMP` lines would be timezone-proof but only
/// query events emit them (an idle tail has none), so the headers are the
/// robust choice, with TZ pinned to UTC because mysqlbinlog formats them in
/// its own local zone (see pitr::parse_binlog_event_header_utc).
async fn last_binlog_event_time(path: &Path) -> Result<DateTime<Utc>> {
    use tokio::io::{AsyncBufReadExt, BufReader};

    let mut mysqlbinlog = Command::new("mysqlbinlog")
        .arg("--base64-output=decode-rows")
        .arg(path)
        .env("TZ", "UTC")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning mysqlbinlog (achieved recovery point pass)")?;
    let stdout = mysqlbinlog
        .stdout
        .take()
        .context("mysqlbinlog stdout was not piped")?;

    let mut last = None;
    let mut lines = BufReader::new(stdout).lines();
    while let Some(line) = lines
        .next_line()
        .await
        .context("reading mysqlbinlog output (achieved recovery point pass)")?
    {
        if let Some(ts) = pitr::parse_binlog_event_header_utc(&line) {
            last = Some(ts);
        }
    }

    let status = mysqlbinlog
        .wait()
        .await
        .context("waiting for mysqlbinlog (achieved recovery point pass)")?;
    if !status.success() {
        anyhow::bail!("mysqlbinlog (achieved recovery point pass) exited with {status}");
    }
    last.with_context(|| format!("no event header found in {}", path.display()))
}

/// The recovery-target check itself: the achieved point may trail the target
/// by at most the rotation-bounded window (`pitr::achieved_lag_bound_seconds`
/// over the archiver's own BINLOG_ROTATE_INTERVAL_SECONDS knob — reaching
/// the exact target is impossible, everything inside the last rotation
/// window still lives in the never-uploaded active binlog). Anything worse
/// fails exactly like the lineage-gap check: loudly, with the InProgress
/// marker left in place, naming what was asked, what was reached, and the
/// other discovered fulls as fallback options.
fn verify_achieved_point(
    achieved: DateTime<Utc>,
    target: DateTime<Utc>,
    config: &Config,
    fulls: &[FullBackupRef],
    full: &FullBackupRef,
) -> Result<()> {
    let bound = pitr::achieved_lag_bound_seconds(config.binlog_rotate_interval_seconds);
    if pitr::achieved_point_within_bound(target, achieved, bound) {
        info!(
            achieved = %pitr::format_rfc3339_millis(achieved),
            target = %pitr::format_rfc3339_millis(target),
            bound_seconds = bound,
            "achieved recovery point is within the rotation bound of the target"
        );
        return Ok(());
    }
    error!(
        achieved = %pitr::format_rfc3339_millis(achieved),
        target = %pitr::format_rfc3339_millis(target),
        bound_seconds = bound,
        "the archive ends short of the requested point-in-time target: replay ran out of \
         binlogs well before the target instant — reporting success would silently serve a \
         database missing everything in between"
    );
    anyhow::bail!(
        "recovery target not reached: requested {} but the selected full backup's archive \
         only reaches {} (more than the allowed {}s rotation-bounded lag behind the target) \
         — the binlogs covering the rest were never shipped (archiver stopped, or the target \
         lies inside/beyond the never-uploaded active binlog); pick a target at or before \
         the achieved point, or restore from another full backup \
         (other discovered full backups: {})",
        pitr::format_rfc3339_millis(target),
        pitr::format_rfc3339_millis(achieved),
        bound,
        pitr::describe_fallback_fulls(fulls, full),
    )
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
        write_restore_marker(&dir, RestoreStatus::InProgress, t(), None).unwrap();
        assert!(crashed_mid_restore(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn completed_marker_is_not_a_crash_and_records_the_achieved_point() {
        let dir = temp_dir("completed");
        write_restore_marker(&dir, RestoreStatus::InProgress, t(), None).unwrap();
        let achieved = pitr::parse_target_time("2026-08-13T13:59:10.000Z").unwrap();
        write_restore_marker(&dir, RestoreStatus::Completed, t(), Some(achieved)).unwrap();
        assert!(!crashed_mid_restore(&dir));
        let marker = read_restore_marker(&dir).unwrap();
        assert_eq!(marker.status, RestoreStatus::Completed);
        assert_eq!(marker.target_time, "2026-08-13T14:00:00.000Z");
        assert_eq!(
            marker.achieved_time.as_deref(),
            Some("2026-08-13T13:59:10.000Z")
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn marker_write_publishes_atomically() {
        let dir = temp_dir("atomic");
        write_restore_marker(&dir, RestoreStatus::InProgress, t(), None).unwrap();
        // The tmp staging file must never survive a successful publish — a
        // stray one would mean the rename pattern regressed to two files.
        assert!(!Path::new(&dir)
            .join(format!("{RESTORE_STATE_FILE}.tmp"))
            .exists());
        assert!(crashed_mid_restore(&dir));
        // A pre-achieved-time marker (no achieved_time field) still parses.
        std::fs::write(
            restore_marker_path(&dir),
            r#"{"status":"completed","target_time":"2026-08-13T14:00:00.000Z","updated_at":"2026-08-13T14:05:00.000Z"}"#,
        )
        .unwrap();
        let marker = read_restore_marker(&dir).unwrap();
        assert_eq!(marker.status, RestoreStatus::Completed);
        assert_eq!(marker.achieved_time, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reset_partial_restore_wipes_everything_but_the_runtime_lock() {
        let dir = temp_dir("reset");
        write_restore_marker(&dir, RestoreStatus::InProgress, t(), None).unwrap();
        std::fs::create_dir_all(Path::new(&dir).join("mysql")).unwrap();
        std::fs::write(Path::new(&dir).join("mysql").join("ibdata1"), "junk").unwrap();
        std::fs::write(Path::new(&dir).join("binlog.000001"), "junk").unwrap();
        let lock = Path::new(&dir).join(crate::volume_lock::RUNTIME_LOCK_FILE);
        std::fs::write(&lock, "held-by-this-boot").unwrap();

        reset_partial_restore(&dir).unwrap();

        assert!(!crashed_mid_restore(&dir), "marker must be gone");
        assert!(
            !Path::new(&dir).join("mysql").exists(),
            "partial datadir must be gone"
        );
        assert!(!Path::new(&dir).join("binlog.000001").exists());
        assert_eq!(
            std::fs::read_to_string(&lock).unwrap(),
            "held-by-this-boot",
            "the held runtime lock must survive the sweep"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn garbage_marker_file_fails_closed_as_crashed_mid_restore() {
        // A marker that EXISTS but doesn't parse is a torn write on the one
        // file recording whether a restore completed — never a fresh volume.
        // Degrading it to "absent" (the old behavior) booted a vanilla
        // mysqld straight onto a half-restored datadir; it must read as a
        // crash so main.rs's wipe-and-retry/refuse logic runs instead.
        let dir = temp_dir("garbage");
        std::fs::write(restore_marker_path(&dir), "not json").unwrap();
        assert!(read_restore_marker(&dir).is_none());
        assert!(crashed_mid_restore(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_marker_file_fails_closed_as_crashed_mid_restore() {
        // The classic torn-write shape: the file was created but nothing
        // (durable) ever landed in it. Same fail-closed posture as garbage.
        let dir = temp_dir("empty-marker");
        std::fs::write(restore_marker_path(&dir), "").unwrap();
        assert!(read_restore_marker(&dir).is_none());
        assert!(crashed_mid_restore(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }
}
