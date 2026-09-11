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
//!        still present past it FAILS the restore loudly for any target the
//!        replayed run does not provably cover (see replay_binlogs and
//!        gap_blocks_target): the last replayed event must be at or past the
//!        target, with no rotation tolerance — the missing file, unlike a
//!        not-yet-shipped active binlog, is never coming. A target at or
//!        before that event is served exactly; the hole is irrelevant to it.
//!      - Shared history (GTIDs): the full's lineage from its coordinate,
//!        then EVERY other lineage from its first archived file (see
//!        replay_shared_history); a lineage's files past a sequence gap
//!        replay in a later round, after every lineage's gap-free run. The
//!        restore-phase mysqld runs with GTIDs on
//!        (shared_history_restore_args) so the dump's GTID set loads and
//!        the server skips each GTID it already holds, whichever lineage
//!        delivers it — that is what makes a failed-over primary's
//!        never-uploaded tail recoverable from the next primary's lineage.
//!        Completeness is then proven on the result, not assumed from the
//!        file names: every replayed binlog opens with the set of
//!        transactions its server had executed when the file was created
//!        (its Previous_gtids event), and each such set — for every file
//!        opened before the target — must be contained in the restored
//!        `gtid_executed`, as must the dump's own set; a transaction some
//!        file vouches for that no lineage delivered FAILS the restore
//!        loudly. The files' own testimony is exact whatever the group's
//!        GTID assignment block size: a group that hands each member a
//!        block of a million numbers (the default, and every group formed
//!        before this image pinned it to 1) leaves block-sized jumps in
//!        its sequence that are not lost transactions, which is why the
//!        set's intervals themselves are not the signal. That is also why
//!        a gap is not fatal on its own, and why the files past it must
//!        still replay: the file after the gap is the one that vouches
//!        for what the gap held.
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
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tracing::{error, info, warn};

const RESTORE_STATE_FILE: &str = ".pitr_restore_state.json";
const SCRATCH_DIR: &str = ".pitr_restore_binlogs";

/// Extra mysqld flags for the restore-phase server when the archive is one
/// shared GTID history (see the module doc). Replaying another lineage
/// relies on the server skipping GTIDs it already holds, so the phase runs
/// with GTIDs on; enforce_gtid_consistency=ON is what any gtid_mode above
/// OFF_PERMISSIVE requires.
///
/// `ON_PERMISSIVE`, never `ON`: a shared archive can hold anonymous
/// transactions next to GTID ones, and only ON_PERMISSIVE replays both in
/// one pass. A standalone converted to HA has anonymous binlogs before its
/// GTID ones begin; a member reverted to standalone by an image that did not
/// keep GTIDs on for it (see mysql_conf.rs) wrote anonymous binlogs AFTER a
/// GTID full. Under `ON` the second shape died half-way (`ERROR 1782:
/// GTID_NEXT cannot be set to ANONYMOUS when GTID_MODE = ON`, 2026-09-11).
/// ON_PERMISSIVE still loads the dump's `GTID_PURGED` (only `OFF` refuses
/// that) and still skips every GTID the server already holds.
///
/// Restore-phase only: the serving mysqld main.rs starts afterwards is
/// spawned with the service's own args.
fn shared_history_restore_args() -> [String; 2] {
    [
        "--gtid-mode=ON_PERMISSIVE".to_string(),
        "--enforce-gtid-consistency=ON".to_string(),
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoreStatus {
    InProgress,
    Completed,
    /// The archive cannot serve the target (a gap, a target past what was
    /// shipped, no qualifying full, an unsafe bind). Deterministic for the
    /// archive as it was; the next boot retries against the archive as it is.
    Refused,
    /// The attempt broke for a reason that is not the archive's (transport,
    /// a tool exiting non-zero). Retried the same way.
    Failed,
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
    /// Why the last attempt ended `Refused`/`Failed`, verbatim from the error
    /// chain — so a later boot can say what happened before it retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// How many restore attempts this volume has seen, counting this one. The
    /// marker survives `reset_partial_restore` precisely so this survives the
    /// wipe: it is what bounds the wipe-and-retry loop (see
    /// `MAX_RESTORE_ATTEMPTS`). Older markers carry none and read as 0.
    #[serde(default)]
    pub attempts: u32,
    pub updated_at: String,
}

/// How many times a volume's restore is attempted before the wrapper stops
/// retrying. Every deterministic refusal or failure used to become an
/// unbounded loop paced only by the restart policy — each pass streaming the
/// dump and downloading the binlog run again — until someone deleted the
/// fork. Three attempts cover a transient (a bucket blip, a client dropping
/// mid-load) without turning a refused restore into a standing egress bill;
/// the workflow that started the restore fails on the FIRST verdict anyway
/// (mono #38619), so nothing waits on the later ones.
pub const MAX_RESTORE_ATTEMPTS: u32 = 3;

/// How many attempts the marker on this volume records so far (0 when there
/// is no readable marker).
pub fn recorded_attempts(data_dir: &str) -> u32 {
    match read_marker_file(data_dir) {
        MarkerFile::Present(m) => m.attempts,
        _ => 0,
    }
}

/// A restore the archive cannot serve, as opposed to one that broke.
///
/// Every deterministic refusal in this module is one of these, keyed by a
/// stable `kind` the platform can act on without parsing prose: the verdict
/// line carries it, the marker keeps it, and backboard's pre-flight names the
/// same kinds when it refuses a target up front.
#[derive(Debug)]
pub struct RestoreRefusal {
    pub kind: &'static str,
    pub reason: String,
}

impl std::fmt::Display for RestoreRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for RestoreRefusal {}

fn refusal(kind: &'static str, reason: String) -> anyhow::Error {
    anyhow::Error::new(RestoreRefusal { kind, reason })
}

/// (verdict, kind) for the terminal log line and the marker. A refusal keeps
/// its kind through any `.context(..)` layers on the way out.
pub fn classify_restore_error(e: &anyhow::Error) -> (&'static str, &'static str) {
    match e.downcast_ref::<RestoreRefusal>() {
        Some(r) => ("refused", r.kind),
        None => ("failed", "error"),
    }
}

/// What the previous attempt on this volume ended as, for the boot log.
pub fn previous_attempt(data_dir: &str) -> Option<(RestoreStatus, Option<String>)> {
    match read_marker_file(data_dir) {
        MarkerFile::Present(m) => Some((m.status, m.reason)),
        _ => None,
    }
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
        MarkerFile::Present(marker) => matches!(
            marker.status,
            RestoreStatus::InProgress | RestoreStatus::Refused | RestoreStatus::Failed
        ),
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
    let keep_lock = std::ffi::OsStr::new(crate::volume_lock::RUNTIME_LOCK_FILE);
    // The marker stays too: it is the attempt counter that bounds this very
    // retry (MAX_RESTORE_ATTEMPTS), and a retry that forgot how many times it
    // had already run could never stop. The next attempt overwrites it with
    // InProgress on its first write, so a kept Refused/Failed marker never
    // outlives the wipe by more than the moment before that write.
    let keep_marker = std::ffi::OsStr::new(RESTORE_STATE_FILE);
    for entry in std::fs::read_dir(data_dir)
        .with_context(|| format!("listing {data_dir} to reset a partial restore"))?
    {
        let entry = entry.with_context(|| format!("listing {data_dir}"))?;
        if entry.file_name() == keep_lock || entry.file_name() == keep_marker {
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
    reason: Option<&str>,
) -> Result<()> {
    use std::io::Write;

    // The attempt count carries over from whatever marker is already there:
    // an InProgress write at the start of an attempt bumps it; the terminal
    // write of the same attempt keeps it.
    let previous = match read_marker_file(data_dir) {
        MarkerFile::Present(m) => m.attempts,
        _ => 0,
    };
    let attempts = if matches!(status, RestoreStatus::InProgress) {
        previous + 1
    } else {
        previous.max(1)
    };
    let marker = RestoreMarker {
        status,
        target_time: pitr::format_rfc3339_millis(target),
        achieved_time: achieved.map(pitr::format_rfc3339_millis),
        reason: reason.map(str::to_string),
        attempts,
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
/// `run`, plus the one line the platform reads. Every attempt ends with a
/// `point-in-time restore verdict` record — `verdict` completed/refused/failed,
/// `kind` for refusals, `reason`, `elapsed_seconds` — and a refused or failed
/// attempt leaves its reason in the marker for the next boot to repeat. The
/// fork's deployment turns healthy the moment its container is up, so this
/// line is what tells anyone outside the container how the restore went.
/// MySQL's hard maximum for max_allowed_packet (1 GiB). See the restore-phase
/// argv: a bulk statement's row events replay as one BINLOG literal.
const MAX_ALLOWED_PACKET: u64 = 1024 * 1024 * 1024;

/// The last few KiB a child wrote to stderr, for the error it died with — the
/// `mysql` client says WHY it quit ("Got a packet bigger than
/// 'max_allowed_packet' bytes") where the relay only sees a broken pipe.
async fn stderr_tail(stderr: Option<tokio::process::ChildStderr>) -> String {
    use tokio::io::AsyncReadExt;
    let Some(mut stderr) = stderr else {
        return String::new();
    };
    let mut buf = Vec::new();
    let _ = stderr.read_to_end(&mut buf).await;
    let start = buf.len().saturating_sub(4096);
    String::from_utf8_lossy(&buf[start..]).trim().to_string()
}

fn with_said(what: &str, said: &str) -> String {
    if said.is_empty() {
        what.to_string()
    } else {
        format!("{what}: {said}")
    }
}

pub async fn run_reporting(config: &Config) -> Result<()> {
    let started = std::time::Instant::now();
    match run(config, started).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let (verdict, kind) = classify_restore_error(&e);
            let reason = format!("{e:#}");
            let target = config.recovery_target_time();
            error!(
                verdict,
                kind,
                reason = %reason,
                target = ?target.map(pitr::format_rfc3339_millis),
                elapsed_seconds = started.elapsed().as_secs(),
                "point-in-time restore verdict"
            );
            if let Some(target) = target {
                let status = if verdict == "refused" {
                    RestoreStatus::Refused
                } else {
                    RestoreStatus::Failed
                };
                if let Err(m) =
                    write_restore_marker(&config.data_dir, status, target, None, Some(&reason))
                {
                    warn!(error = %m, "could not record the restore verdict in the marker");
                }
            }
            Err(e)
        }
    }
}

pub async fn run(config: &Config, started: std::time::Instant) -> Result<()> {
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
            refusal(
                "no-full",
                format!(
                    "no full backup found at or before target time {} under the configured bucket/path",
                    pitr::format_rfc3339_millis(target)
                ),
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
    //
    // `--bind-address=127.0.0.1` is not an optimisation: it is what makes the
    // restore phase unobservable. Everything this phase talks to mysqld with
    // goes over the unix socket — the control connection below, the `mysql`
    // client that loads the dump, and the one that replays the binlogs — so
    // the restore never needs a reachable port, while a port that IS reachable
    // serves a freshly-initialised, EMPTY database to anyone who dials it.
    // On a PITR fork that window is reachable: the platform marks the
    // deployment healthy as soon as the container is up, and a client
    // connecting then gets an empty database that looks like a completed
    // restore.
    //
    // Two earlier attempts at this are worth not repeating.
    //
    // It began as `SET GLOBAL skip_networking = ON`, issued after the server
    // was already accepting connections. That never worked: `skip_networking`
    // is READ-ONLY at runtime on the 8.4 series this image bundles, so the
    // statement errored on every restore, the caller only warned, and the port
    // stayed open for the whole restore.
    //
    // Passing `--skip-networking` on argv instead does close the port — and
    // deadlocks the restore. `skip_networking` is this wrapper's IDENTITY
    // MARKER for docker-entrypoint's init-phase temp server: `is_init_temp_server`
    // reads exactly that variable, and restore, archiver, gr and self_heal all
    // wait for it to turn false before touching the server. A restore-phase
    // server carrying the flag is indistinguishable from the temp instance
    // forever, so `wait_for_ready_or_exit` never returns.
    //
    // Binding to loopback keeps that marker untouched and still leaves nothing
    // for an outside client to reach: the platform dials the container's
    // address, not its loopback.
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    args.push("--bind-address=127.0.0.1".to_string());
    // `bind_address` governs the classic port only. The X Plugin listens on
    // its own port (33060) under `mysqlx_bind_address`, which stays `*`, so
    // without this the half-loaded database the loopback bind hides on 3306
    // is served to anyone dialing 33060 during the restore. The serving
    // mysqld that boots on the finished datadir runs with the image's normal
    // settings, X Plugin included.
    args.push("--mysqlx=OFF".to_string());
    // The restore-phase server is disposable: a crash mid-load is wiped and
    // retried from scratch (see crashed_mid_restore), so the durability knobs
    // that make a serving server safe only make this one slow. No redo fsync
    // per commit, no doublewrite, and no binlog of a load whose history the
    // archive already holds — the serving mysqld that boots on the finished
    // datadir runs with the image's normal settings.
    args.push("--innodb-flush-log-at-trx-commit=0".to_string());
    args.push("--sync-binlog=0".to_string());
    args.push("--innodb-doublewrite=OFF".to_string());
    args.push("--skip-log-bin".to_string());
    // mysqlbinlog flushes a statement's row events as ONE `BINLOG '…'`
    // literal (at STMT_END_F), so a bulk INSERT of N MiB arrives as a single
    // ~1.33N MiB packet. The 64 MiB server default and the 16 MiB client
    // default refused a 64 MiB load statement on 2026-09-09 (prod, 5 GB): the
    // client quit, the relay saw a broken pipe, the restore looped forever.
    // 1 GiB is MySQL's own cap and what its replica applier allows
    // (replica_max_allowed_packet); the clients below ask for the same.
    args.push(format!("--max-allowed-packet={MAX_ALLOWED_PACKET}"));
    if shared_history {
        let gtid_args = shared_history_restore_args();
        info!(
            gtid_args = ?gtid_args,
            "the archive is one shared GTID history (Group Replication); the restore-phase \
             mysqld runs with GTIDs on and every lineage is replayed"
        );
        args.extend(gtid_args);
    }
    let mut child = process_manager::spawn_mysqld(&args)
        .await
        .context("spawning the restore-phase mysqld")?;

    write_marker_once_datadir_exists(&data_dir, &mut child, target).await?;

    let sql = Sql::connect_root_over_socket(&config.socket_path, &config.mysql_root_password);
    wait_for_ready_or_exit(&mut child, &sql).await?;
    info!("restore-phase mysqld is ready");

    // Belt and braces behind the argv flag above: an older docker-entrypoint
    // that drops unknown server options, or a my.cnf that binds a wider
    // address, would otherwise leave the port reachable silently. Verified,
    // not assumed — if the server is reachable at this point the restore
    // refuses rather than serving an empty database to whoever dials it.
    ensure_bound_to_loopback(&sql).await?;

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
    // The dump carried the source's accounts as of the target — root's
    // password included. This service's wrapper, health server and connection
    // URL hold MYSQL_ROOT_PASSWORD for THIS service; once the serving mysqld
    // reloads the grant tables, the source's password as it was at the target
    // would be the one enforced, and a password rotated after the target, or
    // minted for the fork, would lock the wrapper out of its own server for
    // good (/health 503, every candidate denied). Reconcile root to the
    // environment while the restore-phase server is still ours.
    sql.set_root_password_everywhere(&config.mysql_root_password)
        .await
        .context("reconciling root's password to MYSQL_ROOT_PASSWORD after the restore")?;
    info!("root's password reconciled to MYSQL_ROOT_PASSWORD on every restored root account");

    let _ = sql.shutdown_server().await;
    match child.wait().await {
        Ok(status) if !status.success() => {
            warn!(?status, "restore-phase mysqld exited non-zero on shutdown (harmless — the data was already loaded)");
        }
        Ok(_) => {}
        Err(e) => warn!(error = %e, "error waiting for the restore-phase mysqld to exit"),
    }

    write_restore_marker(
        &data_dir,
        RestoreStatus::Completed,
        target,
        Some(achieved),
        None,
    )?;
    info!(
        achieved = %pitr::format_rfc3339_millis(achieved),
        "point-in-time restore completed; the normal boot flow starts mysqld in serving mode next"
    );
    info!(
        verdict = "completed",
        kind = "ok",
        target = %pitr::format_rfc3339_millis(target),
        achieved = %pitr::format_rfc3339_millis(achieved),
        elapsed_seconds = started.elapsed().as_secs(),
        "point-in-time restore verdict"
    );
    Ok(())
}

/// Refuse to continue a restore on a server anything outside the container
/// could reach.
///
/// The restore phase loads a full backup into a database that, until replay
/// finishes, holds only part of the customer's data — and starts out holding
/// none of it at all. Serving that to a client is the one outcome a restore
/// must never produce, because it is indistinguishable from a completed
/// restore of an empty database.
///
/// `--bind-address=127.0.0.1` on the spawn argv is what prevents it; this
/// verifies the server agrees. Failing the restore here is safe and
/// recoverable: the in-progress marker is already on the volume, so the next
/// boot wipes the partial datadir and retries (see `crashed_mid_restore`).
async fn ensure_bound_to_loopback(sql: &Sql) -> Result<()> {
    match sql.bind_address().await {
        Ok(Some(addr)) if is_loopback_bind(&addr) => Ok(()),
        Ok(addr) => {
            error!(
                bind_address = ?addr,
                "the restore-phase mysqld is reachable from outside the container; refusing \
                 to continue, because a client reaching it before replay finishes would be \
                 served a database missing some or all of its data and could not tell that \
                 apart from a completed restore"
            );
            return Err(refusal(
                "unsafe-bind",
                format!(
                    "restore-phase mysqld is bound to {:?}, not loopback \
                 (--bind-address did not take effect); refusing to load a partial \
                 database that clients could read",
                    addr
                ),
            ));
        }
        Err(e) => {
            // Reading the variable is not the guarantee — the argv flag is.
            // A transport hiccup on this one query must not fail an otherwise
            // healthy restore, so it degrades to a warning.
            warn!(
                error = %e,
                "could not confirm the restore-phase mysqld's bind address; continuing on \
                 the strength of the --bind-address spawn flag"
            );
            Ok(())
        }
    }
}

/// Whether a `bind_address` value keeps the server inside the container.
///
/// MySQL renders the variable back as it was given, so the comparison is on
/// the literal forms the flag can take rather than a parsed address — and a
/// value listing several addresses (8.0.13+ accepts a comma-separated list)
/// only counts when EVERY one of them is loopback.
pub(crate) fn is_loopback_bind(value: &str) -> bool {
    let entries: Vec<&str> = value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    !entries.is_empty()
        && entries
            .iter()
            .all(|e| matches!(*e, "127.0.0.1" | "::1" | "localhost"))
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
                    write_restore_marker(data_dir, RestoreStatus::InProgress, target, None, None)?;
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
        .arg(format!("--max-allowed-packet={MAX_ALLOWED_PACKET}"))
        .env("MYSQL_PWD", &config.mysql_root_password)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning mysql")?;
    let mysql_said = tokio::spawn(stderr_tail(mysql.stderr.take()));

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
    let gunzip_status = gunzip.wait().await.context("waiting for gunzip")?;
    let mysql_status = mysql.wait().await.context("waiting for mysql")?;
    let mysql_said = mysql_said.await.unwrap_or_default();
    // The client's own words first: a quit on error is the cause the relay's
    // broken pipe only reflects.
    if !mysql_status.success() {
        anyhow::bail!(
            "{}",
            with_said(
                &format!("mysql (loading the full backup) exited with {mysql_status}"),
                &mysql_said
            )
        );
    }
    in_result
        .context("relay task panicked")?
        .context("streaming the dump from S3 into gunzip")?;
    out_result
        .context("relay task panicked")?
        .with_context(|| with_said("streaming gunzip's output into mysql", &mysql_said))?;
    if !gunzip_status.success() {
        anyhow::bail!("gunzip exited with {gunzip_status}");
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
    let gap = plan.gap.clone();
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
        if let Some(gap) = &gap {
            // A hole right after the coordinate file: the dump alone reaches
            // its own instant, and nothing later is reachable.
            if gap_blocks_target(achieved, target) {
                return Err(gap_refusal(gap, fulls, full));
            }
        }
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
    let replayed = result?;
    if let Some(gap) = &gap {
        // A hole with files past it is fatal ONLY for a target past the hole.
        // Everything up to the last replayed event is present, so a target at
        // or before it is served exactly; past it, the missing file may hold
        // events before the target, and the rotation tolerance that forgives
        // a not-yet-shipped active binlog must not forgive a lost one.
        if gap_blocks_target(replayed.last_event, target) {
            return Err(gap_refusal(gap, fulls, full));
        }
        info!(
            after = %gap.after,
            next_present = %gap.next_present,
            last_event = %pitr::format_rfc3339_millis(replayed.last_event),
            target = %pitr::format_rfc3339_millis(target),
            "the lineage has a sequence gap past this target: every event up to the target \
             replayed from the files before the hole, so the hole is irrelevant here"
        );
    }
    verify_achieved_point(replayed.achieved, target, config, fulls, full)?;
    Ok(replayed.achieved)
}

/// With a hole in the lineage, only a target the replayed run provably covers
/// is served: the last replayed event must be at or past the target. Anything
/// later might have lived in the missing file — and the missing file, unlike
/// a not-yet-shipped active binlog, is never coming.
fn gap_blocks_target(last_event: DateTime<Utc>, target: DateTime<Utc>) -> bool {
    last_event < target
}

fn gap_refusal(
    gap: &pitr::BinlogGap,
    fulls: &[FullBackupRef],
    full: &FullBackupRef,
) -> anyhow::Error {
    let after = if gap.after.is_empty() {
        full.meta.binlog_file.as_str()
    } else {
        gap.after.as_str()
    };
    error!(
        after = %gap.after,
        next_present = %gap.next_present,
        start_file = %full.meta.binlog_file,
        "binlog lineage has a gap: a binlog is missing from the archive while later \
         binlogs exist past it — the requested point-in-time target lies past the hole \
         and cannot be reached; replaying short of it would silently lose the data after \
         the gap"
    );
    refusal(
        "binlog-gap",
        format!(
            "binlog lineage gap: no binlog follows {after:?} but {:?} exists past the hole — \
             the archive is missing at least one binlog (expired, deleted, or lost before \
             upload), so a restore to the requested target is impossible; pick a target \
             at or before the last event of {after:?}, or restore from another full backup \
             (other discovered full backups: {})",
            gap.next_present,
            pitr::describe_fallback_fulls(fulls, full),
        ),
    )
}

/// One gap-free run of one lineage's binlogs in a shared-history replay.
struct LineageRun {
    server_uuid: String,
    /// Gap-free, in order.
    files: Vec<String>,
    /// `--start-position` for the first file — only the selected full's own
    /// gap-free run from its start has one (its recorded dump coordinate);
    /// every other run replays from the start of its first file and lets the
    /// server skip what is already executed.
    start_position: Option<u64>,
    /// Replay order across lineages: 0 for a lineage's gap-free run from its
    /// start, one more for each hole a run sits past. Every round-0 run
    /// replays before any round-1 run, so a lineage that carried the missing
    /// transactions (the next primary's, after a failover) delivers them
    /// before a stream that skips them replays what came after — the
    /// group's own commit order, as far as the archive allows.
    round: usize,
}

/// One replayed binlog's testimony: the transactions its server had executed
/// when the file was opened (its Previous_gtids), all of which committed
/// before the target when the file was opened before the target's second.
struct BinlogWitness {
    server_uuid: String,
    file: String,
    previous_gtids: String,
}

/// The head of a staged binlog (creation time and Previous_gtids), read from
/// its first bytes — the two events sit at the very start of the file.
fn read_binlog_head(path: &Path) -> Result<pitr::BinlogHead> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(pitr::BINLOG_HEAD_READ_BYTES as u64)
        .read_to_end(&mut bytes)
        .with_context(|| format!("reading the head of {}", path.display()))?;
    pitr::parse_binlog_head(&bytes)
}

fn note_lineage_gap(server_uuid: &str, plan: &pitr::BinlogReplayPlan) {
    if let Some(gap) = &plan.gap {
        warn!(
            lineage = %server_uuid,
            after = %gap.after,
            next_present = %gap.next_present,
            "lineage has a sequence gap; its files past the hole replay after every lineage's \
             gap-free run — another lineage may carry the missing transactions, and the GTID \
             completeness check after replay decides"
        );
    }
}

/// Queue a lineage's files past its gap (when it has one) as later-round
/// runs, one round per hole they sit past. Skipping them instead would hide
/// the hole: the replayed set would end short but contiguous, and the
/// completeness check faults holes, not endings.
fn push_runs_past_gap(
    runs: &mut Vec<LineageRun>,
    server_uuid: &str,
    files: Vec<String>,
    gap: Option<&pitr::BinlogGap>,
) {
    let Some(gap) = gap else {
        return;
    };
    for (i, run) in pitr::binlog_runs_past_gap(files, &gap.next_present)
        .into_iter()
        .enumerate()
    {
        runs.push(LineageRun {
            server_uuid: server_uuid.to_string(),
            files: run,
            start_position: None,
            round: i + 1,
        });
    }
}

/// The shared-history replay (module doc, step 5): the selected full's own
/// lineage from its recorded coordinate, then every other lineage's gap-free
/// run from its first archived file, then — in later rounds — every run that
/// sits past a sequence gap, each cut at `--stop-datetime`. Every lineage is
/// an in-order stream of the same group history and the server applies each
/// GTID once, whichever stream delivers it first, so the full's lineage goes
/// first only because it continues the dump exactly, and the post-gap runs
/// go last so a stream that has the missing transactions (the next
/// primary's, after a failover) delivers them before anything that came
/// after them replays. A sequence gap inside one lineage is therefore not
/// fatal here — it is logged and the verdict left to the completeness check
/// at the end: the restored server's `gtid_executed` must contain every
/// transaction a replayed binlog's Previous_gtids vouches for (each file
/// opened before the target names what its server had executed by then),
/// and everything the dump declared. Either failing is the loud, fail-closed
/// refusal: the archive lost a transaction on every lineage that held it.
/// The files past a gap MUST replay for that check to mean anything: the
/// file after the hole is exactly the witness to what the hole carried, and
/// a lineage cut at its hole left a merely short history that a target
/// within the rotation bound of that end accepted with the rows past the
/// hole silently gone (the shape a group's binlog expiry leaves behind a
/// stuck archiver).
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
    let own_plan = pitr::binlogs_to_replay(own.clone(), &full.meta.binlog_file);
    note_lineage_gap(&full.server_uuid, &own_plan);
    runs.push(LineageRun {
        server_uuid: full.server_uuid.clone(),
        files: own_plan.run,
        start_position: Some(full.meta.binlog_pos),
        round: 0,
    });
    push_runs_past_gap(&mut runs, &full.server_uuid, own, own_plan.gap.as_ref());
    for (uuid, mut names) in by_lineage {
        names.sort_by(|a, b| pitr::binlog_name_cmp(a, b));
        let Some(first) = names.first().cloned() else {
            continue;
        };
        let plan = pitr::binlogs_to_replay(names.clone(), &first);
        note_lineage_gap(&uuid, &plan);
        if !plan.run.is_empty() {
            runs.push(LineageRun {
                server_uuid: uuid.clone(),
                files: plan.run,
                start_position: None,
                round: 0,
            });
        }
        push_runs_past_gap(&mut runs, &uuid, names, plan.gap.as_ref());
    }
    // Stable: within a round the full's own lineage stays first.
    runs.sort_by_key(|r| r.round);
    info!(
        lineages = runs
            .iter()
            .map(|r| r.server_uuid.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        files = runs.iter().map(|r| r.files.len()).sum::<usize>(),
        runs_past_gaps = runs.iter().filter(|r| r.round > 0).count(),
        "replaying the shared history across lineages"
    );

    let scratch = Path::new(&config.data_dir).join(SCRATCH_DIR);
    let mut achieved = full.meta.taken_at;
    // Every replayed file's testimony about the history before it, taken
    // while the file is on disk; judged against the restored set after the
    // replay (see the completeness check below).
    let mut witnesses: Vec<BinlogWitness> = Vec::new();
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
                let head = read_binlog_head(&local)
                    .with_context(|| format!("reading the head of {key}"))?;
                if pitr::binlog_opened_before_cutoff(head.created_at, target)
                    && !head.previous_gtids.is_empty()
                {
                    witnesses.push(BinlogWitness {
                        server_uuid: run.server_uuid.clone(),
                        file: name.clone(),
                        previous_gtids: head.previous_gtids,
                    });
                }
                local_paths.push(local);
            }
            info!(
                lineage = %run.server_uuid,
                files = ?run.files,
                start_position = ?run.start_position,
                round = run.round,
                "replaying lineage"
            );
            let reached = replay_downloaded(&local_paths, run.start_position, target, config)
                .await
                .map(|r| r.achieved)
                .with_context(|| {
                    if run.round > 0 {
                        format!(
                            "replaying lineage {} past its sequence gap (round {}) — a \
                             transaction there may depend on one the archive lost",
                            run.server_uuid, run.round
                        )
                    } else {
                        format!("replaying lineage {}", run.server_uuid)
                    }
                })?;
            achieved = achieved.max(reached);
            let _ = std::fs::remove_dir_all(&dir);
        }
        Ok(())
    }
    .await;
    let _ = std::fs::remove_dir_all(&scratch);
    replay?;

    // Completeness, proven on the result. Every replayed binlog opened with
    // the set of transactions its server had executed by then; a file opened
    // before the target therefore vouches for transactions that all
    // committed before the target, and every one of them must be in the
    // restored gtid_executed — whichever lineage delivered it. One that is
    // not is a transaction the archive lost on every lineage that held it:
    // the shared-history form of the single-lineage gap check, and the reason
    // a per-lineage gap above was not fatal on its own. The set's own
    // intervals are deliberately NOT the signal: a group whose GTID
    // assignment block size is above 1 leaves block-sized jumps between
    // members' numbers that are not lost transactions.
    let executed = sql
        .executed_gtid_set()
        .await
        .context("reading gtid_executed from the restored server")?;
    let mut holes: Vec<String> = Vec::new();
    for witness in &witnesses {
        let lost = sql
            .gtid_subtract(&witness.previous_gtids, &executed)
            .await
            .with_context(|| {
                format!(
                    "checking the transactions {}/{} vouches for against the restored server",
                    witness.server_uuid, witness.file
                )
            })?;
        if !lost.is_empty() {
            holes.push(format!(
                "{lost} (executed before {}/{} was opened)",
                witness.server_uuid, witness.file
            ));
        }
    }
    if !holes.is_empty() {
        let listed = holes.join("; ");
        error!(
            holes = %listed,
            gtid_executed = %executed,
            "restored GTID history has holes: the archive is missing the binlogs that carried \
             these transactions on every lineage that held them — the requested point-in-time \
             target cannot be reached, and serving the result would silently lose them"
        );
        return Err(refusal(
            "gtid-hole",
            format!(
                "gtid history has holes: {listed} — the archive is missing at least one binlog \
             (expired, deleted, or lost before upload) on every lineage that carried these \
             transactions, so a restore to the requested target is impossible; pick an earlier \
             target, or restore from another full backup (other discovered full backups: {})",
                pitr::describe_fallback_fulls(fulls, full),
            ),
        ));
    }
    if let Some(purged) = full.meta.gtid_purged.as_deref().filter(|p| !p.is_empty()) {
        // gtid_compare(mine, peer) -> (peer ⊆ mine, mine ⊆ peer).
        let (dump_within_result, _) = sql
            .gtid_compare(&executed, purged)
            .await
            .context("checking the dump's GTID set against the restored server")?;
        if !dump_within_result {
            return Err(refusal(
                "dump-incomplete",
                format!(
                    "the restored server lacks transactions the full backup declared it contains \
                 (dump GTID set {purged}, restored gtid_executed {executed}) — the dump did not \
                 load completely"
                ),
            ));
        }
    }
    info!(
        gtid_executed = %executed,
        witnesses = witnesses.len(),
        "restored GTID history holds every transaction the replayed binlogs vouch for and the \
         full backup's set"
    );

    verify_achieved_point(achieved, target, config, fulls, full)?;
    Ok(achieved)
}

/// The `mysqlbinlog | mysql` replay over the already-staged files, followed
/// by the local achieved-point pass over the last of them. Split out of
/// `replay_binlogs` so the caller can clean the scratch directory up on
/// every path.
/// What a replay reached: `achieved` is the recovery point (the last event,
/// capped at the target — events past `--stop-datetime` were deliberately not
/// applied); `last_event` is the raw timestamp of the last event in the last
/// file, uncapped, which is what the gap rule compares against the target.
struct Replayed {
    achieved: DateTime<Utc>,
    last_event: DateTime<Utc>,
}

async fn replay_downloaded(
    local_paths: &[PathBuf],
    start_position: Option<u64>,
    target: DateTime<Utc>,
    config: &Config,
) -> Result<Replayed> {
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
        .arg(format!("--max-allowed-packet={MAX_ALLOWED_PACKET}"))
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
    let mysql_said = tokio::spawn(stderr_tail(mysql.stderr.take()));
    let binlog_said = tokio::spawn(stderr_tail(mysqlbinlog.stderr.take()));
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
    let mysql_said = mysql_said.await.unwrap_or_default();
    let binlog_said = binlog_said.await.unwrap_or_default();

    // The client's own words first: when it quits on an error, the relay's
    // broken pipe is the consequence, not the cause.
    if !mysql_status.success() {
        anyhow::bail!(
            "{}",
            with_said(
                &format!("mysql (replaying binlogs) exited with {mysql_status}"),
                &mysql_said
            )
        );
    }
    relay_result
        .context("relay task panicked")?
        .with_context(|| with_said("streaming mysqlbinlog's output into mysql", &mysql_said))?;
    if !binlog_status.success() {
        anyhow::bail!(
            "{}",
            with_said(
                &format!("mysqlbinlog exited with {binlog_status}"),
                &binlog_said
            )
        );
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
    Ok(Replayed {
        achieved: last_event.min(target),
        last_event,
    })
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
    return Err(refusal(
        "target-unreachable",
        format!(
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
        ),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gap_blocks_only_targets_past_the_last_replayed_event() {
        let last = t();
        // Target at the last event: every event up to it replayed → served.
        assert!(!gap_blocks_target(last, last));
        // Target before the last event: covered.
        assert!(!gap_blocks_target(
            last,
            last - chrono::Duration::seconds(30)
        ));
        // Target one second past: the missing file may hold it → refused,
        // however small the distance (no rotation tolerance across a hole).
        assert!(gap_blocks_target(last, last + chrono::Duration::seconds(1)));
    }

    #[test]
    fn refusals_carry_a_stable_kind_and_anything_else_is_a_failure() {
        let e = refusal("binlog-gap", "no binlog follows binlog.000006".to_string());
        assert_eq!(classify_restore_error(&e), ("refused", "binlog-gap"));
        assert_eq!(e.to_string(), "no binlog follows binlog.000006");
        // The `.context("replaying binlogs")?` on the way out must not hide
        // the refusal from the classifier.
        let wrapped = e.context("replaying binlogs");
        assert_eq!(classify_restore_error(&wrapped), ("refused", "binlog-gap"));
        let plain = anyhow::anyhow!("mysqlbinlog exited with signal 9");
        assert_eq!(classify_restore_error(&plain), ("failed", "error"));
    }

    #[test]
    fn a_refused_or_failed_attempt_reads_as_crashed_mid_restore_and_keeps_its_reason() {
        let dir = temp_dir("refused-marker");
        write_restore_marker(
            &dir,
            RestoreStatus::Refused,
            t(),
            None,
            Some("binlog lineage gap"),
        )
        .unwrap();
        assert!(crashed_mid_restore(&dir));
        let (status, reason) = previous_attempt(&dir).unwrap();
        assert_eq!(status, RestoreStatus::Refused);
        assert_eq!(reason.as_deref(), Some("binlog lineage gap"));
        write_restore_marker(
            &dir,
            RestoreStatus::Failed,
            t(),
            None,
            Some("gunzip exited with 1"),
        )
        .unwrap();
        assert!(crashed_mid_restore(&dir));
        write_restore_marker(&dir, RestoreStatus::Completed, t(), Some(t()), None).unwrap();
        assert!(!crashed_mid_restore(&dir));
        assert_eq!(previous_attempt(&dir).unwrap().1, None);
    }

    #[test]
    fn only_an_all_loopback_bind_keeps_the_restore_unreachable() {
        assert!(is_loopback_bind("127.0.0.1"));
        assert!(is_loopback_bind("::1"));
        assert!(is_loopback_bind("localhost"));
        assert!(is_loopback_bind(" 127.0.0.1 , ::1 "));

        // The dangerous values, which are also the DEFAULTS a my.cnf or an
        // older entrypoint would leave in place: a wildcard bind is exactly
        // the state this check exists to catch.
        assert!(!is_loopback_bind("*"));
        assert!(!is_loopback_bind("::"));
        assert!(!is_loopback_bind("0.0.0.0"));
        assert!(!is_loopback_bind(""));

        // One reachable address in a list is enough to serve a half-loaded
        // database to someone.
        assert!(!is_loopback_bind("127.0.0.1,0.0.0.0"));
        assert!(!is_loopback_bind("::1, 10.0.0.5"));
    }

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
        write_restore_marker(&dir, RestoreStatus::InProgress, t(), None, None).unwrap();
        assert!(crashed_mid_restore(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn completed_marker_is_not_a_crash_and_records_the_achieved_point() {
        let dir = temp_dir("completed");
        write_restore_marker(&dir, RestoreStatus::InProgress, t(), None, None).unwrap();
        let achieved = pitr::parse_target_time("2026-08-13T13:59:10.000Z").unwrap();
        write_restore_marker(&dir, RestoreStatus::Completed, t(), Some(achieved), None).unwrap();
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
        write_restore_marker(&dir, RestoreStatus::InProgress, t(), None, None).unwrap();
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
    fn reset_partial_restore_wipes_everything_but_the_runtime_lock_and_the_marker() {
        let dir = temp_dir("reset");
        write_restore_marker(&dir, RestoreStatus::InProgress, t(), None, None).unwrap();
        std::fs::create_dir_all(Path::new(&dir).join("mysql")).unwrap();
        std::fs::write(Path::new(&dir).join("mysql").join("ibdata1"), "junk").unwrap();
        std::fs::write(Path::new(&dir).join("binlog.000001"), "junk").unwrap();
        let lock = Path::new(&dir).join(crate::volume_lock::RUNTIME_LOCK_FILE);
        std::fs::write(&lock, "held-by-this-boot").unwrap();

        reset_partial_restore(&dir).unwrap();

        // The marker survives the wipe: it is the attempt counter that bounds
        // the retry (MAX_RESTORE_ATTEMPTS). The datadir itself is gone — the
        // `mysql` schema directory is what datadir_is_initialized() reads.
        assert!(crashed_mid_restore(&dir), "the marker must survive the wipe");
        assert_eq!(recorded_attempts(&dir), 1);
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
    fn attempts_count_each_start_and_survive_the_terminal_write() {
        let dir = temp_dir("attempts");
        assert_eq!(recorded_attempts(&dir), 0, "no marker, no attempts");
        // Attempt 1: starts, then is refused.
        write_restore_marker(&dir, RestoreStatus::InProgress, t(), None, None).unwrap();
        assert_eq!(recorded_attempts(&dir), 1);
        write_restore_marker(&dir, RestoreStatus::Refused, t(), None, Some("binlog lineage gap"))
            .unwrap();
        assert_eq!(recorded_attempts(&dir), 1, "a terminal write keeps the count");
        // The wipe between attempts keeps the marker, so attempt 2 counts on.
        reset_partial_restore(&dir).unwrap();
        write_restore_marker(&dir, RestoreStatus::InProgress, t(), None, None).unwrap();
        assert_eq!(recorded_attempts(&dir), 2);
        write_restore_marker(&dir, RestoreStatus::Failed, t(), None, Some("gunzip exited with 1"))
            .unwrap();
        reset_partial_restore(&dir).unwrap();
        write_restore_marker(&dir, RestoreStatus::InProgress, t(), None, None).unwrap();
        assert_eq!(recorded_attempts(&dir), MAX_RESTORE_ATTEMPTS);
        // A marker written before the field existed reads as 0 attempts.
        std::fs::write(
            restore_marker_path(&dir),
            r#"{"status":"refused","target_time":"2026-08-13T14:00:00.000Z","updated_at":"2026-08-13T14:05:00.000Z"}"#,
        )
        .unwrap();
        assert_eq!(recorded_attempts(&dir), 0);
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
