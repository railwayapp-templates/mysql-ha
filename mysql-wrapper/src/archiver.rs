//! Continuous binlog archiving to an S3-compatible bucket — standalone mode
//! only (see main.rs's gate: this is never spawned while GR_SEEDS is set).
//!
//! Four independent loops, spawned together by `run` once mysqld is ready:
//!   - full backups: one immediately if the bucket holds none for this
//!     server_uuid, then every `BINLOG_FULL_BACKUP_INTERVAL_SECONDS`.
//!   - binlog shipping (~every 10s): upload every CLOSED binlog not yet
//!     uploaded, then reclaim (`PURGE BINARY LOGS TO`) whatever is now
//!     provably safe — never a file that hasn't been confirmed uploaded, so
//!     the volume is the spool during a bucket outage (uploads retry with
//!     backoff; purge waits).
//!   - rotation: `FLUSH BINARY LOGS` every `BINLOG_ROTATE_INTERVAL_SECONDS`,
//!     bounding the recovery point objective — the same role
//!     `archive_timeout` plays for a WAL archive.
//!   - retention (hourly, only when `BINLOG_RETENTION_DAYS` is set): expire
//!     archive objects outside the promised window. Note the asymmetry with
//!     the reclaim above: that one frees LOCAL disk and never touches the
//!     bucket, this one is the only thing that ever deletes from the bucket.
//!     All of its rules live in `pitr::plan_retention` so they are testable
//!     without a bucket; this module only executes a plan.
//!
//! Every failure is logged loudly (and reported via telemetry) and retried
//! on the next cycle; nothing here may ever crash mysqld or block its
//! startup — main.rs only ever `tokio::spawn`s this, fire-and-forget.

use crate::config::Config;
use crate::pitr::{self, FullBackupMeta, S3Location};
use crate::s3::S3Client;
use crate::sql::Sql;
use anyhow::{Context, Result};
use common::{Telemetry, TelemetryEvent};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::Command;
use tracing::{error, info, warn};

const SHIP_POLL: Duration = Duration::from_secs(10);
const FULL_BACKUP_RETRY_DELAY: Duration = Duration::from_secs(60);
const UPLOAD_STATE_FILE: &str = ".pitr_uploaded_binlogs.json";
/// mysqldump emits the `CHANGE MASTER TO` / `CHANGE REPLICATION SOURCE TO`
/// coordinate line within the first few KB of output; this cap bounds how
/// much of the (uncompressed) stream is buffered in memory to find it.
const COORD_SCAN_CAP: usize = 256 * 1024;

/// Spawn and run the three archiver loops forever. Only returns if one of
/// them panics (logged, not propagated) or the S3 client/server_uuid can't
/// be obtained at startup.
pub async fn run(config: Arc<Config>, sql: Sql, telemetry: Arc<Telemetry>) {
    wait_for_mysqld(&sql).await;

    let location = config
        .archive_s3_location()
        .expect("archiver::run is only spawned when Config::archive_enabled()");
    let s3 = match S3Client::new(&location).await {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "could not build the PITR archive S3 client; archiving is disabled for this boot");
            telemetry.send(TelemetryEvent::ComponentError {
                component: "mysql-wrapper".to_string(),
                error: e.to_string(),
                context: "pitr_archiver_s3_client".to_string(),
            });
            return;
        }
    };

    let server_uuid = match sql.server_uuid().await {
        Ok(u) => u,
        Err(e) => {
            error!(error = %e, "could not read server_uuid; PITR archiving is disabled for this boot");
            return;
        }
    };
    info!(
        %server_uuid,
        bucket = %location.bucket,
        path = %location.path,
        "starting PITR archiver"
    );

    // A container that crashed before ever confirming a HEAD must not trust
    // its own "uploaded" bookkeeping — reconcile against the bucket once,
    // up front, every boot.
    reconcile_upload_state(&s3, &location, &config.data_dir, &server_uuid).await;

    let full_task = tokio::spawn(full_backup_loop(
        config.clone(),
        sql.clone(),
        telemetry.clone(),
        s3.clone(),
        location.clone(),
        server_uuid.clone(),
    ));
    let ship_task = tokio::spawn(binlog_shipping_loop(
        config.clone(),
        sql.clone(),
        telemetry.clone(),
        s3.clone(),
        location.clone(),
        server_uuid.clone(),
    ));
    let rotate_task = tokio::spawn(rotation_loop(
        config.clone(),
        sql.clone(),
        telemetry.clone(),
    ));
    let retention_task = tokio::spawn(retention_loop(
        config.clone(),
        telemetry.clone(),
        s3.clone(),
        location.clone(),
        server_uuid.clone(),
    ));

    // None of these loops return in normal operation; if one panics, say so
    // loudly instead of the archiver silently going dark (mirrors
    // health_server::run_health_server_supervised's rationale).
    for (name, task) in [
        ("full_backup", full_task),
        ("binlog_shipping", ship_task),
        ("rotation", rotate_task),
        ("retention", retention_task),
    ] {
        if let Err(e) = task.await {
            error!(loop_name = name, error = ?e, "PITR archiver loop exited unexpectedly");
            telemetry.send(TelemetryEvent::ComponentError {
                component: "mysql-wrapper".to_string(),
                error: format!("PITR archiver loop {name} exited unexpectedly: {e}"),
                context: "pitr_archiver_loop".to_string(),
            });
        }
    }
}

async fn wait_for_mysqld(sql: &Sql) {
    let mut attempts = 0u32;
    loop {
        if let Ok(false) = sql.is_init_temp_server().await {
            return;
        }
        if attempts.is_multiple_of(30) {
            info!("PITR archiver waiting for mysqld");
        }
        attempts += 1;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

// --- full backups -----------------------------------------------------------

async fn full_backup_loop(
    config: Arc<Config>,
    sql: Sql,
    telemetry: Arc<Telemetry>,
    s3: S3Client,
    location: S3Location,
    server_uuid: String,
) {
    let interval = Duration::from_secs(config.binlog_full_backup_interval_seconds);

    let needs_initial = match s3
        .list_keys_with_prefix(&pitr::full_prefix(&location, &server_uuid))
        .await
    {
        Ok(keys) => !keys.iter().any(|k| k.ends_with(".meta.json")),
        Err(e) => {
            warn!(error = %e, "could not check the bucket for an existing full backup; assuming none and taking one now");
            true
        }
    };

    if needs_initial {
        loop {
            match take_full_backup(&config, &sql, &s3, &location, &server_uuid).await {
                Ok(()) => {
                    info!("initial full backup completed");
                    break;
                }
                Err(e) => {
                    error!(error = %e, "initial full backup failed; retrying");
                    telemetry.send(TelemetryEvent::ComponentError {
                        component: "mysql-wrapper".to_string(),
                        error: e.to_string(),
                        context: "pitr_full_backup".to_string(),
                    });
                    tokio::time::sleep(FULL_BACKUP_RETRY_DELAY).await;
                }
            }
        }
    }

    loop {
        tokio::time::sleep(interval).await;
        match take_full_backup(&config, &sql, &s3, &location, &server_uuid).await {
            Ok(()) => info!("scheduled full backup completed"),
            Err(e) => {
                error!(error = %e, "scheduled full backup failed; will retry next cycle");
                telemetry.send(TelemetryEvent::ComponentError {
                    component: "mysql-wrapper".to_string(),
                    error: e.to_string(),
                    context: "pitr_full_backup".to_string(),
                });
            }
        }
    }
}

/// `mysqldump --single-transaction --routines --events --triggers
/// --all-databases <data-flag>`, gzipped, streamed to S3 as it's produced —
/// nothing here buffers the whole (potentially huge) dump. The coordinate
/// line the data-flag emits is scanned out of the first
/// [`COORD_SCAN_CAP`] bytes of mysqldump's own output (before gzip) and
/// becomes this full's `meta.json`.
async fn take_full_backup(
    config: &Config,
    sql: &Sql,
    s3: &S3Client,
    location: &S3Location,
    server_uuid: &str,
) -> Result<()> {
    let taken_at = chrono::Utc::now();
    let rfc = pitr::format_rfc3339_millis(taken_at);
    let dump_key = pitr::full_dump_key(location, server_uuid, &rfc);
    let meta_key = pitr::full_meta_key(location, server_uuid, &rfc);

    let data_flag = probe_dump_data_flag(sql).await;
    info!(data_flag, %dump_key, "starting full backup");

    // mysqldump is a separate process and cannot ride the pool's
    // resolved credential: a drifted MYSQL_ROOT_PASSWORD edit would keep
    // the pool working (it starts on the pinned password) while every
    // full backup fails in a loop. Resolve the pinned password the same
    // way the boot pool does, at every attempt so a rotated pin is picked
    // up without restarting the archiver.
    let dump_pin = crate::password_pin::read_pin(&config.data_dir);
    let dump_password =
        crate::password_pin::initial_password(&config.mysql_root_password, dump_pin.as_deref());

    let mut mysqldump = Command::new("mysqldump")
        .arg(format!("--socket={}", config.socket_path))
        .arg("-uroot")
        .env("MYSQL_PWD", &dump_password)
        .arg("--single-transaction")
        .arg("--routines")
        .arg("--events")
        .arg("--triggers")
        .arg("--all-databases")
        .arg(data_flag)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning mysqldump")?;
    let mut gzip = Command::new("gzip")
        .arg("-c")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning gzip")?;

    let dump_stdout = mysqldump
        .stdout
        .take()
        .context("mysqldump stdout was not piped")?;
    let gzip_stdin = gzip.stdin.take().context("gzip stdin was not piped")?;
    let gzip_stdout = gzip.stdout.take().context("gzip stdout was not piped")?;

    // Tee mysqldump's plaintext output into gzip's stdin while scanning the
    // head of it for the coordinate line; concurrently, stream gzip's output
    // straight to S3 via a multipart upload (unbounded length — no full-dump
    // buffering on either side of the pipe).
    let tee_task = tokio::spawn(tee_and_scan(dump_stdout, gzip_stdin, COORD_SCAN_CAP));
    let upload_result = s3.upload_multipart(&dump_key, gzip_stdout).await;

    let scanned = tee_task
        .await
        .context("tee/scan task panicked")?
        .context("copying mysqldump output into gzip")?;
    let mysqldump_status = mysqldump.wait().await.context("waiting for mysqldump")?;
    let gzip_status = gzip.wait().await.context("waiting for gzip")?;
    upload_result.context("uploading the full backup to S3")?;

    if !mysqldump_status.success() {
        anyhow::bail!("mysqldump exited with {mysqldump_status}");
    }
    if !gzip_status.success() {
        anyhow::bail!("gzip exited with {gzip_status}");
    }

    let dump_head = String::from_utf8_lossy(&scanned);
    let (binlog_file, binlog_pos) = pitr::parse_change_master_coords(&dump_head).with_context(
        || "could not find a CHANGE MASTER TO / CHANGE REPLICATION SOURCE TO coordinate line in mysqldump's output",
    )?;
    let mysql_version = sql
        .mysql_version()
        .await
        .unwrap_or_else(|_| "unknown".to_string());

    let meta = FullBackupMeta {
        taken_at,
        binlog_file,
        binlog_pos,
        server_uuid: server_uuid.to_string(),
        mysql_version,
    };
    let meta_json = serde_json::to_vec_pretty(&meta).context("serializing full-backup meta")?;
    s3.put_object_bytes(&meta_key, meta_json)
        .await
        .context("uploading full-backup meta.json")?;

    Ok(())
}

/// Probe the installed `mysqldump`'s supported coordinate flag via its own
/// `--help` output; only falls back to a major-version guess when the probe
/// itself can't run (binary missing/exec error), which should never happen
/// in the shipped image.
async fn probe_dump_data_flag(sql: &Sql) -> &'static str {
    match Command::new("mysqldump").arg("--help").output().await {
        Ok(output) => pitr::pick_dump_data_flag(&String::from_utf8_lossy(&output.stdout)),
        Err(e) => {
            warn!(error = %e, "could not run `mysqldump --help`; falling back to a version-based guess");
            let major = sql
                .mysql_version()
                .await
                .ok()
                .and_then(|v| pitr::mysql_major_version(&v))
                .unwrap_or(8);
            pitr::dump_data_flag_by_major(major)
        }
    }
}

/// Copy `src` into `dst` byte-for-byte, capturing up to `scan_cap` bytes of
/// the earliest data read (for the coordinate-line scan) without holding the
/// rest in memory.
async fn tee_and_scan(
    mut src: impl AsyncRead + Unpin,
    mut dst: impl AsyncWrite + Unpin,
    scan_cap: usize,
) -> Result<Vec<u8>> {
    let mut scanned = Vec::with_capacity(scan_cap.min(64 * 1024));
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = src
            .read(&mut buf)
            .await
            .context("reading mysqldump output")?;
        if n == 0 {
            break;
        }
        dst.write_all(&buf[..n])
            .await
            .context("writing into gzip's stdin")?;
        if scanned.len() < scan_cap {
            let take = (scan_cap - scanned.len()).min(n);
            scanned.extend_from_slice(&buf[..take]);
        }
    }
    dst.shutdown().await.context("closing gzip's stdin")?;
    Ok(scanned)
}

// --- binlog shipping ---------------------------------------------------------

async fn binlog_shipping_loop(
    config: Arc<Config>,
    sql: Sql,
    telemetry: Arc<Telemetry>,
    s3: S3Client,
    location: S3Location,
    server_uuid: String,
) {
    loop {
        if let Err(e) = ship_once(&config, &sql, &s3, &location, &server_uuid).await {
            warn!(error = %e, "binlog shipping pass failed; retrying next cycle");
            telemetry.send(TelemetryEvent::ComponentError {
                component: "mysql-wrapper".to_string(),
                error: e.to_string(),
                context: "pitr_binlog_shipping".to_string(),
            });
        }
        tokio::time::sleep(SHIP_POLL).await;
    }
}

// --- archive retention ------------------------------------------------------

/// Sweep cadence. Slow on purpose: retention is a housekeeping job whose
/// horizon is measured in days, and every pass lists the whole archive.
const RETENTION_POLL: Duration = Duration::from_secs(3600);

/// Delay before the FIRST sweep, so a boot storm never has several containers
/// listing and deleting at once, and so this server's own lineage has had time
/// to establish itself (the planner refuses to act before that anyway).
const RETENTION_INITIAL_DELAY: Duration = Duration::from_secs(300);

async fn retention_loop(
    config: Arc<Config>,
    telemetry: Arc<Telemetry>,
    s3: S3Client,
    location: S3Location,
    server_uuid: String,
) {
    let Some(days) = config.binlog_retention_days else {
        info!(
            "BINLOG_RETENTION_DAYS is unset; the archive is never expired (unbounded growth). \
             Set it to promise a restorable window and bound storage."
        );
        return;
    };
    let horizon = chrono::Duration::days(days as i64);
    info!(
        retention_days = days,
        dry_run = config.binlog_retention_dry_run,
        min_active_fulls_kept = pitr::MIN_ACTIVE_FULLS_KEPT,
        "PITR archive retention enabled"
    );

    tokio::time::sleep(RETENTION_INITIAL_DELAY).await;
    loop {
        if let Err(e) = retention_pass(&config, &s3, &location, &server_uuid, horizon).await {
            warn!(error = %e, "retention pass failed; retrying next cycle");
            telemetry.send(TelemetryEvent::ComponentError {
                component: "mysql-wrapper".to_string(),
                error: e.to_string(),
                context: "pitr_retention".to_string(),
            });
        }
        tokio::time::sleep(RETENTION_POLL).await;
    }
}

/// One sweep: read the whole archive, plan, then delete exactly what the plan
/// names — nothing is decided here, so every rule stays unit-testable in
/// `pitr::plan_retention`.
async fn retention_pass(
    config: &Config,
    s3: &S3Client,
    location: &S3Location,
    server_uuid: &str,
    horizon: chrono::Duration,
) -> Result<()> {
    let now = chrono::Utc::now();
    let lineages = read_archive_lineages(s3, location, now).await?;
    let input = pitr::RetentionInput {
        lineages,
        // Passing our OWN uuid is what makes "dead lineage" meaningful. The
        // planner refuses to expire anything when this is None.
        active_server_uuid: Some(server_uuid.to_string()),
        now,
        horizon,
    };
    let plan = pitr::plan_retention(&input);

    for note in &plan.notes {
        info!(note = %note, "retention");
    }
    if plan.is_empty() {
        return Ok(());
    }
    if config.binlog_retention_dry_run {
        info!(
            objects = plan.object_count(),
            fulls = plan.expired_full_keys.len(),
            binlogs = plan.expired_binlogs.len(),
            orphans = plan.orphan_dump_keys.len(),
            retired_lineages = ?plan.retired_lineages,
            "BINLOG_RETENTION_DRY_RUN is set; would delete these objects but will not"
        );
        for key in plan.expired_full_keys.iter().chain(&plan.orphan_dump_keys) {
            info!(key = %key, "retention (dry run) would delete");
        }
        for (uuid, name) in &plan.expired_binlogs {
            info!(key = %pitr::binlog_key(location, uuid, name), "retention (dry run) would delete");
        }
        return Ok(());
    }

    // The absolute age rail, enforced here because this is where an object's
    // real last-modified time is available. A policy bug upstream cannot get
    // past it: whatever the plan says, nothing younger than
    // RETENTION_MIN_OBJECT_AGE_SECONDS is deleted.
    let min_age = chrono::Duration::seconds(pitr::RETENTION_MIN_OBJECT_AGE_SECONDS);
    let mut deleted = 0usize;
    let mut spared_young = 0usize;

    let binlog_keys: Vec<String> = plan
        .expired_binlogs
        .iter()
        .map(|(uuid, name)| pitr::binlog_key(location, uuid, name))
        .collect();

    for key in plan
        .expired_full_keys
        .iter()
        .chain(&plan.orphan_dump_keys)
        .chain(&binlog_keys)
    {
        match s3.last_modified(key).await {
            Ok(Some(modified)) if now - modified < min_age => {
                spared_young += 1;
                continue;
            }
            Ok(None) => continue, // already gone
            Ok(Some(_)) => {}
            Err(e) => {
                // Could not establish the age: keep it. An unreadable HEAD is
                // never a licence to delete a backup.
                warn!(error = %e, %key, "could not read object age; keeping it this pass");
                continue;
            }
        }
        match s3.delete_object(key).await {
            Ok(()) => {
                deleted += 1;
                info!(%key, "retention deleted");
            }
            Err(e) => warn!(error = %e, %key, "retention could not delete an object; will retry"),
        }
    }

    info!(
        deleted,
        spared_young,
        retired_lineages = ?plan.retired_lineages,
        "retention pass complete"
    );
    Ok(())
}

/// Group the whole archive into per-lineage objects for the planner. Reads
/// every `full/*.meta.json` (the completeness marker: the dump is uploaded
/// first, the meta after, so a dump without one is incomplete), and HEADs only
/// the orphan dumps, whose age is not recoverable any other way.
async fn read_archive_lineages(
    s3: &S3Client,
    location: &S3Location,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<pitr::LineageObjects>> {
    use std::collections::BTreeMap;

    let base = pitr::base_prefix(location);
    let keys = s3
        .list_keys_with_prefix(&base)
        .await
        .context("listing the PITR archive bucket for retention")?;

    struct Raw {
        metas: Vec<String>,
        dumps: BTreeSet<String>,
        binlogs: Vec<String>,
    }
    let mut per_lineage: BTreeMap<String, Raw> = BTreeMap::new();

    for key in &keys {
        let Some(uuid) = pitr::server_uuid_from_key(location, key) else {
            continue;
        };
        let entry = per_lineage.entry(uuid).or_insert_with(|| Raw {
            metas: Vec::new(),
            dumps: BTreeSet::new(),
            binlogs: Vec::new(),
        });
        if key.contains("/full/") {
            if key.ends_with(".meta.json") {
                entry.metas.push(key.clone());
            } else if key.ends_with(".sql.gz") {
                entry.dumps.insert(key.clone());
            }
        } else if key.contains("/binlog/") {
            if let Some(name) = key.rsplit('/').next() {
                entry.binlogs.push(name.to_string());
            }
        }
    }

    let mut out = Vec::new();
    for (uuid, raw) in per_lineage {
        let mut fulls = Vec::new();
        let mut paired_dumps: BTreeSet<String> = BTreeSet::new();
        for meta_key in &raw.metas {
            let Some(stem) = meta_key.strip_suffix(".meta.json") else {
                continue;
            };
            let dump_key = format!("{stem}.sql.gz");
            // A meta whose dump is gone is not a restorable full. Record the
            // pairing anyway so the dump is not then also treated as an
            // orphan, and let the meta itself age out with its lineage.
            paired_dumps.insert(dump_key.clone());
            if !raw.dumps.contains(&dump_key) {
                warn!(%meta_key, "full-backup meta has no dump object; not counting it as restorable");
                continue;
            }
            let bytes = match s3.get_object_bytes(meta_key).await {
                Ok(b) => b,
                Err(e) => {
                    // Unreadable meta: treat the full as present-but-unknown
                    // by skipping it, which keeps it (it is neither counted as
                    // retainable nor listed for deletion).
                    warn!(error = %e, %meta_key, "could not read a full-backup meta.json during retention; keeping this full");
                    paired_dumps.insert(dump_key);
                    continue;
                }
            };
            let meta: FullBackupMeta = match serde_json::from_slice(&bytes) {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, %meta_key, "could not parse a full-backup meta.json during retention; keeping this full");
                    continue;
                }
            };
            fulls.push(pitr::FullBackupRef {
                server_uuid: uuid.clone(),
                dump_key,
                meta,
            });
        }

        let mut orphan_dumps = Vec::new();
        for dump_key in &raw.dumps {
            if paired_dumps.contains(dump_key) {
                continue;
            }
            match s3.last_modified(dump_key).await {
                Ok(Some(modified)) => orphan_dumps.push((dump_key.clone(), modified)),
                // No age readable: pass `now` so it always looks too young to
                // expire, i.e. keep it.
                Ok(None) => {}
                Err(e) => {
                    warn!(error = %e, %dump_key, "could not read an orphan dump's age; keeping it");
                    orphan_dumps.push((dump_key.clone(), now));
                }
            }
        }

        out.push(pitr::LineageObjects {
            server_uuid: uuid,
            fulls,
            orphan_dumps,
            binlogs: raw.binlogs,
        });
    }
    Ok(out)
}

async fn ship_once(
    config: &Config,
    sql: &Sql,
    s3: &S3Client,
    location: &S3Location,
    server_uuid: &str,
) -> Result<()> {
    let (active, _pos) = sql
        .binary_log_status()
        .await
        .context("SHOW BINARY LOG STATUS / SHOW MASTER STATUS")?;
    let disk_files =
        local_binlog_index(&config.data_dir).context("reading the local binlog index")?;
    let mut state = read_upload_state(&config.data_dir);

    for name in &disk_files {
        if !pitr::binlog_is_closed(name, &active)
            || state.uploaded.contains(name)
            || state.lost.contains(name)
        {
            continue;
        }
        let path = Path::new(&config.data_dir).join(name);
        if !path.is_file() {
            // A closed binlog this boot never confirmed uploading, gone from
            // disk: it was purged or lost before it could ship, and the
            // archive lineage now has a PERMANENT hole — a restore past this
            // point will refuse rather than silently stop short (see
            // restore.rs). Our own reclaim only ever purges uploaded files,
            // so this is never the archiver's doing. Recorded in the state
            // file so the loss is reported exactly once, not every poll.
            state.lost.insert(name.clone());
            write_upload_state(&config.data_dir, &state)?;
            error!(
                file = %name,
                "binlog lost from disk before upload — the archive lineage now has a \
                 permanent gap at this file; point-in-time restores past it will refuse \
                 rather than silently lose the data after it"
            );
            continue;
        }
        let key = pitr::binlog_key(location, server_uuid, name);
        s3.put_object_from_file(&key, &path)
            .await
            .with_context(|| format!("uploading {name}"))?;
        state.uploaded.insert(name.clone());
        write_upload_state(&config.data_dir, &state)?;
        info!(file = %name, "binlog uploaded");
    }

    if let Some(cut) = pitr::purge_cut(&disk_files, &active, &state.uploaded, &state.lost) {
        sql.purge_binary_logs_to(&cut)
            .await
            .with_context(|| format!("PURGE BINARY LOGS TO {cut}"))?;
        // The purged names are gone from disk — nothing left to verify on a
        // future startup reconciliation pass, so drop them from the state
        // file too (keeps it from growing unbounded over the volume's life).
        let mut changed = false;
        for name in disk_files.iter().take_while(|f| f.as_str() != cut) {
            changed |= state.uploaded.remove(name);
        }
        if changed {
            write_upload_state(&config.data_dir, &state)?;
        }
        info!(cut = %cut, "reclaimed uploaded binlogs");
    }

    Ok(())
}

/// The lineage's binlog file names, oldest first, straight from mysqld's own
/// index file (`<datadir>/binlog.index`) — one entry per line, sometimes a
/// bare name and sometimes a path depending on how `log_bin` was configured;
/// only the basename matters for naming/ordering. Absent index (binlog not
/// yet enabled/rotated) reads as empty, not an error.
fn local_binlog_index(data_dir: &str) -> Result<Vec<String>> {
    let index_path = Path::new(data_dir).join("binlog.index");
    let content = match std::fs::read_to_string(&index_path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", index_path.display())),
    };
    let mut names: Vec<String> = content
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(|l| l.rsplit('/').next().unwrap_or(l).to_string())
        .collect();
    names.sort_by(|a, b| pitr::binlog_name_cmp(a, b));
    Ok(names)
}

/// Which closed binlogs this boot has confirmed uploaded — a small JSON file
/// in the datadir (`UPLOAD_STATE_FILE`), reconciled against the bucket with a
/// HEAD pass at startup (see `reconcile_upload_state`) since a crash between
/// upload and persisting this file must not be trusted blind.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
struct UploadState {
    uploaded: BTreeSet<String>,
    /// Closed binlogs that vanished from disk before they were ever
    /// uploaded — each is a permanent hole in the archive lineage, reported
    /// (once) where detected in ship_once. `serde(default)` so state files
    /// written before this field existed still parse.
    #[serde(default)]
    lost: BTreeSet<String>,
}

fn upload_state_path(data_dir: &str) -> PathBuf {
    Path::new(data_dir).join(UPLOAD_STATE_FILE)
}

fn read_upload_state(data_dir: &str) -> UploadState {
    std::fs::read_to_string(upload_state_path(data_dir))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn write_upload_state(data_dir: &str, state: &UploadState) -> Result<()> {
    use std::io::Write;

    let path = upload_state_path(data_dir);
    let json = serde_json::to_string(state).context("serializing PITR upload state")?;
    // Publish atomically: a torn write reads back as JSON garbage, and the
    // default-on-parse-failure silently forgets every recorded upload. The
    // sync_all before the rename keeps a power cut from publishing an empty
    // tmp file (same discipline as password_pin::write_pin).
    let tmp = path.with_extension("tmp");
    let mut file =
        std::fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    file.write_all(json.as_bytes())
        .and_then(|()| file.sync_all())
        .with_context(|| format!("writing {}", tmp.display()))?;
    drop(file);
    std::fs::rename(&tmp, &path).with_context(|| format!("publishing {}", path.display()))
}

/// Startup-only: a locally-recorded "uploaded" entry that the bucket doesn't
/// actually have (a crash between the PUT and persisting the state file)
/// must be re-uploaded, not trusted — HEAD every entry once and drop the
/// ones the bucket doesn't confirm.
async fn reconcile_upload_state(
    s3: &S3Client,
    location: &S3Location,
    data_dir: &str,
    server_uuid: &str,
) {
    let mut state = read_upload_state(data_dir);
    let names: Vec<String> = state.uploaded.iter().cloned().collect();
    let mut changed = false;
    for name in names {
        let key = pitr::binlog_key(location, server_uuid, &name);
        match s3.exists(&key).await {
            Ok(true) => {}
            Ok(false) => {
                warn!(file = %name, "locally-recorded binlog upload is missing from the bucket; will re-upload");
                state.uploaded.remove(&name);
                changed = true;
            }
            Err(e) => {
                warn!(error = %e, file = %name, "could not verify upload state against the bucket at startup; trusting the local record for now");
            }
        }
    }
    if changed {
        if let Err(e) = write_upload_state(data_dir, &state) {
            warn!(error = %e, "could not persist reconciled upload state");
        }
    }
}

// --- rotation ----------------------------------------------------------------

async fn rotation_loop(config: Arc<Config>, sql: Sql, telemetry: Arc<Telemetry>) {
    let interval = Duration::from_secs(config.binlog_rotate_interval_seconds);
    loop {
        tokio::time::sleep(interval).await;
        if let Err(e) = sql.flush_binary_logs().await {
            warn!(error = %e, "FLUSH BINARY LOGS failed; will retry next cycle");
            telemetry.send(TelemetryEvent::ComponentError {
                component: "mysql-wrapper".to_string(),
                error: e.to_string(),
                context: "pitr_binlog_rotation".to_string(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "mysql-wrapper-archiver-test-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    #[test]
    fn local_binlog_index_reads_bare_and_pathlike_entries_oldest_first() {
        let dir = temp_dir("index");
        std::fs::write(
            Path::new(&dir).join("binlog.index"),
            "./binlog.000003\nbinlog.000001\n./binlog.000002\n",
        )
        .unwrap();
        assert_eq!(
            local_binlog_index(&dir).unwrap(),
            vec!["binlog.000001", "binlog.000002", "binlog.000003"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn local_binlog_index_absent_is_empty_not_an_error() {
        let dir = temp_dir("no-index");
        assert_eq!(local_binlog_index(&dir).unwrap(), Vec::<String>::new());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn upload_state_roundtrips_and_degrades_on_garbage() {
        let dir = temp_dir("state");
        assert_eq!(read_upload_state(&dir), UploadState::default());

        let mut state = UploadState::default();
        state.uploaded.insert("binlog.000001".to_string());
        state.uploaded.insert("binlog.000002".to_string());
        write_upload_state(&dir, &state).unwrap();
        assert_eq!(read_upload_state(&dir), state);

        std::fs::write(upload_state_path(&dir), "not json").unwrap();
        assert_eq!(read_upload_state(&dir), UploadState::default());
        std::fs::remove_dir_all(&dir).ok();
    }
}
