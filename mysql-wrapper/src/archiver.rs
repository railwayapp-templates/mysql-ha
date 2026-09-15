//! Continuous binlog archiving to an S3-compatible bucket, in either of the
//! wrapper's two modes (`ArchiveMode`):
//!
//!   - **Standalone**: main.rs spawns `run` directly; this server's lineage is
//!     the whole archive.
//!   - **Group Replication**: main.rs spawns `run_group_primary_supervisor`,
//!     which runs `run` only while THIS node is the group's writable primary
//!     (the /role fence's own verdict) and stops it the moment it isn't. Every
//!     member logs the same group transactions under the same GTIDs, so the
//!     primary's binlogs are a complete stream of the group's history, and
//!     after a switchover or failover the NEW primary's archiver picks up in
//!     its own `server-<uuid>/` lineage — including, in its retained closed
//!     binlogs, the transactions the old primary's never-uploaded active file
//!     took down with it. Restore stitches those lineages back together by
//!     GTID (restore.rs).
//!
//! Four independent loops, spawned together by `run` once mysqld is ready:
//!   - full backups: one when the archive holds none, then every
//!     `BINLOG_FULL_BACKUP_INTERVAL_SECONDS` after the newest one. "The
//!     archive" is this server's own lineage standalone, and EVERY lineage
//!     for a group primary — a member that takes over mid-interval inherits
//!     the cadence rather than dumping the whole dataset on failover, which
//!     is exactly when the cluster can least afford it.
//!   - binlog shipping (~every 10s): upload every CLOSED binlog not yet
//!     uploaded, then reclaim (`PURGE BINARY LOGS TO`) whatever is now
//!     provably safe — never a file that hasn't been confirmed uploaded, so
//!     the volume is the spool during a bucket outage (uploads retry with
//!     backoff; purge waits). A group primary adds one rule: an uploaded
//!     file also stays for `pitr::GR_BINLOG_EXPIRE_SECONDS`, because its
//!     peers recover from each other out of retained binlogs (a purged donor
//!     forces a full clone). While it archives, the primary switches mysqld's
//!     own expiry off (`run_group_primary_supervisor`) — mysqld cannot know
//!     what has shipped, so its blind expiry could reclaim an unshipped file
//!     during a long upload outage and punch a permanent hole into the
//!     archive; the archiver's reclaim honors both the window and the upload.
//!     Secondaries keep mysqld's expiry: nothing on them ships.
//!   - rotation: `FLUSH BINARY LOGS` every `BINLOG_ROTATE_INTERVAL_SECONDS`,
//!     bounding the recovery point objective — the same role
//!     `archive_timeout` plays for a WAL archive.
//!   - retention (hourly, on by default; off only when `BINLOG_RETENTION_DAYS`
//!     is explicitly `0`): expire archive objects outside the promised window.
//!     The horizon defaults to `pitr::DEFAULT_BINLOG_RETENTION_DAYS`. Note the
//!     asymmetry with
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
use crate::sql::{
    role_is_writable_primary, ReadLockAttempt, RunningStatement, Sql, GLOBAL_READ_LOCK_WAIT,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use common::{Telemetry, TelemetryEvent};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{oneshot, Notify};
use tracing::{error, info, warn};

const SHIP_POLL: Duration = Duration::from_secs(10);
const FULL_BACKUP_RETRY_DELAY: Duration = Duration::from_secs(60);
const UPLOAD_STATE_FILE: &str = ".pitr_uploaded_binlogs.json";
/// How often the group-mode supervisor re-reads this node's role. The same
/// order as HAProxy's /role probe: a demoted primary stops archiving within
/// one poll, and a promoted one starts within one.
const ROLE_POLL: Duration = Duration::from_secs(5);
const FULL_BACKUP_IDLE: u8 = 0;
const FULL_BACKUP_QUEUED: u8 = 1;
const FULL_BACKUP_RUNNING: u8 = 2;

/// How the archiver relates to the server it runs beside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveMode {
    /// A lone server: this lineage is the whole archive, and the archiver
    /// itself reclaims uploaded binlogs from local disk (mysqld's own expiry
    /// is off — see `mysql_conf::render_standalone_archive_conf`).
    Standalone,
    /// The writable primary of a Group Replication group. Local binlogs are
    /// reclaimed only once uploaded AND older than the group's recovery
    /// window (see the module doc), and fulls are due archive-wide rather
    /// than per lineage.
    GroupPrimary,
}

impl ArchiveMode {
    fn label(self) -> &'static str {
        match self {
            ArchiveMode::Standalone => "standalone",
            ArchiveMode::GroupPrimary => "group-primary",
        }
    }
}

/// What the archiver is doing right now, as the health server's `/pitr`
/// endpoint reports it (JSON). Read by the platform's enable workflow to
/// confirm archiving actually started on the node it just promoted, and by
/// the fleet monitor. Purely informational — nothing routes on it.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct PitrStatusSnapshot {
    /// `BINLOG_ARCHIVE_BUCKET` (and siblings) are set on this node.
    pub archive_configured: bool,
    /// The archiver loops are running on THIS node right now. False on a
    /// group secondary even with the contract configured — its primary
    /// archives for it.
    pub archiving: bool,
    /// `standalone` / `group-primary`, while archiving.
    pub mode: Option<String>,
    /// The lineage this node archives under (`server-<uuid>/`).
    pub server_uuid: Option<String>,
    pub last_full_backup_at: Option<String>,
    pub last_shipped_binlog: Option<String>,
    pub last_shipped_at: Option<String>,
    /// The most recent loop failure, if any, verbatim.
    pub last_error: Option<String>,
}

/// Shared, cheaply-cloned handle to the live `PitrStatusSnapshot`.
pub struct PitrStatus {
    inner: RwLock<PitrStatusSnapshot>,
    full_backup_state: AtomicU8,
    full_backup_wake: Notify,
    /// Why the queued full was asked for, so the log line names the gap the
    /// dump is closing rather than reading like an operator's own request.
    full_backup_gap_recovery: AtomicBool,
}

impl PitrStatus {
    pub fn new(archive_configured: bool) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(PitrStatusSnapshot {
                archive_configured,
                ..PitrStatusSnapshot::default()
            }),
            full_backup_state: AtomicU8::new(FULL_BACKUP_IDLE),
            full_backup_wake: Notify::new(),
            full_backup_gap_recovery: AtomicBool::new(false),
        })
    }

    pub fn snapshot(&self) -> PitrStatusSnapshot {
        self.inner
            .read()
            .map(|s| s.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone())
    }

    fn update(&self, f: impl FnOnce(&mut PitrStatusSnapshot)) {
        match self.inner.write() {
            Ok(mut guard) => f(&mut guard),
            Err(poisoned) => f(&mut poisoned.into_inner()),
        }
    }

    /// The archive contract is present but cannot be used this boot (a
    /// missing sibling, a malformed bucket or endpoint — `Config::
    /// archive_refusal`). Recorded where the platform already reads archive
    /// trouble: the monitor's credential banner and the e2e harness read
    /// `last_error` off `/pitr`. `archiving` stays false.
    pub fn note_refusal(&self, reason: &str) {
        let text = reason.to_string();
        self.update(|s| s.last_error = Some(text));
    }

    fn note_error(&self, error: &anyhow::Error) {
        // The whole chain — the operation that failed AND why. The outermost
        // context alone ("HEAD binlog/owner.json") says nothing about a
        // rejected credential; the platform's credential banner and the e2e
        // harness read the rejection off this field.
        let text = format!("{error:#}");
        self.update(|s| s.last_error = Some(text));
    }

    /// A fault named in words rather than carried by an error value (the
    /// reclaim deferral that outlived its excuse).
    fn note_text(&self, text: String) {
        self.update(|s| s.last_error = Some(text));
    }

    /// Clear `last_error` when the text it holds satisfies `ours` — so a
    /// fault that is over goes away without wiping a different, live one.
    fn clear_error_if(&self, ours: impl FnOnce(&str) -> bool) {
        self.update(|s| {
            if s.last_error.as_deref().is_some_and(ours) {
                s.last_error = None;
            }
        });
    }

    /// Queue one out-of-cadence full backup. The health endpoint calls this
    /// after authentication; the archiver loop consumes it. One atomic state
    /// covers queued and running work, preventing duplicate concurrent dumps.
    pub fn request_full_backup(&self) -> Result<(), &'static str> {
        self.queue_full_backup(false)
    }

    /// The archiver's own trigger, for the one case the cadence cannot cover:
    /// the lineage just lost a binlog it never shipped, so every restore past
    /// that file is refused until a NEW full re-anchors the chain. The server
    /// still holds the data the archive lost, so this dump is what closes the
    /// hole — the sooner it runs, the shorter the window with no recoverable
    /// point after the gap.
    fn request_gap_recovery_full_backup(&self) -> Result<(), &'static str> {
        self.queue_full_backup(true)
    }

    fn queue_full_backup(&self, gap_recovery: bool) -> Result<(), &'static str> {
        let snapshot = self.snapshot();
        if !snapshot.archive_configured {
            return Err("PITR archiving is not configured");
        }
        if !snapshot.archiving {
            return Err("this node is not currently archiving");
        }
        // Set BEFORE the latch: whoever wins the compare-exchange must find
        // the reason already published, never a half-written request.
        if gap_recovery {
            self.full_backup_gap_recovery.store(true, Ordering::Release);
        }
        self.full_backup_state
            .compare_exchange(
                FULL_BACKUP_IDLE,
                FULL_BACKUP_QUEUED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| "a full backup is already queued or running")?;
        self.full_backup_wake.notify_one();
        Ok(())
    }

    /// `(run, kind)` — "gap-recovery" when the queued request came from a lost
    /// binlog, "triggered" when it came from the endpoint.
    fn begin_requested_full_backup(&self) -> Option<(FullBackupRun<'_>, &'static str)> {
        self.full_backup_state
            .compare_exchange(
                FULL_BACKUP_QUEUED,
                FULL_BACKUP_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .ok()
            .map(|_| {
                let kind = if self.full_backup_gap_recovery.swap(false, Ordering::AcqRel) {
                    "gap-recovery"
                } else {
                    "triggered"
                };
                (FullBackupRun { status: self }, kind)
            })
    }

    fn begin_scheduled_full_backup(&self) -> Option<FullBackupRun<'_>> {
        self.full_backup_state
            .compare_exchange(
                FULL_BACKUP_IDLE,
                FULL_BACKUP_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .ok()
            .map(|_| FullBackupRun { status: self })
    }

    async fn wait_for_full_backup_request(&self) {
        self.full_backup_wake.notified().await;
    }

    fn stop_archiving(&self) {
        self.full_backup_state
            .store(FULL_BACKUP_IDLE, Ordering::Release);
        self.full_backup_gap_recovery
            .store(false, Ordering::Release);
        self.update(|s| {
            s.archiving = false;
            s.mode = None;
        });
    }

    #[cfg(test)]
    pub(crate) fn mark_archiving_for_test(&self) {
        self.update(|s| {
            s.archive_configured = true;
            s.archiving = true;
        });
    }
}

/// Reset the queue/running latch even when role loss aborts the archiver task
/// while a dump is in flight.
struct FullBackupRun<'a> {
    status: &'a PitrStatus,
}

impl Drop for FullBackupRun<'_> {
    fn drop(&mut self) {
        self.status
            .full_backup_state
            .store(FULL_BACKUP_IDLE, Ordering::Release);
    }
}

/// Group Replication mode: run the archiver on this node exactly while it is
/// the group's writable primary, re-deciding every `ROLE_POLL` from the same
/// verdict the /role fence answers with (`sql::role_is_writable_primary`,
/// outranked by the membership fence). A demotion aborts the running loops
/// mid-flight — the new primary archives from here on, and anything this
/// node had not confirmed uploaded is re-shipped by whoever is primary next
/// (its own upload state is per lineage, so on re-promotion it resumes its
/// own). A verdict that cannot be read leaves the current state alone: a
/// transient SQL error must neither stop a healthy archiver nor start one.
pub async fn run_group_primary_supervisor(
    config: Arc<Config>,
    sql: Sql,
    telemetry: Arc<Telemetry>,
    membership_fenced: Arc<AtomicBool>,
    status: Arc<PitrStatus>,
) {
    wait_for_mysqld(&sql).await;
    info!("PITR archiving is configured on a Group Replication member; archiving follows the writable-primary role");

    let mut running: Option<tokio::task::JoinHandle<()>> = None;
    // Whether this node has switched mysqld's binlog expiry off in favor of
    // the archiver's reclaim (module doc). Tracked apart from `running`: an
    // archiver that failed to start still leaves expiry handed over, and a
    // demotion must hand it back regardless.
    let mut owns_expiry = false;
    loop {
        if running.as_ref().is_some_and(|h| h.is_finished()) {
            // `run` returns when it could not even start (no S3 client, no
            // server_uuid) or when every loop has ended; it has logged why.
            // The next primary verdict starts it again.
            running = None;
            status.update(|s| s.archiving = false);
        }

        let verdict = async {
            let self_uuid = sql.server_uuid().await?;
            let members = sql.group_members().await?;
            anyhow::Ok(
                role_is_writable_primary(&members, &self_uuid)
                    && !membership_fenced.load(Ordering::Acquire),
            )
        }
        .await;
        let demoted = matches!(verdict, Ok(false));

        match (verdict, running.is_some()) {
            (Ok(true), false) => {
                info!("this node is the group's writable primary; starting the PITR archiver");
                // While this node archives, mysqld must not reclaim a binlog
                // the archiver hasn't uploaded: expiry is handed to the
                // archiver, whose reclaim keeps uploaded files for the same
                // window (ship_once). Dynamic, not persisted — a restart boots
                // with the config file's value until this branch runs again.
                match sql.set_global_binlog_expire_logs_seconds(0).await {
                    Ok(()) => owns_expiry = true,
                    Err(e) => {
                        warn!(error = %e, "could not hand binlog expiry to the archiver; mysqld's own expiry stays in effect on this primary");
                        status.note_error(&e);
                    }
                }
                running = Some(tokio::spawn(run(
                    config.clone(),
                    sql.clone(),
                    telemetry.clone(),
                    ArchiveMode::GroupPrimary,
                    status.clone(),
                )));
            }
            (Ok(false), true) => {
                warn!(
                    "this node is no longer the group's writable primary; stopping the PITR \
                     archiver — the new primary archives from here on"
                );
                if let Some(handle) = running.take() {
                    handle.abort();
                    // Wait for cancellation to drop a possibly-running dump's
                    // latch before accepting requests against the stopped
                    // archiver. Otherwise that late Drop could erase a newly
                    // queued request.
                    let _ = handle.await;
                }
                status.stop_archiving();
            }
            _ => {}
        }
        if demoted && owns_expiry {
            // Nothing ships from a secondary, so mysqld's own expiry is the
            // only thing bounding its disk again.
            match sql
                .set_global_binlog_expire_logs_seconds(pitr::GR_BINLOG_EXPIRE_SECONDS)
                .await
            {
                Ok(()) => owns_expiry = false,
                Err(e) => {
                    warn!(error = %e, "could not hand binlog expiry back to mysqld after demotion; retrying next poll")
                }
            }
        }
        tokio::time::sleep(ROLE_POLL).await;
    }
}
/// mysqldump emits the `CHANGE MASTER TO` / `CHANGE REPLICATION SOURCE TO`
/// coordinate line within the first few KB of output; this cap bounds how
/// much of the (uncompressed) stream is buffered in memory to find it.
const COORD_SCAN_CAP: usize = 256 * 1024;

/// Spawn and run the four archiver loops forever. Only returns if one of
/// them panics (logged, not propagated) or the S3 client/server_uuid can't
/// be obtained at startup.
pub async fn run(
    config: Arc<Config>,
    sql: Sql,
    telemetry: Arc<Telemetry>,
    mode: ArchiveMode,
    status: Arc<PitrStatus>,
) {
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
    // One root, one database history: refuse a root another service claimed
    // before writing a byte into it (see pitr::archive_ownership_verdict).
    // Loud and re-tried on the next start: the record names the owner and
    // the remedy, /pitr carries it as last_error, and mysqld itself is
    // unaffected.
    if let Err(e) = ensure_archive_ownership(&s3, &location, &config, &sql, mode).await {
        // Refusal or an unreadable record alike; the chain says which (a
        // rejected credential surfaces here first, as the HEAD's HTTP status).
        error!(
            error = %format!("{e:#}"),
            bucket = %location.bucket,
            path = %location.path,
            "PITR archiving refused: this archive root is not this service's, or its owner record \
             could not be read; archiving is disabled for this boot"
        );
        telemetry.send(TelemetryEvent::ComponentError {
            component: "mysql-wrapper".to_string(),
            error: format!("{e:#}"),
            context: "pitr_archive_ownership".to_string(),
        });
        status.note_error(&e);
        return;
    }
    if mode == ArchiveMode::GroupPrimary {
        // Declare the archive one shared history before the first byte of
        // GTID binlog lands in it (see pitr::shared_history_marker_key).
        // Idempotent; a failure here is retried on the next start and is
        // never silent for a restore — an undeclared GTID binlog fails
        // loudly under the anonymous replay rather than replaying wrong.
        let marker = pitr::shared_history_marker_key(&location);
        if let Err(e) = s3
            .put_object_bytes(&marker, b"group-replication".to_vec())
            .await
        {
            warn!(error = %e, key = %marker, "could not write the shared-history marker; will retry on the next archiver start");
            status.note_error(&e);
        }
    }
    info!(
        %server_uuid,
        bucket = %location.bucket,
        path = %location.path,
        mode = mode.label(),
        "starting PITR archiver"
    );
    status.update(|s| {
        s.archiving = true;
        s.mode = Some(mode.label().to_string());
        s.server_uuid = Some(server_uuid.clone());
    });

    // A container that crashed before ever confirming a HEAD must not trust
    // its own "uploaded" bookkeeping — reconcile against the bucket once,
    // up front, every boot.
    reconcile_upload_state(&s3, &location, &config.data_dir, &server_uuid).await;

    // One JoinSet owns every loop, so dropping it aborts them all together —
    // which is exactly what the group-mode supervisor's abort of this task
    // does on demotion. Plain `tokio::spawn` handles would detach instead:
    // the loops would keep shipping from a secondary after the supervisor
    // had already reported the archiver stopped.
    let mut loops = tokio::task::JoinSet::new();
    loops.spawn({
        let (config, sql, telemetry, s3, location, server_uuid, status) = (
            config.clone(),
            sql.clone(),
            telemetry.clone(),
            s3.clone(),
            location.clone(),
            server_uuid.clone(),
            status.clone(),
        );
        async move {
            full_backup_loop(
                config,
                sql,
                telemetry,
                s3,
                location,
                server_uuid,
                mode,
                status,
            )
            .await;
            "full_backup"
        }
    });
    loops.spawn({
        let (config, sql, telemetry, s3, location, server_uuid, status) = (
            config.clone(),
            sql.clone(),
            telemetry.clone(),
            s3.clone(),
            location.clone(),
            server_uuid.clone(),
            status.clone(),
        );
        async move {
            binlog_shipping_loop(
                config,
                sql,
                telemetry,
                s3,
                location,
                server_uuid,
                mode,
                status,
            )
            .await;
            "binlog_shipping"
        }
    });
    loops.spawn({
        let (config, sql, telemetry) = (config.clone(), sql.clone(), telemetry.clone());
        async move {
            rotation_loop(config, sql, telemetry).await;
            "rotation"
        }
    });
    loops.spawn({
        let (config, telemetry, s3, location, server_uuid) = (
            config.clone(),
            telemetry.clone(),
            s3.clone(),
            location.clone(),
            server_uuid.clone(),
        );
        async move {
            retention_loop(config, telemetry, s3, location, server_uuid).await;
            "retention"
        }
    });

    // None of these loops return in normal operation (retention does when
    // opted out, and says so itself); if one panics, say so loudly instead of
    // the archiver silently going dark (mirrors
    // health_server::run_health_server_supervised's rationale).
    while let Some(outcome) = loops.join_next().await {
        match outcome {
            Ok(name) => info!(loop_name = name, "PITR archiver loop finished"),
            Err(e) => {
                error!(error = ?e, "PITR archiver loop exited unexpectedly");
                telemetry.send(TelemetryEvent::ComponentError {
                    component: "mysql-wrapper".to_string(),
                    error: format!("PITR archiver loop exited unexpectedly: {e}"),
                    context: "pitr_archiver_loop".to_string(),
                });
            }
        }
    }
}

/// Read the root's owner record, judge it (pitr::archive_ownership_verdict)
/// and write the claim or the updated record. A bucket read or write that
/// fails is retried a few times, then fails this boot: an unreadable record
/// is never taken for an absent one.
async fn ensure_archive_ownership(
    s3: &S3Client,
    location: &S3Location,
    config: &Config,
    sql: &Sql,
    mode: ArchiveMode,
) -> Result<()> {
    const ATTEMPTS: u32 = 5;
    let key = pitr::owner_key(location);
    let root = pitr::base_prefix(location);
    let history = crate::gr::resolve_group_name(config);
    let environment_id = common::RailwayEnv::environment_id();
    let service_id = common::RailwayEnv::service_id();
    // A standalone server's own history is what tells a reverted member
    // from a stranger; unreadable reads as empty and the rules stay strict.
    let executed = sql.executed_gtid_set().await.unwrap_or_default();
    let me = pitr::ArchiveClaimant {
        environment_id: &environment_id,
        service_id: &service_id,
        history: &history,
        group_primary: mode == ArchiveMode::GroupPrimary,
        executed_gtid_set: &executed,
    };
    let mut last_err = None;
    for attempt in 1..=ATTEMPTS {
        let outcome: Result<()> = async {
            let existing: Option<pitr::ArchiveOwner> = if s3.exists(&key).await? {
                let bytes = s3.get_object_bytes(&key).await?;
                Some(serde_json::from_slice(&bytes).with_context(|| {
                    format!("{key} is not a readable owner record; delete it to let this service claim the root")
                })?)
            } else {
                None
            };
            match pitr::archive_ownership_verdict(&root, existing.as_ref(), &me, Utc::now()) {
                pitr::OwnershipVerdict::Keep => Ok(()),
                pitr::OwnershipVerdict::Write(record) => {
                    let json = serde_json::to_vec_pretty(&record)
                        .context("serializing the archive owner record")?;
                    s3.put_object_bytes(&key, json)
                        .await
                        .with_context(|| format!("writing {key}"))?;
                    info!(
                        %key,
                        environment_id = %record.environment_id,
                        services = ?record.service_ids,
                        "archive root recorded as this service's"
                    );
                    Ok(())
                }
                pitr::OwnershipVerdict::Refuse(reason) => Err(anyhow::anyhow!(reason)),
            }
        }
        .await;
        match outcome {
            Ok(()) => return Ok(()),
            Err(e) => {
                // A refusal is a verdict, not a transient: no retry.
                if e.to_string().contains("owner.json") {
                    return Err(e);
                }
                warn!(error = %e, attempt, "could not settle the archive root's ownership; retrying");
                last_err = Some(e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
    Err(last_err
        .unwrap_or_else(|| anyhow::anyhow!("could not settle the archive root's ownership")))
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

/// The newest complete full's `taken_at` in the part of the archive this
/// mode's cadence is measured against: this server's own lineage standalone,
/// every lineage for a group primary (see the module doc). Read off the
/// listing alone — the instant is encoded in the object name.
async fn newest_full_taken_at(
    s3: &S3Client,
    location: &S3Location,
    server_uuid: &str,
    mode: ArchiveMode,
) -> Result<Option<DateTime<Utc>>> {
    let prefix = match mode {
        ArchiveMode::Standalone => pitr::full_prefix(location, server_uuid),
        ArchiveMode::GroupPrimary => pitr::base_prefix(location),
    };
    let keys = s3.list_keys_with_prefix(&prefix).await?;
    Ok(keys
        .iter()
        .filter(|k| k.ends_with(".meta.json"))
        .filter_map(|k| pitr::full_taken_at_from_key(k))
        .max())
}

async fn full_backup_loop(
    config: Arc<Config>,
    sql: Sql,
    telemetry: Arc<Telemetry>,
    s3: S3Client,
    location: S3Location,
    server_uuid: String,
    mode: ArchiveMode,
    status: Arc<PitrStatus>,
) {
    let interval = Duration::from_secs(config.binlog_full_backup_interval_seconds);

    loop {
        // Re-read the archive every cycle rather than sleeping a fixed
        // interval from our own last dump: in group mode another member may
        // have taken a full while this one waited (or was a secondary), and
        // that full resets the cadence for everyone.
        let now = Utc::now();
        let (newest, listing_failed) = match newest_full_taken_at(
            &s3,
            &location,
            &server_uuid,
            mode,
        )
        .await
        {
            Ok(newest) => (newest, false),
            Err(e) => {
                warn!(error = %e, "could not check the archive for an existing full backup; assuming none and taking one now");
                (None, true)
            }
        };
        let not_due_for = if let Some(taken_at) = newest {
            let due_at = taken_at + chrono::Duration::from_std(interval).unwrap_or_default();
            if due_at > now {
                Some((due_at - now).to_std().unwrap_or(interval).min(interval))
            } else {
                None
            }
        } else {
            None
        };

        let (kind, run) = if let Some((run, kind)) = status.begin_requested_full_backup() {
            (kind, run)
        } else if let Some(wait) = not_due_for {
            tokio::select! {
                _ = tokio::time::sleep(wait) => {
                    let Some(run) = status.begin_scheduled_full_backup() else {
                        continue;
                    };
                    ("scheduled", run)
                }
                _ = status.wait_for_full_backup_request() => {
                    let Some((run, kind)) = status.begin_requested_full_backup() else {
                        // A stale notification can survive a role change;
                        // only the atomic queued state authorizes a dump.
                        continue;
                    };
                    (kind, run)
                }
            }
        } else {
            let Some(run) = status.begin_scheduled_full_backup() else {
                continue;
            };
            (
                if newest.is_none() && !listing_failed {
                    "initial"
                } else {
                    "scheduled"
                },
                run,
            )
        };
        // Read before the dump opens its snapshot: only a dump that STARTED
        // after the loss was recorded is proof the gap is closed. One that was
        // already running took its snapshot before the hole and re-anchors
        // nothing, so it must not clear the flag.
        let anchors_a_gap = read_upload_state(&config.data_dir).gap_anchor_pending;
        let outcome = {
            let _run = run;
            take_full_backup(&config, &sql, &s3, &location, &server_uuid).await
        };
        match outcome {
            Ok(FullBackupOutcome::Deferred(reason)) => {
                // The server was busy in a way the dump would have made the
                // customer pay for. Nothing failed: ask again after the retry
                // delay, and nothing lands on /pitr or reaches the monitor.
                info!(reason, kind, "full backup deferred; retrying");
                tokio::select! {
                    _ = tokio::time::sleep(FULL_BACKUP_RETRY_DELAY) => {}
                    _ = status.wait_for_full_backup_request() => {}
                }
            }
            Ok(FullBackupOutcome::Taken(taken_at)) => {
                status.update(|s| {
                    s.last_full_backup_at = Some(pitr::format_rfc3339_millis(taken_at));
                    // A full that landed proves the bucket, the credentials
                    // and mysqldump all work again: whatever the last loop
                    // failure was, it is over. Left in place it would read as
                    // a live fault forever (nothing else clears it).
                    s.last_error = None;
                });
                if anchors_a_gap {
                    clear_gap_anchor(&config.data_dir);
                }
                // Both spellings are load-bearing for the e2e harness, which
                // waits on them by name.
                if kind == "initial" {
                    info!("initial full backup completed");
                } else if kind == "triggered" {
                    info!("triggered full backup completed");
                } else if kind == "gap-recovery" {
                    info!(
                        "gap-recovery full backup completed — the archive is anchored past the \
                         lost binlog again"
                    );
                } else {
                    info!("scheduled full backup completed");
                }
            }
            Err(e) => {
                error!(error = %e, kind, "full backup failed; retrying");
                status.note_error(&e);
                telemetry.send(TelemetryEvent::ComponentError {
                    component: "mysql-wrapper".to_string(),
                    error: e.to_string(),
                    context: "pitr_full_backup".to_string(),
                });
                tokio::select! {
                    _ = tokio::time::sleep(FULL_BACKUP_RETRY_DELAY) => {}
                    _ = status.wait_for_full_backup_request() => {}
                }
            }
        }
    }
}

/// How long the dump gets to open its snapshot once mysqldump is started —
/// the window the global read lock stays up. mysqldump opens the snapshot
/// before it writes its first database, within a second on any server; a
/// dump that has not by then is killed and this full fails (the loop
/// retries) rather than holding the customer's writes any longer.
const SNAPSHOT_OPEN_WAIT: Duration = Duration::from_secs(60);

/// What one pass of the full-backup loop did.
enum FullBackupOutcome {
    Taken(DateTime<Utc>),
    /// The server was busy in a way a dump would have made the customer pay
    /// for: a statement the global read lock would have stalled every write
    /// behind, or that lock's own wait running out. Nothing is wrong — the
    /// loop asks again after the retry delay, and nothing lands on /pitr.
    Deferred(String),
}

/// Would `FLUSH TABLES WITH READ LOCK` wait on this statement past its own
/// bound? The lock waits for every statement already running, and every new
/// write queues behind it while it waits: a statement that has already run
/// as long as the lock is willing to wait is one the lock will time out on —
/// after stalling writes for the whole wait. Stepping aside beforehand costs
/// no one anything.
fn read_lock_would_wait(running: &RunningStatement, wait: Duration) -> bool {
    running.seconds >= wait.as_secs()
}

/// `mysqldump --single-transaction --routines --events --triggers
/// --all-databases`, gzipped, streamed to S3 as it's produced — nothing here
/// buffers the whole (potentially huge) dump.
///
/// The binlog coordinates the full starts replay from are read by the
/// wrapper itself, under a global read lock that stands until mysqldump has
/// opened its snapshot, and become this full's `meta.json`; the GTID set the
/// dump holds is scanned out of the first [`COORD_SCAN_CAP`] bytes of
/// mysqldump's own output (its `SET @@GLOBAL.GTID_PURGED`).
///
/// The lock order is the one MySQL Shell's dump utility uses, and it is not
/// interchangeable:
///
///   0. No statement has been running longer than the read lock would wait
///      (`Sql::longest_running_statement`); otherwise this attempt is
///      deferred before anyone is made to wait.
///   1. `FLUSH TABLES WITH READ LOCK` — waits (at most
///      [`GLOBAL_READ_LOCK_WAIT`]) for statements already running; while it
///      waits, and until it is released, every new write queues behind it.
///      A wait that runs out defers the attempt.
///   2. `LOCK INSTANCE FOR BACKUP` — immediate under the read lock (no DDL
///      can be in flight), held across the whole dump AND its upload, since
///      the dump is streamed and mysqldump cannot finish before the bucket
///      has taken its output: DDL, `TRUNCATE`, account statements and
///      `PURGE BINARY LOGS` wait, DML flows, and `--single-transaction`
///      reads one consistent snapshot. Every multipart call is bounded
///      (s3.rs), so the hold is bounded by a throughput floor, not by the
///      bucket's goodwill; the hold's length is logged on release.
///   3. `SHOW BINARY LOG STATUS` on the read-locking session: nothing
///      commits while it stands, so these are the coordinates of the
///      snapshot mysqldump opens next.
///   4. mysqldump starts; the read lock is released the moment its output
///      shows the snapshot is open (its first per-database line). Writes
///      flow again from here — normally well under a second after step 1.
///
/// INVARIANT: mysqldump 8.4 takes a `FLUSH TABLES WITH READ LOCK` of its own
/// on any GTID server under `--single-transaction`, `--source-data` or not.
/// That is harmless ONLY because the wrapper's read lock is already held
/// when mysqldump starts: no DDL can be holding a table open, so mysqldump's
/// flush has nothing to wait for. The other order — the backup lock first,
/// then a read lock — deadlocked under a DDL storm (e2e, 2026-09-11): an
/// ALTER queued on the backup lock already held its table open, FLUSH TABLES
/// waited for that table, and the dump never started while the storm's DDL
/// waited on the dump. Never release the wrapper's read lock before
/// mysqldump has opened its snapshot.
async fn take_full_backup(
    config: &Config,
    sql: &Sql,
    s3: &S3Client,
    location: &S3Location,
    server_uuid: &str,
) -> Result<FullBackupOutcome> {
    // Floored to the millisecond so the meta records exactly the instant the
    // object name carries: the platform reads that name to offer the oldest
    // restorable point, and restore judges "at or before" at the same
    // precision (`pitr::newest_qualifying_full`).
    let taken_at = pitr::floor_to_millis(Utc::now());
    let rfc = pitr::format_rfc3339_millis(taken_at);
    let dump_key = pitr::full_dump_key(location, server_uuid, &rfc);
    let meta_key = pitr::full_meta_key(location, server_uuid, &rfc);

    info!(%dump_key, "starting full backup");
    // Step 0: step aside before making anyone wait. The check is a courtesy
    // to the customer's traffic, not a gate on the backup — if it cannot be
    // read, the read lock's own bounded wait still holds.
    match sql.longest_running_statement().await {
        Ok(Some(running)) if read_lock_would_wait(&running, GLOBAL_READ_LOCK_WAIT) => {
            return Ok(FullBackupOutcome::Deferred(format!(
                "a {} statement has been running for {} s ({}); the global read lock would stall every write behind it",
                running.command, running.seconds, running.info_head
            )));
        }
        Ok(_) => {}
        Err(e) => {
            warn!(error = %e, "could not look for long-running statements before the global read lock; asking for it anyway")
        }
    }
    // Step 1: the global read lock, on a session of its own (see the order
    // above). Dropping the guard on any early exit below releases it.
    let mut read_lock = match sql
        .flush_tables_with_read_lock()
        .await
        .context("could not take the global read lock for the full backup's snapshot")?
    {
        ReadLockAttempt::Locked(lock) => lock,
        ReadLockAttempt::Busy(reason) => return Ok(FullBackupOutcome::Deferred(reason)),
    };
    // Step 2: held across the dump and its upload (the dump is streamed, so
    // the two end together): a concurrent ALTER/CREATE/DROP/RENAME/TRUNCATE
    // on a table being dumped makes `--single-transaction` read wrong
    // contents or fail, and a restore from such a full is wrong from its
    // base. DDL waits for the dump; DML does not wait on this lock. See
    // Sql::lock_instance_for_backup.
    let backup_lock = sql
        .lock_instance_for_backup()
        .await
        .context("could not take the instance backup lock for the full backup")?;
    let lock_taken = Instant::now();
    // Step 3: the coordinates of the snapshot about to open.
    let (binlog_file, binlog_pos) = read_lock
        .binary_log_status()
        .await
        .context("reading the binlog coordinates under the global read lock")?;
    info!(
        %binlog_file,
        binlog_pos,
        "holding the instance backup lock for the dump (DDL waits, DML flows)"
    );
    // Measured before the dump so it describes the data the dump captures,
    // not the binlogs the dump itself generates while running.
    let datadir_bytes = dir_size_bytes(&config.data_dir).await;

    let outcome = dump_and_upload(config, s3, &dump_key, read_lock).await;
    drop(backup_lock);
    let held_secs = lock_taken.elapsed().as_secs();
    let (scanned, dump_bytes) = match outcome {
        Ok(dumped) => {
            info!(held_secs, %dump_key, "instance backup lock released; the dump is in the bucket");
            dumped
        }
        Err(e) => {
            warn!(held_secs, %dump_key, "instance backup lock released; the dump did not complete");
            return Err(e);
        }
    };

    let dump_head = String::from_utf8_lossy(&scanned);
    // Present exactly when the source runs with GTIDs (every Group
    // Replication member does): the set of transactions the dump already
    // holds, by identity. Restore replays other lineages against it.
    let gtid_purged = pitr::parse_gtid_purged(&dump_head);
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
        gtid_purged,
        dump_bytes: Some(dump_bytes),
        datadir_bytes,
    };
    let meta_json = serde_json::to_vec_pretty(&meta).context("serializing full-backup meta")?;
    s3.put_object_bytes(&meta_key, meta_json)
        .await
        .context("uploading full-backup meta.json")?;
    info!(
        dump_bytes,
        datadir_bytes = ?datadir_bytes,
        "full backup sizes recorded in the meta (the platform's restore disk estimate reads them)"
    );

    Ok(FullBackupOutcome::Taken(taken_at))
}

/// Steps 4 onward of [`take_full_backup`]: run mysqldump under the locks the
/// caller holds, release the read lock the moment the snapshot is open, and
/// stream the gzipped dump to the bucket. Returns the scanned head of the
/// dump and its plaintext size. The caller owns the instance backup lock
/// for the whole call and releases it right after — on success and on every
/// error alike, with the hold's length logged.
async fn dump_and_upload(
    config: &Config,
    s3: &S3Client,
    dump_key: &str,
    read_lock: crate::sql::GlobalReadLock,
) -> Result<(Vec<u8>, u64)> {
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
        // The server's own GTID bookkeeping must never travel inside the
        // dump: `SET @@GLOBAL.GTID_PURGED` (which mysqldump emits for a GTID
        // source) is what carries the dump's GTID state, and a restore that
        // ALSO replayed this table's rows would collide with the rows the
        // restored server writes for the same GTIDs (duplicate primary key
        // on (source_uuid, interval_start)) — aborting the load. Empty on a
        // gtid_mode=OFF standalone, so this changes nothing there.
        .arg("--ignore-table=mysql.gtid_executed")
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
    // head of it for the GTID set and for the snapshot-open marker; then
    // stream gzip's output straight to S3 via a multipart upload (unbounded
    // length — no full-dump buffering on either side of the pipe).
    let (snapshot_open_tx, snapshot_open_rx) = oneshot::channel();
    let tee_task = tokio::spawn(tee_and_scan(
        dump_stdout,
        gzip_stdin,
        COORD_SCAN_CAP,
        Some(snapshot_open_tx),
    ));
    // Step 4: the read lock goes the moment the dump's snapshot is open —
    // mysqldump opens it before writing its first database, so that line in
    // its output is the proof, and nothing committed between the coordinates
    // above and the snapshot. The header alone is a few KiB; it reaches the
    // tee long before gzip's un-drained output could stall the pipe.
    match tokio::time::timeout(SNAPSHOT_OPEN_WAIT, snapshot_open_rx).await {
        Ok(Ok(())) => {
            read_lock
                .release()
                .await
                .context("releasing the global read lock once the dump's snapshot was open")?;
            info!("dump snapshot open at the recorded coordinates; global read lock released (writes flow)");
        }
        // The tee ended before any per-database line: mysqldump exited early.
        // Release the lock now; its exit status below says why.
        Ok(Err(_)) => {
            read_lock
                .release()
                .await
                .context("releasing the global read lock after mysqldump ended early")?;
            warn!("mysqldump ended before opening a snapshot; the global read lock was released");
        }
        Err(_) => {
            let _ = mysqldump.kill().await;
            let _ = gzip.kill().await;
            drop(read_lock);
            anyhow::bail!(
                "mysqldump did not open its snapshot within {SNAPSHOT_OPEN_WAIT:?}; the global read lock was released and this full is abandoned"
            );
        }
    }
    // A failed upload drops gzip's stdout reader: gzip dies on the broken
    // pipe, the tee's write into it fails, and the `?` below ends this call
    // — mysqldump and gzip are killed on drop, and the caller releases the
    // backup lock. Nothing here can wait on a bucket that stopped answering
    // (every multipart call is bounded, s3.rs).
    let upload_result = s3.upload_multipart(dump_key, gzip_stdout).await;

    let (scanned, dump_bytes) = tee_task
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
    Ok((scanned, dump_bytes))
}

/// The first line mysqldump writes AFTER it has opened its snapshot: it
/// starts the transaction (`--single-transaction`), writes the header (the
/// `SET @@GLOBAL.GTID_PURGED` among it), and only then dumps databases. Any
/// of these therefore proves the snapshot is open; `--all-databases` always
/// produces the first one (the `mysql` schema at least).
const SNAPSHOT_OPEN_MARKERS: &[&[u8]] = &[
    b"\n-- Current Database: ",
    b"\n-- Table structure for table",
    b"\nCREATE TABLE ",
    b"\n-- Dumping events for database",
    b"\n-- Dump completed",
];

/// Whether `head` (the start of mysqldump's output) shows the snapshot open.
fn dump_shows_snapshot_open(head: &[u8]) -> bool {
    SNAPSHOT_OPEN_MARKERS
        .iter()
        .any(|m| head.windows(m.len()).any(|w| w == *m))
}

/// Copy `src` into `dst` byte-for-byte, capturing up to `scan_cap` bytes of
/// the earliest data read (for the GTID-set scan) without holding the rest
/// in memory. `snapshot_open`, when given, fires once the captured head
/// shows mysqldump has opened its snapshot (see SNAPSHOT_OPEN_MARKERS); it is
/// dropped unfired if the copy ends first.
async fn tee_and_scan(
    mut src: impl AsyncRead + Unpin,
    mut dst: impl AsyncWrite + Unpin,
    scan_cap: usize,
    mut snapshot_open: Option<oneshot::Sender<()>>,
) -> Result<(Vec<u8>, u64)> {
    let mut scanned = Vec::with_capacity(scan_cap.min(64 * 1024));
    let mut buf = [0u8; 64 * 1024];
    let mut total: u64 = 0;
    loop {
        let n = src
            .read(&mut buf)
            .await
            .context("reading mysqldump output")?;
        if n == 0 {
            break;
        }
        total += n as u64;
        dst.write_all(&buf[..n])
            .await
            .context("writing into gzip's stdin")?;
        if scanned.len() < scan_cap {
            let take = (scan_cap - scanned.len()).min(n);
            scanned.extend_from_slice(&buf[..take]);
            if snapshot_open.is_some() && dump_shows_snapshot_open(&scanned) {
                if let Some(tx) = snapshot_open.take() {
                    let _ = tx.send(());
                }
            }
        }
    }
    dst.shutdown().await.context("closing gzip's stdin")?;
    Ok((scanned, total))
}

/// Bytes under `dir`, recursively, as they are at this instant. `None` when
/// the walk fails part-way: a wrong figure in the meta would be worse than
/// none, since the platform sizes a restore's volume by it.
async fn dir_size_bytes(dir: &str) -> Option<u64> {
    let root = std::path::PathBuf::from(dir);
    tokio::task::spawn_blocking(move || {
        fn walk(path: &Path) -> std::io::Result<u64> {
            let mut total = 0u64;
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let meta = entry.metadata()?;
                if meta.is_dir() {
                    total += walk(&entry.path())?;
                } else if meta.is_file() {
                    total += meta.len();
                }
            }
            Ok(total)
        }
        walk(&root).ok()
    })
    .await
    .ok()
    .flatten()
}

// --- binlog shipping ---------------------------------------------------------

async fn binlog_shipping_loop(
    config: Arc<Config>,
    sql: Sql,
    telemetry: Arc<Telemetry>,
    s3: S3Client,
    location: S3Location,
    server_uuid: String,
    mode: ArchiveMode,
    status: Arc<PitrStatus>,
) {
    let mut reclaim_watch = ReclaimWatch::default();
    loop {
        if let Err(e) = ship_once(
            &config,
            &sql,
            &s3,
            &location,
            &server_uuid,
            mode,
            &status,
            &telemetry,
            &mut reclaim_watch,
        )
        .await
        {
            warn!(error = %e, "binlog shipping pass failed; retrying next cycle");
            status.note_error(&e);
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
            "BINLOG_RETENTION_DAYS=0; retention is opted out and the archive is never expired \
             (unbounded growth). Unset it, or set a positive horizon, to bound storage."
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
    // Fail the pass rather than guess: reading this as "absent" on an S3
    // error would apply the independent-histories rules to a shared history
    // and could retire a full-less primary lineage.
    let shared_history_marker = s3
        .exists(&pitr::shared_history_marker_key(location))
        .await
        .context("checking the archive for the shared-history marker")?;
    let input = pitr::RetentionInput {
        lineages,
        // Passing our OWN uuid is what makes "dead lineage" meaningful. The
        // planner refuses to expire anything when this is None.
        active_server_uuid: Some(server_uuid.to_string()),
        now,
        horizon,
        shared_history_marker,
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
    // RETENTION_MIN_OBJECT_AGE_SECONDS is deleted (the config field defaults
    // to exactly that constant; only a test workspace ever overrides it).
    let min_age = chrono::Duration::seconds(config.test_retention_min_object_age_seconds);
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
    let base = pitr::base_prefix(location);
    let objects = s3
        .list_objects_with_prefix(&base)
        .await
        .context("listing the PITR archive bucket for retention")?;

    struct Raw {
        metas: Vec<String>,
        dumps: BTreeSet<String>,
        binlogs: Vec<String>,
        binlog_ages: BTreeMap<String, DateTime<Utc>>,
    }
    let mut per_lineage: BTreeMap<String, Raw> = BTreeMap::new();

    for (key, modified) in &objects {
        let Some(uuid) = pitr::server_uuid_from_key(location, key) else {
            continue;
        };
        let entry = per_lineage.entry(uuid).or_insert_with(|| Raw {
            metas: Vec::new(),
            dumps: BTreeSet::new(),
            binlogs: Vec::new(),
            binlog_ages: BTreeMap::new(),
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
                if let Some(modified) = modified {
                    entry.binlog_ages.insert(name.to_string(), *modified);
                }
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
                    // Unreadable meta: leave it out of `fulls` so it is
                    // neither counted as retainable nor listed for deletion.
                    // `full_objects_seen` below is what stops the planner
                    // reading this omission as "the lineage has no fulls".
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
            // Every full-backup meta OBJECT the bucket holds, not just the
            // ones parsed above. This is what lets the planner tell "no fulls
            // here" apart from "its fulls exist but this pass could not read
            // them" — the second must never make a lineage's binlogs
            // expirable.
            full_objects_seen: raw.metas.len(),
            orphan_dumps,
            binlogs: raw.binlogs,
            binlog_ages: raw.binlog_ages,
        });
    }
    Ok(out)
}

/// One shipping pass: upload every closed binlog not yet uploaded, then
/// reclaim local disk by the mode's rule — everything uploaded standalone,
/// everything uploaded AND past the group's recovery window on a primary
/// (module doc).
#[allow(clippy::too_many_arguments)]
async fn ship_once(
    config: &Config,
    sql: &Sql,
    s3: &S3Client,
    location: &S3Location,
    server_uuid: &str,
    mode: ArchiveMode,
    status: &PitrStatus,
    telemetry: &Telemetry,
    reclaim_watch: &mut ReclaimWatch,
) -> Result<()> {
    let full_interval = Duration::from_secs(config.binlog_full_backup_interval_seconds);
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
            // The hole is permanent, but the server still holds everything
            // the archive lost, so a full taken NOW re-anchors the chain past
            // it: restores to any target after that dump work again, with the
            // rows the missing file carried. Persisted rather than kept in
            // memory so a crash between here and the dump still re-anchors on
            // the next boot (requested again at the top of every pass below).
            state.gap_anchor_pending = true;
            write_upload_state(&config.data_dir, &state)?;
            error!(
                file = %name,
                "binlog lost from disk before upload — the archive lineage now has a \
                 permanent gap at this file; point-in-time restores past it will refuse \
                 rather than silently lose the data after it. Taking a full backup now to \
                 re-anchor the archive past the gap"
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
        status.update(|s| {
            s.last_shipped_binlog = Some(name.clone());
            s.last_shipped_at = Some(pitr::format_rfc3339_millis(Utc::now()));
            // An upload that landed is the recovery the platform monitor
            // (and an operator reading /pitr) needs to see: a failure that
            // stays on the status after shipping resumed is a false alarm.
            s.last_error = None;
        });
    }

    // Asked once per pass, not once per loss: the request latch makes a
    // redundant ask a no-op, and the flag only clears when a dump that STARTED
    // after the loss has landed. That is what carries the re-anchor across a
    // crash, a restart, or a dump that failed — a gap that never got its full
    // is retried every SHIP_POLL until one does.
    if state.gap_anchor_pending && status.request_gap_recovery_full_backup().is_ok() {
        info!("queued a full backup to re-anchor the archive past the lost binlog");
    }

    let cut = match mode {
        ArchiveMode::Standalone => {
            pitr::purge_cut(&disk_files, &active, &state.uploaded, &state.lost)
        }
        ArchiveMode::GroupPrimary => pitr::purge_cut_retaining_recent(
            &disk_files,
            &active,
            &state.uploaded,
            &state.lost,
            &|name| file_age(&config.data_dir, name),
            Duration::from_secs(pitr::GR_BINLOG_EXPIRE_SECONDS),
        ),
    };
    let cut = match reclaim_decision(cut, sql.backup_lock_held()) {
        Reclaim::Nothing => {
            end_reclaim_deferral(reclaim_watch, status);
            return Ok(());
        }
        Reclaim::Defer(cut) => {
            // Our own full backup is dumping under LOCK INSTANCE FOR BACKUP,
            // which refuses PURGE BINARY LOGS outright (manual §15.3.5;
            // server error 4085). Everything above already shipped; the files
            // stay on disk until the next pass, which is not a failure and
            // must not be reported as one — a shipping-loop Err lands on
            // /pitr's last_error and reaches the platform monitor. Only a
            // dump older than the interval between fulls is named (below).
            info!(
                cut = %cut,
                "binlog reclaim deferred: this node's full backup holds the instance \
                 backup lock; the next shipping pass reclaims"
            );
            if let Some(reason) =
                reclaim_watch.deferred(DeferralCause::OwnFull, full_interval, Instant::now())
            {
                report_reclaim_fault(&reason, status, telemetry);
            }
            return Ok(());
        }
        Reclaim::Purge(cut) => cut,
    };
    match sql
        .purge_binary_logs_to(&cut)
        .await
        .with_context(|| format!("PURGE BINARY LOGS TO {cut}"))?
    {
        crate::sql::PurgeOutcome::DeferredByBackupLock => {
            // Another session's backup lock: `backup_lock_held` reads false
            // only once our own UNLOCK INSTANCE has run, so this is a lock
            // that is not this node's full — a customer-run backup tool, or
            // (rarely) a full of ours that started between the flag read and
            // the PURGE. One pass is given the benefit of the doubt; a second
            // in a row is a fault worth naming, because nothing else would
            // ever say why the volume keeps growing.
            info!(
                cut = %cut,
                "binlog reclaim deferred: a session holds the instance backup lock; \
                 the next shipping pass reclaims"
            );
            if let Some(reason) =
                reclaim_watch.deferred(DeferralCause::Foreign, full_interval, Instant::now())
            {
                report_reclaim_fault(&reason, status, telemetry);
            }
            return Ok(());
        }
        crate::sql::PurgeOutcome::Purged => end_reclaim_deferral(reclaim_watch, status),
    }
    {
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

/// What the shipping pass does with the reclaim cut it computed.
#[derive(Debug, PartialEq, Eq)]
enum Reclaim {
    /// Nothing uploaded is reclaimable yet.
    Nothing,
    /// This node's full backup holds `LOCK INSTANCE FOR BACKUP`, under which
    /// `PURGE BINARY LOGS` is refused (not queued): keep the uploaded files
    /// for one more pass instead of reporting a failure.
    Defer(String),
    Purge(String),
}

/// Reclaiming is the only step of a shipping pass that the archiver's own
/// backup lock forbids, so it is the only step the lock defers — the uploads
/// before it are unaffected and already recorded.
fn reclaim_decision(cut: Option<String>, backup_lock_held: bool) -> Reclaim {
    match cut {
        None => Reclaim::Nothing,
        Some(cut) if backup_lock_held => Reclaim::Defer(cut),
        Some(cut) => Reclaim::Purge(cut),
    }
}

/// Why a reclaim was deferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeferralCause {
    /// This node's own full backup holds the lock (`Sql::backup_lock_held`).
    OwnFull,
    /// Some other session's `LOCK INSTANCE FOR BACKUP`: server error 4085
    /// with no full of ours holding the lock.
    Foreign,
}

/// The first words of the `last_error` a deferral that outlived its excuse
/// writes to /pitr — and the key by which the next successful reclaim
/// clears it, and nothing else.
const RECLAIM_DEFERRED_ERROR_PREFIX: &str = "binlog reclaim deferred";

/// One episode of deferred reclaims, across shipping passes. A deferral is
/// nothing to report while it is this node's own full doing the deferring
/// and that full is younger than the interval fulls are due at. It becomes
/// a fault to name — on /pitr, where the platform reads archive trouble —
/// when a session that is NOT this node's full holds the lock for two
/// passes in a row (a customer's backup tool that never unlocked), or when
/// even our own full has held it past the interval between fulls. Uploaded
/// binlogs are safe either way; what grows is the volume, silently, and
/// this is the only place that would ever say why.
#[derive(Debug, Default)]
struct ReclaimWatch {
    since: Option<Instant>,
    passes: u32,
    escalated: bool,
}

impl ReclaimWatch {
    /// Record one deferred pass. `Some(reason)` exactly once per episode,
    /// the pass it crosses from "expected" into "a fault to name".
    fn deferred(
        &mut self,
        cause: DeferralCause,
        full_interval: Duration,
        now: Instant,
    ) -> Option<String> {
        let since = *self.since.get_or_insert(now);
        self.passes += 1;
        if self.escalated {
            return None;
        }
        let elapsed = now.saturating_duration_since(since);
        let fault = match cause {
            DeferralCause::Foreign => self.passes >= 2,
            DeferralCause::OwnFull => elapsed > full_interval,
        };
        if !fault {
            return None;
        }
        self.escalated = true;
        Some(match cause {
            DeferralCause::Foreign => format!(
                "{RECLAIM_DEFERRED_ERROR_PREFIX} for {} passes ({} s): a session other than this node's \
                 full backup holds LOCK INSTANCE FOR BACKUP, under which PURGE BINARY LOGS is refused \
                 (server error 4085); uploaded binlogs stay on the volume until it is released",
                self.passes,
                elapsed.as_secs()
            ),
            DeferralCause::OwnFull => format!(
                "{RECLAIM_DEFERRED_ERROR_PREFIX} for {} s: this node's own full backup has held LOCK \
                 INSTANCE FOR BACKUP longer than the interval between fulls ({} s); uploaded binlogs \
                 stay on the volume until the dump ends",
                elapsed.as_secs(),
                full_interval.as_secs()
            ),
        })
    }

    /// A pass that reclaimed, or found nothing left to reclaim, ends the
    /// episode. `true` when a fault had been named and is now over.
    fn resolved(&mut self) -> bool {
        let named = self.escalated;
        *self = Self::default();
        named
    }
}

/// The deferral crossed into a fault: say so once, where the platform reads
/// archive trouble (`/pitr.last_error`, telemetry) and in the log.
fn report_reclaim_fault(reason: &str, status: &PitrStatus, telemetry: &Telemetry) {
    warn!(
        reason,
        "binlog reclaim deferred past what a full backup of ours explains"
    );
    status.note_text(reason.to_string());
    telemetry.send(TelemetryEvent::ComponentError {
        component: "mysql-wrapper".to_string(),
        error: reason.to_string(),
        context: "pitr_binlog_reclaim".to_string(),
    });
}

/// The episode is over: if it had been named on /pitr, take the name back —
/// and only that name, never a different live fault.
fn end_reclaim_deferral(watch: &mut ReclaimWatch, status: &PitrStatus) {
    if watch.resolved() {
        info!("binlog reclaim resumed; the deferral named on /pitr is over");
        status.clear_error_if(|text| text.starts_with(RECLAIM_DEFERRED_ERROR_PREFIX));
    }
}

/// A binlog's age on disk by its mtime — the group primary's stand-in for
/// mysqld's expiry clock (`pitr::purge_cut_retaining_recent`). `None` when
/// it cannot be read; the caller then keeps the file.
fn file_age(data_dir: &str, name: &str) -> Option<Duration> {
    let modified = std::fs::metadata(Path::new(data_dir).join(name))
        .ok()?
        .modified()
        .ok()?;
    std::time::SystemTime::now().duration_since(modified).ok()
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
    /// A loss has been recorded and no full backup taken since: the archive
    /// still has no restorable point after the gap. Cleared only by a full
    /// that started after the loss (`full_backup_loop`), so the re-anchor
    /// survives a crash or a failed dump.
    #[serde(default)]
    gap_anchor_pending: bool,
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

/// The gap is closed: a full backup that started after the loss has landed, so
/// the archive has a restorable point past it again. A write failure here only
/// costs a redundant full on the next boot, which is why it warns rather than
/// failing the loop.
fn clear_gap_anchor(data_dir: &str) {
    let mut state = read_upload_state(data_dir);
    if !state.gap_anchor_pending {
        return;
    }
    state.gap_anchor_pending = false;
    if let Err(e) = write_upload_state(data_dir, &state) {
        warn!(error = %e, "could not clear the PITR gap-anchor flag; a redundant full backup may be taken");
    }
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

    #[test]
    fn full_backup_trigger_requires_an_active_archiver_and_queues_once() {
        let disabled = PitrStatus::new(false);
        assert_eq!(
            disabled.request_full_backup(),
            Err("PITR archiving is not configured")
        );

        let inactive = PitrStatus::new(true);
        assert_eq!(
            inactive.request_full_backup(),
            Err("this node is not currently archiving")
        );

        inactive.mark_archiving_for_test();
        assert_eq!(inactive.request_full_backup(), Ok(()));
        assert_eq!(
            inactive.request_full_backup(),
            Err("a full backup is already queued or running")
        );
        let (run, kind) = inactive
            .begin_requested_full_backup()
            .expect("queued request starts");
        assert_eq!(kind, "triggered");
        assert_eq!(
            inactive.request_full_backup(),
            Err("a full backup is already queued or running")
        );
        drop(run);
        assert_eq!(inactive.request_full_backup(), Ok(()));
    }

    /// A lost binlog asks for its own full, and the run that consumes it knows
    /// it is closing a gap — the reason must not leak into the NEXT request,
    /// which would mislabel an operator's own trigger as gap recovery.
    #[test]
    fn a_lost_binlog_queues_a_gap_recovery_full_backup() {
        let status = PitrStatus::new(true);
        status.mark_archiving_for_test();

        assert_eq!(status.request_gap_recovery_full_backup(), Ok(()));
        // The endpoint asking meanwhile does not get a second dump, and does
        // not overwrite the reason the queued one already carries.
        assert_eq!(
            status.request_full_backup(),
            Err("a full backup is already queued or running")
        );
        let (run, kind) = status
            .begin_requested_full_backup()
            .expect("queued request starts");
        assert_eq!(kind, "gap-recovery");
        drop(run);

        assert_eq!(status.request_full_backup(), Ok(()));
        let (_run, kind) = status
            .begin_requested_full_backup()
            .expect("queued request starts");
        assert_eq!(kind, "triggered");
    }

    /// Losing the role mid-gap must not leave the reason armed: the next
    /// primary's first endpoint-triggered full would report as gap recovery.
    #[test]
    fn stopping_the_archiver_disarms_a_pending_gap_recovery_reason() {
        let status = PitrStatus::new(true);
        status.mark_archiving_for_test();
        assert_eq!(status.request_gap_recovery_full_backup(), Ok(()));

        status.stop_archiving();
        status.mark_archiving_for_test();

        assert_eq!(status.request_full_backup(), Ok(()));
        let (_run, kind) = status
            .begin_requested_full_backup()
            .expect("queued request starts");
        assert_eq!(kind, "triggered");
    }

    #[test]
    fn dropping_a_running_backup_releases_the_latch() {
        let status = PitrStatus::new(true);
        status.mark_archiving_for_test();
        let run = status
            .begin_scheduled_full_backup()
            .expect("scheduled backup starts");
        drop(run);
        assert_eq!(status.request_full_backup(), Ok(()));
    }

    #[test]
    fn reclaim_is_deferred_while_this_node_holds_the_backup_lock() {
        assert_eq!(
            reclaim_decision(Some("binlog.000007".to_string()), true),
            Reclaim::Defer("binlog.000007".to_string())
        );
    }

    #[test]
    fn reclaim_runs_when_no_backup_lock_is_held() {
        assert_eq!(
            reclaim_decision(Some("binlog.000007".to_string()), false),
            Reclaim::Purge("binlog.000007".to_string())
        );
    }

    #[test]
    fn no_reclaim_cut_means_nothing_to_do_lock_or_not() {
        assert_eq!(reclaim_decision(None, true), Reclaim::Nothing);
        assert_eq!(reclaim_decision(None, false), Reclaim::Nothing);
    }

    #[test]
    fn our_own_full_defers_reclaim_silently_within_the_interval_between_fulls() {
        let mut watch = ReclaimWatch::default();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let interval = Duration::from_secs(3600);
        for pass in 0..6u64 {
            let verdict = watch.deferred(DeferralCause::OwnFull, interval, at(pass * 10));
            assert_eq!(verdict, None);
        }
        // Nothing was named, so nothing is taken back.
        assert!(!watch.resolved());
    }

    #[test]
    fn our_own_full_holding_the_lock_past_the_interval_is_named_once() {
        let mut watch = ReclaimWatch::default();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let interval = Duration::from_secs(60);
        let own = DeferralCause::OwnFull;
        assert_eq!(watch.deferred(own, interval, at(0)), None);
        assert_eq!(watch.deferred(own, interval, at(30)), None);
        let named = watch
            .deferred(own, interval, at(61))
            .expect("past the interval the deferral is a fault");
        assert!(named.starts_with(RECLAIM_DEFERRED_ERROR_PREFIX));
        assert!(named.contains("own full backup"));
        // Once per episode: the next pass says nothing new.
        assert_eq!(watch.deferred(own, interval, at(70)), None);
        // A named fault is taken back when reclaim resumes.
        assert!(watch.resolved());
    }

    #[test]
    fn a_foreign_backup_lock_gets_one_pass_of_doubt_then_is_named() {
        let mut watch = ReclaimWatch::default();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let interval = Duration::from_secs(86400);
        let foreign = DeferralCause::Foreign;
        // The first refused PURGE may be our own full that started between the
        // flag read and the statement — not a fault yet.
        assert_eq!(watch.deferred(foreign, interval, at(0)), None);
        let named = watch
            .deferred(foreign, interval, at(10))
            .expect("a second refused pass in a row names the foreign lock");
        assert!(named.starts_with(RECLAIM_DEFERRED_ERROR_PREFIX));
        assert!(named.contains("other than this node's full backup"));
        assert!(named.contains("4085"));
        assert_eq!(watch.deferred(foreign, interval, at(20)), None);
    }

    #[test]
    fn a_reclaim_that_succeeds_ends_the_episode_and_a_new_one_starts_clean() {
        let mut watch = ReclaimWatch::default();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let interval = Duration::from_secs(86400);
        let foreign = DeferralCause::Foreign;
        assert_eq!(watch.deferred(foreign, interval, at(0)), None);
        // A purge went through: one refused pass was never a fault.
        assert!(!watch.resolved());
        // The next episode starts from zero — again one pass of doubt.
        assert_eq!(watch.deferred(foreign, interval, at(100)), None);
    }

    #[test]
    fn the_read_lock_steps_aside_for_a_statement_as_old_as_its_own_wait() {
        let wait = Duration::from_secs(5);
        let running = |seconds| RunningStatement {
            seconds,
            command: "Query".to_string(),
            info_head: "ALTER TABLE t.big ADD COLUMN c INT".to_string(),
        };
        assert!(!read_lock_would_wait(&running(0), wait));
        assert!(!read_lock_would_wait(&running(4), wait));
        assert!(read_lock_would_wait(&running(5), wait));
        assert!(read_lock_would_wait(&running(3600), wait));
    }

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

    /// The pending re-anchor is what survives a restart: a state file written
    /// before the field existed still parses (no gap pending), a recorded gap
    /// reads back as pending, and only a landed full clears it.
    #[test]
    fn the_gap_anchor_flag_persists_until_a_full_backup_clears_it() {
        let dir = temp_dir("gap-anchor");

        std::fs::write(
            upload_state_path(&dir),
            r#"{"uploaded":["binlog.000001"],"lost":[]}"#,
        )
        .unwrap();
        assert!(!read_upload_state(&dir).gap_anchor_pending);

        let mut state = read_upload_state(&dir);
        state.lost.insert("binlog.000002".to_string());
        state.gap_anchor_pending = true;
        write_upload_state(&dir, &state).unwrap();
        assert!(read_upload_state(&dir).gap_anchor_pending);

        clear_gap_anchor(&dir);
        let after = read_upload_state(&dir);
        assert!(!after.gap_anchor_pending);
        // Clearing the re-anchor never forgets the loss itself: the hole is
        // permanent and the restore side still has to refuse across it.
        assert!(after.lost.contains("binlog.000002"));
        assert!(after.uploaded.contains("binlog.000001"));

        std::fs::remove_dir_all(&dir).ok();
    }
}
