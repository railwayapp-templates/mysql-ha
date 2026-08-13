//! Image-owned recovery for a member that cannot reach a connectable/ONLINE
//! state on its own: a datadir mysqld can no longer boot (InnoDB
//! crash-recovery loop, corrupted system files), or a live member wedged in
//! ERROR / RECOVERING-without-progress while the rest of the group is
//! healthy. Without this, such a node stays broken until something outside
//! the container replaces it.
//!
//! Two detectors, one remedy — discard the local copy and reprovision from
//! the group, through the same donor-pick/clone machinery the divergence
//! self-heal uses (see gr.rs):
//!
//!   - Boot-loop (`preboot` + `boot_watch`): consecutive supervised mysqld
//!     starts that never reached accepting-connections, counted in a volume
//!     marker (each failed start exits the container, so the count must
//!     survive restarts). At the threshold, the pre-spawn check discards the
//!     wedged datadir; the next boot initializes empty and provisions from
//!     the live group exactly like a brand-new node.
//!   - Stuck-live (`stuck_watch`): mysqld answers, but this member sits in
//!     ERROR, or in RECOVERING with no observable progress, past a dwell.
//!     Remedy in place: stop the plugin, clone from a live donor; the clone
//!     recipient shuts down and the restarted boot rejoins on the cloned
//!     data.
//!
//! Safety gates, shared by both detectors:
//!
//!   1. NEVER act unless a peer answers /role 200 RIGHT NOW — a
//!      quorum-confirmed ONLINE primary (the endpoint fails closed, see
//!      health_server.rs). Group Replication's majority rule guarantees
//!      every committed transaction lives on that side; only then is the
//!      local copy expendable.
//!   2. With no /role 200 anywhere (total outage), the bootstrap guard's
//!      recovery elects the most advanced dataset — the wedged copy may be
//!      it. Both detectors keep detecting and do nothing: fail closed.
//!   3. Attempts are capped and backed off (a clone is heavy on the donor),
//!      with the ledger persisted on the volume so container restarts don't
//!      reset it. Past the cap the node keeps booting for observability but
//!      never discards data again.
//!   4. A slow member is not a stuck member: RECOVERING with advancing
//!      GTIDs, recovery streaming, or clone progress resets the dwell; a
//!      boot grinding through a long crash recovery is only cut short while
//!      gate 1 holds (see boot_watch).

use crate::config::Config;
use crate::gr;
use crate::sql::Sql;
use anyhow::{Context, Result};
use common::{Telemetry, TelemetryEvent};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{error, info, warn};

const BOOT_ATTEMPTS_MARKER: &str = ".railway_boot_attempts";
const HEAL_LEDGER_MARKER: &str = ".railway_selfheal_attempts";
const STUCK_POLL: Duration = Duration::from_secs(5);
const BOOT_WATCH_POLL: Duration = Duration::from_secs(3);
/// ONLINE this long continuously closes the incident: the attempt ledger
/// resets, so an unrelated incident months later starts with a full budget
/// instead of inheriting a spent one.
const HEALTHY_LEDGER_RESET: Duration = Duration::from_secs(3600);

fn boot_attempts_path(data_dir: &str) -> std::path::PathBuf {
    Path::new(data_dir).join(BOOT_ATTEMPTS_MARKER)
}

fn ledger_path(data_dir: &str) -> std::path::PathBuf {
    Path::new(data_dir).join(HEAL_LEDGER_MARKER)
}

/// Consecutive supervised mysqld starts that never reached
/// accepting-connections. 0 when absent or unreadable — degrading to "no
/// failures" can only delay a heal, never trigger one spuriously.
pub fn read_boot_attempts(data_dir: &str) -> u32 {
    std::fs::read_to_string(boot_attempts_path(data_dir))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn persist_boot_attempts(data_dir: &str, attempts: u32) -> Result<()> {
    let path = boot_attempts_path(data_dir);
    std::fs::write(&path, format!("{attempts}\n"))
        .with_context(|| format!("writing {}", path.display()))
}

/// The persisted record of self-heal attempts: how many times this volume's
/// data has been discarded-and-reprovisioned, and when the last attempt ran.
/// Lives on the volume so container restarts can't reset the cap; degrades
/// to zero on absence or garbage (never panics, never blocks a first heal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HealLedger {
    pub attempts: u32,
    pub last_unix: u64,
}

impl HealLedger {
    fn bump(self, now_unix: u64) -> Self {
        Self {
            attempts: self.attempts.saturating_add(1),
            last_unix: now_unix,
        }
    }
}

pub fn read_ledger(data_dir: &str) -> HealLedger {
    let Ok(raw) = std::fs::read_to_string(ledger_path(data_dir)) else {
        return HealLedger::default();
    };
    let mut parts = raw.split_whitespace();
    let attempts = parts.next().and_then(|s| s.parse().ok());
    let last_unix = parts.next().and_then(|s| s.parse().ok());
    match (attempts, last_unix) {
        (Some(attempts), Some(last_unix)) => HealLedger {
            attempts,
            last_unix,
        },
        _ => HealLedger::default(),
    }
}

fn persist_ledger(data_dir: &str, ledger: HealLedger) -> Result<()> {
    let path = ledger_path(data_dir);
    std::fs::write(&path, format!("{} {}\n", ledger.attempts, ledger.last_unix))
        .with_context(|| format!("writing {}", path.display()))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Seconds that must pass after attempt N before attempt N+1 may run:
/// base * 2^(N-1). The first attempt is immediate; the shift is capped so
/// a corrupt ledger can't overflow into "never again".
fn backoff_gap_seconds(base: u64, attempts: u32) -> u64 {
    if attempts == 0 {
        return 0;
    }
    base.saturating_mul(1u64 << u32::min(attempts - 1, 16))
}

/// The cap/backoff verdict over the persisted ledger — pure, so the exact
/// arithmetic is unit-tested. The donor gate is separate (and checked last,
/// closest to the destructive action).
#[derive(Debug, PartialEq, Eq)]
enum HealGate {
    /// Budget and pacing allow an attempt.
    Go,
    /// The attempt budget is spent; leave the data alone for inspection.
    CapReached,
    /// Inside the backoff window after the previous attempt.
    BackingOff,
}

fn heal_gate(ledger: HealLedger, cap: u32, backoff_base: u64, now_unix: u64) -> HealGate {
    if ledger.attempts >= cap {
        return HealGate::CapReached;
    }
    let gap = backoff_gap_seconds(backoff_base, ledger.attempts);
    if now_unix < ledger.last_unix.saturating_add(gap) {
        return HealGate::BackingOff;
    }
    HealGate::Go
}

/// The first declared peer whose /role answers 200 — a quorum-confirmed
/// ONLINE primary (fail-closed endpoint: it requires the answering node to
/// be ONLINE PRIMARY with a reachable majority in its own view). This is the
/// only proof strong enough to make the local copy expendable.
pub async fn quorum_confirmed_peer(
    client: &reqwest::Client,
    peer_hosts: &[String],
    health_port: u16,
    timeout: Duration,
) -> Option<String> {
    for host in peer_hosts {
        let url = format!("http://{host}:{health_port}/role");
        if let Ok(resp) = client.get(&url).timeout(timeout).send().await {
            if resp.status().is_success() {
                return Some(host.clone());
            }
        }
    }
    None
}

/// Remove every entry under the datadir. Called only pre-spawn (no mysqld
/// running) and only behind the donor gate. The datadir must end up EMPTY —
/// `mysqld --initialize` refuses anything else — which is also why the
/// bumped ledger can't be written back here: it rides in memory until
/// boot_watch persists it once mysqld is up (see PrebootState).
fn wipe_datadir(data_dir: &str) -> Result<()> {
    for entry in std::fs::read_dir(data_dir).with_context(|| format!("reading {data_dir}"))? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        }
        .with_context(|| format!("removing {}", path.display()))?;
    }
    Ok(())
}

/// Entries `mysqld --initialize` tolerates in an otherwise-empty datadir.
/// Verified against 8.4: lost+found passes, ANY other entry — dotfiles
/// included — aborts initialization ("data directory has files in it").
/// .snapshot is on the server's own tolerance list alongside lost+found.
const INIT_TOLERATED_ENTRIES: &[&str] = &["lost+found", ".snapshot"];

/// True when the datadir is one `mysqld --initialize` could still turn into
/// a fresh instance: empty, or nothing but tolerated entries. Planting a
/// marker file in such a datadir would abort that initialization — so no
/// boot accounting may touch it. Anything else (an initialized datadir, or
/// wreckage a crashed init left behind that already dooms re-init) is safe
/// to count on. Unreadable reads as fresh: never plant a marker blind.
fn init_could_succeed(data_dir: &str) -> bool {
    match std::fs::read_dir(data_dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .all(|e| INIT_TOLERATED_ENTRIES.contains(&e.file_name().to_string_lossy().as_ref())),
        Err(_) => true,
    }
}

/// What preboot hands the rest of the boot.
pub struct PrebootState {
    /// Set when this boot discarded a wedged datadir: the bumped attempt
    /// ledger, to be persisted the moment markers become writable (mysqld
    /// up). Until then the count lives only here — an interruption in that
    /// window under-counts by one, which the boot-loop threshold absorbs.
    pub pending_ledger: Option<HealLedger>,
    /// Flipped by boot_watch when mysqld reaches accepting-connections;
    /// the planned-shutdown note reads it to tell an interrupted boot from
    /// a failed one.
    pub ready: Arc<AtomicBool>,
}

/// Pre-spawn boot accounting and, at the threshold, the wedged-datadir heal.
/// Runs before mysqld (and before the group-name/fresh-datadir reads in
/// main.rs — a wipe changes both). HA mode only.
pub async fn preboot(config: &Config, telemetry: &Telemetry) -> PrebootState {
    let data_dir = &config.data_dir;
    let ready = Arc::new(AtomicBool::new(false));

    if init_could_succeed(data_dir) {
        // First boot on a fresh volume: nothing to heal, and no marker may
        // be written — docker-entrypoint is about to run `mysqld
        // --initialize`, which aborts on any entry it doesn't tolerate
        // (see INIT_TOLERATED_ENTRIES).
        return PrebootState {
            pending_ledger: None,
            ready,
        };
    }

    let failed_boots = read_boot_attempts(data_dir);
    let mut pending_ledger = None;

    if failed_boots >= config.boot_loop_threshold {
        let ledger = read_ledger(data_dir);
        match heal_gate(
            ledger,
            config.self_heal_attempt_cap,
            config.self_heal_backoff_base_seconds,
            now_unix(),
        ) {
            HealGate::CapReached => {
                error!(
                    failed_boots,
                    attempts = ledger.attempts,
                    cap = config.self_heal_attempt_cap,
                    "datadir is boot-wedged but the self-heal attempt budget is spent; leaving the data in place for inspection"
                );
                telemetry.send(TelemetryEvent::ComponentError {
                    component: "mysql-wrapper".to_string(),
                    error: format!(
                        "boot-wedged datadir, self-heal cap reached after {} attempts",
                        ledger.attempts
                    ),
                    context: "self_heal_cap".to_string(),
                });
            }
            HealGate::BackingOff => {
                info!(
                    failed_boots,
                    attempts = ledger.attempts,
                    "datadir is boot-wedged; inside the backoff window after the previous heal attempt"
                );
            }
            HealGate::Go => {
                let client = reqwest::Client::new();
                let timeout = Duration::from_millis(config.peer_query_timeout_ms);
                match quorum_confirmed_peer(
                    &client,
                    &config.peer_hosts(),
                    config.health_port,
                    timeout,
                )
                .await
                {
                    None => {
                        // Total outage (or full isolation): the recovery
                        // path elects the most advanced dataset, and this
                        // wedged copy may be it. Hold.
                        warn!(
                            failed_boots,
                            "mysqld failed to start repeatedly, but no peer answers /role 200; refusing to discard the local datadir (it may be the best surviving copy)"
                        );
                    }
                    Some(primary) => {
                        // Durable evidence of exactly what is discarded —
                        // its files are gone right after this line. The
                        // executed GTID set can't be read from a datadir
                        // mysqld won't boot; the identity markers are the
                        // best available fingerprint.
                        let group_name = gr::read_group_name_marker(data_dir);
                        let waiver_generation = gr::read_waiver_generation(data_dir);
                        warn!(
                            failed_boots,
                            %primary,
                            group_name = group_name.as_deref().unwrap_or("<none>"),
                            waiver_generation,
                            attempt = ledger.attempts + 1,
                            "datadir is boot-wedged and a quorum-confirmed primary is live; discarding local state to reprovision from the group"
                        );
                        telemetry.send(TelemetryEvent::ComponentError {
                            component: "mysql-wrapper".to_string(),
                            error: format!(
                                "boot-wedged datadir discarded after {failed_boots} failed boots; reprovisioning from {primary} (attempt {})",
                                ledger.attempts + 1
                            ),
                            context: "boot_loop_self_heal".to_string(),
                        });
                        match wipe_datadir(data_dir) {
                            Ok(()) => pending_ledger = Some(ledger.bump(now_unix())),
                            Err(e) => {
                                error!(error = %e, "could not discard the wedged datadir");
                                telemetry.send(TelemetryEvent::ComponentError {
                                    component: "mysql-wrapper".to_string(),
                                    error: e.to_string(),
                                    context: "boot_loop_self_heal".to_string(),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    // Count this boot as failed until proven otherwise; boot_watch resets
    // the marker the moment mysqld accepts connections. Skipped when the
    // wipe just emptied the datadir — a marker there would abort the
    // re-initialization.
    if pending_ledger.is_none() {
        if let Err(e) = persist_boot_attempts(data_dir, failed_boots.saturating_add(1)) {
            warn!(error = %e, "could not persist the boot attempt counter");
        }
    }

    PrebootState {
        pending_ledger,
        ready,
    }
}

/// Watch this boot reach accepting-connections: reset the failed-boot
/// counter (and persist a pending ledger) on success, or — over the budget,
/// and ONLY while a quorum-confirmed primary is live — exit the container so
/// the restart counts the attempt. With no healthy group anywhere a slow
/// crash recovery is left to grind to the end, however long it takes: the
/// copy it is recovering may be the best one left.
pub async fn boot_watch(
    config: Arc<Config>,
    sql: Sql,
    telemetry: Arc<Telemetry>,
    state: PrebootState,
) {
    let budget = Duration::from_secs(config.boot_ready_budget_seconds);
    let started = Instant::now();
    let client = reqwest::Client::new();
    let peer_hosts = config.peer_hosts();
    let timeout = Duration::from_millis(config.peer_query_timeout_ms);
    let mut over_budget_logged = false;

    loop {
        // The same readiness test the orchestrator uses: the FINAL mysqld
        // (not docker-entrypoint's init temp server) answering queries.
        if let Ok(false) = sql.is_init_temp_server().await {
            state.ready.store(true, Ordering::Relaxed);
            if let Err(e) = persist_boot_attempts(&config.data_dir, 0) {
                warn!(error = %e, "could not reset the boot attempt counter");
            }
            if let Some(ledger) = state.pending_ledger {
                if let Err(e) = persist_ledger(&config.data_dir, ledger) {
                    warn!(error = %e, "could not persist the self-heal ledger");
                }
            }
            return;
        }

        if started.elapsed() >= budget {
            if let Some(primary) =
                quorum_confirmed_peer(&client, &peer_hosts, config.health_port, timeout).await
            {
                error!(
                    budget = ?budget,
                    %primary,
                    "mysqld did not accept connections within the boot budget while the group is healthy; exiting so the restart counts this boot attempt"
                );
                telemetry.send(TelemetryEvent::ComponentError {
                    component: "mysql-wrapper".to_string(),
                    error: format!(
                        "mysqld not accepting connections after {budget:?} with a healthy group live"
                    ),
                    context: "boot_budget_exceeded".to_string(),
                });
                std::process::exit(1);
            } else if !over_budget_logged {
                over_budget_logged = true;
                warn!(
                    budget = ?budget,
                    "mysqld is over the boot budget, but no peer answers /role 200; letting it keep trying (fail closed)"
                );
            }
        }

        sleep(BOOT_WATCH_POLL).await;
    }
}

/// Handed to the supervisor's signal path: a SIGTERM'd boot was interrupted,
/// not failed — undo this boot's pre-counted attempt so a burst of redeploys
/// can never masquerade as a boot loop. No-op once mysqld was seen ready
/// (the counter is already 0 by then).
pub struct PlannedShutdownNote {
    pub data_dir: String,
    pub ready: Arc<AtomicBool>,
}

impl PlannedShutdownNote {
    pub fn note_planned_shutdown(&self) {
        if self.ready.load(Ordering::Relaxed) {
            return;
        }
        let n = read_boot_attempts(&self.data_dir);
        if n > 0 {
            let _ = persist_boot_attempts(&self.data_dir, n - 1);
        }
    }
}

/// Perpetual watchdog for a member that mysqld can run but Group Replication
/// cannot heal: continuously ERROR, or RECOVERING with no observable
/// progress, past the dwell. Remedies in place through the same machinery as
/// the divergence self-heal — stop the plugin, clone from a live donor; the
/// clone recipient shuts down and the restarted boot rejoins on the cloned
/// data.
///
/// `healing` pauses the orchestrator's join retries (gr::orchestrate checks
/// it): a START GROUP_REPLICATION between our STOP and the clone would make
/// the recipient refuse the clone forever.
pub async fn stuck_watch(
    config: Arc<Config>,
    sql: Sql,
    telemetry: Arc<Telemetry>,
    healing: Arc<AtomicBool>,
) {
    let dwell = Duration::from_secs(config.stuck_member_dwell_seconds);
    let peer_hosts = config.peer_hosts();
    let timeout = Duration::from_millis(config.peer_query_timeout_ms);
    let client = reqwest::Client::new();
    let recovery_password = config
        .gr_replication_password
        .clone()
        .expect("HA mode requires GR_REPLICATION_PASSWORD (validated in Config::from_env)");

    let mut stuck_since: Option<Instant> = None;
    let mut last_progress_sig = String::new();
    let mut online_since: Option<Instant> = None;
    // Set once we stopped the plugin ourselves: the member reads OFFLINE
    // from then on, but the heal must finish (or be abandoned to the
    // orchestrator), not be re-detected from scratch. Carries the already-
    // counted ledger so the post-clone re-persist can't regress it.
    let mut mid_heal: Option<HealLedger> = None;
    let mut last_note = String::new();

    loop {
        sleep(STUCK_POLL).await;

        let my_state = async {
            let uuid = sql.server_uuid().await?;
            let members = sql.group_members().await?;
            anyhow::Ok(
                members
                    .iter()
                    .find(|m| m.member_id.eq_ignore_ascii_case(&uuid))
                    .map(|m| m.state.clone()),
            )
        }
        .await;

        let state = match my_state {
            Ok(s) => s,
            Err(_) => {
                // mysqld not answering: the boot-loop detector's territory.
                stuck_since = None;
                online_since = None;
                continue;
            }
        };

        match state.as_deref() {
            Some("ONLINE") => {
                stuck_since = None;
                last_progress_sig.clear();
                mid_heal = None;
                healing.store(false, Ordering::Relaxed);
                let since = *online_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= HEALTHY_LEDGER_RESET
                    && read_ledger(&config.data_dir) != HealLedger::default()
                    && persist_ledger(&config.data_dir, HealLedger::default()).is_ok()
                {
                    info!(
                        healthy_for = ?since.elapsed(),
                        "member has stayed ONLINE; self-heal attempt ledger reset"
                    );
                }
                continue;
            }
            Some("ERROR") => {
                // ERROR is terminal for the plugin (auto-rejoin covers
                // expulsion/majority loss, not applier failures) — the dwell
                // alone decides, there is no progress to observe. Reaching
                // ERROR mid-heal means something restarted the plugin under
                // us: re-detect from scratch.
                online_since = None;
                mid_heal = None;
                stuck_since.get_or_insert_with(Instant::now);
            }
            Some("RECOVERING") => {
                // Same mid-heal reasoning: RECOVERING means the join
                // machinery took over — the heal is no longer ours.
                online_since = None;
                mid_heal = None;
                let sig = sql
                    .recovery_progress_signature()
                    .await
                    .unwrap_or_else(|_| "unreadable".to_string());
                if sig != last_progress_sig {
                    // Progress observed — a slow member is not a stuck one.
                    last_progress_sig = sig;
                    stuck_since = Some(Instant::now());
                } else {
                    stuck_since.get_or_insert_with(Instant::now);
                }
            }
            _ => {
                // OFFLINE / not in the view: the orchestrator's join
                // machinery owns this state — unless we put the member here
                // ourselves mid-heal, in which case the heal must finish.
                online_since = None;
                if mid_heal.is_none() {
                    stuck_since = None;
                    healing.store(false, Ordering::Relaxed);
                    continue;
                }
            }
        }

        if mid_heal.is_none() {
            let Some(since) = stuck_since else { continue };
            if since.elapsed() < dwell {
                continue;
            }

            let ledger = read_ledger(&config.data_dir);
            match heal_gate(
                ledger,
                config.self_heal_attempt_cap,
                config.self_heal_backoff_base_seconds,
                now_unix(),
            ) {
                HealGate::CapReached => {
                    if note_once(
                        &mut last_note,
                        "member is stuck past the dwell but the self-heal attempt budget is spent; leaving it up for inspection",
                    ) {
                        telemetry.send(TelemetryEvent::ComponentError {
                            component: "mysql-wrapper".to_string(),
                            error: format!(
                                "stuck member, self-heal cap reached after {} attempts",
                                ledger.attempts
                            ),
                            context: "self_heal_cap".to_string(),
                        });
                    }
                    continue;
                }
                HealGate::BackingOff => continue,
                HealGate::Go => {}
            }
        }

        // Donor gate — checked immediately before every destructive step,
        // mid-heal retries included: if the group died since the last pass,
        // hold and hand the (still intact) local copy back to the
        // orchestrator's total-outage recovery.
        let Some(_primary) =
            quorum_confirmed_peer(&client, &peer_hosts, config.health_port, timeout).await
        else {
            note_once(
                &mut last_note,
                "member is stuck past the dwell, but no peer answers /role 200; refusing to discard the local datadir (it may be the best surviving copy)",
            );
            healing.store(false, Ordering::Relaxed);
            continue;
        };

        // Pause the orchestrator's join retries for the whole heal.
        healing.store(true, Ordering::Relaxed);

        // Same donor choice as the divergence self-heal: the PRIMARY when
        // visible, else any active member.
        let mut answers = Vec::with_capacity(peer_hosts.len());
        for host in &peer_hosts {
            let answer = crate::peers::query_peer(&client, host, config.health_port, timeout).await;
            answers.push((host.clone(), answer));
        }
        let Some(donor) = gr::pick_donor(&answers) else {
            continue;
        };

        let counted = match mid_heal {
            Some(counted) => counted,
            None => {
                let bumped = read_ledger(&config.data_dir).bump(now_unix());
                let executed = sql.executed_gtid_set().await.unwrap_or_default();
                warn!(
                    member_state = state.as_deref().unwrap_or("unknown"),
                    %donor,
                    executed_gtid = %executed,
                    attempt = bumped.attempts,
                    "member is provably stuck while the group is healthy; discarding local state and recloning from the group"
                );
                telemetry.send(TelemetryEvent::ComponentError {
                    component: "mysql-wrapper".to_string(),
                    error: format!(
                        "stuck member ({}) recloning from {donor} (attempt {})",
                        state.as_deref().unwrap_or("unknown"),
                        bumped.attempts
                    ),
                    context: "stuck_member_self_heal".to_string(),
                });
                // Count the attempt BEFORE the clone so a crash mid-clone
                // still counts against the budget.
                if let Err(e) = persist_ledger(&config.data_dir, bumped) {
                    warn!(error = %e, "could not persist the self-heal ledger");
                }
                // CLONE INSTANCE refuses a recipient with the plugin running.
                if let Err(e) = sql.stop_group_replication().await {
                    warn!(error = %e, "could not stop group replication ahead of the reclone; retrying");
                    continue;
                }
                mid_heal = Some(bumped);
                bumped
            }
        };

        match sql
            .clone_from_donor(
                &donor,
                config.mysql_port,
                gr::RECOVERY_USER,
                &recovery_password,
            )
            .await
        {
            // Same contract as the divergence self-heal: on success the
            // recipient replaces its datadir and shuts down; the supervisor
            // exits the container and the next boot rejoins on the cloned
            // data. An Err is either the expected connection drop of that
            // shutdown or a busy donor (one clone per donor at a time) —
            // the next pass retries until the donor frees up.
            Ok(()) => {
                info!("reclone completed; server will shut down and rejoin on restart");
            }
            Err(e) => {
                info!(error = %e, "reclone did not complete this pass (donor busy, or the expected shutdown drop); will retry");
            }
        }
        // Best-effort re-persist: if the clone landed, the pre-clone marker
        // went with the old datadir — try to carry the count into the new
        // one (a clone-recreated datadir may still drop it, in which case a
        // SUCCESSFUL heal is what reset the ledger — acceptable, and the
        // dwell still paces any follow-on attempt). Harmless double-write
        // when the clone didn't start.
        let _ = persist_ledger(&config.data_dir, counted);
    }
}

/// Log a note only when it changes — these loops repeat every few seconds.
/// Returns true on a transition so side effects (telemetry) dedupe the same
/// way. Mirrors gr::wait_log_once.
fn note_once(last: &mut String, note: &str) -> bool {
    if last != note {
        warn!("{note}");
        *last = note.to_string();
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "mysql-wrapper-selfheal-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    #[test]
    fn boot_attempts_marker_roundtrips_and_degrades() {
        let dir = temp_dir("boot");
        assert_eq!(read_boot_attempts(&dir), 0);
        persist_boot_attempts(&dir, 3).unwrap();
        assert_eq!(read_boot_attempts(&dir), 3);
        std::fs::write(boot_attempts_path(&dir), "garbage").unwrap();
        assert_eq!(read_boot_attempts(&dir), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn ledger_roundtrips_and_degrades() {
        let dir = temp_dir("ledger");
        assert_eq!(read_ledger(&dir), HealLedger::default());
        let ledger = HealLedger {
            attempts: 2,
            last_unix: 1_700_000_000,
        };
        persist_ledger(&dir, ledger).unwrap();
        assert_eq!(read_ledger(&dir), ledger);
        std::fs::write(ledger_path(&dir), "not numbers at all").unwrap();
        assert_eq!(read_ledger(&dir), HealLedger::default());
        std::fs::write(ledger_path(&dir), "5").unwrap();
        assert_eq!(read_ledger(&dir), HealLedger::default());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn backoff_doubles_from_the_second_attempt_and_never_overflows() {
        assert_eq!(backoff_gap_seconds(60, 0), 0); // first attempt: immediate
        assert_eq!(backoff_gap_seconds(60, 1), 60);
        assert_eq!(backoff_gap_seconds(60, 2), 120);
        assert_eq!(backoff_gap_seconds(60, 3), 240);
        // A corrupt/huge attempt count must saturate, not wrap into 0.
        assert!(backoff_gap_seconds(60, u32::MAX) >= backoff_gap_seconds(60, 17));
        assert_eq!(backoff_gap_seconds(u64::MAX, 5), u64::MAX);
    }

    #[test]
    fn heal_gate_enforces_cap_then_backoff_then_goes() {
        let cap = 3;
        let base = 60;
        let fresh = HealLedger::default();
        assert_eq!(heal_gate(fresh, cap, base, 1000), HealGate::Go);

        let one = HealLedger {
            attempts: 1,
            last_unix: 1000,
        };
        assert_eq!(heal_gate(one, cap, base, 1030), HealGate::BackingOff);
        assert_eq!(heal_gate(one, cap, base, 1060), HealGate::Go);

        let two = HealLedger {
            attempts: 2,
            last_unix: 1000,
        };
        assert_eq!(heal_gate(two, cap, base, 1119), HealGate::BackingOff);
        assert_eq!(heal_gate(two, cap, base, 1120), HealGate::Go);

        let spent = HealLedger {
            attempts: 3,
            last_unix: 1000,
        };
        // Past the cap, no amount of elapsed time reopens the gate.
        assert_eq!(heal_gate(spent, cap, base, u64::MAX), HealGate::CapReached);
    }

    #[test]
    fn init_could_succeed_only_on_fresh_or_tolerated_content() {
        let dir = temp_dir("fresh");
        // Empty: init runs — hands off.
        assert!(init_could_succeed(&dir));
        // Tolerated entries only: init still runs — hands off.
        std::fs::create_dir(Path::new(&dir).join("lost+found")).unwrap();
        assert!(init_could_succeed(&dir));
        // Any other entry (even a dotfile) already aborts init — counting
        // on such a datadir is safe.
        std::fs::write(Path::new(&dir).join(".railway_boot_attempts"), "1\n").unwrap();
        assert!(!init_could_succeed(&dir));
        std::fs::remove_file(Path::new(&dir).join(".railway_boot_attempts")).unwrap();
        // An initialized datadir is the ordinary counting target.
        std::fs::create_dir(Path::new(&dir).join("mysql")).unwrap();
        assert!(!init_could_succeed(&dir));
        // Unreadable reads as fresh — never plant a marker blind.
        assert!(init_could_succeed("/nonexistent/path/for/this/test"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn wipe_datadir_clears_nested_content_and_tolerates_empty() {
        let dir = temp_dir("wipe");
        std::fs::write(Path::new(&dir).join("ibdata1"), "x").unwrap();
        std::fs::create_dir_all(Path::new(&dir).join("mysql/nested")).unwrap();
        std::fs::write(Path::new(&dir).join("mysql/nested/f.ibd"), "y").unwrap();
        wipe_datadir(&dir).unwrap();
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        // Idempotent on an already-empty dir.
        wipe_datadir(&dir).unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn planned_shutdown_undoes_one_attempt_only_before_ready() {
        let dir = temp_dir("note");
        persist_boot_attempts(&dir, 2).unwrap();

        let note = PlannedShutdownNote {
            data_dir: dir.clone(),
            ready: Arc::new(AtomicBool::new(false)),
        };
        note.note_planned_shutdown();
        assert_eq!(read_boot_attempts(&dir), 1);

        // Once mysqld was seen ready the counter is already reset; the note
        // must not touch it.
        persist_boot_attempts(&dir, 0).unwrap();
        note.ready.store(true, Ordering::Relaxed);
        note.note_planned_shutdown();
        assert_eq!(read_boot_attempts(&dir), 0);

        // Never underflows.
        let fresh = PlannedShutdownNote {
            data_dir: dir.clone(),
            ready: Arc::new(AtomicBool::new(false)),
        };
        persist_boot_attempts(&dir, 0).unwrap();
        fresh.note_planned_shutdown();
        assert_eq!(read_boot_attempts(&dir), 0);

        std::fs::remove_dir_all(&dir).ok();
    }
}
