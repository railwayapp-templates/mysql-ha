//! Group Replication orchestration: decide bootstrap-vs-join, guarded.
//!
//! Runs as a background task next to the mysqld supervisor. Every boot goes
//! through this (group_replication_start_on_boot is OFF), so the same state
//! machine covers first deploy, single-node restart, full-cluster cold
//! restart, and scale-up.
//!
//! Invariants:
//!   - NEVER bootstrap while any declared peer is unreachable or not yet
//!     answering: an unreachable peer's dataset can't be compared, and
//!     bootstrapping past it is how a stale dataset silently becomes the new
//!     source of truth. (First deploys don't trip this: health servers come
//!     up well before mysqld finishes initializing, so peers answer
//!     "no group, empty GTID set" almost immediately.)
//!     ONE exception, with proof: a peer whose NAME is authoritatively gone
//!     (continuous NXDOMAIN for the whole `peer_gone_dwell_seconds`) stops
//!     being waited on. GR_SEEDS is stamped at deploy time and scale-down
//!     never restamps the survivors, so a deleted member would otherwise
//!     freeze every future total-outage recovery forever. The private
//!     resolver answers NXDOMAIN only when zero live containers are
//!     registered behind the name; a partition yields SERVFAIL, never
//!     NXDOMAIN (see dns_probe.rs). Residual case, now fully self-healing:
//!     a STOPPED service also has zero live containers, so a peer stopped
//!     for longer than the dwell during a total outage can be waived even
//!     though its volume still holds data. If it was the most advanced
//!     node, its tail forks from the group that re-forms without it — so a
//!     waiver bootstrap mints a FRESH group identity (see the bootstrap
//!     arm), which turns that fork from an undetectable GTID-coordinate
//!     collision into ordinary divergence that the stranded node resolves
//!     by discarding its orphaned tail and recloning. No human ever
//!     required; the stranded tail is discarded visibly, never merged
//!     silently. The dwell is long precisely to keep
//!     that window out of ordinary redeploy/restart timelines.
//!   - NEVER bootstrap unless every reachable peer's executed-GTID set is a
//!     subset of this node's — after a full outage the most-advanced node
//!     must be the one to bootstrap, and each node can verify that claim
//!     about itself locally. Candidacy is DYNAMIC: whichever node holds the
//!     most complete dataset bootstraps, not a fixed seed — a fixed candidate
//!     deadlocks the whole group the moment it is behind (any failover
//!     followed by a full outage) or permanently gone.
//!   - Identical datasets tie-break deterministically: a node holding
//!     pre-GTID data (an adopted standalone volume, whose base data GTIDs
//!     can't describe) outranks fresh nodes, then declared seed order
//!     decides. Every node computes the same order, so exactly one wins.
//!   - DIVERGED histories (two nodes each holding transactions the other
//!     lacks) are resolved deterministically, never frozen for a human:
//!     the more recent authority wins — higher waiver generation first,
//!     then declared seed order — and the losing side self-heals by
//!     discarding its orphaned tail (logged as evidence) and recloning. A
//!     fork can only arise past a waiver bootstrap's fresh identity, so the
//!     generation always separates the two sides cleanly.
//!   - The bootstrap decision must hold stable for a dwell period before it
//!     is acted on, so a slow-starting peer gets a window to contradict it.
//!   - Joining is the default: any peer reporting a live group means this
//!     node joins it, candidate or not.
//!   - A joiner whose server_uuid already lives in the group (a restored
//!     byte copy of a member's volume) regenerates its identity — drop
//!     auto.cnf, restart — instead of retrying a join the group will refuse
//!     forever.

use crate::config::Config;
use crate::dns_probe::{probe_name_detailed, NameVerdict};
use crate::peers::{query_peer, GrState, PeerAnswer};
use crate::sql::{role_is_writable_primary, Sql};
use anyhow::{Context, Result};
use common::{RailwayEnv, Telemetry, TelemetryEvent};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use uuid::Uuid;

pub const RECOVERY_USER: &str = "gr_recovery";
const GROUP_NAME_MARKER: &str = ".railway_gr_group_name";
const PRE_GTID_DATA_MARKER: &str = ".railway_pre_gtid_data";
const WAIVER_GENERATION_MARKER: &str = ".railway_gr_waiver_generation";
const POLL_INTERVAL: Duration = Duration::from_secs(3);

fn marker_path(config: &Config) -> PathBuf {
    Path::new(&config.data_dir).join(GROUP_NAME_MARKER)
}

fn pre_gtid_marker_path(data_dir: &str) -> PathBuf {
    Path::new(data_dir).join(PRE_GTID_DATA_MARKER)
}

fn waiver_generation_path(data_dir: &str) -> PathBuf {
    Path::new(data_dir).join(WAIVER_GENERATION_MARKER)
}

/// The group name this volume's history belongs to, straight from the
/// marker. None before the first persist (a first boot pre-orchestration) —
/// peers only consume this from group-active nodes, which have long since
/// persisted theirs.
fn read_group_name_marker(data_dir: &str) -> Option<String> {
    let content = std::fs::read_to_string(Path::new(data_dir).join(GROUP_NAME_MARKER)).ok()?;
    let trimmed = content.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// How many waiver bootstraps this volume's history has been through. 0 for
/// pre-waiver volumes and fresh nodes (marker absent) — see GrState's
/// waiver_generation for what it decides.
pub fn read_waiver_generation(data_dir: &str) -> u64 {
    std::fs::read_to_string(waiver_generation_path(data_dir))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn persist_waiver_generation(data_dir: &str, generation: u64) -> Result<()> {
    let path = waiver_generation_path(data_dir);
    std::fs::write(&path, format!("{generation}\n"))
        .with_context(|| format!("writing {}", path.display()))
}

/// Does this node hold data that predates its GTID history? True for an
/// adopted standalone volume (Railway's standalone template runs with binlog
/// off). Persisted as a marker the moment it is detected, because the
/// condition itself ("data present, GTID set empty") stops being observable
/// once the group starts minting GTIDs.
pub fn has_pre_gtid_data(data_dir: &str) -> bool {
    pre_gtid_marker_path(data_dir).exists()
}

/// Resolve the group name: explicit env > marker persisted on the volume >
/// derived (UUIDv5 of the Railway environment id — deterministic, so every
/// member of a first deploy derives the same name with no coordination).
///
/// Read-only: called before mysqld is spawned. The marker is only WRITTEN
/// after mysqld is up, because `mysqld --initialize` refuses a non-empty
/// datadir and the marker lives in the datadir (the only persistent volume).
pub fn resolve_group_name(config: &Config) -> String {
    if let Some(name) = &config.gr_group_name {
        return name.clone();
    }
    if let Ok(persisted) = std::fs::read_to_string(marker_path(config)) {
        let persisted = persisted.trim();
        if !persisted.is_empty() {
            return persisted.to_string();
        }
    }
    Uuid::new_v5(
        &Uuid::NAMESPACE_DNS,
        format!("mysql-ha:{}", RailwayEnv::environment_id()).as_bytes(),
    )
    .to_string()
}

/// Persist the resolved group name so later boots keep it even if the
/// derivation inputs change. The group name is embedded in every group
/// transaction's GTID — once a group has run, it must never change.
pub fn persist_group_name(config: &Config, group_name: &str) -> Result<()> {
    let path = marker_path(config);
    if let Ok(existing) = std::fs::read_to_string(&path) {
        if existing.trim() == group_name {
            return Ok(());
        }
    }
    std::fs::write(&path, format!("{group_name}\n"))
        .with_context(|| format!("writing {}", path.display()))
}

/// This node's own Group Replication state, in the same shape peers report.
pub async fn local_gr_state(sql: &Sql, data_dir: &str) -> Result<GrState> {
    let self_uuid = sql.server_uuid().await?;
    let members = sql.group_members().await?;
    let gtid = sql.executed_gtid_set().await?;

    let me = members
        .iter()
        .find(|m| m.member_id.eq_ignore_ascii_case(&self_uuid));
    let member_state = me.map(|m| m.state.clone());
    let group_active = matches!(member_state.as_deref(), Some("ONLINE") | Some("RECOVERING"));

    Ok(GrState {
        group_active,
        member_state,
        member_role: me.map(|m| m.role.clone()),
        gtid_executed: Some(gtid),
        members_total: members.len(),
        members_reachable: members.iter().filter(|m| m.state != "UNREACHABLE").count(),
        // File marker: the adopting node itself, detected pre-bootstrap.
        // DB flag: replicated group-level truth — survives clones and the
        // adopting node's deletion.
        pre_gtid_data: has_pre_gtid_data(data_dir) || sql.group_pre_gtid_flag().await,
        server_uuid: Some(self_uuid),
        group_name: read_group_name_marker(data_dir),
        waiver_generation: read_waiver_generation(data_dir),
    })
}

/// The host of a live-group peer that reports THIS node's server_uuid, if
/// any. A joiner in this state can never get in: Group Replication refuses a
/// member whose uuid is already present in the group ("There is already a
/// member with server_uuid ..."), so START GROUP_REPLICATION fails on every
/// retry, forever. In practice it happens exactly one way: every data node
/// was restored from a volume backup of ONE node, so auto.cnf — where mysqld
/// keeps its uuid — is byte-identical across the fleet. Only group-active
/// peers count: two not-yet-joined nodes sharing a uuid resolve through this
/// same path once one of them is in.
fn uuid_collision_peer(my_uuid: &str, answers: &[(String, PeerAnswer)]) -> Option<String> {
    answers.iter().find_map(|(host, answer)| match answer {
        PeerAnswer::State(GrState {
            group_active: true,
            server_uuid: Some(peer_uuid),
            ..
        }) if peer_uuid.eq_ignore_ascii_case(my_uuid) => Some(host.clone()),
        _ => None,
    })
}

/// How long each unreachable peer's NAME has been authoritatively gone.
///
/// The bootstrap guard refuses to decide while any declared peer is
/// unreachable — correct for crashes and partitions, but GR_SEEDS is never
/// restamped on scale-down, so a DELETED peer would freeze every future
/// total-outage recovery forever. This tracker turns "unreachable AND its
/// name has answered NXDOMAIN continuously for the whole dwell" into a
/// waiver: the peer is dropped from the round, as if it were no longer
/// declared. Any non-Gone observation (records, NODATA, SERVFAIL, timeout —
/// see dns_probe.rs for why a partition can't fake Gone) resets its clock,
/// so the proof must hold uninterrupted.
struct GoneTracker {
    gone_since: HashMap<String, Instant>,
}

impl GoneTracker {
    fn new() -> Self {
        Self {
            gone_since: HashMap::new(),
        }
    }

    fn observe(&mut self, host: &str, verdict: NameVerdict, now: Instant) {
        match verdict {
            NameVerdict::Gone => {
                self.gone_since.entry(host.to_string()).or_insert(now);
            }
            NameVerdict::ExistsOrUnknown => {
                self.gone_since.remove(host);
            }
        }
    }

    /// A reachable peer is present again by definition — its clock resets.
    fn observe_reachable(&mut self, host: &str) {
        self.gone_since.remove(host);
    }

    fn is_waived(&self, host: &str, now: Instant, dwell: Duration) -> bool {
        self.gone_since
            .get(host)
            .is_some_and(|since| now.duration_since(*since) >= dwell)
    }
}

/// How one peer's dataset relates to this node's, for the bootstrap decision.
#[derive(Debug, Clone, PartialEq)]
enum PeerRelation {
    /// Strict subset of ours — the peer is behind; we outrank it.
    Behind,
    /// Identical executed-GTID set. Broken by pre-GTID data, then seed order.
    Equal { pre_gtid_data: bool },
    /// Strict superset of ours — the peer should bootstrap, not us.
    Ahead,
    /// Each side holds transactions the other lacks. classify_round resolves
    /// this into Behind/Ahead deterministically (waiver generation, then
    /// seed order) so the losing side self-heals instead of freezing; kept
    /// as a distinct relation for decide()'s defensive handling and tests.
    Diverged,
    /// Unreachable, not ready, or answered without a GTID set.
    Unknown,
}

/// One peer's standing in a bootstrap round: who it is, where it sits in the
/// declared seed order, and how its dataset compares to ours.
#[derive(Debug)]
struct PeerStanding {
    host: String,
    seed_rank: usize,
    relation: PeerRelation,
}

/// What this node concluded from one round of peer answers.
#[derive(Debug, PartialEq)]
enum BootstrapVerdict {
    /// Some peer is in a live group — join it.
    JoinExistingGroup,
    /// Every peer answered, none has a group, every dataset is a subset of
    /// ours, and we win every tie — safe to bootstrap once the dwell passes.
    SafeToBootstrap,
    /// A peer holds transactions we don't — under dynamic candidacy it
    /// reaches SafeToBootstrap itself; we wait for its group and join it.
    PeerIsMoreAdvanced(String),
    /// A peer's dataset ties ours and it precedes us in the tie-break — it
    /// bootstraps, we join its group.
    DeferToPeer(String),
    /// A peer's history has DIVERGED from ours and could not be auto-
    /// resolved. Defensive only: classify_round resolves divergence into
    /// Behind/Ahead before decide() ever sees it.
    Diverged(String),
    /// At least one peer is unreachable/not-ready — no safe decision exists.
    Undecidable,
}

async fn classify_round(
    sql: &Sql,
    config: &Config,
    my_gtid: &str,
    answers: &[(String, PeerAnswer)],
) -> Result<BootstrapVerdict> {
    if answers
        .iter()
        .any(|(_, a)| matches!(a, PeerAnswer::State(s) if s.group_active))
    {
        return Ok(BootstrapVerdict::JoinExistingGroup);
    }

    let my_generation = read_waiver_generation(&config.data_dir);
    let my_seed_rank = config.my_seed_rank().unwrap_or(usize::MAX);

    let mut peers = Vec::with_capacity(answers.len());
    for (host, answer) in answers {
        let relation = match answer {
            PeerAnswer::State(GrState {
                gtid_executed: Some(peer_gtid),
                pre_gtid_data,
                waiver_generation,
                ..
            }) => {
                let (peer_sub_mine, mine_sub_peer) = sql.gtid_compare(my_gtid, peer_gtid).await?;
                match (peer_sub_mine, mine_sub_peer) {
                    (true, true) => PeerRelation::Equal {
                        pre_gtid_data: *pre_gtid_data,
                    },
                    (true, false) => PeerRelation::Behind,
                    (false, true) => PeerRelation::Ahead,
                    // Both sides hold transactions the other lacks. Nothing
                    // here may EVER merge them — but nothing may freeze
                    // waiting for a human either: resolve deterministically,
                    // and the losing side self-heals by discarding its
                    // orphaned tail (with evidence) and recloning from the
                    // winner's group once it is live. The winner is the more
                    // recent authority: higher waiver generation first (a
                    // fresh waiver bootstrap IS the newer history), seed
                    // order on a tie. Both inputs are identical on every
                    // node, so all nodes agree on the single winner.
                    (false, false) => {
                        let peer_rank = config.seed_rank(host).unwrap_or(usize::MAX);
                        let peer_wins = diverged_peer_wins(
                            my_generation,
                            my_seed_rank,
                            *waiver_generation,
                            peer_rank,
                        );
                        warn!(
                            %host,
                            peer_generation = waiver_generation,
                            my_generation,
                            peer_wins,
                            "GTID histories DIVERGED; auto-resolving by waiver generation then seed order — the losing side will discard its orphaned tail and reclone"
                        );
                        if peer_wins {
                            PeerRelation::Ahead
                        } else {
                            PeerRelation::Behind
                        }
                    }
                }
            }
            // A reachable peer that didn't report a GTID set is as
            // undecidable as an unreachable one.
            PeerAnswer::State(_) | PeerAnswer::NotReady | PeerAnswer::Unreachable => {
                PeerRelation::Unknown
            }
        };
        peers.push(PeerStanding {
            host: host.clone(),
            seed_rank: config.seed_rank(host).unwrap_or(usize::MAX),
            relation,
        });
    }

    Ok(decide(
        has_pre_gtid_data(&config.data_dir),
        my_seed_rank,
        &peers,
    ))
}

/// The deterministic winner when two histories have diverged (each holds
/// transactions the other lacks). Returns true when the PEER's history is
/// authoritative and this node must self-heal to it. The more recent
/// authority wins — higher waiver generation first (a waiver bootstrap mints
/// a strictly greater generation, so the group that re-formed most recently
/// outranks a stale fork), declared seed order on a tie. Every node feeds
/// identical inputs, so all agree on one winner and exactly one side reclones.
fn diverged_peer_wins(
    my_generation: u64,
    my_seed_rank: usize,
    peer_generation: u64,
    peer_seed_rank: usize,
) -> bool {
    match peer_generation.cmp(&my_generation) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => peer_seed_rank < my_seed_rank,
    }
}

/// The pure bootstrap decision, given every peer's standing. Candidacy is
/// dynamic: any node may bootstrap when it provably holds the most complete
/// dataset. Blocking verdicts are checked in order of how definitive they
/// are: an unanswered peer voids the whole round (its dataset can't be
/// compared), divergence freezes everything, a strictly-ahead peer takes the
/// job, and only exact ties fall through to the deterministic tie-break —
/// pre-GTID data first (an adopted standalone volume must beat the fresh
/// nodes its base data hasn't reached), then declared seed order. Both
/// inputs of the tie-break are identical on every node, so all nodes agree
/// on the single winner.
fn decide(my_pre_gtid: bool, my_seed_rank: usize, peers: &[PeerStanding]) -> BootstrapVerdict {
    if peers.iter().any(|p| p.relation == PeerRelation::Unknown) {
        return BootstrapVerdict::Undecidable;
    }
    if let Some(p) = peers.iter().find(|p| p.relation == PeerRelation::Diverged) {
        return BootstrapVerdict::Diverged(p.host.clone());
    }
    if let Some(p) = peers.iter().find(|p| p.relation == PeerRelation::Ahead) {
        return BootstrapVerdict::PeerIsMoreAdvanced(p.host.clone());
    }
    for p in peers {
        if let PeerRelation::Equal { pre_gtid_data } = p.relation {
            let peer_wins = match (pre_gtid_data, my_pre_gtid) {
                // An adopted volume's base data is invisible to GTID
                // comparison — the holder MUST win the tie, or a fresh node
                // would bootstrap an empty group over it.
                (true, false) => true,
                (false, true) => false,
                _ => p.seed_rank < my_seed_rank,
            };
            if peer_wins {
                return BootstrapVerdict::DeferToPeer(p.host.clone());
            }
        }
    }
    BootstrapVerdict::SafeToBootstrap
}

/// The main orchestration loop. Returns once this node is an active group
/// member (the health server takes over from there).
///
/// `fresh_datadir` — true when the datadir was EMPTY at wrapper start, i.e.
/// docker-entrypoint initialized this instance during this very boot. Gates
/// the local GTID-history reset (see reset_fresh_instance_gtid_history).
pub async fn orchestrate(
    config: Arc<Config>,
    sql: Sql,
    telemetry: Arc<Telemetry>,
    mut group_name: String,
    fresh_datadir: bool,
) {
    // 1. Wait for the FINAL mysqld. docker-entrypoint's first-boot
    //    initialization runs setup SQL against a temp server whose socket is
    //    already live (`--skip-networking`) — touching it would corrupt the
    //    init, so wait until networking is on. No timeout: the supervisor
    //    exits the container if mysqld dies.
    let mut attempts = 0u32;
    loop {
        match sql.is_init_temp_server().await {
            Ok(false) => break,
            Ok(true) => {
                if attempts % 30 == 0 {
                    info!("waiting out docker-entrypoint's init temp server");
                }
            }
            Err(e) => {
                if attempts % 30 == 0 {
                    info!(attempts, error = %e, "still waiting for mysqld");
                }
            }
        }
        attempts += 1;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    info!("mysqld is answering");

    // 2a. Fresh instance: purge the GTIDs docker-entrypoint's own init SQL
    //     just minted (they're local-only noise under this node's uuid, and
    //     they wedge both the bootstrap guard and Group Replication joins).
    //     Must happen before the write fence — RESET needs a writable server.
    if fresh_datadir {
        match sql.reset_fresh_instance_gtid_history().await {
            Ok(()) => info!("fresh instance: local init GTID history reset"),
            Err(e) => {
                error!(error = %e, "could not reset fresh-instance GTID history");
                telemetry.send(TelemetryEvent::ComponentError {
                    component: "mysql-wrapper".to_string(),
                    error: e.to_string(),
                    context: "reset_fresh_instance_gtid_history".to_string(),
                });
            }
        }
    } else if !has_pre_gtid_data(&config.data_dir) {
        // Adoption detection: a NON-fresh datadir with an EMPTY GTID set is
        // pre-existing data that was never binlogged — an adopted standalone
        // volume (Railway's standalone template runs --disable-log-bin).
        // Persist the fact now: once the group starts minting GTIDs the
        // condition becomes unobservable, and joiners need it forever to
        // know that binlog-based recovery can never reconstruct this node's
        // base data (see the clone path below).
        match sql.executed_gtid_set().await {
            Ok(set) if set.is_empty() => {
                if let Err(e) = std::fs::write(pre_gtid_marker_path(&config.data_dir), "1\n") {
                    warn!(error = %e, "could not persist pre-GTID data marker");
                } else {
                    info!("adopted volume detected (data present, no GTID history)");
                }
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, "could not read GTID set for adoption detection"),
        }
    }

    // 2b. Recovery user, on EVERY node, unlogged, BEFORE the write fence:
    //     the MYSQL communication stack authenticates inbound group
    //     connections against this user locally — the plugin's own local
    //     connectivity self-test fails without it, bootstrap node included.
    //     Must precede super_read_only (CREATE USER is a write, even
    //     unlogged).
    let recovery_password = config
        .gr_replication_password
        .clone()
        .expect("HA mode requires GR_REPLICATION_PASSWORD (validated in Config::from_env)");
    if let Err(e) = sql
        .ensure_recovery_user(RECOVERY_USER, &recovery_password)
        .await
    {
        error!(error = %e, "failed to ensure recovery user");
        telemetry.send(TelemetryEvent::ComponentError {
            component: "mysql-wrapper".to_string(),
            error: e.to_string(),
            context: "ensure_recovery_user".to_string(),
        });
    }

    // 2c. Fence writes: nothing may write to this node until the group
    //    decides its role. GR lifts this on the elected primary.
    if let Err(e) = sql.set_super_read_only().await {
        warn!(error = %e, "could not set super_read_only");
        telemetry.send(TelemetryEvent::ComponentError {
            component: "mysql-wrapper".to_string(),
            error: e.to_string(),
            context: "set_super_read_only".to_string(),
        });
    }

    if let Err(e) = persist_group_name(&config, &group_name) {
        warn!(error = %e, "could not persist group name marker");
        telemetry.send(TelemetryEvent::ComponentError {
            component: "mysql-wrapper".to_string(),
            error: e.to_string(),
            context: "persist_group_name".to_string(),
        });
    }

    // 3. Recovery channel credentials — local metadata, needed before any
    //    join, allowed under super_read_only, idempotent.
    if let Err(e) = sql
        .configure_recovery_channel(RECOVERY_USER, &recovery_password)
        .await
    {
        // Non-fatal here: retried implicitly because a join without it fails
        // and loops back. But it should never fail — surface it loudly.
        error!(error = %e, "failed to configure recovery channel");
        telemetry.send(TelemetryEvent::ComponentError {
            component: "mysql-wrapper".to_string(),
            error: e.to_string(),
            context: "configure_recovery_channel".to_string(),
        });
    }

    let peer_hosts = config.peer_hosts();
    let peer_timeout = Duration::from_millis(config.peer_query_timeout_ms);
    let dwell = Duration::from_secs(config.bootstrap_dwell_seconds);
    let http = reqwest::Client::new();

    info!(
        first_seed = config.is_bootstrap_candidate(),
        ?peer_hosts,
        group_name = %group_name,
        "starting group replication orchestration"
    );

    let mut safe_since: Option<Instant> = None;
    let mut last_wait_reason = String::new();
    let mut gone_tracker = GoneTracker::new();
    let mut last_waiver_note = String::new();
    let gone_dwell = Duration::from_secs(config.peer_gone_dwell_seconds);
    let dns_deadline = Duration::from_millis(config.peer_query_timeout_ms);

    loop {
        // Already an active member? (Covers both "join succeeded last
        // iteration" and "this node restarted while the group kept running".)
        match local_gr_state(&sql, &config.data_dir).await {
            Ok(state) if state.group_active => {
                info!(
                    state = ?state.member_state,
                    role = ?state.member_role,
                    "node is an active group member"
                );
                telemetry.send(TelemetryEvent::NodeStarted {
                    node: config.private_domain.clone(),
                    role: state
                        .member_role
                        .unwrap_or_else(|| "unknown".to_string())
                        .to_lowercase(),
                });
                return;
            }
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, "could not read local group state");
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
        }

        // Query all declared peers concurrently.
        let mut answers = Vec::with_capacity(peer_hosts.len());
        let futures: Vec<_> = peer_hosts
            .iter()
            .map(|host| query_peer(&http, host, config.health_port, peer_timeout))
            .collect();
        for (host, answer) in peer_hosts.iter().zip(futures_join_all(futures).await) {
            answers.push((host.clone(), answer));
        }

        // Deletion tracking: an unreachable peer's name is probed against the
        // resolver; only continuous NXDOMAIN across the whole dwell earns a
        // waiver (see GoneTracker). Reachable peers reset their clock.
        let now = Instant::now();
        for (host, answer) in &answers {
            if matches!(answer, PeerAnswer::Unreachable) {
                let (verdict, detail) = probe_name_detailed(host, dns_deadline).await;
                gone_tracker.observe(host, verdict, now);
                if verdict == NameVerdict::Gone && !gone_tracker.is_waived(host, now, gone_dwell) {
                    info!(
                        %host,
                        ?detail,
                        dwell = ?gone_dwell,
                        "unreachable peer's name is authoritatively gone; will stop waiting on it if this persists for the whole dwell"
                    );
                }
            } else {
                gone_tracker.observe_reachable(host);
            }
        }
        let waived: Vec<&String> = answers
            .iter()
            .filter(|(host, answer)| {
                matches!(answer, PeerAnswer::Unreachable)
                    && gone_tracker.is_waived(host, now, gone_dwell)
            })
            .map(|(host, _)| host)
            .collect();
        let waiver_note = if waived.is_empty() {
            String::new()
        } else {
            format!(
                "peers {waived:?} are deleted (name gone past the dwell); no longer waiting on them"
            )
        };
        if last_waiver_note != waiver_note {
            if !waiver_note.is_empty() {
                warn!("{waiver_note}");
            }
            last_waiver_note = waiver_note;
        }
        // Owned before `answers` moves into `considered` below; drives the
        // fresh-identity fence in the bootstrap arm.
        let any_waived = !waived.is_empty();

        let group_seen = answers
            .iter()
            .any(|(_, a)| matches!(a, PeerAnswer::State(s) if s.group_active));

        if group_seen {
            safe_since = None;

            // Identity collision: a live member already carries this node's
            // server_uuid — this datadir is a byte copy of that member's (a
            // volume backup of one node restored onto every data node). The
            // group will refuse this joiner on every attempt, so regenerate
            // identity instead of retrying: drop auto.cnf and shut mysqld
            // down. The supervisor exits the container, the platform's
            // restart policy boots it back up, and the fresh boot mints a
            // new server_uuid and joins normally — its GTID history
            // (identical to the group's at backup time) recovers over the
            // binlog. Checked BEFORE the clone path: clone does not replace
            // auto.cnf, so a cloned datadir would still carry the colliding
            // uuid.
            match sql.server_uuid().await {
                Ok(my_uuid) => {
                    if let Some(peer) = uuid_collision_peer(&my_uuid, &answers) {
                        warn!(
                            %peer,
                            uuid = %my_uuid,
                            "a live member already holds this node's server_uuid (restored datadir copy) — regenerating identity"
                        );
                        let auto_cnf = Path::new(&config.data_dir).join("auto.cnf");
                        match std::fs::remove_file(&auto_cnf) {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => {
                                error!(error = %e, "could not remove auto.cnf; identity cannot be regenerated");
                                telemetry.send(TelemetryEvent::ComponentError {
                                    component: "mysql-wrapper".to_string(),
                                    error: e.to_string(),
                                    context: "regenerate_server_uuid".to_string(),
                                });
                                tokio::time::sleep(POLL_INTERVAL).await;
                                continue;
                            }
                        }
                        match sql.shutdown_server().await {
                            Ok(()) => info!("auto.cnf removed; mysqld shutting down to mint a fresh server_uuid on restart"),
                            Err(e) => info!(error = %e, "auto.cnf removed; shutdown issued (connection drop on the way down is expected)"),
                        }
                        tokio::time::sleep(POLL_INTERVAL).await;
                        continue;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "could not read local server_uuid");
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
            }

            // Identity adoption: a waiver bootstrap mints a FRESH random
            // group name (see the bootstrap arm below), so the live group's
            // name can no longer be derived — joiners take it from the
            // group's own /gr/state advert. Also carries the group's waiver
            // generation forward, so this node votes with the group's
            // lineage in any future divergence tie-break. An explicit
            // GR_GROUP_NAME env pin wins over adoption — the operator said
            // exactly which group this node belongs to.
            let live_identity = answers.iter().find_map(|(_, a)| match a {
                PeerAnswer::State(s) if s.group_active => {
                    s.group_name.clone().map(|name| (name, s.waiver_generation))
                }
                _ => None,
            });
            // Identity adoption: a waiver bootstrap mints a fresh random group
            // name, so the live group's name can no longer be derived — a
            // joiner takes it from the group's /gr/state advert (and carries
            // the group's waiver generation forward, so it votes with the
            // group's lineage in any future divergence tie-break). Only when
            // the identities differ; an explicit GR_GROUP_NAME env pin wins —
            // the operator said exactly which group this node belongs to.
            let identity_differs = live_identity
                .as_ref()
                .map(|(name, _)| *name != group_name)
                .unwrap_or(false);
            if identity_differs {
                let (live_name, live_generation) =
                    live_identity.expect("identity_differs implies Some");
                if config.gr_group_name.is_some() {
                    wait_log_once(
                        &mut last_wait_reason,
                        &format!(
                            "live group runs as {live_name} but GR_GROUP_NAME pins {group_name}; refusing to adopt an identity the operator overrode"
                        ),
                    );
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
                match sql.set_group_name(&live_name).await {
                    Ok(()) => {
                        info!(
                            from = %group_name,
                            to = %live_name,
                            generation = live_generation,
                            "adopting the live group's identity"
                        );
                        if let Err(e) = persist_group_name(&config, &live_name) {
                            warn!(error = %e, "could not persist adopted group name");
                        }
                        let my_generation = read_waiver_generation(&config.data_dir);
                        if live_generation > my_generation {
                            if let Err(e) =
                                persist_waiver_generation(&config.data_dir, live_generation)
                            {
                                warn!(error = %e, "could not persist adopted waiver generation");
                            }
                        }
                        group_name = live_name;
                    }
                    Err(e) => {
                        warn!(error = %e, "could not adopt the live group's identity; retrying");
                        tokio::time::sleep(POLL_INTERVAL).await;
                        continue;
                    }
                }
            }

            // Divergence self-heal: this node holds committed transactions the
            // LIVE group does not — a stale fork's tail (the group re-formed
            // without it past a waiver bootstrap) or an errant local write.
            // They can never be merged, and Group Replication would refuse
            // this joiner forever. Heal automatically: log exactly what is
            // discarded (the orphaned GTID set — its binlogs are gone once the
            // clone lands, so this line IS the durable evidence), then reclone.
            //
            // Checked on EVERY pass while a group is live, not just the pass
            // that adopts a foreign identity: the reclone can be refused
            // transiently — MySQL allows one clone per donor at a time, so two
            // nodes healing off the same primary collide — and the retry has
            // to keep coming until the donor frees up. Gating this on
            // "identity just changed" wedged the loser of that race forever.
            // For an ordinary returning member (subset GTID) this is two cheap
            // reads that no-op.
            let my_gtid_now = sql.executed_gtid_set().await.unwrap_or_default();
            let live_peer_gtid = answers.iter().find_map(|(_, a)| match a {
                PeerAnswer::State(s) if s.group_active => s.gtid_executed.clone(),
                _ => None,
            });
            if !my_gtid_now.is_empty() {
                if let Some(peer_gtid) = live_peer_gtid {
                    match sql.gtid_compare(&my_gtid_now, &peer_gtid).await {
                        Ok((_, mine_sub_peer)) if !mine_sub_peer => {
                            let orphaned = sql
                                .gtid_subtract(&my_gtid_now, &peer_gtid)
                                .await
                                .unwrap_or_else(|_| "unavailable".to_string());
                            warn!(
                                %orphaned,
                                "this node's history diverged from the live group; discarding the orphaned transactions and recloning"
                            );
                            telemetry.send(TelemetryEvent::ComponentError {
                                component: "mysql-wrapper".to_string(),
                                error: format!(
                                    "diverged from the live group; discarded orphaned transactions: {orphaned}"
                                ),
                                context: "divergence_self_heal".to_string(),
                            });
                            if let Some(donor) = pick_donor(&answers) {
                                match sql
                                    .clone_from_donor(
                                        &donor,
                                        config.mysql_port,
                                        RECOVERY_USER,
                                        &recovery_password,
                                    )
                                    .await
                                {
                                    Ok(()) => info!("divergence reclone completed; server will shut down and rejoin on restart"),
                                    // A donor already serving another clone
                                    // returns ER_CLONE_TOO_MANY_CONCURRENT
                                    // (3862); the next pass retries once it is
                                    // free. Any other error is the expected
                                    // connection drop of a clone that DID start
                                    // and shut the server down.
                                    Err(e) => info!(error = %e, "divergence reclone did not complete this pass (donor busy, or the expected shutdown drop); will retry"),
                                }
                            }
                            tokio::time::sleep(POLL_INTERVAL).await;
                            continue;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(error = %e, "could not compare GTID history against the live group");
                            tokio::time::sleep(POLL_INTERVAL).await;
                            continue;
                        }
                    }
                }
            }

            // Clone-first path: the group carries data that predates its
            // GTID history (adopted standalone volume), and this node has no
            // GTID history of its own to prove it holds that base data.
            // Binlog-based recovery would join "successfully" while silently
            // skipping everything that was never binlogged — clone instead.
            let group_has_pre_gtid_data = answers.iter().any(
                |(_, a)| matches!(a, PeerAnswer::State(s) if s.group_active && s.pre_gtid_data),
            );
            let i_hold_pre_gtid_data = has_pre_gtid_data(&config.data_dir);
            let my_gtid_is_empty = sql
                .executed_gtid_set()
                .await
                .map(|s| s.is_empty())
                .unwrap_or(false);

            if group_has_pre_gtid_data && !i_hold_pre_gtid_data && my_gtid_is_empty {
                if let Some(donor) = pick_donor(&answers) {
                    info!(%donor, "group holds pre-GTID data — cloning instead of binlog recovery");
                    match sql
                        .clone_from_donor(
                            &donor,
                            config.mysql_port,
                            RECOVERY_USER,
                            &recovery_password,
                        )
                        .await
                    {
                        // Either way the expected outcome is the same: the
                        // recipient server replaces its datadir and shuts
                        // down (no in-container monitor for self-restart),
                        // the supervisor exits the container, and the next
                        // boot joins on the cloned data. An Err here is
                        // almost always the dropped connection of that
                        // shutdown, not a failure.
                        Ok(()) => {
                            info!("clone completed; server will shut down and rejoin on restart")
                        }
                        Err(e) => {
                            info!(error = %e, "clone initiated (connection drop on shutdown is expected)")
                        }
                    }
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
            }

            info!("a live group exists — joining");
            match sql.start_group_replication().await {
                Ok(()) => info!("START GROUP_REPLICATION succeeded"),
                Err(e) => {
                    // Clone-based recovery shuts the server down mid-join
                    // (no in-container monitor process to restart mysqld);
                    // the supervisor exits the container and the next boot
                    // joins with the cloned datadir. Everything else retries.
                    warn!(error = %e, "join attempt failed; retrying");
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
            continue;
        }

        // No live group anywhere: decide whether bootstrapping HERE is
        // provably safe. Every node runs this — candidacy is dynamic.
        let my_gtid = match sql.executed_gtid_set().await {
            Ok(g) => g,
            Err(e) => {
                warn!(error = %e, "could not read local GTID set");
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
        };

        // Waived (deleted) peers are dropped from the round — as if no
        // longer declared. A waived host that comes back under the same name
        // answers again as a fresh empty node, which is exactly the
        // fresh-first-seed recovery path.
        let considered: Vec<(String, PeerAnswer)> = answers
            .into_iter()
            .filter(|(host, answer)| {
                !(matches!(answer, PeerAnswer::Unreachable)
                    && gone_tracker.is_waived(host, now, gone_dwell))
            })
            .collect();

        match classify_round(&sql, &config, &my_gtid, &considered).await {
            Ok(BootstrapVerdict::SafeToBootstrap) => {
                let since = *safe_since.get_or_insert_with(Instant::now);
                let held = since.elapsed();
                if held < dwell {
                    wait_log_once(
                        &mut last_wait_reason,
                        "bootstrap looks safe; holding for the dwell period",
                    );
                } else {
                    // Fencing: a bootstrap that WAIVED a peer is resuming
                    // authority past a member whose data could not be
                    // compared — if that member was merely stopped (its name
                    // resolves gone either way) and it was AHEAD, the two
                    // histories fork here. Under the derived (deterministic)
                    // group name, both forks would mint the SAME GTID
                    // coordinates for DIFFERENT transactions, and every
                    // GTID-set comparison downstream — including Group
                    // Replication's own join admission — would read the
                    // forked histories as EQUAL: the returning member would
                    // be readmitted silently and the data split would be
                    // permanent and invisible. A FRESH random name makes the
                    // fork structurally detectable (the Postgres-timeline /
                    // Raft-term / fencing-token move): the stale member's
                    // tail lives under a UUID the new group never issued, so
                    // it surfaces as ordinary divergence and self-heals via
                    // the reclone path above. Routine bootstraps (nobody
                    // waived) keep the persisted name — regenerating it on
                    // every restart would force a full reclone of every
                    // member each time.
                    if any_waived {
                        let fresh_name = Uuid::new_v4().to_string();
                        let peer_generations = considered
                            .iter()
                            .filter_map(|(_, a)| match a {
                                PeerAnswer::State(s) => Some(s.waiver_generation),
                                _ => None,
                            })
                            .max()
                            .unwrap_or(0);
                        let next_generation =
                            read_waiver_generation(&config.data_dir).max(peer_generations) + 1;
                        match sql.set_group_name(&fresh_name).await {
                            Ok(()) => {
                                warn!(
                                    old_name = %group_name,
                                    new_name = %fresh_name,
                                    generation = next_generation,
                                    "bootstrapping past waived peers — minting a fresh group identity to fence off any stale fork"
                                );
                                telemetry.send(TelemetryEvent::ComponentError {
                                    component: "mysql-wrapper".to_string(),
                                    error: format!(
                                        "waiver bootstrap: fresh group identity {fresh_name} (generation {next_generation}) fences the waived peers' potential stale fork"
                                    ),
                                    context: "waiver_bootstrap_fence".to_string(),
                                });
                                if let Err(e) = persist_group_name(&config, &fresh_name) {
                                    warn!(error = %e, "could not persist fresh group name");
                                }
                                if let Err(e) =
                                    persist_waiver_generation(&config.data_dir, next_generation)
                                {
                                    warn!(error = %e, "could not persist waiver generation");
                                }
                                group_name = fresh_name;
                            }
                            Err(e) => {
                                error!(error = %e, "could not set the fresh group name; refusing to bootstrap under the shared identity");
                                safe_since = None;
                                tokio::time::sleep(POLL_INTERVAL).await;
                                continue;
                            }
                        }
                    }
                    info!(?held, group_name = %group_name, "bootstrapping a new group");
                    match sql.bootstrap_group().await {
                        Ok(()) => {
                            if let Err(e) = post_bootstrap(&sql).await {
                                error!(error = %e, "post-bootstrap setup failed");
                                telemetry.send(TelemetryEvent::ComponentError {
                                    component: "mysql-wrapper".to_string(),
                                    error: e.to_string(),
                                    context: "post_bootstrap".to_string(),
                                });
                            } else if has_pre_gtid_data(&config.data_dir) {
                                // Adopting root: replicate the group-level
                                // "our dataset predates our GTIDs" fact so
                                // every present and future member knows
                                // joiners must clone (see the clone path).
                                // Retried: if this write is lost AND the
                                // adopting node later disappears (taking its
                                // file marker with it), future joiners would
                                // binlog-recover past the base data — the
                                // exact corruption the flag exists to stop.
                                let mut flag_result = sql.set_group_pre_gtid_flag().await;
                                for _ in 0..4 {
                                    if flag_result.is_ok() {
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                    flag_result = sql.set_group_pre_gtid_flag().await;
                                }
                                if let Err(e) = flag_result {
                                    error!(error = %e, "failed to persist group pre-GTID flag");
                                    telemetry.send(TelemetryEvent::ComponentError {
                                        component: "mysql-wrapper".to_string(),
                                        error: e.to_string(),
                                        context: "set_group_pre_gtid_flag".to_string(),
                                    });
                                }
                            }
                            telemetry.send(TelemetryEvent::RoleChanged {
                                node: config.private_domain.clone(),
                                old_role: "none".to_string(),
                                new_role: "primary".to_string(),
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "bootstrap failed; re-evaluating");
                            telemetry.send(TelemetryEvent::ComponentError {
                                component: "mysql-wrapper".to_string(),
                                error: e.to_string(),
                                context: "bootstrap_group".to_string(),
                            });
                            safe_since = None;
                        }
                    }
                }
            }
            Ok(BootstrapVerdict::PeerIsMoreAdvanced(host)) => {
                safe_since = None;
                // Under dynamic candidacy that peer reaches SafeToBootstrap
                // itself — this node just waits for its group to appear and
                // joins it. Normal operation after a failover followed by a
                // full outage (the ex-primary is ahead), not an alert.
                wait_log_once(
                    &mut last_wait_reason,
                    &format!("peer {host} holds transactions this node lacks; waiting for it to bootstrap"),
                );
            }
            Ok(BootstrapVerdict::DeferToPeer(host)) => {
                safe_since = None;
                wait_log_once(
                    &mut last_wait_reason,
                    &format!("dataset ties peer {host}, which precedes this node in the tie-break; waiting for it to bootstrap"),
                );
            }
            Ok(BootstrapVerdict::Diverged(host)) => {
                safe_since = None;
                // Defensive only: classify_round resolves divergence into
                // Behind/Ahead (waiver generation, then seed order) before
                // decide() runs, so this arm should be unreachable. If it
                // ever fires, hold rather than guess — but surface it loudly,
                // because reaching here means the deterministic resolution
                // failed to apply. Telemetry on the transition only.
                let changed = wait_log_once(
                    &mut last_wait_reason,
                    &format!("GTID history DIVERGED from peer {host} and did not auto-resolve; holding (unexpected — classify_round should have resolved this)"),
                );
                if changed {
                    telemetry.send(TelemetryEvent::ComponentError {
                        component: "mysql-wrapper".to_string(),
                        error: format!("unresolved diverged GTID history with peer {host}"),
                        context: "bootstrap_guard".to_string(),
                    });
                }
            }
            Ok(BootstrapVerdict::Undecidable) => {
                safe_since = None;
                wait_log_once(
                    &mut last_wait_reason,
                    "not all peers are answering; refusing to bootstrap without a full picture",
                );
            }
            Ok(BootstrapVerdict::JoinExistingGroup) => {
                // Raced: a group appeared between the two checks above; the
                // next iteration joins it.
                safe_since = None;
            }
            Err(e) => {
                safe_since = None;
                warn!(error = %e, "bootstrap guard round failed");
            }
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// After a successful bootstrap, wait for the promotion to land (GR lifts
/// read_only asynchronously) so the RoleChanged event reports reality. The
/// recovery user already exists everywhere — every member creates its own,
/// unlogged, pre-fence (step 2b).
async fn post_bootstrap(sql: &Sql) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let self_uuid = sql.server_uuid().await?;
        let members = sql.group_members().await?;
        if role_is_writable_primary(&members, &self_uuid) {
            return Ok(());
        }
        if Instant::now() > deadline {
            anyhow::bail!("bootstrapped node did not become writable primary within 60s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Log a wait reason only when it changes — these loops run every few
/// seconds and would otherwise flood the logs with the same line. Returns
/// true on a transition, so callers can rate-limit side effects (telemetry)
/// the same way.
fn wait_log_once(last: &mut String, reason: &str) -> bool {
    if last != reason {
        info!("{reason}");
        *last = reason.to_string();
        return true;
    }
    false
}

/// The clone donor among the live group's members — the PRIMARY when one is
/// visible (its dataset is the authority by definition), else any active
/// member.
fn pick_donor(answers: &[(String, PeerAnswer)]) -> Option<String> {
    answers
        .iter()
        .filter_map(|(host, a)| match a {
            PeerAnswer::State(s) if s.group_active => Some((host.clone(), s.member_role.clone())),
            _ => None,
        })
        .max_by_key(|(_, role)| role.as_deref() == Some("PRIMARY"))
        .map(|(host, _)| host)
}

/// Tiny join_all so we don't pull the futures crate in for one call site.
async fn futures_join_all(
    futures: Vec<impl std::future::Future<Output = PeerAnswer>>,
) -> Vec<PeerAnswer> {
    let mut results = Vec::with_capacity(futures.len());
    for f in futures {
        results.push(f.await);
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    // The GTID comparisons themselves run inside mysqld (classify_round →
    // Sql::gtid_compare, exercised in test/e2e.sh); decide() is the pure
    // decision over those comparisons and is fully covered here.

    fn standing(host: &str, seed_rank: usize, relation: PeerRelation) -> PeerStanding {
        PeerStanding {
            host: host.to_string(),
            seed_rank,
            relation,
        }
    }

    fn equal(pre_gtid_data: bool) -> PeerRelation {
        PeerRelation::Equal { pre_gtid_data }
    }

    #[test]
    fn gone_tracker_waives_only_after_a_continuous_dwell() {
        let dwell = Duration::from_secs(60);
        let t0 = Instant::now();
        let mut tracker = GoneTracker::new();

        tracker.observe("mysql-3", NameVerdict::Gone, t0);
        assert!(!tracker.is_waived("mysql-3", t0, dwell));
        assert!(!tracker.is_waived("mysql-3", t0 + Duration::from_secs(59), dwell));
        assert!(tracker.is_waived("mysql-3", t0 + Duration::from_secs(60), dwell));

        // The clock does not restart while Gone persists.
        tracker.observe("mysql-3", NameVerdict::Gone, t0 + Duration::from_secs(30));
        assert!(tracker.is_waived("mysql-3", t0 + Duration::from_secs(60), dwell));
    }

    #[test]
    fn gone_tracker_resets_on_any_non_gone_observation() {
        let dwell = Duration::from_secs(60);
        let t0 = Instant::now();
        let mut tracker = GoneTracker::new();

        tracker.observe("mysql-3", NameVerdict::Gone, t0);
        // SERVFAIL/timeout/records all read as ExistsOrUnknown — a partition
        // or a comeback mid-dwell voids the proof entirely.
        tracker.observe(
            "mysql-3",
            NameVerdict::ExistsOrUnknown,
            t0 + Duration::from_secs(30),
        );
        assert!(!tracker.is_waived("mysql-3", t0 + Duration::from_secs(120), dwell));

        // Starting over requires a full fresh dwell.
        tracker.observe("mysql-3", NameVerdict::Gone, t0 + Duration::from_secs(40));
        assert!(!tracker.is_waived("mysql-3", t0 + Duration::from_secs(60), dwell));
        assert!(tracker.is_waived("mysql-3", t0 + Duration::from_secs(100), dwell));
    }

    #[test]
    fn gone_tracker_reachable_peer_resets_and_unknown_host_is_never_waived() {
        let dwell = Duration::from_secs(60);
        let t0 = Instant::now();
        let mut tracker = GoneTracker::new();

        assert!(!tracker.is_waived("mysql-2", t0, dwell));

        tracker.observe("mysql-2", NameVerdict::Gone, t0);
        tracker.observe_reachable("mysql-2");
        assert!(!tracker.is_waived("mysql-2", t0 + Duration::from_secs(600), dwell));
    }

    #[test]
    fn first_deploy_all_empty_first_seed_wins() {
        // Every node is fresh: all GTID sets empty ⇒ all Equal. The first
        // seed bootstraps; everyone else defers to it.
        let peers = vec![
            standing("mysql-2", 1, equal(false)),
            standing("mysql-3", 2, equal(false)),
        ];
        assert_eq!(decide(false, 0, &peers), BootstrapVerdict::SafeToBootstrap);

        let peers_of_second = vec![
            standing("mysql-1", 0, equal(false)),
            standing("mysql-3", 2, equal(false)),
        ];
        assert_eq!(
            decide(false, 1, &peers_of_second),
            BootstrapVerdict::DeferToPeer("mysql-1".to_string())
        );
    }

    #[test]
    fn most_advanced_node_bootstraps_regardless_of_seed_order() {
        // The regression that motivated dynamic candidacy: after a failover
        // followed by a full outage, the ex-primary (NOT the first seed)
        // holds transactions the first seed lacks. It must bootstrap...
        let peers_of_ex_primary = vec![
            standing("mysql-1", 0, PeerRelation::Behind),
            standing("mysql-3", 2, equal(false)),
        ];
        assert_eq!(
            decide(false, 1, &peers_of_ex_primary),
            BootstrapVerdict::SafeToBootstrap
        );

        // ...while the behind first seed waits for it instead of deadlocking.
        let peers_of_first_seed = vec![
            standing("mysql-2", 1, PeerRelation::Ahead),
            standing("mysql-3", 2, PeerRelation::Ahead),
        ];
        assert_eq!(
            decide(false, 0, &peers_of_first_seed),
            BootstrapVerdict::PeerIsMoreAdvanced("mysql-2".to_string())
        );
    }

    #[test]
    fn fresh_replacement_of_a_lost_seed_never_outranks_data_holders() {
        // First seed's volume was destroyed and it came back empty: both
        // peers are Ahead ⇒ it waits, no matter that it is seed[0].
        let peers = vec![
            standing("mysql-2", 1, PeerRelation::Ahead),
            standing("mysql-3", 2, PeerRelation::Ahead),
        ];
        assert_eq!(
            decide(false, 0, &peers),
            BootstrapVerdict::PeerIsMoreAdvanced("mysql-2".to_string())
        );

        // And the surviving data holders tie among themselves — the earliest
        // surviving seed bootstraps even with the fresh node answering.
        let peers_of_survivor = vec![
            standing("mysql-1", 0, PeerRelation::Behind),
            standing("mysql-3", 2, equal(false)),
        ];
        assert_eq!(
            decide(false, 1, &peers_of_survivor),
            BootstrapVerdict::SafeToBootstrap
        );
    }

    #[test]
    fn adopted_volume_outranks_fresh_equal_nodes() {
        // Conversion: the adopting root's base data is invisible to GTID
        // comparison (empty set, like the fresh replicas). The pre-GTID
        // holder must win the tie even from the LAST seed slot...
        let peers = vec![
            standing("mysql-2", 0, equal(false)),
            standing("mysql-3", 1, equal(false)),
        ];
        assert_eq!(decide(true, 2, &peers), BootstrapVerdict::SafeToBootstrap);

        // ...and a fresh first seed must defer to it.
        let peers_of_fresh = vec![
            standing("mysql-adopting", 2, equal(true)),
            standing("mysql-3", 1, equal(false)),
        ];
        assert_eq!(
            decide(false, 0, &peers_of_fresh),
            BootstrapVerdict::DeferToPeer("mysql-adopting".to_string())
        );
    }

    #[test]
    fn diverged_relation_is_defensive_only_but_still_freezes_decide() {
        // classify_round never emits Diverged anymore (it resolves into
        // Behind/Ahead via diverged_peer_wins), but decide() keeps freezing
        // on it as a belt-and-suspenders guard should one ever reach it.
        let peers = vec![
            standing("mysql-2", 1, PeerRelation::Diverged),
            standing("mysql-3", 2, PeerRelation::Behind),
        ];
        assert_eq!(
            decide(false, 0, &peers),
            BootstrapVerdict::Diverged("mysql-2".to_string())
        );
    }

    #[test]
    fn divergence_resolves_by_generation_then_seed_order() {
        // Higher waiver generation is the newer authority and wins outright,
        // regardless of seed rank.
        assert!(diverged_peer_wins(0, 0, 1, 9)); // peer gen 1 > my gen 0
        assert!(!diverged_peer_wins(2, 9, 1, 0)); // my gen 2 > peer gen 1

        // Equal generation falls through to seed order: lower rank wins.
        assert!(diverged_peer_wins(1, 2, 1, 1)); // peer rank 1 < my rank 2
        assert!(!diverged_peer_wins(1, 1, 1, 2)); // my rank 1 < peer rank 2

        // Symmetry: exactly one side self-heals. For any (myGen,myRank) vs
        // (peerGen,peerRank) with distinct ranks, the two nodes reach
        // opposite verdicts.
        let a = diverged_peer_wins(1, 0, 1, 1);
        let b = diverged_peer_wins(1, 1, 1, 0);
        assert_ne!(a, b);
    }

    #[test]
    fn waiver_generation_marker_roundtrips() {
        let dir =
            std::env::temp_dir().join(format!("mysql-wrapper-gen-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let data_dir = dir.to_string_lossy().into_owned();

        // Absent marker reads as generation 0 (pre-waiver volumes).
        assert_eq!(read_waiver_generation(&data_dir), 0);

        persist_waiver_generation(&data_dir, 3).unwrap();
        assert_eq!(read_waiver_generation(&data_dir), 3);

        // A blank/garbage marker degrades to 0, never panics.
        std::fs::write(waiver_generation_path(&data_dir), "  \n").unwrap();
        assert_eq!(read_waiver_generation(&data_dir), 0);
        std::fs::write(waiver_generation_path(&data_dir), "not-a-number").unwrap();
        assert_eq!(read_waiver_generation(&data_dir), 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn any_unknown_peer_voids_the_round() {
        // Unknown outranks every other relation — even a peer we know is
        // ahead can't make the round decidable, because the silent peer
        // might be further ahead still.
        let peers = vec![
            standing("mysql-2", 1, PeerRelation::Unknown),
            standing("mysql-3", 2, PeerRelation::Ahead),
        ];
        assert_eq!(decide(false, 0, &peers), BootstrapVerdict::Undecidable);
    }

    #[test]
    fn behind_peers_alone_do_not_block() {
        let peers = vec![
            standing("mysql-2", 1, PeerRelation::Behind),
            standing("mysql-3", 2, PeerRelation::Behind),
        ];
        // Rank is irrelevant when strictly ahead of everyone.
        assert_eq!(
            decide(false, usize::MAX, &peers),
            BootstrapVerdict::SafeToBootstrap
        );
    }

    fn peer_state(group_active: bool, server_uuid: Option<&str>) -> PeerAnswer {
        PeerAnswer::State(GrState {
            group_active,
            member_state: group_active.then(|| "ONLINE".to_string()),
            member_role: None,
            gtid_executed: Some(String::new()),
            members_total: usize::from(group_active),
            members_reachable: usize::from(group_active),
            pre_gtid_data: false,
            server_uuid: server_uuid.map(str::to_string),
            group_name: None,
            waiver_generation: 0,
        })
    }

    #[test]
    fn uuid_collision_detected_on_live_group_member() {
        // The restore signature: a live member reports OUR uuid.
        let answers = vec![
            ("mysql-1".to_string(), peer_state(true, Some("AAAA-1111"))),
            ("mysql-3".to_string(), peer_state(false, Some("AAAA-1111"))),
        ];
        assert_eq!(
            uuid_collision_peer("aaaa-1111", &answers),
            Some("mysql-1".to_string()),
            "case-insensitive match against a group-active peer must fire"
        );
    }

    #[test]
    fn uuid_collision_ignores_inactive_peers_and_distinct_uuids() {
        // A NOT-yet-joined peer sharing our uuid is not a collision with the
        // group (both sides resolve once one of them joins); a live peer
        // with a distinct uuid is the healthy case; a peer without the field
        // (older build) can't vote.
        let answers = vec![
            ("mysql-2".to_string(), peer_state(false, Some("AAAA-1111"))),
            ("mysql-1".to_string(), peer_state(true, Some("BBBB-2222"))),
            ("mysql-3".to_string(), peer_state(true, None)),
            ("mysql-4".to_string(), PeerAnswer::Unreachable),
        ];
        assert_eq!(uuid_collision_peer("AAAA-1111", &answers), None);
    }

    #[test]
    fn group_name_derivation_is_deterministic() {
        std::env::set_var("RAILWAY_ENVIRONMENT_ID", "env-123");
        let config = test_config();
        let a = resolve_group_name(&config);
        let b = resolve_group_name(&config);
        assert_eq!(a, b);
        assert!(Uuid::parse_str(&a).is_ok());
    }

    #[test]
    fn explicit_group_name_wins() {
        let mut config = test_config();
        config.gr_group_name = Some("11111111-2222-3333-4444-555555555555".to_string());
        assert_eq!(
            resolve_group_name(&config),
            "11111111-2222-3333-4444-555555555555"
        );
    }

    #[test]
    fn persisted_marker_wins_over_derivation() {
        let dir = std::env::temp_dir().join(format!("mysql-ha-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut config = test_config();
        config.data_dir = dir.to_string_lossy().to_string();

        persist_group_name(&config, "99999999-8888-7777-6666-555555555555").unwrap();
        assert_eq!(
            resolve_group_name(&config),
            "99999999-8888-7777-6666-555555555555"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    fn test_config() -> Config {
        // Construct directly rather than via from_env to keep tests
        // independent of process-global environment state.
        Config {
            mysql_root_password: "pw".to_string(),
            mysql_port: 3306,
            server_id: None,
            gr_seeds: Some("mysql-1.railway.internal:3306".to_string()),
            gr_enabled_flag: true,
            gr_group_name: None,
            gr_replication_password: Some("rp".to_string()),
            health_port: 8080,
            private_domain: "mysql-1.railway.internal".to_string(),
            socket_path: "/tmp/nonexistent.sock".to_string(),
            data_dir: "/tmp".to_string(),
            conf_dir: "/tmp".to_string(),
            peer_query_timeout_ms: 100,
            bootstrap_dwell_seconds: 1,
            innodb_buffer_pool_mb: None,
            mysql_max_connections: None,
            demote_timeout_ms: 20_000,
            peer_gone_dwell_seconds: 1800,
        }
    }
}
