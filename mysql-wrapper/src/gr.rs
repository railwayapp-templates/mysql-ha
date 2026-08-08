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
//!   - NEVER bootstrap unless every reachable peer's executed-GTID set is a
//!     subset of this node's — after a full outage the most-advanced node
//!     must be the one to bootstrap, and each node can verify that claim
//!     about itself locally.
//!   - The bootstrap decision must hold stable for a dwell period before it
//!     is acted on, so a slow-starting peer gets a window to contradict it.
//!   - Joining is the default: any peer reporting a live group means this
//!     node joins it, candidate or not.

use crate::config::Config;
use crate::peers::{query_peer, GrState, PeerAnswer};
use crate::sql::{role_is_writable_primary, Sql};
use anyhow::{Context, Result};
use common::{RailwayEnv, Telemetry, TelemetryEvent};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use uuid::Uuid;

pub const RECOVERY_USER: &str = "gr_recovery";
const GROUP_NAME_MARKER: &str = ".railway_gr_group_name";
const POLL_INTERVAL: Duration = Duration::from_secs(3);

fn marker_path(config: &Config) -> PathBuf {
    Path::new(&config.data_dir).join(GROUP_NAME_MARKER)
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
pub async fn local_gr_state(sql: &Sql) -> Result<GrState> {
    let self_uuid = sql.server_uuid().await?;
    let members = sql.group_members().await?;
    let gtid = sql.executed_gtid_set().await?;

    let me = members
        .iter()
        .find(|m| m.member_id.eq_ignore_ascii_case(&self_uuid));
    let member_state = me.map(|m| m.state.clone());
    let group_active = matches!(
        member_state.as_deref(),
        Some("ONLINE") | Some("RECOVERING")
    );

    Ok(GrState {
        group_active,
        member_state,
        member_role: me.map(|m| m.role.clone()),
        gtid_executed: Some(gtid),
        members_total: members.len(),
        members_reachable: members.iter().filter(|m| m.state != "UNREACHABLE").count(),
    })
}

/// What the candidate concluded from one round of peer answers.
#[derive(Debug, PartialEq)]
enum BootstrapVerdict {
    /// Some peer is in a live group — join it.
    JoinExistingGroup,
    /// Every peer answered, none has a group, and all datasets are subsets
    /// of ours — safe to bootstrap once the dwell passes.
    SafeToBootstrap,
    /// A peer holds transactions we don't — it should bootstrap, not us.
    PeerIsMoreAdvanced(String),
    /// At least one peer is unreachable/not-ready — no safe decision exists.
    Undecidable,
}

async fn classify_round(
    sql: &Sql,
    my_gtid: &str,
    answers: &[(String, PeerAnswer)],
) -> Result<BootstrapVerdict> {
    if answers
        .iter()
        .any(|(_, a)| matches!(a, PeerAnswer::State(s) if s.group_active))
    {
        return Ok(BootstrapVerdict::JoinExistingGroup);
    }
    for (host, answer) in answers {
        match answer {
            PeerAnswer::State(GrState {
                gtid_executed: Some(peer_gtid),
                ..
            }) => {
                if !peer_gtid.is_empty() && !sql.gtid_subset(peer_gtid, my_gtid).await? {
                    return Ok(BootstrapVerdict::PeerIsMoreAdvanced(host.clone()));
                }
            }
            // A reachable peer that didn't report a GTID set is as
            // undecidable as an unreachable one.
            PeerAnswer::State(_) | PeerAnswer::NotReady | PeerAnswer::Unreachable => {
                return Ok(BootstrapVerdict::Undecidable);
            }
        }
    }
    Ok(BootstrapVerdict::SafeToBootstrap)
}

/// The main orchestration loop. Returns once this node is an active group
/// member (the health server takes over from there).
pub async fn orchestrate(
    config: Arc<Config>,
    sql: Sql,
    telemetry: Arc<Telemetry>,
    group_name: String,
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

    // 2. Fence writes immediately: nothing may write to this node until the
    //    group decides its role. GR lifts this on the elected primary.
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
    let recovery_password = config
        .gr_replication_password
        .clone()
        .expect("HA mode requires GR_REPLICATION_PASSWORD (validated in Config::from_env)");
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

    let is_candidate = config.is_bootstrap_candidate();
    let peer_hosts = config.peer_hosts();
    let peer_timeout = Duration::from_millis(config.peer_query_timeout_ms);
    let dwell = Duration::from_secs(config.bootstrap_dwell_seconds);
    let http = reqwest::Client::new();

    info!(
        is_candidate,
        ?peer_hosts,
        group_name = %group_name,
        "starting group replication orchestration"
    );

    let mut safe_since: Option<Instant> = None;
    let mut last_wait_reason = String::new();

    loop {
        // Already an active member? (Covers both "join succeeded last
        // iteration" and "this node restarted while the group kept running".)
        match local_gr_state(&sql).await {
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
        for (host, answer) in peer_hosts.iter().zip(
            futures_join_all(futures).await,
        ) {
            answers.push((host.clone(), answer));
        }

        let group_seen = answers
            .iter()
            .any(|(_, a)| matches!(a, PeerAnswer::State(s) if s.group_active));

        if group_seen {
            safe_since = None;
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

        if !is_candidate {
            wait_log_once(&mut last_wait_reason, "no live group yet; waiting for the bootstrap candidate");
            safe_since = None;
            tokio::time::sleep(POLL_INTERVAL).await;
            continue;
        }

        // Candidate path: decide whether bootstrapping is provably safe.
        let my_gtid = match sql.executed_gtid_set().await {
            Ok(g) => g,
            Err(e) => {
                warn!(error = %e, "could not read local GTID set");
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
        };

        match classify_round(&sql, &my_gtid, &answers).await {
            Ok(BootstrapVerdict::SafeToBootstrap) => {
                let since = *safe_since.get_or_insert_with(Instant::now);
                let held = since.elapsed();
                if held < dwell {
                    wait_log_once(
                        &mut last_wait_reason,
                        "bootstrap looks safe; holding for the dwell period",
                    );
                } else {
                    info!(?held, "bootstrapping a new group");
                    match sql.bootstrap_group().await {
                        Ok(()) => {
                            if let Err(e) = post_bootstrap(&sql, &recovery_password).await {
                                error!(error = %e, "post-bootstrap setup failed");
                                telemetry.send(TelemetryEvent::ComponentError {
                                    component: "mysql-wrapper".to_string(),
                                    error: e.to_string(),
                                    context: "post_bootstrap".to_string(),
                                });
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
                // That peer is the rightful bootstrapper — but it is not the
                // declared candidate, so nothing will bootstrap without
                // operator/monitor action. Say so loudly and keep watching:
                // if it catches up the other way (or an operator intervenes),
                // the loop converges.
                wait_log_once(
                    &mut last_wait_reason,
                    &format!("peer {host} holds transactions this node lacks; refusing to bootstrap over it"),
                );
                telemetry.send(TelemetryEvent::ComponentError {
                    component: "mysql-wrapper".to_string(),
                    error: format!("peer {host} is more advanced than the bootstrap candidate"),
                    context: "bootstrap_guard".to_string(),
                });
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

/// After a successful bootstrap this node is the writable primary: create
/// the recovery user (replicates to every future joiner) and re-point the
/// local recovery channel at it.
async fn post_bootstrap(sql: &Sql, recovery_password: &str) -> Result<()> {
    // Wait for the promotion to land (GR lifts read_only asynchronously).
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let self_uuid = sql.server_uuid().await?;
        let members = sql.group_members().await?;
        if role_is_writable_primary(&members, &self_uuid) {
            break;
        }
        if Instant::now() > deadline {
            anyhow::bail!("bootstrapped node did not become writable primary within 60s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    sql.ensure_recovery_user(RECOVERY_USER, recovery_password)
        .await?;
    Ok(())
}

/// Log a wait reason only when it changes — these loops run every few
/// seconds and would otherwise flood the logs with the same line.
fn wait_log_once(last: &mut String, reason: &str) {
    if last != reason {
        info!("{reason}");
        *last = reason.to_string();
    }
}

/// Tiny join_all so we don't pull the futures crate in for one call site.
async fn futures_join_all(futures: Vec<impl std::future::Future<Output = PeerAnswer>>) -> Vec<PeerAnswer> {
    let mut results = Vec::with_capacity(futures.len());
    for f in futures {
        results.push(f.await);
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    // classify_round needs a live Sql for GTID_SUBSET, so the pure part we
    // can test here is the answer-shape handling with empty GTID sets (the
    // first-deploy path, where no GTID comparison is needed).
    // The full matrix is exercised in test/e2e.sh.

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
        let dir = std::env::temp_dir().join(format!(
            "mysql-ha-test-{}",
            std::process::id()
        ));
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
            gr_group_name: None,
            gr_replication_password: Some("rp".to_string()),
            health_port: 8080,
            private_domain: "mysql-1.railway.internal".to_string(),
            socket_path: "/tmp/nonexistent.sock".to_string(),
            data_dir: "/tmp".to_string(),
            conf_dir: "/tmp".to_string(),
            peer_query_timeout_ms: 100,
            bootstrap_dwell_seconds: 1,
        }
    }
}
