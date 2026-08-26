//! HTTP health server embedded in each MySQL node.
//!
//! Three probe endpoints, all fail-closed (any error, timeout, or uncertain
//! read answers 503), plus one action endpoint the Railway dashboard drives:
//!
//!   GET /health — liveness: 200 iff mysqld answers `SELECT 1`.
//!   GET /role   — write-routing fence: 200 iff this node is the writable
//!                 Group Replication primary AND its view of the group has a
//!                 reachable majority (see sql::role_is_writable_primary).
//!                 HAProxy's write frontend routes exclusively on this.
//!                 In standalone mode (no GR_SEEDS) it degrades to liveness:
//!                 a lone node is trivially its own primary.
//!   GET /gr/state — peer exchange (JSON, see peers::GrState): group
//!                 membership + executed-GTID set, consumed by peers'
//!                 bootstrap guards. 503 until mysqld answers, so a peer
//!                 mid-boot reads as "not ready", never as "empty dataset".
//!   POST /switchover — ask THIS node to become the primary (the generic
//!                 clusterWiring.dataNodeSwitchover contract). Runs Group
//!                 Replication's own planned-handoff primitive,
//!                 group_replication_set_as_primary, against this node's own
//!                 uuid — synchronous through the group's consensus, so 200
//!                 means the switch completed (which /role then reflects);
//!                 503 carries the group's own refusal as the reason.

use crate::gr::local_gr_state;
use crate::sql::{role_is_writable_primary, Sql};
use anyhow::Context;
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use common::{Telemetry, TelemetryEvent};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{error, info, warn};

pub struct AppState {
    pub sql: Sql,
    pub standalone: bool,
    /// Datadir path — /gr/state reads the pre-GTID-data marker from it.
    pub data_dir: String,
    /// Raised by `gr::orchestrate` once its one-time adoption detection
    /// (the pre-GTID-data marker write / fresh-instance GTID reset) has
    /// completed. `/gr/state` refuses to answer before that — see its own
    /// doc comment above and `adoption_checked`'s own doc comment in gr.rs
    /// for why: `local_gr_state` reads that marker LIVE off disk, and
    /// mysqld answering queries does not imply the marker is written yet.
    /// Pre-set `true` in standalone mode, where no orchestrate task ever
    /// runs to raise it.
    pub adoption_checked: Arc<AtomicBool>,
    /// Raised by gr::majority_watch while the group sits below a majority of
    /// its DECLARED members (issue #33): /role answers 503 so HAProxy stops
    /// routing writes, matching the super_read_only fence the watch imposes.
    /// Always `false` in standalone mode.
    pub membership_fenced: Arc<AtomicBool>,
}

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.sql.ping().await {
        Ok(()) => (StatusCode::OK, "ok"),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "mysqld not answering"),
    }
}

async fn role(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if state.standalone {
        // No group to fence against — alive means writable.
        return match state.sql.ping().await {
            Ok(()) => (StatusCode::OK, "primary (standalone)"),
            Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "mysqld not answering"),
        };
    }

    let verdict = async {
        let self_uuid = state.sql.server_uuid().await?;
        let members = state.sql.group_members().await?;
        anyhow::Ok(role_is_writable_primary(&members, &self_uuid))
    }
    .await;

    match verdict {
        Ok(true) => {
            // The membership fence outranks the view: a lone survivor of a
            // graceful double stop IS its own view's writable primary, and
            // is exactly the node that must not take writes.
            if state.membership_fenced.load(Ordering::Acquire) {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "fenced: group below a majority of its declared members",
                )
            } else {
                (StatusCode::OK, "primary")
            }
        }
        Ok(false) => (StatusCode::SERVICE_UNAVAILABLE, "not primary"),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "state unavailable"),
    }
}

async fn gr_state(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if !state.adoption_checked.load(Ordering::Acquire) {
        // mysqld answering queries does not mean this node's identity is
        // fully known yet: adoption detection (has this datadir got
        // pre-GTID data?) is a one-time SQL round trip orchestrate() still
        // has to run. Answering early here would report "empty dataset"
        // for a node that is, in fact, an unmarked adopted volume —
        // indistinguishable to a peer from a genuinely fresh node. See
        // AppState::adoption_checked.
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "adoption detection not yet complete",
        )
            .into_response();
    }
    match local_gr_state(&state.sql, &state.data_dir).await {
        Ok(s) => (StatusCode::OK, Json(s)).into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "state unavailable").into_response(),
    }
}

/// How long group_replication_set_as_primary waits for the outgoing
/// primary's in-flight transactions to drain before the switch forces
/// through — enforced server-side by the function's own timeout argument,
/// so the handler's outer deadline only needs to sit above it.
const SET_PRIMARY_DRAIN_TIMEOUT_SECS: u32 = 15;
const SWITCHOVER_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// Promote this node via Group Replication's own primitive. Unlike Sentinel
/// (redis-ha's equivalent endpoint biases an election), GR ships a direct
/// "make member X primary" function that runs the whole handoff through the
/// group's consensus and blocks until it completes — no bias/restore dance,
/// no background settling, and concurrent group-configuration actions are
/// refused by the group itself with a clear error rather than racing.
async fn switchover(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if state.standalone {
        // No group — a lone node is trivially its own primary.
        return match state.sql.ping().await {
            Ok(()) => (StatusCode::OK, "already primary (standalone)".to_string()),
            Err(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "mysqld not answering".to_string(),
            ),
        };
    }

    let outcome = tokio::time::timeout(SWITCHOVER_DEADLINE, async {
        let self_uuid = state.sql.server_uuid().await?;
        let members = state.sql.group_members().await?;
        if role_is_writable_primary(&members, &self_uuid) {
            return anyhow::Ok(true);
        }
        state
            .sql
            .set_as_primary(&self_uuid, SET_PRIMARY_DRAIN_TIMEOUT_SECS)
            .await?;
        anyhow::Ok(false)
    })
    .await;

    match outcome {
        Ok(Ok(true)) => (StatusCode::OK, "already primary".to_string()),
        Ok(Ok(false)) => {
            info!("switchover complete: this node is now the primary");
            (StatusCode::OK, "switchover complete".to_string())
        }
        Ok(Err(e)) => {
            warn!(error = %e, "switchover refused");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("switchover refused: {e:#}"),
            )
        }
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("switchover timed out after {SWITCHOVER_DEADLINE:?}"),
        ),
    }
}

async fn run_health_server(health_port: u16, state: Arc<AppState>) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/role", get(role))
        .route("/gr/state", get(gr_state))
        .route("/switchover", post(switchover))
        .with_state(state);

    // Bind the IPv6 unspecified address rather than 0.0.0.0: Railway's private
    // network is IPv6 (fd12::... hostnames), and an IPv4-only listener refuses
    // every connection HAProxy's health check makes over it. Linux dual-stack
    // sockets accept IPv4-mapped connections on the same listener by default.
    // (Carried over from redis-ha's health_server, where this was load-bearing.)
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], health_port));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("health server bind failed")?;
    info!(port = health_port, "health server listening");

    axum::serve(listener, app)
        .await
        .context("health server exited")?;
    Ok(())
}

// A run that stayed up at least this long was healthy in between — the next
// failure is a new incident, not a continuation of the same crash loop, and
// earns its own telemetry event. Same thresholds as redis-ha's supervisor.
const HEALTHY_RUN_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(60);
const RESPAWN_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

/// Run the health server FOREVER, rebinding after any failure. This server is
/// the node's entire external interface — HAProxy's routing probe and every
/// peer's bootstrap-guard query go through it — so a dead server makes the
/// node invisible (a primary drops out of write rotation with mysqld
/// perfectly healthy, and peers read the node as unreachable, freezing
/// bootstrap decisions). The original shape (`expect(...)` inside a fire-and-
/// forget `tokio::spawn`) panicked the task silently and left the node in
/// exactly that state; mysqld's supervisor never noticed.
///
/// Mirrors redis-ha's supervisor: each attempt runs in its own spawned task
/// so a PANIC surfaces as a caught JoinError instead of killing this
/// supervision loop (which would silently recreate the original bug), and
/// telemetry is deduped per incident via HEALTHY_RUN_THRESHOLD so a crash
/// loop emits one ComponentError, not one every respawn.
pub async fn run_health_server_supervised(
    health_port: u16,
    state: Arc<AppState>,
    telemetry: Arc<Telemetry>,
) {
    let mut alerted_for_current_incident = false;

    loop {
        let attempt_state = state.clone();
        let started_at = std::time::Instant::now();
        let handle =
            tokio::task::spawn(async move { run_health_server(health_port, attempt_state).await });
        let outcome = handle.await;
        let ran_for = started_at.elapsed();

        let failure = match outcome {
            Ok(Ok(())) => {
                // axum::serve only returns on a graceful-shutdown signal we
                // never send — unexpected, but the answer is the same.
                error!("health server returned unexpectedly; restarting");
                "run loop returned cleanly".to_string()
            }
            Ok(Err(e)) => {
                error!(error = %e, "health server failed; restarting");
                format!("bind/serve failed: {e:#}")
            }
            Err(e) if e.is_panic() => {
                error!(panic = ?e, "health server panicked; restarting");
                "task panicked".to_string()
            }
            Err(e) => {
                error!(error = %e, "health server task was cancelled; restarting");
                "task cancelled".to_string()
            }
        };

        if ran_for >= HEALTHY_RUN_THRESHOLD {
            alerted_for_current_incident = false;
        }
        if !alerted_for_current_incident {
            alerted_for_current_incident = true;
            telemetry.send(TelemetryEvent::ComponentError {
                component: "mysql-wrapper".to_string(),
                error: failure,
                context: "health_server".to_string(),
            });
        }

        tokio::time::sleep(RESPAWN_DELAY).await;
    }
}
