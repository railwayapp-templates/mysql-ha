//! HTTP health server embedded in each MySQL node.
//!
//! Three endpoints, all fail-closed (any error, timeout, or uncertain read
//! answers 503):
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

use crate::gr::local_gr_state;
use crate::sql::{role_is_writable_primary, Sql};
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

pub struct AppState {
    pub sql: Sql,
    pub standalone: bool,
    /// Datadir path — /gr/state reads the pre-GTID-data marker from it.
    pub data_dir: String,
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
        Ok(true) => (StatusCode::OK, "primary"),
        Ok(false) => (StatusCode::SERVICE_UNAVAILABLE, "not primary"),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "state unavailable"),
    }
}

async fn gr_state(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match local_gr_state(&state.sql, &state.data_dir).await {
        Ok(s) => (StatusCode::OK, Json(s)).into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "state unavailable").into_response(),
    }
}

pub async fn run_health_server(health_port: u16, state: Arc<AppState>) {
    let app = Router::new()
        .route("/health", get(health))
        .route("/role", get(role))
        .route("/gr/state", get(gr_state))
        .with_state(state);

    // Bind the IPv6 unspecified address rather than 0.0.0.0: Railway's private
    // network is IPv6 (fd12::... hostnames), and an IPv4-only listener refuses
    // every connection HAProxy's health check makes over it. Linux dual-stack
    // sockets accept IPv4-mapped connections on the same listener by default.
    // (Carried over from redis-ha's health_server, where this was load-bearing.)
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], health_port));
    info!(port = health_port, "health server listening");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("health server bind failed");

    axum::serve(listener, app)
        .await
        .expect("health server failed");
}
