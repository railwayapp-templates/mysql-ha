//! HTTP health server embedded in each MySQL node.
//!
//! v0 scope: BOTH endpoints fail closed (503, body "not implemented"). This
//! is deliberate, not an oversight — the platform contract is that HAProxy's
//! write frontend only routes to a node whose `/role` returns 200, so until
//! this wrapper can actually confirm Group Replication state, failing closed
//! is the only safe answer. A stub that returned 200 unconditionally would
//! let HAProxy route writes to every node simultaneously.
//!
//! TODO(/health): return 200 once MySQL answers a liveness probe (e.g.
//! `SELECT 1` or `mysqladmin ping`), 503 otherwise.
//!
//! TODO(/role): return 200 iff BOTH conditions hold, mirroring redis-ha's
//! Sentinel-confirmed /role fence:
//!   1. `performance_schema.replication_group_members` shows this node's own
//!      `MEMBER_ID` with `MEMBER_STATE = 'ONLINE'` and `MEMBER_ROLE = 'PRIMARY'`.
//!   2. The group currently has quorum (a majority of declared members are
//!      ONLINE) — the split-brain fence. A primary that has lost contact
//!      with the rest of the group must not keep accepting writes just
//!      because MySQL still thinks it's the primary locally.
//! Any error, timeout, or uncertain read must be treated as "not primary" —
//! fail-closed, the same rule /health follows.

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use std::net::SocketAddr;
use tracing::info;

const NOT_IMPLEMENTED: &str = "not implemented";

async fn health() -> impl IntoResponse {
    // TODO: replace with a real liveness probe against MySQL.
    (StatusCode::SERVICE_UNAVAILABLE, NOT_IMPLEMENTED)
}

async fn role() -> impl IntoResponse {
    // TODO: replace with the replication_group_members + quorum check
    // described above. Fail-closed until then.
    (StatusCode::SERVICE_UNAVAILABLE, NOT_IMPLEMENTED)
}

pub async fn run_health_server(health_port: u16) {
    let app = Router::new()
        .route("/health", get(health))
        .route("/role", get(role));

    // Bind the IPv6 unspecified address rather than 0.0.0.0: Railway's private
    // network is IPv6 (fd12::... hostnames), and an IPv4-only listener refuses
    // every connection HAProxy's health check makes over it. Linux dual-stack
    // sockets accept IPv4-mapped connections on the same listener by default.
    // (Carried over from redis-ha's health_server, where this was load-bearing.)
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], health_port));
    info!(port = health_port, "health server listening (stub — fails closed on /health and /role)");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("health server bind failed");

    axum::serve(listener, app)
        .await
        .expect("health server failed");
}
