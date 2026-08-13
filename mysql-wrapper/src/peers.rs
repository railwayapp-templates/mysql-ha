//! Peer state exchange over the health servers.
//!
//! Each node serves GET /gr/state (see health_server.rs); the orchestrator
//! queries its declared peers with it before making any bootstrap/join
//! decision. This is the Group Replication analogue of redis-ha's
//! peer-Sentinel boot query: never trust only local state when deciding
//! whether a live group exists.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::debug;

/// A node's self-reported Group Replication state. Also the /gr/state
/// response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrState {
    /// True when this node is an active member (ONLINE or RECOVERING) of a
    /// group — i.e. "a live group exists, join it, do not bootstrap".
    pub group_active: bool,
    pub member_state: Option<String>,
    pub member_role: Option<String>,
    /// Whitespace-normalized executed-GTID set. The bootstrap candidate uses
    /// it to prove its dataset is a superset of every reachable peer's before
    /// bootstrapping after a full outage.
    pub gtid_executed: Option<String>,
    pub members_total: usize,
    pub members_reachable: usize,
    /// True when this node holds data that predates its GTID history — an
    /// adopted standalone volume (Railway's standalone template runs with
    /// binlog disabled). Joiners MUST clone from such a group instead of
    /// using binlog-based recovery: GTID-wise a fresh joiner "already has"
    /// the un-GTID'd base data, so binlog recovery would silently skip it.
    #[serde(default)]
    pub pre_gtid_data: bool,
    /// This node's server_uuid. Lets a joiner detect that a live member
    /// already carries ITS identity — the signature of a datadir that is a
    /// byte copy of that member's (a volume backup of one node restored onto
    /// all of them) — instead of burning join attempts the group will refuse
    /// forever. Absent from peers on older wrapper builds.
    #[serde(default)]
    pub server_uuid: Option<String>,
    /// The group name this node runs (or would run) — its resolved identity.
    /// A waiver bootstrap mints a FRESH random name (see gr.rs), so joiners
    /// can no longer derive it: they adopt the live group's advertised name
    /// from here before START GROUP_REPLICATION. Absent on older builds.
    #[serde(default)]
    pub group_name: Option<String>,
    /// How many waiver bootstraps this node's history has been through — the
    /// divergence tie-break. When two nodes hold histories that BOTH contain
    /// transactions the other lacks and no group is live, the higher
    /// generation (the more recent authority) wins and the loser self-heals
    /// by recloning; equal generations fall through to seed order. Absent
    /// (0) on older builds and pre-waiver volumes.
    #[serde(default)]
    pub waiver_generation: u64,
}

/// One peer's answer, or why there isn't one. The distinction matters: an
/// unreachable peer blocks bootstrap (its dataset can't be compared), while
/// a reachable peer with no group is a vote in favor.
#[derive(Debug)]
pub enum PeerAnswer {
    State(GrState),
    /// The health server answered but mysqld behind it isn't ready (503) —
    /// treated the same as unreachable: no dataset to compare yet.
    NotReady,
    Unreachable,
}

pub async fn query_peer(
    client: &reqwest::Client,
    host: &str,
    health_port: u16,
    timeout: Duration,
) -> PeerAnswer {
    let url = format!("http://{host}:{health_port}/gr/state");
    match client.get(&url).timeout(timeout).send().await {
        Ok(resp) if resp.status().is_success() => match resp.json::<GrState>().await {
            Ok(state) => PeerAnswer::State(state),
            Err(e) => {
                debug!(host, error = %e, "peer /gr/state returned unparseable body");
                PeerAnswer::NotReady
            }
        },
        Ok(resp) => {
            debug!(host, status = %resp.status(), "peer /gr/state not ready");
            PeerAnswer::NotReady
        }
        Err(e) => {
            debug!(host, error = %e, "peer /gr/state unreachable");
            PeerAnswer::Unreachable
        }
    }
}
