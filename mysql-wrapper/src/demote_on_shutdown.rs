//! Hand the primary role off BEFORE mysqld is signaled to stop, so a
//! *planned* shutdown (redeploy, restart, scale) pays a consensual
//! switchover instead of the detection-timeout failover the group would
//! otherwise run once it notices the primary vanished.
//!
//! ## The gap this closes
//! Without this, every redeploy of the primary is an unplanned failover:
//! SIGTERM stops mysqld, the group only reacts after its suspicion timeout
//! expires and an election runs — a multi-second write blackout plus an RST
//! for every connection HAProxy had pinned to the old primary. Group
//! Replication ships the exact primitive for the planned case:
//! `group_replication_set_as_primary`, which drains in-flight transactions
//! and switches the primary through the group's own consensus. redis-ha does
//! the analogous demote-on-shutdown through Sentinel; Patroni does it for
//! postgres-ha.
//!
//! ## Where this runs
//! From `process_manager::supervise`'s SIGTERM/SIGINT arms, strictly BEFORE
//! `graceful_shutdown` signals mysqld — the server must still be up to drive
//! the handoff. HA mode only (the caller gates on a context being present);
//! a standalone node has nobody to hand off to.
//!
//! ## Sequence
//! 1. Am I the writable primary? (One local read; a secondary's shutdown
//!    does no further work.)
//! 2. Pick a target: any ONLINE secondary — deterministically the
//!    lexicographically-smallest host, so retries and logs agree. The
//!    function itself guarantees the target has applied its backlog before
//!    the switch completes, so freshness does not affect correctness.
//! 3. Call `group_replication_set_as_primary(target, drain)` bounded by the
//!    overall deadline. Any refusal, absence of a target, or timeout is
//!    logged at warn and shutdown proceeds unchanged — a failed demote must
//!    never block or slow the shutdown it was trying to smooth.
//!
//! ## Budget
//! The demote deadline (default 20s, `DEMOTE_TIMEOUT_MS`) leaves the drain
//! bound (15s, same value the /switchover endpoint uses) room to complete
//! while staying inside a typical container stop grace window;
//! `graceful_shutdown` then waits up to 30s for mysqld itself.

use crate::sql::{role_is_writable_primary, MemberRow, Sql};
use std::time::Duration;
use tracing::{info, warn};

/// Same drain bound the /switchover endpoint passes to
/// `group_replication_set_as_primary` — enforced server-side.
const DRAIN_TIMEOUT_SECS: u32 = 15;

pub struct DemoteCtx {
    pub sql: Sql,
    /// Overall bound on the whole demote attempt, milliseconds.
    pub deadline_ms: u64,
}

/// The ONLINE secondary that should inherit the primary role, or None when
/// the group offers nobody to hand off to. Deterministic (smallest host) so
/// logs and any concurrent observer agree on the pick.
fn choose_target(members: &[MemberRow], self_uuid: &str) -> Option<String> {
    members
        .iter()
        .filter(|m| {
            m.state == "ONLINE"
                && m.role == "SECONDARY"
                && !m.member_id.eq_ignore_ascii_case(self_uuid)
        })
        .min_by(|a, b| a.host.cmp(&b.host))
        .map(|m| m.member_id.clone())
}

/// Demote this node if it is the current writable primary. Never errors and
/// never blocks past the deadline: shutdown always proceeds.
pub async fn demote_if_primary(ctx: &DemoteCtx) {
    let deadline = Duration::from_millis(ctx.deadline_ms);
    let outcome = tokio::time::timeout(deadline, async {
        let self_uuid = ctx.sql.server_uuid().await?;
        let members = ctx.sql.group_members().await?;
        if !role_is_writable_primary(&members, &self_uuid) {
            return anyhow::Ok(None);
        }
        let Some(target) = choose_target(&members, &self_uuid) else {
            warn!("primary shutting down with no ONLINE secondary to hand off to");
            return anyhow::Ok(None);
        };
        let target_host = members
            .iter()
            .find(|m| m.member_id == target)
            .map(|m| m.host.clone())
            .unwrap_or_default();
        info!(%target_host, "handing primary to a secondary before shutdown");
        ctx.sql.set_as_primary(&target, DRAIN_TIMEOUT_SECS).await?;
        anyhow::Ok(Some(target_host))
    })
    .await;

    match outcome {
        Ok(Ok(Some(target_host))) => {
            info!(%target_host, "demoted before shutdown: primary handed off");
        }
        Ok(Ok(None)) => {}
        Ok(Err(e)) => {
            warn!(error = %e, "demote-on-shutdown failed; proceeding with shutdown");
        }
        Err(_) => {
            warn!(
                deadline_ms = ctx.deadline_ms,
                "demote-on-shutdown timed out; proceeding with shutdown"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, host: &str, state: &str, role: &str) -> MemberRow {
        MemberRow {
            member_id: id.to_string(),
            host: host.to_string(),
            state: state.to_string(),
            role: role.to_string(),
        }
    }

    #[test]
    fn picks_the_smallest_host_among_online_secondaries() {
        let members = vec![
            member("a", "mysql-1", "ONLINE", "PRIMARY"),
            member("c", "mysql-3", "ONLINE", "SECONDARY"),
            member("b", "mysql-2", "ONLINE", "SECONDARY"),
        ];
        assert_eq!(choose_target(&members, "a"), Some("b".to_string()));
    }

    #[test]
    fn skips_unhealthy_members_and_self() {
        let members = vec![
            member("a", "mysql-1", "ONLINE", "PRIMARY"),
            member("b", "mysql-2", "RECOVERING", "SECONDARY"),
            member("c", "mysql-3", "UNREACHABLE", "SECONDARY"),
        ];
        assert_eq!(choose_target(&members, "a"), None);

        // A self row mislabeled SECONDARY mid-transition is never the target.
        let only_self = vec![member("a", "mysql-1", "ONLINE", "SECONDARY")];
        assert_eq!(choose_target(&only_self, "a"), None);
    }

    #[test]
    fn empty_view_offers_no_target() {
        assert_eq!(choose_target(&[], "a"), None);
    }
}
