//! Local MySQL access for the wrapper — root connection over the unix socket.
//!
//! Every query the wrapper issues goes through here. Short reads carry a
//! 2s timeout so a hung mysqld degrades the health endpoints to fail-closed
//! 503s instead of hanging them; group-membership operations (START GROUP
//! REPLICATION with a clone in flight can run for minutes) are deliberately
//! un-timed.

use anyhow::{anyhow, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Opts, OptsBuilder, Pool};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

const SHORT_QUERY_TIMEOUT: Duration = Duration::from_secs(2);
const PASSWORD_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq)]
pub struct MemberRow {
    pub member_id: String,
    pub host: String,
    pub state: String,
    pub role: String,
}

#[derive(Clone)]
pub struct Sql {
    socket_path: Arc<String>,
    /// Swappable: the pool is built with the boot-time password guess, and
    /// the password-pin resolver replaces it once it has proven which
    /// password mysqld actually enforces (see password_pin.rs). Every clone
    /// of this handle observes the swap.
    pool: Arc<RwLock<Pool>>,
}

fn root_opts(socket_path: &str, root_password: &str) -> Opts {
    OptsBuilder::default()
        .socket(Some(socket_path))
        .user(Some("root"))
        .pass(Some(root_password))
        .into()
}

/// Outcome of a single-connection authentication probe, distinguishing "the
/// password is wrong" from "mysqld is not up yet" — the password-pin resolver
/// must never treat a booting server as a credential verdict.
#[derive(Debug)]
pub enum RootPasswordProbe {
    Works,
    AccessDenied,
    NotReady(String),
}

/// Try one throwaway connection with the given password. Kept off the shared
/// pool on purpose: probing candidates must not poison pooled connections.
pub async fn probe_root_password(socket_path: &str, password: &str) -> RootPasswordProbe {
    let attempt = async {
        let mut conn = mysql_async::Conn::new(root_opts(socket_path, password)).await?;
        conn.query_drop("SELECT 1").await?;
        conn.disconnect().await?;
        Ok::<_, mysql_async::Error>(())
    };
    match tokio::time::timeout(PASSWORD_PROBE_TIMEOUT, attempt).await {
        Ok(Ok(())) => RootPasswordProbe::Works,
        // ER_ACCESS_DENIED_ERROR — the server is up and rejected the
        // credential. Anything else (refused socket, timeout, init still
        // running) is indistinguishable from "not booted yet".
        Ok(Err(mysql_async::Error::Server(e))) if e.code == 1045 => {
            RootPasswordProbe::AccessDenied
        }
        Ok(Err(e)) => RootPasswordProbe::NotReady(e.to_string()),
        Err(_) => RootPasswordProbe::NotReady("probe timed out".to_string()),
    }
}

impl Sql {
    pub fn connect_root_over_socket(socket_path: &str, root_password: &str) -> Self {
        Self {
            socket_path: Arc::new(socket_path.to_string()),
            pool: Arc::new(RwLock::new(Pool::new(root_opts(socket_path, root_password)))),
        }
    }

    /// Rebuild the pool with a different root password (the pin resolver's
    /// one call). The old pool is drained in the background — its broken
    /// connections must not linger under the new identity.
    pub async fn swap_root_password(&self, root_password: &str) {
        let new_pool = Pool::new(root_opts(&self.socket_path, root_password));
        let old_pool = {
            let mut guard = self.pool.write().await;
            std::mem::replace(&mut *guard, new_pool)
        };
        tokio::spawn(async move {
            let _ = old_pool.disconnect().await;
        });
    }

    async fn conn(&self) -> Result<mysql_async::Conn> {
        let pool = self.pool.read().await.clone();
        Ok(pool.get_conn().await?)
    }

    async fn short<T>(
        &self,
        fut: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        tokio::time::timeout(SHORT_QUERY_TIMEOUT, fut)
            .await
            .map_err(|_| anyhow!("query timed out after {SHORT_QUERY_TIMEOUT:?}"))?
    }

    pub async fn ping(&self) -> Result<()> {
        self.short(async {
            let mut conn = self.conn().await?;
            conn.query_drop("SELECT 1").await?;
            Ok(())
        })
        .await
    }

    /// True while the server is docker-entrypoint's init-phase temp instance
    /// (`--skip-networking`). The orchestrator must not touch that instance:
    /// its socket is live, but the entrypoint is still running setup SQL
    /// against it.
    pub async fn is_init_temp_server(&self) -> Result<bool> {
        self.short(async {
            let mut conn = self.conn().await?;
            let skip: Option<i64> = conn.query_first("SELECT @@skip_networking").await?;
            Ok(skip == Some(1))
        })
        .await
    }

    /// Fence writes until Group Replication decides this node's role.
    /// Deliberately a runtime action, not a my.cnf directive — the rendered
    /// config is also read by docker-entrypoint's first-boot init, whose
    /// setup SQL must stay writable.
    pub async fn set_super_read_only(&self) -> Result<()> {
        self.short(async {
            let mut conn = self.conn().await?;
            conn.query_drop("SET GLOBAL super_read_only = ON").await?;
            Ok(())
        })
        .await
    }

    /// Purge this instance's local binlog/GTID history. ONLY safe on a
    /// provably-fresh instance (the datadir was empty when this boot
    /// started): docker-entrypoint's first-boot init runs with binlog+GTID
    /// on, so every node is born with a few local-only GTIDs under its own
    /// server_uuid. Left in place, those wedge the whole topology — the
    /// bootstrap guard reads them as "peer holds transactions I lack" in
    /// every direction, and Group Replication refuses joiners whose local
    /// GTID set isn't a subset of the group's. The same reset is what
    /// InnoDB Cluster's own provisioning (dba.configureInstance) does.
    pub async fn reset_fresh_instance_gtid_history(&self) -> Result<()> {
        self.short(async {
            let mut conn = self.conn().await?;
            // 8.4+/9.x syntax first; RESET MASTER for anything older.
            if conn.query_drop("RESET BINARY LOGS AND GTIDS").await.is_err() {
                conn.query_drop("RESET MASTER").await?;
            }
            Ok(())
        })
        .await
    }

    pub async fn server_uuid(&self) -> Result<String> {
        self.short(async {
            let mut conn = self.conn().await?;
            let uuid: Option<String> = conn.query_first("SELECT @@server_uuid").await?;
            uuid.context("@@server_uuid returned no row")
        })
        .await
    }

    /// Every member in this node's current view of the group. Empty when
    /// Group Replication has never been started on this node.
    pub async fn group_members(&self) -> Result<Vec<MemberRow>> {
        self.short(async {
            let mut conn = self.conn().await?;
            let rows: Vec<(String, Option<String>, String, String)> = conn
                .query(
                    "SELECT MEMBER_ID, MEMBER_HOST, MEMBER_STATE, MEMBER_ROLE \
                     FROM performance_schema.replication_group_members",
                )
                .await?;
            Ok(rows
                .into_iter()
                .map(|(member_id, host, state, role)| MemberRow {
                    member_id,
                    host: host.unwrap_or_default(),
                    state,
                    role,
                })
                .collect())
        })
        .await
    }

    /// This node's executed GTID set, whitespace-normalized (the server
    /// pretty-prints it with newlines).
    pub async fn executed_gtid_set(&self) -> Result<String> {
        self.short(async {
            let mut conn = self.conn().await?;
            let set: Option<String> = conn.query_first("SELECT @@GLOBAL.gtid_executed").await?;
            Ok(set
                .unwrap_or_default()
                .split_whitespace()
                .collect::<String>())
        })
        .await
    }

    /// Both subset directions in one round trip, evaluated by mysqld itself —
    /// the only component that parses GTID sets correctly. Returns
    /// `(peer ⊆ mine, mine ⊆ peer)`: both true means identical sets, both
    /// false means diverged histories (each side holds transactions the other
    /// lacks). Empty sets need no special-casing — GTID_SUBSET('', X) is 1.
    pub async fn gtid_compare(&self, mine: &str, peer: &str) -> Result<(bool, bool)> {
        self.short(async {
            let mut conn = self.conn().await?;
            let row: Option<(i64, i64)> = conn
                .exec_first(
                    "SELECT GTID_SUBSET(?, ?), GTID_SUBSET(?, ?)",
                    (peer, mine, mine, peer),
                )
                .await?;
            let (peer_sub_mine, mine_sub_peer) =
                row.context("GTID_SUBSET returned no row")?;
            Ok((peer_sub_mine == 1, mine_sub_peer == 1))
        })
        .await
    }

    /// Create/converge the distributed-recovery user and its grants, WITHOUT
    /// binlogging (`sql_log_bin = 0`, session-scoped).
    ///
    /// Runs on EVERY node before the write fence and before any START GROUP
    /// REPLICATION: with the MYSQL communication stack, each member
    /// authenticates INCOMING group connections against this user locally —
    /// the plugin's own local connectivity self-test fails without it, on the
    /// bootstrap node included. Unlogged because a joiner minting binlogged
    /// GTIDs of its own pre-join would be refused by the group (local set no
    /// longer a subset of the group's). This is the documented provisioning
    /// order for InnoDB Cluster / MYSQL-stack Group Replication.
    pub async fn ensure_recovery_user(&self, user: &str, password: &str) -> Result<()> {
        let user_lit = sql_string_literal(user);
        let pass_lit = sql_string_literal(password);
        // All statements on ONE connection: sql_log_bin is session-scoped.
        let mut conn = self.conn().await?;
        conn.query_drop("SET SESSION sql_log_bin = 0").await?;
        conn.query_drop(format!(
            "CREATE USER IF NOT EXISTS {user_lit}@'%' IDENTIFIED BY {pass_lit}"
        ))
        .await?;
        // Converge the password for pre-existing users (e.g. a re-run after a
        // crashed first boot with a since-rotated secret).
        conn.query_drop(format!(
            "ALTER USER {user_lit}@'%' IDENTIFIED BY {pass_lit}"
        ))
        .await?;
        conn.query_drop(format!(
            "GRANT REPLICATION SLAVE, CONNECTION_ADMIN, BACKUP_ADMIN, \
             GROUP_REPLICATION_STREAM ON *.* TO {user_lit}@'%'"
        ))
        .await?;
        conn.query_drop("SET SESSION sql_log_bin = 1").await?;
        Ok(())
    }

    /// Point the distributed-recovery channel at the shared credentials.
    /// Local metadata only — allowed under super_read_only, needed on every
    /// node before it can join or be rejoined.
    pub async fn configure_recovery_channel(&self, user: &str, password: &str) -> Result<()> {
        let user_lit = sql_string_literal(user);
        let pass_lit = sql_string_literal(password);
        let mut conn = self.conn().await?;
        conn.query_drop(format!(
            "CHANGE REPLICATION SOURCE TO SOURCE_USER = {user_lit}, \
             SOURCE_PASSWORD = {pass_lit} FOR CHANNEL 'group_replication_recovery'"
        ))
        .await?;
        Ok(())
    }

    /// Bootstrap a brand-new group from this node. Only the orchestrator's
    /// guarded path calls this.
    pub async fn bootstrap_group(&self) -> Result<()> {
        let mut conn = self.conn().await?;
        conn.query_drop("SET GLOBAL group_replication_bootstrap_group = ON")
            .await?;
        let start = conn.query_drop("START GROUP_REPLICATION").await;
        // Always clear the flag, even if START failed — a lingering ON is a
        // standing invitation to split-brain on the next START.
        conn.query_drop("SET GLOBAL group_replication_bootstrap_group = OFF")
            .await?;
        start?;
        Ok(())
    }

    /// Ask mysqld to exit. Used by remediations that need a fresh boot to
    /// take effect (regenerating server_uuid after auto.cnf removal): there
    /// is no in-container monitor process, so the supervisor exits the
    /// container with mysqld and the platform's restart policy boots it back
    /// up. The connection routinely drops while the statement executes — the
    /// caller must treat that error as the shutdown proceeding, same as the
    /// clone path.
    pub async fn shutdown_server(&self) -> Result<()> {
        let mut conn = self.conn().await?;
        conn.query_drop("SHUTDOWN").await?;
        Ok(())
    }

    /// Join an existing group. Blocks while distributed recovery runs; a
    /// clone-based recovery shuts the server down mid-way (no monitor process
    /// in-container to restart it), which surfaces here as an error while the
    /// supervisor exits the container — the next boot comes up with the
    /// cloned datadir and joins cleanly. Expected, documented behavior.
    pub async fn start_group_replication(&self) -> Result<()> {
        let mut conn = self.conn().await?;
        conn.query_drop("START GROUP_REPLICATION").await?;
        Ok(())
    }

    /// Hand the group's primary role to the member named by `uuid` — Group
    /// Replication's own planned-switchover primitive, run through the
    /// group's consensus. Blocks while the outgoing primary's in-flight
    /// transactions drain (bounded by `timeout_secs`, which the function
    /// enforces server-side), so this is deliberately NOT wrapped in
    /// `short()` — the caller owns the deadline, like
    /// `start_group_replication`.
    pub async fn set_as_primary(&self, uuid: &str, timeout_secs: u32) -> Result<()> {
        let mut conn = self.conn().await?;
        conn.query_drop(format!(
            "SELECT group_replication_set_as_primary({}, {timeout_secs})",
            sql_string_literal(uuid)
        ))
        .await?;
        Ok(())
    }

    /// Record group-level metadata: this group's dataset includes data that
    /// predates its GTID history (adopted standalone volume). Written by the
    /// adopting node once it is the writable primary; binlogged, so it
    /// replicates to every member and survives clones and the eventual
    /// deletion of the adopting node itself — unlike a file marker, which a
    /// clone-recreated datadir loses.
    pub async fn set_group_pre_gtid_flag(&self) -> Result<()> {
        let mut conn = self.conn().await?;
        conn.query_drop("CREATE SCHEMA IF NOT EXISTS railway_ha").await?;
        conn.query_drop(
            "CREATE TABLE IF NOT EXISTS railway_ha.meta \
             (k VARCHAR(64) PRIMARY KEY, v VARCHAR(255) NOT NULL)",
        )
        .await?;
        conn.query_drop(
            "INSERT INTO railway_ha.meta (k, v) VALUES ('pre_gtid_data', '1') \
             ON DUPLICATE KEY UPDATE v = '1'",
        )
        .await?;
        Ok(())
    }

    /// Read the group-level pre-GTID-data flag. False when the schema/table
    /// doesn't exist (plain groups never create it).
    pub async fn group_pre_gtid_flag(&self) -> bool {
        self.short(async {
            let mut conn = self.conn().await?;
            let v: Option<String> = conn
                .query_first("SELECT v FROM railway_ha.meta WHERE k = 'pre_gtid_data'")
                .await
                .unwrap_or(None);
            Ok(v.as_deref() == Some("1"))
        })
        .await
        .unwrap_or(false)
    }

    /// Explicitly clone this (empty) instance from a live group member.
    /// Used instead of START GROUP_REPLICATION when the group carries
    /// pre-GTID data (an adopted standalone volume): binlog-based recovery
    /// would silently skip that data, because GTID-wise the fresh joiner
    /// "already has" everything that was never binlogged.
    ///
    /// On success the server REPLACES its datadir and shuts itself down
    /// (there is no in-container monitor for its self-restart), so the
    /// expected outcome is a dropped connection followed by the supervisor
    /// exiting the container; the restarted container boots on the cloned
    /// data and joins through the normal path. REQUIRE SSL: the donor user
    /// is caching_sha2 and MySQL auto-generates server certs, so SSL is the
    /// reliable way for clone's internal client to authenticate.
    pub async fn clone_from_donor(
        &self,
        donor_host: &str,
        donor_port: u16,
        user: &str,
        password: &str,
    ) -> Result<()> {
        let donor = format!("{donor_host}:{donor_port}");
        let mut conn = self.conn().await?;
        // CLONE INSTANCE replaces the datadir, so it is a write — the write
        // fence (super_read_only, set at boot) blocks it with ERROR 1290.
        // Lift it just for the clone. Safe: this node is not in the group and
        // HAProxy fails its /role closed, so no external writes can land in
        // the window; and the clone shuts the server down on success, with
        // the fence re-applied by the orchestrator on the post-clone boot.
        conn.query_drop("SET GLOBAL super_read_only = OFF").await?;
        conn.query_drop("SET GLOBAL read_only = OFF").await?;
        conn.query_drop(format!(
            "SET GLOBAL clone_valid_donor_list = {}",
            sql_string_literal(&donor)
        ))
        .await?;
        conn.query_drop(format!(
            "CLONE INSTANCE FROM {}@{}:{} IDENTIFIED BY {} REQUIRE SSL",
            sql_string_literal(user),
            sql_string_literal(donor_host),
            donor_port,
            sql_string_literal(password),
        ))
        .await?;
        Ok(())
    }
}

/// Quote a string as a MySQL literal (single-quoted, backslash-escaped).
/// DDL doesn't take placeholders, so CREATE USER / CHANGE REPLICATION SOURCE
/// go through this.
pub fn sql_string_literal(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{escaped}'")
}

/// The /role fence, as a pure function (unit-tested; the health server calls
/// it with live rows).
///
/// 200 iff BOTH:
///   1. this node's own row is ONLINE + PRIMARY, and
///   2. a strict majority of the members in this node's current view are
///      reachable (state != UNREACHABLE).
///
/// Rule 2 is the split-brain fence: a primary partitioned into the minority
/// keeps reporting itself ONLINE/PRIMARY while it sees the majority as
/// UNREACHABLE — Group Replication blocks its writes, and this makes HAProxy
/// stop routing to it within one probe interval instead of piling up hung
/// connections.
pub fn role_is_writable_primary(members: &[MemberRow], self_uuid: &str) -> bool {
    if members.is_empty() {
        return false;
    }
    let total = members.len();
    let reachable = members.iter().filter(|m| m.state != "UNREACHABLE").count();
    if reachable * 2 <= total {
        return false;
    }
    members
        .iter()
        .find(|m| m.member_id.eq_ignore_ascii_case(self_uuid))
        .is_some_and(|m| m.state == "ONLINE" && m.role == "PRIMARY")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, state: &str, role: &str) -> MemberRow {
        MemberRow {
            member_id: id.to_string(),
            host: format!("{id}.railway.internal"),
            state: state.to_string(),
            role: role.to_string(),
        }
    }

    #[test]
    fn healthy_primary_passes() {
        let members = vec![
            member("a", "ONLINE", "PRIMARY"),
            member("b", "ONLINE", "SECONDARY"),
            member("c", "ONLINE", "SECONDARY"),
        ];
        assert!(role_is_writable_primary(&members, "a"));
        assert!(!role_is_writable_primary(&members, "b"));
    }

    #[test]
    fn minority_partitioned_primary_is_fenced() {
        // This node still believes it is the primary, but sees the other two
        // members as UNREACHABLE: 1 reachable of 3 is not a strict majority.
        let members = vec![
            member("a", "ONLINE", "PRIMARY"),
            member("b", "UNREACHABLE", "SECONDARY"),
            member("c", "UNREACHABLE", "SECONDARY"),
        ];
        assert!(!role_is_writable_primary(&members, "a"));
    }

    #[test]
    fn majority_side_with_one_unreachable_still_serves() {
        let members = vec![
            member("a", "ONLINE", "PRIMARY"),
            member("b", "ONLINE", "SECONDARY"),
            member("c", "UNREACHABLE", "SECONDARY"),
        ];
        assert!(role_is_writable_primary(&members, "a"));
    }

    #[test]
    fn recovering_or_offline_self_is_not_primary() {
        let members = vec![
            member("a", "RECOVERING", "SECONDARY"),
            member("b", "ONLINE", "PRIMARY"),
        ];
        assert!(!role_is_writable_primary(&members, "a"));
        // Empty view (GR never started) fails closed.
        assert!(!role_is_writable_primary(&[], "a"));
    }

    #[test]
    fn single_node_group_is_its_own_majority() {
        let members = vec![member("a", "ONLINE", "PRIMARY")];
        assert!(role_is_writable_primary(&members, "a"));
    }

    #[test]
    fn literal_escaping() {
        assert_eq!(sql_string_literal("plain"), "'plain'");
        assert_eq!(sql_string_literal("o'brien"), "'o\\'brien'");
        assert_eq!(sql_string_literal("back\\slash"), "'back\\\\slash'");
    }
}
