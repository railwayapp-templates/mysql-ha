//! Point-in-time recovery: pure domain logic shared by the archiver and the
//! restore-on-boot path — S3 object naming, full-backup metadata, mysqldump
//! coordinate parsing, and the newest-qualifying-full/purge-safety selection
//! rules. Kept free of any I/O (network, mysqld, subprocess) so every rule
//! here is exercised by a plain unit test; `s3.rs` (the bucket client),
//! `archiver.rs` and `restore.rs` (the mysqld/subprocess orchestration) are
//! the only callers.
//!
//! Object layout (per server lineage — multiple lineages can share one
//! bucket path, e.g. across a standalone volume's history):
//!
//! ```text
//! <PATH>/server-<server_uuid>/full/<RFC3339>.sql.gz
//! <PATH>/server-<server_uuid>/full/<RFC3339>.meta.json
//! <PATH>/server-<server_uuid>/binlog/<name>
//! ```
//!
//! Two kinds of archive live in that one layout, and several rules here
//! branch on which one they are looking at:
//!
//!   - **Independent histories** — a standalone server whose lineage changes
//!     only when its volume is reset. Lineages are unrelated datasets; a
//!     restore replays exactly one lineage from one of ITS fulls, and binlogs
//!     carry anonymous transactions (gtid_mode=OFF), so nothing can be
//!     cross-checked between lineages.
//!   - **One shared history, several writers** — a Group Replication cluster
//!     archiving from whichever member is the writable primary. Every member
//!     logs the same group transactions under the same GTIDs, so any
//!     lineage's binlogs are a valid stream of the group's history, and a
//!     restore from one lineage's full legitimately replays every other
//!     lineage too: the server skips the GTIDs it already has, and a hole in
//!     the result's `gtid_executed` is proof the archive lost a transaction.
//!     A full's meta records the dump's own GTID set (`gtid_purged`); its
//!     presence anywhere in the archive is what marks it shared
//!     (`archive_shares_history`).

use anyhow::{Context, Result};
use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Where the archive/restore bucket lives and how to reach it — built from
/// one gate's worth of env vars (either the `BINLOG_ARCHIVE_*` or the
/// `BINLOG_RECOVER_FROM_*` family; see config.rs). Deliberately explicit:
/// every field comes straight from the env contract, never ambient AWS
/// config/credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Location {
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub endpoint: String,
    /// Base path prefix under the bucket (env default "/binlog"). Leading
    /// slash tolerated and stripped — S3 keys never start with one.
    pub path: String,
}

impl S3Location {
    /// The path with any leading/trailing slashes trimmed, for key building.
    fn base(&self) -> &str {
        self.path.trim_matches('/')
    }
}

/// The bucket path prefix with slashes normalized, exposed for callers that
/// need to list the WHOLE archive tree (every lineage) rather than one
/// server's own prefix — restore's full-backup discovery, which must
/// consider every `server-*/full/` lineage, not just one.
pub fn base_prefix(loc: &S3Location) -> String {
    loc.base().to_string()
}

/// A server's own object prefix: `<path>/server-<uuid>`.
pub fn server_prefix(loc: &S3Location, server_uuid: &str) -> String {
    let base = loc.base();
    if base.is_empty() {
        format!("server-{server_uuid}")
    } else {
        format!("{base}/server-{server_uuid}")
    }
}

/// Archive-level marker a Group Replication primary writes when it starts
/// archiving (`<path>/shared-history`). Restore and retention read it
/// alongside the fulls' `gtid_purged` to decide the archive is one shared
/// history: a standalone server converted to HA keeps its `server_uuid`, so
/// until its first post-conversion full lands nothing else in the archive
/// says its newer binlogs carry GTIDs. Never removed — a history that once
/// had GTID writers is replayed as one from then on, which only ever errs
/// toward replaying more (the server dedups by GTID).
pub fn shared_history_marker_key(loc: &S3Location) -> String {
    let base = base_prefix(loc);
    if base.is_empty() {
        "shared-history".to_string()
    } else {
        format!("{base}/shared-history")
    }
}

/// Prefix every full backup for one lineage lives under.
pub fn full_prefix(loc: &S3Location, server_uuid: &str) -> String {
    format!("{}/full", server_prefix(loc, server_uuid))
}

/// Prefix every shipped binlog for one lineage lives under.
pub fn binlog_prefix(loc: &S3Location, server_uuid: &str) -> String {
    format!("{}/binlog", server_prefix(loc, server_uuid))
}

pub fn full_dump_key(loc: &S3Location, server_uuid: &str, rfc3339: &str) -> String {
    format!("{}/{rfc3339}.sql.gz", full_prefix(loc, server_uuid))
}

pub fn full_meta_key(loc: &S3Location, server_uuid: &str, rfc3339: &str) -> String {
    format!("{}/{rfc3339}.meta.json", full_prefix(loc, server_uuid))
}

pub fn binlog_key(loc: &S3Location, server_uuid: &str, name: &str) -> String {
    format!("{}/{name}", binlog_prefix(loc, server_uuid))
}

/// Pull the `server_uuid` lineage out of one of this module's own keys
/// (`<path>/server-<uuid>/...`). `None` for anything that doesn't match the
/// shape — e.g. a key from an unrelated prefix sharing the bucket.
pub fn server_uuid_from_key(loc: &S3Location, key: &str) -> Option<String> {
    let base = loc.base();
    let rest = if base.is_empty() {
        key
    } else {
        key.strip_prefix(base)?.strip_prefix('/')?
    };
    let rest = rest.strip_prefix("server-")?;
    let uuid = rest.split('/').next()?;
    (!uuid.is_empty()).then(|| uuid.to_string())
}

/// The current UTC instant, formatted the same way on every object name and
/// every `meta.json.taken_at` — millisecond precision, `Z` suffix, matching
/// the `MYSQL_RECOVERY_TARGET_TIME` example in the env contract exactly
/// (`2026-08-13T14:00:00.000Z`).
pub fn format_rfc3339_millis(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// The instant floored to the millisecond — the precision a full backup's
/// object name carries (`format_rfc3339_millis`), and so the precision at
/// which the archive advertises what it holds: the platform lists names, not
/// metas, to offer the oldest restorable point.
pub fn floor_to_millis(t: DateTime<Utc>) -> DateTime<Utc> {
    let nanos = t.nanosecond();
    // `with_nanosecond` refuses only values of two seconds or more; a
    // floored in-range value (leap-second representation included) never is.
    t.with_nanosecond(nanos - nanos % 1_000_000).unwrap_or(t)
}

/// Parse an operator-supplied ISO-8601 UTC timestamp (`MYSQL_RECOVERY_TARGET_TIME`).
pub fn parse_target_time(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| {
            format!("MYSQL_RECOVERY_TARGET_TIME {s:?} is not a valid ISO-8601 timestamp")
        })
}

/// `<path>/owner.json` — which service instance(s) an archive root belongs
/// to. One root, one database history: two services archiving into the same
/// bucket path interleave two histories in one archive, and a restore picks
/// the newest full across lineages — the other service's data, served as a
/// success. Nothing upstream prevents the configuration (a duplicated
/// service keeps its variables; a forked environment may keep its bucket),
/// so the archiver claims the root on first use and refuses a root that is
/// not its own (see `archive_ownership_verdict`).
pub fn owner_key(loc: &S3Location) -> String {
    let base = base_prefix(loc);
    if base.is_empty() {
        "owner.json".to_string()
    } else {
        format!("{base}/owner.json")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveOwner {
    /// RAILWAY_ENVIRONMENT_ID of the service(s) archiving here.
    pub environment_id: String,
    /// The history's GTID identity: the group name — the UUID every group
    /// transaction carries — which this image derives from the environment
    /// for a standalone server too, so a server converted to a group, or a
    /// member reverted to standalone, keeps it.
    pub history: String,
    /// Every service that has archived into this root: the one standalone
    /// server, or each member of a group that has held the primary role.
    pub service_ids: Vec<String>,
    pub claimed_at: DateTime<Utc>,
}

/// The server about to archive, as the ownership rules see it.
pub struct ArchiveClaimant<'a> {
    pub environment_id: &'a str,
    pub service_id: &'a str,
    pub history: &'a str,
    /// Archiving as a group's writable primary (any member may hold it).
    pub group_primary: bool,
    /// This server's `gtid_executed`: a standalone server that carries the
    /// root's history is a former member of its group, not a stranger.
    pub executed_gtid_set: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipVerdict {
    /// Write this record (a first claim, or a group member recording itself).
    Write(ArchiveOwner),
    /// The root is this service's already; nothing to write.
    Keep,
    /// Not this service's root; the reason names the owner and the remedy.
    Refuse(String),
}

/// The ownership rules for an archive root:
///   - unclaimed → claimed by this service;
///   - claimed by another environment → refused, whatever the mode (a forked
///     environment that kept the bucket, another project on the same path);
///   - same environment, archiving as a group primary → the group's members
///     share the root by design; this member is recorded;
///   - same environment, standalone, recorded → kept;
///   - same environment, standalone, not recorded → refused unless this
///     server carries the root's history in its `gtid_executed` (a member
///     reverted to standalone), in which case it is recorded. A duplicated
///     service — fresh data, another service id — is the case refused.
pub fn archive_ownership_verdict(
    root: &str,
    existing: Option<&ArchiveOwner>,
    me: &ArchiveClaimant<'_>,
    now: DateTime<Utc>,
) -> OwnershipVerdict {
    let Some(owner) = existing else {
        return OwnershipVerdict::Write(ArchiveOwner {
            environment_id: me.environment_id.to_string(),
            history: me.history.to_string(),
            service_ids: vec![me.service_id.to_string()],
            claimed_at: now,
        });
    };
    let owners = if owner.service_ids.is_empty() {
        "(unrecorded)".to_string()
    } else {
        owner.service_ids.join(", ")
    };
    if owner.environment_id != me.environment_id {
        return OwnershipVerdict::Refuse(format!(
            "archive root {root:?} belongs to environment {} (service {owners}); this service \
             runs in environment {} — archiving here would interleave two databases' histories \
             in one archive, and a restore could serve the wrong one. Point BINLOG_ARCHIVE_PATH \
             at a path of this service's own, or delete {root}/owner.json if that archive is \
             abandoned and this service is to take the root over",
            owner.environment_id, me.environment_id
        ));
    }
    let recorded = owner.service_ids.iter().any(|id| id == me.service_id);
    let record_me = || {
        let mut updated = owner.clone();
        updated.service_ids.push(me.service_id.to_string());
        OwnershipVerdict::Write(updated)
    };
    if recorded {
        return OwnershipVerdict::Keep;
    }
    if me.group_primary {
        return record_me();
    }
    let carries_history = !owner.history.is_empty()
        && me
            .executed_gtid_set
            .split(',')
            .any(|entry| entry.trim_start().starts_with(&owner.history));
    if carries_history {
        return record_me();
    }
    OwnershipVerdict::Refuse(format!(
        "archive root {root:?} belongs to service {owners} in this environment; this standalone \
         service ({}) carries none of that archive's history — archiving here would interleave \
         two databases' histories in one archive, and a restore could serve the wrong one. \
         Point BINLOG_ARCHIVE_PATH at a path of this service's own, or delete {root}/owner.json \
         if that archive is abandoned and this service is to take the root over",
        if me.service_id.is_empty() { "unknown service id" } else { me.service_id }
    ))
}

/// A full backup's sidecar metadata (`<...>.meta.json`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FullBackupMeta {
    pub taken_at: DateTime<Utc>,
    pub binlog_file: String,
    pub binlog_pos: u64,
    pub server_uuid: String,
    pub mysql_version: String,
    /// The GTID set the dump embeds (`SET @@GLOBAL.GTID_PURGED`): every
    /// transaction the dump already contains, by identity. Present exactly
    /// when the source ran with GTIDs — a Group Replication member, or any
    /// gtid_mode=ON server — and possibly empty on a GTID server with no
    /// history yet. `None` on dumps from an anonymous-transaction
    /// (gtid_mode=OFF) standalone server, and on metas written before this
    /// field existed (serde default). Restore keys its whole
    /// lineage-stitching strategy on it (see restore.rs), and retention keys
    /// `archive_shares_history` on it.
    #[serde(default)]
    pub gtid_purged: Option<String>,
    /// Bytes of SQL mysqldump produced for this full, before gzip — what the
    /// restore streams into the restore-phase server. With `datadir_bytes` it
    /// turns the platform's disk pre-flight from a floor into an estimate.
    /// `None` on metas written before the field existed.
    #[serde(default)]
    pub dump_bytes: Option<u64>,
    /// Size of the source's data directory when the dump started: the closest
    /// predictor of what the restored data directory will occupy.
    #[serde(default)]
    pub datadir_bytes: Option<u64>,
}

/// One full backup discovered in the bucket, with enough to select it and
/// then locate its dump object and lineage's binlogs.
#[derive(Debug, Clone, PartialEq)]
pub struct FullBackupRef {
    pub server_uuid: String,
    pub dump_key: String,
    pub meta: FullBackupMeta,
}

/// The newest full backup, across every lineage, taken at or before the
/// recovery target — restore's core selection rule. Ties (same instant,
/// different lineages — vanishingly unlikely but not impossible) break on
/// `server_uuid` so the choice is deterministic.
///
/// "At or before" is judged at the precision of the full's NAME
/// (`floor_to_millis`). The platform reads the oldest restorable point off
/// that name and pins a fork's target exactly there when the earliest point
/// is asked for. Every meta written before the archiver floored `taken_at`
/// records the same instant with its nanoseconds — microseconds AFTER the
/// name — and a strict comparison found no full "at or before" a target that
/// IS the full: the fork crash-looped on `no-full` (production, 2026-09-10).
/// A full is restorable from the instant its name carries.
pub fn newest_qualifying_full(
    fulls: &[FullBackupRef],
    target: DateTime<Utc>,
) -> Option<&FullBackupRef> {
    fulls
        .iter()
        .filter(|f| floor_to_millis(f.meta.taken_at) <= target)
        .max_by(|a, b| {
            a.meta
                .taken_at
                .cmp(&b.meta.taken_at)
                .then_with(|| a.server_uuid.cmp(&b.server_uuid))
        })
}

/// How far short of the requested target an achieved recovery point may fall
/// before the restore must refuse (seconds). Reaching the EXACT target is
/// structurally impossible: everything after the last shipped rotation still
/// lives in the active binlog, which is never uploaded (see
/// `binlog_is_closed`), so the check is bounded by the archiver's rotation
/// cadence — two full rotation intervals (the last window itself, plus one
/// interval of shipping lag) plus a fixed 60s of clock/upload slop.
pub fn achieved_lag_bound_seconds(rotate_interval_seconds: u64) -> u64 {
    rotate_interval_seconds.saturating_mul(2).saturating_add(60)
}

/// True when the achieved recovery point is at/past the target, or short of
/// it by no more than `bound_seconds` (see `achieved_lag_bound_seconds`).
pub fn achieved_point_within_bound(
    target: DateTime<Utc>,
    achieved: DateTime<Utc>,
    bound_seconds: u64,
) -> bool {
    target.signed_duration_since(achieved).num_seconds()
        <= i64::try_from(bound_seconds).unwrap_or(i64::MAX)
}

/// Parse one `mysqlbinlog` text-output event header into its timestamp:
///
/// ```text
/// #260813 14:00:02 server id 1  end_log_pos 157 ...
/// #260813  1:02:03 server id 1  end_log_pos 157 ...
/// ```
///
/// Every event prints one of these — user transactions, and the trailing
/// Rotate/Stop event mysqld writes when it closes the file — which is what
/// makes the LAST header the archive's coverage point even for an idle tail.
/// mysqlbinlog formats the timestamp in ITS OWN local time zone (there is no
/// flag to choose one), so callers must pin `TZ=UTC` on the subprocess for
/// this UTC interpretation to hold. The two-digit year follows MySQL's own
/// window (70–99 → 19xx, 00–69 → 20xx); the hour is space-padded (`%2d`).
/// `None` for anything else — including an artificial event's zero timestamp
/// (`#700101  0:00:00`), which is never a real coverage point.
pub fn parse_binlog_event_header_utc(line: &str) -> Option<DateTime<Utc>> {
    use chrono::TimeZone;

    let rest = line.strip_prefix('#')?;
    let date = rest.get(0..6)?;
    if !date.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut parts = rest.get(6..)?.split_whitespace();
    let time = parts.next()?;
    // Require the ` server id ` marker so an unrelated comment line that
    // happens to start with six digits can never be misread as a header.
    if parts.next()? != "server" || parts.next()? != "id" {
        return None;
    }
    let yy: i32 = date[0..2].parse().ok()?;
    let year = if yy >= 70 { 1900 + yy } else { 2000 + yy };
    let month: u32 = date[2..4].parse().ok()?;
    let day: u32 = date[4..6].parse().ok()?;
    let mut hms = time.split(':');
    let hour: u32 = hms.next()?.parse().ok()?;
    let minute: u32 = hms.next()?.parse().ok()?;
    let second: u32 = hms.next()?.parse().ok()?;
    if hms.next().is_some() {
        return None;
    }
    let ts = Utc
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()?;
    (ts.timestamp() > 0).then_some(ts)
}

/// The other discovered full backups, newest first, formatted for restore
/// errors — the operator's concrete fallback options when the selected
/// full's lineage cannot reach the requested target: restore from one of
/// these instead (usually by adjusting MYSQL_RECOVERY_TARGET_TIME to a point
/// that full's lineage covers). Listing only — full SELECTION stays
/// `newest_qualifying_full`; automatically falling back across lineages
/// would resurrect replaced data.
pub fn describe_fallback_fulls(fulls: &[FullBackupRef], selected: &FullBackupRef) -> String {
    let mut others: Vec<&FullBackupRef> = fulls
        .iter()
        .filter(|f| f.dump_key != selected.dump_key)
        .collect();
    if others.is_empty() {
        return "none (this is the only full backup discovered in the bucket)".to_string();
    }
    others.sort_by(|a, b| {
        b.meta
            .taken_at
            .cmp(&a.meta.taken_at)
            .then_with(|| a.server_uuid.cmp(&b.server_uuid))
    });
    others
        .iter()
        .map(|f| {
            format!(
                "server-{} full taken at {}",
                f.server_uuid,
                format_rfc3339_millis(f.meta.taken_at)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Which `mysqldump` coordinate flag to use: MySQL 8.0.23+ renamed
/// `--master-data` to `--source-data` (and the emitted comment from
/// `CHANGE MASTER TO` to `CHANGE REPLICATION SOURCE TO`) as part of the
/// replication-terminology modernization. Probing the installed binary's
/// `--help` output at runtime (this function's input) is what actually
/// decides it; `dump_data_flag_by_major` below is only the fallback for when
/// the probe itself can't run.
pub fn pick_dump_data_flag(mysqldump_help: &str) -> &'static str {
    if mysqldump_help.contains("--source-data") {
        "--source-data=2"
    } else {
        "--master-data=2"
    }
}

/// Fallback when `mysqldump --help` itself couldn't be run: every MySQL 8.x
/// build the wrapper ships (8.0.23+, since the image floors at 8.0/8.4/9.x
/// series) understands `--source-data`; only a pre-8.0.23 server would need
/// the old spelling, which this image line never bundles — kept as a
/// defensive floor, not a live code path.
pub fn dump_data_flag_by_major(mysql_major: u32) -> &'static str {
    if mysql_major >= 8 {
        "--source-data=2"
    } else {
        "--master-data=2"
    }
}

/// The leading major version number out of `@@version` (e.g. "8" from
/// "8.4.3" or "8.0.39-standard"). Only consulted when the `mysqldump --help`
/// probe itself couldn't run (see `dump_data_flag_by_major`).
pub fn mysql_major_version(version: &str) -> Option<u32> {
    version.split(['.', '-']).next()?.parse().ok()
}

/// Parse the coordinate line `mysqldump --source-data=2` (or the older
/// `--master-data=2`) emits, commented out, near the top of the dump:
///
/// ```text
/// -- CHANGE MASTER TO MASTER_LOG_FILE='binlog.000003', MASTER_LOG_POS=157;
/// -- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='binlog.000003', SOURCE_LOG_POS=157;
/// ```
///
/// Handles both spellings; `None` when neither is present (e.g. the flag
/// wasn't actually applied, or `dump_head` didn't reach far enough into the
/// file — see the archiver's scan cap).
pub fn parse_change_master_coords(dump_head: &str) -> Option<(String, u64)> {
    for line in dump_head.lines() {
        if !(line.contains("CHANGE MASTER TO") || line.contains("CHANGE REPLICATION SOURCE TO")) {
            continue;
        }
        let file = extract_quoted(line, "MASTER_LOG_FILE=")
            .or_else(|| extract_quoted(line, "SOURCE_LOG_FILE="))?;
        let pos = extract_number(line, "MASTER_LOG_POS=")
            .or_else(|| extract_number(line, "SOURCE_LOG_POS="))?;
        return Some((file, pos));
    }
    None
}

/// Pull the GTID set a mysqldump embeds out of the head of its output:
///
/// ```text
/// SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '8f0e...:1-13,
/// 9a11...:1-2';
/// ```
///
/// The literal can span several lines (one UUID's ranges per line, comma
/// separated), so this scans from the assignment to the closing quote and
/// drops the whitespace, matching the server's own normalized form. Both
/// spellings are handled — the 8.0+ one with the version-gated `'+'`
/// comment and the bare 5.7 one. `Some("")` for a GTID-enabled source with
/// no history yet; `None` when the dump carries no GTID set at all, which is
/// what mysqldump emits for a gtid_mode=OFF source.
pub fn parse_gtid_purged(dump_head: &str) -> Option<String> {
    const MARKER: &str = "SET @@GLOBAL.GTID_PURGED=";
    let start = dump_head.find(MARKER)? + MARKER.len();
    let rest = &dump_head[start..];
    let open = rest.find('\'')?;
    let mut literal = &rest[open + 1..];
    // The 8.0 form quotes a `+` inside the version comment before the set
    // itself (`/*!80000 '+'*/ '...'`); step past it to the real literal.
    if let Some(after_plus) = literal.strip_prefix("+'") {
        let next_open = after_plus.find('\'')?;
        literal = &after_plus[next_open + 1..];
    }
    let close = literal.find('\'')?;
    Some(literal[..close].split_whitespace().collect())
}

/// The `taken_at` instant a full backup's object NAME encodes
/// (`.../full/<RFC3339>.sql.gz` or `.meta.json`), readable from a listing
/// alone without a GET. The sidecar meta records the same instant — floored
/// to the millisecond since the archiver started doing so; with its
/// nanoseconds in metas written before that. `None` for any key that is not
/// a full-backup object.
pub fn full_taken_at_from_key(key: &str) -> Option<DateTime<Utc>> {
    if !key.contains("/full/") {
        return None;
    }
    let name = key.rsplit('/').next()?;
    let stem = name
        .strip_suffix(".meta.json")
        .or_else(|| name.strip_suffix(".sql.gz"))?;
    parse_target_time(stem).ok()
}

/// What a binlog file's head says about the history before it: when the
/// file was opened (its Format_description event's timestamp — the rotation
/// that created it) and every transaction its server had executed by then
/// (its Previous_gtids event), as a GTID set string mysqld takes back
/// verbatim (`uuid:1-5:8-9,uuid2:1-3`; empty when the server had no GTID
/// history, gtid_mode=OFF included).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinlogHead {
    pub created_at: DateTime<Utc>,
    pub previous_gtids: String,
}

const BINLOG_MAGIC: [u8; 4] = [0xfe, b'b', b'i', b'n'];
/// v4 event header: timestamp(4) type(1) server_id(4) event_length(4)
/// next_position(4) flags(2).
const BINLOG_EVENT_HEADER_LEN: usize = 19;
const FORMAT_DESCRIPTION_EVENT: u8 = 15;
const PREVIOUS_GTIDS_LOG_EVENT: u8 = 35;
/// Events to look through for the Previous_gtids event before giving up: the
/// server writes it second, right after the Format_description event.
const BINLOG_HEAD_EVENT_BUDGET: usize = 4;
/// Bytes of a binlog file the head parser reads: the two events it needs sit
/// at the very start, and a Previous_gtids event grows by ~40 bytes per
/// UUID, so this bounds even a set with thousands of lineages.
pub const BINLOG_HEAD_READ_BYTES: usize = 4 * 1024 * 1024;

/// Parse a binlog file's head (see [`BinlogHead`]) from its first bytes.
/// Fails loudly on anything it cannot read — a foreign magic (an encrypted
/// binlog, a relay log, not a binlog at all), a truncated head, a GTID set
/// in the tagged encoding this image never produces — because a caller
/// asking completeness questions must not mistake "unreadable" for "empty".
pub fn parse_binlog_head(bytes: &[u8]) -> Result<BinlogHead> {
    anyhow::ensure!(
        bytes.len() >= BINLOG_MAGIC.len() && bytes[..BINLOG_MAGIC.len()] == BINLOG_MAGIC,
        "not a binlog file (bad magic)"
    );
    let mut offset = BINLOG_MAGIC.len();
    let mut created_at: Option<DateTime<Utc>> = None;
    for _ in 0..BINLOG_HEAD_EVENT_BUDGET {
        let header = bytes
            .get(offset..offset + BINLOG_EVENT_HEADER_LEN)
            .context("binlog head is truncated inside an event header")?;
        let timestamp = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let type_code = header[4];
        let event_len =
            u32::from_le_bytes([header[9], header[10], header[11], header[12]]) as usize;
        anyhow::ensure!(
            event_len >= BINLOG_EVENT_HEADER_LEN,
            "binlog event of type {type_code} declares an impossible length {event_len}"
        );
        let payload = bytes
            .get(offset + BINLOG_EVENT_HEADER_LEN..offset + event_len)
            .with_context(|| {
                format!("binlog head is truncated inside an event of type {type_code}")
            })?;
        match type_code {
            FORMAT_DESCRIPTION_EVENT => {
                created_at = DateTime::from_timestamp(i64::from(timestamp), 0);
            }
            PREVIOUS_GTIDS_LOG_EVENT => {
                let created_at = created_at
                    .context("binlog has a Previous_gtids event before its Format_description")?;
                let previous_gtids = decode_gtid_set(payload)?;
                return Ok(BinlogHead {
                    created_at,
                    previous_gtids,
                });
            }
            _ => {}
        }
        offset += event_len;
    }
    anyhow::bail!(
        "no Previous_gtids event within the first {BINLOG_HEAD_EVENT_BUDGET} events of the binlog"
    )
}

/// mysqld's binary GTID set: n_sids(8), then per SID: uuid(16) n_intervals(8)
/// and per interval start(8) end(8, exclusive). Trailing bytes (the event's
/// checksum) are ignored. The high byte of n_sids carries the encoding format
/// since 8.3 (0 = this one, 1 = tagged GTIDs), and only the format this
/// image writes is accepted.
fn decode_gtid_set(payload: &[u8]) -> Result<String> {
    fn u64_at(bytes: &[u8], at: usize) -> Result<u64> {
        let b = bytes
            .get(at..at + 8)
            .context("GTID set encoding is truncated")?;
        Ok(u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }
    let n_sids_field = u64_at(payload, 0)?;
    anyhow::ensure!(
        n_sids_field >> 56 == 0,
        "GTID set uses the tagged encoding (format {}), which this image does not read",
        n_sids_field >> 56
    );
    let mut at = 8;
    let mut sids = Vec::with_capacity(n_sids_field as usize);
    for _ in 0..n_sids_field {
        let uuid = payload
            .get(at..at + 16)
            .context("GTID set encoding is truncated inside a UUID")?;
        at += 16;
        let n_intervals = u64_at(payload, at)?;
        at += 8;
        let mut text = format_binlog_uuid(uuid);
        for _ in 0..n_intervals {
            let start = u64_at(payload, at)?;
            let end = u64_at(payload, at + 8)?;
            at += 16;
            anyhow::ensure!(end > start, "GTID interval {start}-{end} is empty or inverted");
            if end == start + 1 {
                text.push_str(&format!(":{start}"));
            } else {
                text.push_str(&format!(":{start}-{}", end - 1));
            }
        }
        sids.push(text);
    }
    Ok(sids.join(","))
}

fn format_binlog_uuid(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Whether a binlog's Previous_gtids are owed to a restore cut at `target`:
/// the replay applies every event stamped before the target's second
/// (`mysqlbinlog --stop-datetime` is whole-second and exclusive), and every
/// transaction in a file's Previous_gtids committed at or before the second
/// the file was opened — so a file opened before the target's second vouches
/// for a set the restore must hold in full. A file opened within the
/// target's second may vouch for transactions the replay deliberately cut,
/// and is not consulted.
pub fn binlog_opened_before_cutoff(created_at: DateTime<Utc>, target: DateTime<Utc>) -> bool {
    created_at.timestamp() < target.timestamp()
}

fn extract_quoted(line: &str, key: &str) -> Option<String> {
    let idx = line.find(key)? + key.len();
    let rest = line[idx..].strip_prefix('\'')?;
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

fn extract_number(line: &str, key: &str) -> Option<u64> {
    let idx = line.find(key)? + key.len();
    let rest = &line[idx..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    rest[..end].parse().ok()
}

/// mysqld binlog file names are a fixed basename plus a zero-padded numeric
/// sequence (`binlog.000042`); the sequence is what orders them.
pub fn binlog_seq(name: &str) -> Option<u64> {
    name.rsplit('.').next()?.parse().ok()
}

/// Sort binlog file names oldest-first by their numeric sequence, falling
/// back to a plain string compare for anything that doesn't parse (never
/// happens for real mysqld-generated names, but must not panic on a stray
/// object).
pub fn binlog_name_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    match (binlog_seq(a), binlog_seq(b)) {
        (Some(x), Some(y)) => x.cmp(&y),
        _ => a.cmp(b),
    }
}

/// True when `name` is strictly older than the currently-active binlog file
/// — i.e. mysqld has closed it and it is safe to ship (never the file
/// mysqld is actively writing).
pub fn binlog_is_closed(name: &str, active: &str) -> bool {
    match (binlog_seq(name), binlog_seq(active)) {
        (Some(a), Some(b)) => a < b,
        _ => name < active,
    }
}

/// The file name to hand `PURGE BINARY LOGS TO '<name>'` (which reclaims
/// everything strictly BEFORE it) — the earliest file, in on-disk order,
/// that is not yet safe to reclaim: the active file itself, or the first
/// closed file this boot hasn't confirmed uploaded. `None` when nothing may
/// be purged yet (the very first file on disk is already the boundary, or
/// the list is empty).
///
/// This is the load-bearing safety rule for the "volume is the spool during
/// bucket outages" contract: a gap in `uploaded` anywhere in the list stops
/// the cut before it, so a failed/slow upload can never be purged out from
/// under itself.
pub fn purge_cut(
    files_oldest_first: &[String],
    active: &str,
    uploaded: &BTreeSet<String>,
    lost: &BTreeSet<String>,
) -> Option<String> {
    // A file recorded as LOST (gone from disk before it was ever uploaded)
    // can never be shipped, so it must not pin the cut forever: skipping it
    // lets the shipped files past it still be reclaimed. The loss itself is
    // reported where it is detected (archiver.rs), not here.
    let boundary = files_oldest_first.iter().find(|f| {
        f.as_str() == active || (!uploaded.contains(f.as_str()) && !lost.contains(f.as_str()))
    })?;
    if files_oldest_first.first() == Some(boundary) {
        return None;
    }
    Some(boundary.clone())
}

/// mysqld's own binlog expiry on a Group Replication member (the GR config
/// in mysql_conf.rs): how long a departed member has to rejoin out of its
/// peers' retained binlogs before recovery falls back to a clone. The
/// archiving primary takes this window over from mysqld (archiver.rs's role
/// supervisor): mysqld's expiry is switched off while it archives, and the
/// archiver reclaims a file only once it is BOTH uploaded and older than
/// this — the same recovery window, without the silent hole mysqld's blind
/// expiry can punch into the archive during a long upload outage.
pub const GR_BINLOG_EXPIRE_SECONDS: u64 = 259_200;

/// `purge_cut` for a group primary. Same two rules — never the active file,
/// never across a file that hasn't been uploaded — plus a third: an uploaded
/// file younger than `min_age` also stops the cut, because peers recover
/// from retained binlogs and need the recovery window mysqld's expiry gave
/// them. `age_of` reads a file's age from disk; a file whose age cannot be
/// read is treated as recent (kept), never as old.
pub fn purge_cut_retaining_recent(
    files_oldest_first: &[String],
    active: &str,
    uploaded: &BTreeSet<String>,
    lost: &BTreeSet<String>,
    age_of: &dyn Fn(&str) -> Option<std::time::Duration>,
    min_age: std::time::Duration,
) -> Option<String> {
    let boundary = files_oldest_first.iter().find(|f| {
        let name = f.as_str();
        if name == active {
            return true;
        }
        if lost.contains(name) {
            return false;
        }
        if !uploaded.contains(name) {
            return true;
        }
        age_of(name).is_none_or(|age| age < min_age)
    })?;
    if files_oldest_first.first() == Some(boundary) {
        return None;
    }
    Some(boundary.clone())
}

/// A sequence hole in the archived lineage, with shipped binlogs still
/// present on the far side: replaying past it is impossible, and replaying
/// UP TO it while the caller asked for a later target would silently lose
/// everything after the hole — the caller must fail loudly instead.
#[derive(Debug, Clone, PartialEq)]
pub struct BinlogGap {
    /// The last replayable file before the hole — empty when the hole is the
    /// start file itself (the full backup's own coordinate file is absent
    /// while later binlogs exist).
    pub after: String,
    /// The first file present past the hole.
    pub next_present: String,
}

/// The replay plan for a lineage: the ordered, gap-free run of files
/// starting at the full backup's own coordinate, plus the gap that
/// terminated it, when one exists.
#[derive(Debug, Clone, PartialEq)]
pub struct BinlogReplayPlan {
    pub run: Vec<String>,
    pub gap: Option<BinlogGap>,
}

/// Given the lineage's binlog files in the archive (any order) and the
/// coordinate where replay must start, the ordered, gap-free run of files to
/// replay — plus the gap that cut it short, when files exist past a missing
/// sequence number. A run that simply ends (no later files) is not a gap:
/// that is the normal shape, since the active binlog is only shipped on
/// rotation.
pub fn binlogs_to_replay(mut files: Vec<String>, start_file: &str) -> BinlogReplayPlan {
    files.sort_by(|a, b| binlog_name_cmp(a, b));
    let Some(start_idx) = files.iter().position(|f| f == start_file) else {
        // The full backup's own coordinate file is not in the archive. Files
        // BEFORE it are covered by the dump itself; any file AFTER it is
        // unreachable without the start file — a gap, not an empty lineage.
        let next_past_start = binlog_seq(start_file).and_then(|start_seq| {
            files
                .iter()
                .find(|f| binlog_seq(f).is_some_and(|s| s > start_seq))
                .cloned()
        });
        return BinlogReplayPlan {
            run: Vec::new(),
            gap: next_past_start.map(|next_present| BinlogGap {
                after: String::new(),
                next_present,
            }),
        };
    };
    let mut run: Vec<String> = Vec::new();
    let mut prev_seq = None;
    for name in &files[start_idx..] {
        if let (Some(prev), Some(cur)) = (prev_seq, binlog_seq(name)) {
            if cur != prev + 1 {
                return BinlogReplayPlan {
                    gap: Some(BinlogGap {
                        after: run.last().cloned().unwrap_or_default(),
                        next_present: name.clone(),
                    }),
                    run,
                };
            }
        }
        run.push(name.clone());
        prev_seq = binlog_seq(name);
    }
    BinlogReplayPlan { run, gap: None }
}

/// The lineage's files PAST its first gap, as gap-free runs in order — what a
/// shared-history restore replays after every lineage's gap-free run (see
/// restore.rs's replay_shared_history). `files` in any order; `next_present`
/// is the gap's first file on the far side (`BinlogGap::next_present`). Each
/// inner run is consecutive, and a new run starts at every further hole:
/// `[5, 6, 8]` past a gap at 5 → `[[5, 6], [8]]`. Empty when `next_present`
/// is not among the files.
pub fn binlog_runs_past_gap(mut files: Vec<String>, next_present: &str) -> Vec<Vec<String>> {
    files.sort_by(|a, b| binlog_name_cmp(a, b));
    let Some(start) = files.iter().position(|f| f == next_present) else {
        return Vec::new();
    };
    let mut runs: Vec<Vec<String>> = Vec::new();
    let mut prev_seq: Option<u64> = None;
    for name in &files[start..] {
        let seq = binlog_seq(name);
        let hole = matches!((prev_seq, seq), (Some(prev), Some(cur)) if cur != prev + 1);
        if hole || runs.is_empty() {
            runs.push(vec![name.clone()]);
        } else if let Some(run) = runs.last_mut() {
            run.push(name.clone());
        }
        prev_seq = seq;
    }
    runs
}

// --- archive retention -------------------------------------------------------
//
// Without this the archive grows forever: fulls accumulate every
// BINLOG_FULL_BACKUP_INTERVAL_SECONDS and no binlog is ever removed from the
// bucket (`purge_cut` above reclaims LOCAL disk only). pgBackRest gives
// postgres-pitr `expire`; this is the MySQL equivalent, and it is deliberately
// pure so every deletion rule is unit-testable without touching a bucket.
//
// The restorability invariant it must never break: for any target T inside the
// promised window there must be a complete full F with `taken_at <= T`, AND a
// gap-free binlog run from `F.meta.binlog_file` through T. `binlogs_to_replay`
// starts AT the full's own coordinate file and treats a missing coordinate file
// as a gap yielding an EMPTY run — so that one file is load-bearing and can
// never be expired while its full is retained.
//
// Policy shape: a TIME horizon (what a customer is actually promised — "you can
// restore to any point in the last N days") plus a hard count floor that is NOT
// configurable. Time alone is unsafe: if archiving has been broken for longer
// than the horizon, a naive sweep deletes the only restorable full. Count alone
// is unpredictable: the window becomes N x interval and drifts whenever the
// interval changes or a backup fails.

/// Complete fulls kept for the ACTIVE lineage regardless of age. A safety
/// invariant, not a knob: it is what makes a time horizon safe to apply at all.
/// Two rather than one so a restore already replaying against the oldest
/// retained full still has a margin when the next sweep moves the floor.
pub const MIN_ACTIVE_FULLS_KEPT: usize = 2;

/// The horizon the image assumes when `BINLOG_RETENTION_DAYS` is unset, so a
/// PITR service bounds its own archive without the platform having to stamp a
/// value onto the template. Matches the window the Backups panel presents.
/// Defaulting is safe because `MIN_ACTIVE_FULLS_KEPT` fulls survive regardless
/// of age — retention can never expire a service into being unrestorable — and
/// an explicit `BINLOG_RETENTION_DAYS=0` remains the opt-out for a service that
/// deliberately wants an unbounded archive.
pub const DEFAULT_BINLOG_RETENTION_DAYS: u64 = 7;

/// No object is ever deleted while younger than this, whatever the policy says.
/// Insurance against expiring something a just-started restore still needs.
pub const RETENTION_MIN_OBJECT_AGE_SECONDS: i64 = 3600;

/// A `.sql.gz` with no sibling `.meta.json` is either an upload still in flight
/// or the wreckage of a failed one. Unrestorable either way (the meta carries
/// the replay coordinate), but it must not be deleted until it is far past any
/// plausible in-flight dump.
pub const ORPHAN_DUMP_GRACE_SECONDS: i64 = 6 * 3600;

/// What one lineage's objects look like to the planner.
#[derive(Debug, Clone, PartialEq)]
pub struct LineageObjects {
    pub server_uuid: String,
    /// Complete fulls (dump AND meta present), any order.
    pub fulls: Vec<FullBackupRef>,
    /// How many full-backup `meta.json` objects the bucket actually holds for
    /// this lineage, whether or not they could be read or parsed this pass.
    ///
    /// Load-bearing: without it an empty `fulls` is ambiguous between "this
    /// lineage never had a full" and "its fulls exist but we could not read
    /// them", and only the first of those makes its binlogs expirable. A
    /// transient S3 error must never be able to turn a good lineage into an
    /// unrestorable one.
    pub full_objects_seen: usize,
    /// `.sql.gz` keys with no sibling `.meta.json`, with their upload time.
    pub orphan_dumps: Vec<(String, DateTime<Utc>)>,
    /// Bare binlog file names (e.g. `binlog.000007`), any order.
    pub binlogs: Vec<String>,
    /// Upload time (S3 LastModified) per binlog name, for the ones the
    /// listing reported one for. Only the shared-history planner reads it —
    /// a binlog's age against the archive-wide floor full is what makes it
    /// redundant there — and a binlog with no known age is kept.
    pub binlog_ages: BTreeMap<String, DateTime<Utc>>,
}

/// Everything the planner needs. Deliberately a snapshot: the caller lists the
/// bucket once, then this decides, so the decision is reproducible in a test.
#[derive(Debug, Clone)]
pub struct RetentionInput {
    pub lineages: Vec<LineageObjects>,
    /// The lineage this server archives under. `None` when the archiver has not
    /// yet established its own lineage — the planner then refuses to delete
    /// anything, because it cannot tell a dead lineage from its own.
    pub active_server_uuid: Option<String>,
    pub now: DateTime<Utc>,
    pub horizon: chrono::Duration,
    /// The archive carries the `shared_history_marker_key` object — a group
    /// primary has archived here. Selects the shared-history rules even
    /// before any GTID full exists (see `archive_shares_history`).
    pub shared_history_marker: bool,
}

/// Objects to delete, plus why — the caller logs the reasons whether or not it
/// is in dry-run mode.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RetentionPlan {
    /// Full-backup objects (both the `.sql.gz` and `.meta.json` of each expired
    /// full) to delete.
    pub expired_full_keys: Vec<String>,
    /// Binlog file NAMES to delete, as `(server_uuid, name)`.
    pub expired_binlogs: Vec<(String, String)>,
    /// Orphan `.sql.gz` keys past the grace window.
    pub orphan_dump_keys: Vec<String>,
    /// Lineages retired whole — informational; their objects are already in the
    /// lists above.
    pub retired_lineages: Vec<String>,
    /// One note per decision worth seeing in the log.
    pub notes: Vec<String>,
}

impl RetentionPlan {
    pub fn is_empty(&self) -> bool {
        self.expired_full_keys.is_empty()
            && self.expired_binlogs.is_empty()
            && self.orphan_dump_keys.is_empty()
    }

    pub fn object_count(&self) -> usize {
        self.expired_full_keys.len() + self.expired_binlogs.len() + self.orphan_dump_keys.len()
    }
}

/// Which fulls a lineage keeps: everything inside the horizon, extended down to
/// `min_kept` when the horizon alone would leave fewer. Newest-first. Always
/// keeps at least one — dropping a lineage's last full is the retire-whole
/// decision, made by the caller, never a side effect here.
///
/// The window also extends by exactly one full past the cutoff when the oldest
/// in-horizon full is strictly newer than it: restore selects the newest full
/// with `taken_at <= target`, so a target between the cutoff and that full has
/// nothing to stand on unless the boundary full — the newest one at-or-before
/// the target — survives. Keeping it is what makes "any point in the last N
/// days" a promise instead of "any full-backup interval inside those days".
fn fulls_to_keep(
    fulls: &[FullBackupRef],
    cutoff: DateTime<Utc>,
    min_kept: usize,
) -> Vec<FullBackupRef> {
    let mut sorted: Vec<FullBackupRef> = fulls.to_vec();
    sorted.sort_by(|a, b| {
        b.meta
            .taken_at
            .cmp(&a.meta.taken_at)
            .then_with(|| a.dump_key.cmp(&b.dump_key))
    });
    let inside = sorted.iter().filter(|f| f.meta.taken_at >= cutoff).count();
    let mut keep = inside.max(min_kept).max(1).min(sorted.len());
    // The oldest kept full is newer than the cutoff while an older full
    // exists: targets between the cutoff and that full's taken_at lose their
    // only qualifying base. Keep exactly one more — the boundary full.
    if keep < sorted.len() && sorted[keep - 1].meta.taken_at > cutoff {
        keep += 1;
    }
    sorted.into_iter().take(keep).collect()
}

/// Plan one sweep. Never deletes anything it cannot prove unnecessary; on any
/// ambiguity it keeps the object and says why in `notes`.
pub fn plan_retention(input: &RetentionInput) -> RetentionPlan {
    let mut plan = RetentionPlan::default();

    let Some(active_uuid) = input.active_server_uuid.as_deref() else {
        plan.notes.push(
            "no active lineage established yet; skipping retention entirely (cannot distinguish \
             a dead lineage from this server's own)"
                .to_string(),
        );
        return plan;
    };

    if input.horizon <= chrono::Duration::zero() {
        plan.notes
            .push("retention horizon is not positive; nothing expires".to_string());
        return plan;
    }

    let cutoff = input.now - input.horizon;
    let orphan_grace = chrono::Duration::seconds(ORPHAN_DUMP_GRACE_SECONDS);

    if input.shared_history_marker || archive_shares_history(&input.lineages) {
        plan_shared_history_retention(input, &mut plan, cutoff, orphan_grace);
        return plan;
    }

    // Nothing expires until the active lineage can itself serve a restore. On
    // a fresh volume the only fulls in the bucket belong to the lineage it
    // replaced, and retiring those — even past the horizon — would leave
    // nothing restorable at all until the first new full lands. Wait for the
    // replacement to exist first.
    let active_has_full = input
        .lineages
        .iter()
        .any(|l| l.server_uuid == active_uuid && !l.fulls.is_empty());
    if !active_has_full {
        plan.notes.push(format!(
            "the active lineage has no complete full backup yet ({active_uuid}); expiring nothing \
             anywhere until it does, so the bucket is never left without a restorable full"
        ));
        return plan;
    }

    // The active lineage must always remain restorable, so it never retires
    // whole and always honors the count floor. A dead lineage exists only to
    // serve targets inside the window (restoring to before a volume reset);
    // once every one of its fulls is past the horizon it can no longer serve
    // anything the window promises, and it retires completely.
    for lineage in &input.lineages {
        let is_active = lineage.server_uuid == active_uuid;

        for (key, uploaded_at) in &lineage.orphan_dumps {
            if input.now - *uploaded_at > orphan_grace {
                plan.orphan_dump_keys.push(key.clone());
            }
        }

        if lineage.fulls.is_empty() {
            if lineage.full_objects_seen > 0 {
                // Its fulls exist; we just could not read them this pass.
                // Expiring the binlogs now would leave those fulls
                // unrestorable past their own coordinates — permanently.
                plan.notes.push(format!(
                    "lineage {}: {} full backup(s) exist but could not be read this pass; \
                     expiring nothing in this lineage",
                    lineage.server_uuid, lineage.full_objects_seen
                ));
                continue;
            }
            // Genuinely no full: nothing here is restorable at all, since a
            // lineage's binlogs are only ever replayed from its own full. Still
            // spare the active lineage, whose first full may simply not have
            // landed yet.
            if !is_active && !lineage.binlogs.is_empty() {
                plan.notes.push(format!(
                    "lineage {} has binlogs but no complete full backup; expiring {} \
                     unrestorable binlog(s)",
                    lineage.server_uuid,
                    lineage.binlogs.len()
                ));
                for name in &lineage.binlogs {
                    plan.expired_binlogs
                        .push((lineage.server_uuid.clone(), name.clone()));
                }
                plan.retired_lineages.push(lineage.server_uuid.clone());
            }
            continue;
        }

        let newest_full_at = lineage
            .fulls
            .iter()
            .map(|f| f.meta.taken_at)
            .max()
            .expect("non-empty checked above");

        if !is_active && newest_full_at < cutoff {
            plan.notes.push(format!(
                "retiring dead lineage {} whole: its newest full ({}) is older than the horizon",
                lineage.server_uuid,
                format_rfc3339_millis(newest_full_at)
            ));
            for full in &lineage.fulls {
                plan.expired_full_keys.push(full.dump_key.clone());
                plan.expired_full_keys
                    .push(meta_key_for_dump(&full.dump_key));
            }
            for name in &lineage.binlogs {
                plan.expired_binlogs
                    .push((lineage.server_uuid.clone(), name.clone()));
            }
            plan.retired_lineages.push(lineage.server_uuid.clone());
            continue;
        }

        let min_kept = if is_active { MIN_ACTIVE_FULLS_KEPT } else { 1 };
        let kept = fulls_to_keep(&lineage.fulls, cutoff, min_kept);
        let floor = kept
            .last()
            .expect("fulls_to_keep always keeps at least one");

        // The floor predating the horizon while in-horizon fulls exist is the
        // boundary extension at work — worth a line in the log so a full that
        // outlived the horizon doesn't read as a sweep that failed to sweep.
        if floor.meta.taken_at < cutoff && kept.iter().any(|f| f.meta.taken_at >= cutoff) {
            plan.notes.push(format!(
                "lineage {}: floor full ({}) predates the horizon; kept because \
                 in-window targets before the oldest in-horizon full restore from it",
                lineage.server_uuid,
                format_rfc3339_millis(floor.meta.taken_at)
            ));
        }

        let kept_dumps: BTreeSet<&str> = kept.iter().map(|f| f.dump_key.as_str()).collect();
        let mut expired_fulls = 0usize;
        for full in &lineage.fulls {
            if kept_dumps.contains(full.dump_key.as_str()) {
                continue;
            }
            plan.expired_full_keys.push(full.dump_key.clone());
            plan.expired_full_keys
                .push(meta_key_for_dump(&full.dump_key));
            expired_fulls += 1;
        }

        // Expire binlogs strictly BELOW the floor full's coordinate file. The
        // coordinate file itself is where replay starts, so it stays; files
        // before it are covered by the dump.
        let Some(floor_seq) = binlog_seq(&floor.meta.binlog_file) else {
            plan.notes.push(format!(
                "lineage {}: the floor full's coordinate file {:?} has no parseable sequence; \
                 keeping every binlog in this lineage",
                lineage.server_uuid, floor.meta.binlog_file
            ));
            continue;
        };
        let mut expired_below = 0usize;
        let mut unparseable = 0usize;
        for name in &lineage.binlogs {
            match binlog_seq(name) {
                Some(seq) if seq < floor_seq => {
                    plan.expired_binlogs
                        .push((lineage.server_uuid.clone(), name.clone()));
                    expired_below += 1;
                }
                Some(_) => {}
                None => unparseable += 1,
            }
        }
        if unparseable > 0 {
            plan.notes.push(format!(
                "lineage {}: kept {} binlog(s) whose name has no parseable sequence",
                lineage.server_uuid, unparseable
            ));
        }
        if expired_fulls > 0 || expired_below > 0 {
            plan.notes.push(format!(
                "lineage {}{}: keeping {} full(s) back to {} (coordinate {}); expiring {} full(s) \
                 and {} binlog(s) below it",
                lineage.server_uuid,
                if is_active { " (active)" } else { "" },
                kept.len(),
                format_rfc3339_millis(floor.meta.taken_at),
                floor.meta.binlog_file,
                expired_fulls,
                expired_below
            ));
        }
    }

    plan
}

/// Whether the archive's lineages share one transaction history — true once
/// any complete full was taken from a GTID-enabled source (see the module
/// doc). Decided from the fulls because a lineage with no full of its own
/// (a primary that took over and never dumped) has nothing else to declare
/// itself with; one GTID full anywhere marks the whole archive, which only
/// ever errs toward keeping more.
pub fn archive_shares_history(lineages: &[LineageObjects]) -> bool {
    lineages
        .iter()
        .any(|l| l.fulls.iter().any(|f| f.meta.gtid_purged.is_some()))
}

/// Retention over an archive whose lineages share one history (see
/// `archive_shares_history`). Fulls rank ARCHIVE-WIDE — the newest full at or
/// before a target is what a restore stands on, whichever lineage took it —
/// and the floor full's `taken_at` is the one instant that divides the
/// archive: a binlog uploaded before the floor full was taken holds only
/// transactions the floor dump already contains (upload time ≥ close time ≥
/// every event in the file), so those expire; everything from the floor
/// onward, in EVERY lineage, is what a restore inside the window may need — a
/// primary that never took a full of its own is still the only carrier of its
/// tenure's transactions.
///
/// What differs from the independent-histories rules above:
///   - a lineage with no full of its own is never "unrestorable";
///   - a dead lineage never retires on its own fulls' age — it retires when
///     none of its objects survive the floor;
///   - the count floor (`MIN_ACTIVE_FULLS_KEPT`) applies to the archive as a
///     whole, not per lineage;
///   - the "active lineage must have a full first" gate becomes "the archive
///     must have a full", since the active primary may legitimately never
///     dump (see the archiver's archive-wide full cadence).
/// A binlog with no known upload time is kept — the pass cannot prove it
/// redundant. The floor full's own lineage expires by sequence below its
/// coordinate file, exactly as before: the precise form of the same rule.
/// Any lineage whose fulls exist but could not be read this pass makes the
/// whole pass inert: the floor might be wrong without them, and a transient
/// listing error must never expire a binlog a readable-next-time full needs.
fn plan_shared_history_retention(
    input: &RetentionInput,
    plan: &mut RetentionPlan,
    cutoff: DateTime<Utc>,
    orphan_grace: chrono::Duration,
) {
    let active_uuid = input.active_server_uuid.as_deref().unwrap_or_default();
    for lineage in &input.lineages {
        for (key, uploaded_at) in &lineage.orphan_dumps {
            if input.now - *uploaded_at > orphan_grace {
                plan.orphan_dump_keys.push(key.clone());
            }
        }
    }

    let unreadable: Vec<&LineageObjects> = input
        .lineages
        .iter()
        .filter(|l| l.full_objects_seen > l.fulls.len())
        .collect();
    if !unreadable.is_empty() {
        plan.notes.push(format!(
            "shared history: {} lineage(s) have full backups that could not be read this pass \
             ({}); the archive-wide floor cannot be trusted without them — expiring nothing",
            unreadable.len(),
            unreadable
                .iter()
                .map(|l| l.server_uuid.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        return;
    }

    let all_fulls: Vec<FullBackupRef> = input
        .lineages
        .iter()
        .flat_map(|l| l.fulls.iter().cloned())
        .collect();
    // archive_shares_history() found a full, so this cannot be empty; the
    // guard keeps the rule honest if the two ever drift apart.
    if all_fulls.is_empty() {
        plan.notes
            .push("shared history: no complete full backup anywhere; expiring nothing".to_string());
        return;
    }

    let kept = fulls_to_keep(&all_fulls, cutoff, MIN_ACTIVE_FULLS_KEPT);
    let floor = kept
        .last()
        .expect("fulls_to_keep always keeps at least one");
    let kept_dumps: BTreeSet<&str> = kept.iter().map(|f| f.dump_key.as_str()).collect();
    if floor.meta.taken_at < cutoff && kept.iter().any(|f| f.meta.taken_at >= cutoff) {
        plan.notes.push(format!(
            "shared history: floor full ({}, lineage {}) predates the horizon; kept because \
             in-window targets before the oldest in-horizon full restore from it",
            format_rfc3339_millis(floor.meta.taken_at),
            floor.server_uuid
        ));
    }
    let floor_seq = binlog_seq(&floor.meta.binlog_file);
    if floor_seq.is_none() {
        plan.notes.push(format!(
            "shared history: the floor full's coordinate file {:?} has no parseable sequence; \
             keeping every binlog in its lineage {}",
            floor.meta.binlog_file, floor.server_uuid
        ));
    }

    for lineage in &input.lineages {
        let is_active = lineage.server_uuid == active_uuid;
        let is_floor_lineage = lineage.server_uuid == floor.server_uuid;
        let mut expired_fulls = 0usize;
        let mut kept_fulls = 0usize;
        for full in &lineage.fulls {
            if kept_dumps.contains(full.dump_key.as_str()) {
                kept_fulls += 1;
                continue;
            }
            plan.expired_full_keys.push(full.dump_key.clone());
            plan.expired_full_keys
                .push(meta_key_for_dump(&full.dump_key));
            expired_fulls += 1;
        }

        let mut expired_binlogs = 0usize;
        let mut kept_binlogs = 0usize;
        let mut unknown_age = 0usize;
        for name in &lineage.binlogs {
            let redundant = if is_floor_lineage {
                matches!((binlog_seq(name), floor_seq), (Some(seq), Some(fs)) if seq < fs)
            } else {
                match lineage.binlog_ages.get(name) {
                    Some(uploaded_at) => *uploaded_at < floor.meta.taken_at,
                    None => {
                        unknown_age += 1;
                        false
                    }
                }
            };
            if redundant {
                plan.expired_binlogs
                    .push((lineage.server_uuid.clone(), name.clone()));
                expired_binlogs += 1;
            } else {
                kept_binlogs += 1;
            }
        }

        let touched = expired_fulls > 0 || expired_binlogs > 0;
        if !is_active && touched && kept_fulls == 0 && kept_binlogs == 0 {
            plan.retired_lineages.push(lineage.server_uuid.clone());
        }
        if unknown_age > 0 {
            plan.notes.push(format!(
                "shared history: lineage {}: kept {} binlog(s) whose upload time is unknown",
                lineage.server_uuid, unknown_age
            ));
        }
        if touched {
            plan.notes.push(format!(
                "shared history: lineage {}{}: keeping {} full(s) and {} binlog(s) at or after \
                 the archive-wide floor ({}, coordinate {} in lineage {}); expiring {} full(s) \
                 and {} binlog(s) before it",
                lineage.server_uuid,
                if is_active { " (active)" } else { "" },
                kept_fulls,
                kept_binlogs,
                format_rfc3339_millis(floor.meta.taken_at),
                floor.meta.binlog_file,
                floor.server_uuid,
                expired_fulls,
                expired_binlogs
            ));
        }
    }
}

/// `<...>/full/<rfc>.sql.gz` -> `<...>/full/<rfc>.meta.json`.
pub fn meta_key_for_dump(dump_key: &str) -> String {
    match dump_key.strip_suffix(".sql.gz") {
        Some(stem) => format!("{stem}.meta.json"),
        None => format!("{dump_key}.meta.json"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn loc(path: &str) -> S3Location {
        S3Location {
            bucket: "b".to_string(),
            access_key: "k".to_string(),
            secret_key: "s".to_string(),
            region: "auto".to_string(),
            endpoint: "https://s3.example.com".to_string(),
            path: path.to_string(),
        }
    }

    #[test]
    fn object_naming_matches_the_documented_layout() {
        let l = loc("/binlog");
        assert_eq!(
            full_dump_key(&l, "uuid-1", "2026-08-13T14:00:00.000Z"),
            "binlog/server-uuid-1/full/2026-08-13T14:00:00.000Z.sql.gz"
        );
        assert_eq!(
            full_meta_key(&l, "uuid-1", "2026-08-13T14:00:00.000Z"),
            "binlog/server-uuid-1/full/2026-08-13T14:00:00.000Z.meta.json"
        );
        assert_eq!(
            binlog_key(&l, "uuid-1", "binlog.000042"),
            "binlog/server-uuid-1/binlog/binlog.000042"
        );
    }

    #[test]
    fn base_path_slashes_are_normalized() {
        for path in ["/binlog", "binlog", "/binlog/", "binlog/"] {
            let l = loc(path);
            assert_eq!(
                full_dump_key(&l, "u", "T"),
                "binlog/server-u/full/T.sql.gz",
                "path {path:?} did not normalize"
            );
        }
        // Root path degrades to no prefix segment at all, cleanly.
        let l = loc("/");
        assert_eq!(full_dump_key(&l, "u", "T"), "server-u/full/T.sql.gz");
        let l = loc("");
        assert_eq!(full_dump_key(&l, "u", "T"), "server-u/full/T.sql.gz");
    }

    #[test]
    fn server_uuid_round_trips_through_key_naming() {
        let l = loc("/binlog");
        let key = full_dump_key(&l, "aaaa-bbbb", "2026-08-13T14:00:00.000Z");
        assert_eq!(
            server_uuid_from_key(&l, &key),
            Some("aaaa-bbbb".to_string())
        );
        let key = binlog_key(&l, "aaaa-bbbb", "binlog.000001");
        assert_eq!(
            server_uuid_from_key(&l, &key),
            Some("aaaa-bbbb".to_string())
        );
        assert_eq!(server_uuid_from_key(&l, "unrelated/key"), None);
        assert_eq!(server_uuid_from_key(&l, "binlog/server-/full/x"), None);
    }

    #[test]
    fn target_time_parses_the_documented_format() {
        let t = parse_target_time("2026-08-13T14:00:00.000Z").unwrap();
        assert_eq!(t, Utc.with_ymd_and_hms(2026, 8, 13, 14, 0, 0).unwrap());
        assert!(parse_target_time("not-a-time").is_err());
        assert!(parse_target_time("2026-08-13").is_err());
    }

    #[test]
    fn format_round_trips_through_parse() {
        let t = Utc.with_ymd_and_hms(2026, 8, 13, 14, 0, 0).unwrap();
        let s = format_rfc3339_millis(t);
        assert_eq!(s, "2026-08-13T14:00:00.000Z");
        assert_eq!(parse_target_time(&s).unwrap(), t);
    }

    fn meta(taken_at: &str, server_uuid: &str) -> FullBackupMeta {
        FullBackupMeta {
            taken_at: parse_target_time(taken_at).unwrap(),
            binlog_file: "binlog.000001".to_string(),
            binlog_pos: 4,
            server_uuid: server_uuid.to_string(),
            mysql_version: "8.4.3".to_string(),
            gtid_purged: None,
            dump_bytes: None,
            datadir_bytes: None,
        }
    }

    fn full(server_uuid: &str, taken_at: &str) -> FullBackupRef {
        FullBackupRef {
            server_uuid: server_uuid.to_string(),
            dump_key: format!("server-{server_uuid}/full/{taken_at}.sql.gz"),
            meta: meta(taken_at, server_uuid),
        }
    }

    #[test]
    fn newest_qualifying_full_picks_the_latest_at_or_before_target() {
        let fulls = vec![
            full("a", "2026-08-13T10:00:00.000Z"),
            full("a", "2026-08-13T12:00:00.000Z"),
            full("b", "2026-08-13T13:00:00.000Z"),
            full("a", "2026-08-13T15:00:00.000Z"), // after target — excluded
        ];
        let target = parse_target_time("2026-08-13T14:00:00.000Z").unwrap();
        let picked = newest_qualifying_full(&fulls, target).unwrap();
        assert_eq!(picked.server_uuid, "b");
        assert_eq!(
            picked.meta.taken_at,
            parse_target_time("2026-08-13T13:00:00.000Z").unwrap()
        );
    }

    #[test]
    fn newest_qualifying_full_exact_match_on_target_qualifies() {
        let fulls = vec![full("a", "2026-08-13T14:00:00.000Z")];
        let target = parse_target_time("2026-08-13T14:00:00.000Z").unwrap();
        assert!(newest_qualifying_full(&fulls, target).is_some());
    }

    /// The production shape of 2026-09-10: the archiver named the full at
    /// millisecond precision and recorded `taken_at` with its nanoseconds;
    /// the platform read the name and pinned the fork's target on it. The
    /// full IS that instant and must qualify — one millisecond earlier is
    /// before the full.
    #[test]
    fn newest_qualifying_full_accepts_a_target_pinned_on_the_fulls_name() {
        let mut f = full("a", "2026-09-10T22:35:47.717Z");
        f.meta.taken_at = parse_target_time("2026-09-10T22:35:47.717480Z").unwrap();
        let target = full_taken_at_from_key(&f.dump_key).unwrap();
        assert_eq!(
            target,
            parse_target_time("2026-09-10T22:35:47.717Z").unwrap()
        );
        let fulls = [f.clone()];
        let picked = newest_qualifying_full(&fulls, target)
            .expect("a full is restorable from the instant its name carries");
        assert_eq!(picked.dump_key, f.dump_key);
        let before = parse_target_time("2026-09-10T22:35:47.716Z").unwrap();
        assert!(newest_qualifying_full(&fulls, before).is_none());
    }

    #[test]
    fn floor_to_millis_keeps_the_instant_the_name_carries() {
        let t = parse_target_time("2026-09-10T22:35:47.717480123Z").unwrap();
        let floored = floor_to_millis(t);
        assert_eq!(format_rfc3339_millis(floored), "2026-09-10T22:35:47.717Z");
        assert_eq!(
            floored,
            parse_target_time("2026-09-10T22:35:47.717Z").unwrap()
        );
        assert_eq!(floor_to_millis(floored), floored);
        let whole = parse_target_time("2026-09-10T22:35:47Z").unwrap();
        assert_eq!(floor_to_millis(whole), whole);
    }

    #[test]
    fn newest_qualifying_full_none_when_everything_is_after_target() {
        let fulls = vec![full("a", "2026-08-13T15:00:00.000Z")];
        let target = parse_target_time("2026-08-13T14:00:00.000Z").unwrap();
        assert!(newest_qualifying_full(&fulls, target).is_none());
        assert!(newest_qualifying_full(&[], target).is_none());
    }

    #[test]
    fn newest_qualifying_full_ties_break_on_server_uuid() {
        let fulls = vec![
            full("z", "2026-08-13T10:00:00.000Z"),
            full("a", "2026-08-13T10:00:00.000Z"),
        ];
        let target = parse_target_time("2026-08-13T10:00:00.000Z").unwrap();
        // Deterministic: same instant, "z" wins the lexicographic tie-break —
        // pinned here so the rule can't silently flip between runs.
        assert_eq!(
            newest_qualifying_full(&fulls, target).unwrap().server_uuid,
            "z"
        );
    }

    #[test]
    fn achieved_lag_bound_is_two_rotations_plus_slack() {
        assert_eq!(achieved_lag_bound_seconds(60), 180);
        assert_eq!(achieved_lag_bound_seconds(30), 120);
        assert_eq!(achieved_lag_bound_seconds(0), 60);
        // A pathological knob value saturates instead of wrapping.
        assert_eq!(achieved_lag_bound_seconds(u64::MAX), u64::MAX);
    }

    #[test]
    fn achieved_point_bound_accepts_within_and_rejects_past() {
        let target = parse_target_time("2026-08-13T14:00:00.000Z").unwrap();
        let bound = achieved_lag_bound_seconds(60); // 180s

        // At or past the target always qualifies.
        assert!(achieved_point_within_bound(target, target, bound));
        let past = parse_target_time("2026-08-13T14:05:00.000Z").unwrap();
        assert!(achieved_point_within_bound(target, past, bound));

        // Short of the target by exactly the bound still qualifies —
        // reaching the exact target is impossible within the last rotation
        // window, so the boundary itself must be inclusive.
        let at_bound = parse_target_time("2026-08-13T13:57:00.000Z").unwrap();
        assert!(achieved_point_within_bound(target, at_bound, bound));

        // One second past the bound does not.
        let too_short = parse_target_time("2026-08-13T13:56:59.000Z").unwrap();
        assert!(!achieved_point_within_bound(target, too_short, bound));

        // An hour short (the "archive ends long before the target" shape
        // this check exists for) is rejected loudly.
        let way_short = parse_target_time("2026-08-13T13:00:00.000Z").unwrap();
        assert!(!achieved_point_within_bound(target, way_short, bound));

        // A huge bound never panics/overflows the comparison.
        assert!(achieved_point_within_bound(target, way_short, u64::MAX));
    }

    #[test]
    fn parses_a_binlog_event_header_as_utc() {
        let line =
            "#260813 14:00:02 server id 1  end_log_pos 157 CRC32 0xabcd1234 \tQuery\tthread_id=8";
        assert_eq!(
            parse_binlog_event_header_utc(line),
            Some(parse_target_time("2026-08-13T14:00:02.000Z").unwrap())
        );
        // mysqlbinlog space-pads the hour (`%2d`).
        let padded = "#260813  1:02:03 server id 1  end_log_pos 200 \tRotate to binlog.000005";
        assert_eq!(
            parse_binlog_event_header_utc(padded),
            Some(parse_target_time("2026-08-13T01:02:03.000Z").unwrap())
        );
        // MySQL's own two-digit-year window: 70–99 → 19xx.
        let last_century = "#991231 23:59:59 server id 1  end_log_pos 4";
        assert_eq!(
            parse_binlog_event_header_utc(last_century),
            Some(parse_target_time("1999-12-31T23:59:59.000Z").unwrap())
        );
    }

    #[test]
    fn non_header_lines_and_artificial_events_parse_as_none() {
        for line in [
            "",
            "# at 4",
            "#comment",
            "SET TIMESTAMP=1755093602/*!*/;",
            "#260813 14:00:02 not a header",
            "#26081 14:00:02 server id 1",     // date too short
            "#260813 14:00 server id 1",       // time missing seconds
            "#260813 14:00:02:99 server id 1", // too many time fields
            "#261340 14:00:02 server id 1",    // month 13 is not a date
            "insert into t values ('#260813 14:00:02 server id 1')",
        ] {
            assert_eq!(parse_binlog_event_header_utc(line), None, "line {line:?}");
        }
        // An artificial event's zero timestamp is never a coverage point.
        assert_eq!(
            parse_binlog_event_header_utc("#700101  0:00:00 server id 1  end_log_pos 0"),
            None
        );
    }

    #[test]
    fn fallback_fulls_listing_excludes_the_selected_and_sorts_newest_first() {
        let fulls = vec![
            full("a", "2026-08-13T10:00:00.000Z"),
            full("b", "2026-08-13T13:00:00.000Z"),
            full("a", "2026-08-13T12:00:00.000Z"),
        ];
        let selected = fulls[2].clone();
        assert_eq!(
            describe_fallback_fulls(&fulls, &selected),
            "server-b full taken at 2026-08-13T13:00:00.000Z, \
             server-a full taken at 2026-08-13T10:00:00.000Z"
        );

        // The only full in the bucket has no fallbacks — said explicitly,
        // never as an empty string.
        let only = vec![full("a", "2026-08-13T10:00:00.000Z")];
        assert_eq!(
            describe_fallback_fulls(&only, &only[0]),
            "none (this is the only full backup discovered in the bucket)"
        );
    }

    #[test]
    fn dump_data_flag_prefers_source_data_when_supported() {
        assert_eq!(
            pick_dump_data_flag("Usage: mysqldump ...\n  --source-data[=name]"),
            "--source-data=2"
        );
        assert_eq!(
            pick_dump_data_flag("Usage: mysqldump ...\n  --master-data[=name]"),
            "--master-data=2"
        );
        assert_eq!(pick_dump_data_flag(""), "--master-data=2");
    }

    #[test]
    fn dump_data_flag_by_major_floors_at_8() {
        assert_eq!(dump_data_flag_by_major(9), "--source-data=2");
        assert_eq!(dump_data_flag_by_major(8), "--source-data=2");
        assert_eq!(dump_data_flag_by_major(5), "--master-data=2");
    }

    #[test]
    fn mysql_major_version_parses_the_leading_number() {
        assert_eq!(mysql_major_version("8.4.3"), Some(8));
        assert_eq!(mysql_major_version("8.0.39-standard"), Some(8));
        assert_eq!(mysql_major_version("9.1.0"), Some(9));
        assert_eq!(mysql_major_version(""), None);
        assert_eq!(mysql_major_version("not-a-version"), None);
    }

    #[test]
    fn parses_change_master_to_spelling() {
        let head = "-- some header\n\
                     --\n\
                     -- Position to start replication or point-in-time recovery from\n\
                     --\n\
                     -- CHANGE MASTER TO MASTER_LOG_FILE='binlog.000003', MASTER_LOG_POS=157;\n\
                     -- more stuff\n";
        assert_eq!(
            parse_change_master_coords(head),
            Some(("binlog.000003".to_string(), 157))
        );
    }

    #[test]
    fn parses_change_replication_source_to_spelling() {
        let head = "-- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='binlog.000012', SOURCE_LOG_POS=98765;\n";
        assert_eq!(
            parse_change_master_coords(head),
            Some(("binlog.000012".to_string(), 98765))
        );
    }

    #[test]
    fn parses_coords_without_a_comment_prefix() {
        // Defensive: some mysqldump builds/flags don't comment the line.
        let head = "CHANGE MASTER TO MASTER_LOG_FILE='binlog.000001', MASTER_LOG_POS=4;\n";
        assert_eq!(
            parse_change_master_coords(head),
            Some(("binlog.000001".to_string(), 4))
        );
    }

    #[test]
    fn coords_absent_reads_as_none() {
        assert_eq!(
            parse_change_master_coords("-- just a regular header\n"),
            None
        );
        assert_eq!(parse_change_master_coords(""), None);
        // A line that names the statement but is missing a coordinate is
        // still None, not a false partial match.
        let malformed = "-- CHANGE MASTER TO MASTER_LOG_FILE='binlog.000001';\n";
        assert_eq!(parse_change_master_coords(malformed), None);
    }

    #[test]
    fn meta_json_round_trips() {
        let m = meta("2026-08-13T14:00:00.000Z", "uuid-1");
        let json = serde_json::to_string(&m).unwrap();
        let back: FullBackupMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn binlog_seq_parses_the_zero_padded_suffix() {
        assert_eq!(binlog_seq("binlog.000042"), Some(42));
        assert_eq!(binlog_seq("binlog.1"), Some(1));
        assert_eq!(binlog_seq("not-a-binlog-name"), None);
    }

    #[test]
    fn binlog_name_cmp_orders_numerically_not_lexicographically() {
        let mut names = vec![
            "binlog.000010".to_string(),
            "binlog.000002".to_string(),
            "binlog.000001".to_string(),
        ];
        names.sort_by(|a, b| binlog_name_cmp(a, b));
        assert_eq!(
            names,
            vec!["binlog.000001", "binlog.000002", "binlog.000010"]
        );
    }

    #[test]
    fn binlog_is_closed_compares_against_the_active_file() {
        assert!(binlog_is_closed("binlog.000001", "binlog.000003"));
        assert!(!binlog_is_closed("binlog.000003", "binlog.000003"));
        assert!(!binlog_is_closed("binlog.000004", "binlog.000003"));
    }

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn files(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn purge_cut_stops_before_the_first_ungapped_or_active_file() {
        let disk = files(&[
            "binlog.000001",
            "binlog.000002",
            "binlog.000003",
            "binlog.000004",
        ]);
        // Everything but the active file is uploaded: cut right at active.
        let uploaded = set(&["binlog.000001", "binlog.000002", "binlog.000003"]);
        assert_eq!(
            purge_cut(&disk, "binlog.000004", &uploaded, &set(&[])),
            Some("binlog.000004".to_string())
        );
    }

    #[test]
    fn purge_cut_never_crosses_an_unuploaded_gap() {
        let disk = files(&[
            "binlog.000001",
            "binlog.000002",
            "binlog.000003",
            "binlog.000004",
        ]);
        // 000002 failed to upload (backoff in progress): nothing at or after
        // it may be reclaimed, even though 000001 (older) is safe.
        let uploaded = set(&["binlog.000001", "binlog.000003"]);
        assert_eq!(
            purge_cut(&disk, "binlog.000004", &uploaded, &set(&[])),
            Some("binlog.000002".to_string())
        );
    }

    #[test]
    fn purge_cut_none_when_the_oldest_file_is_already_the_boundary() {
        let disk = files(&["binlog.000001", "binlog.000002"]);
        // The very first file on disk is itself unuploaded/active: nothing
        // precedes it, so there is nothing to purge yet.
        assert_eq!(
            purge_cut(&disk, "binlog.000001", &set(&[]), &set(&[])),
            None
        );
        let uploaded = set(&[]);
        assert_eq!(
            purge_cut(&disk, "binlog.000002", &uploaded, &set(&[])),
            None
        );
    }

    #[test]
    fn purge_cut_empty_disk_list_is_none() {
        assert_eq!(purge_cut(&[], "binlog.000001", &set(&[]), &set(&[])), None);
    }

    #[test]
    fn binlogs_to_replay_runs_from_the_start_file_and_reports_the_gap() {
        let disk = files(&[
            "binlog.000001",
            "binlog.000002",
            "binlog.000003",
            "binlog.000005",
        ]);
        let plan = binlogs_to_replay(disk.clone(), "binlog.000002");
        assert_eq!(plan.run, vec!["binlog.000002", "binlog.000003"]);
        // 000004 is missing while 000005 exists past it: a hole, not an end —
        // the caller must fail loudly instead of replaying short.
        assert_eq!(
            plan.gap,
            Some(BinlogGap {
                after: "binlog.000003".to_string(),
                next_present: "binlog.000005".to_string(),
            })
        );
        // Starting file missing entirely, with nothing past it -> nothing to
        // replay and no gap (everything up to the dump is in the dump).
        let plan = binlogs_to_replay(disk, "binlog.000099");
        assert_eq!(plan.run, Vec::<String>::new());
        assert_eq!(plan.gap, None);
    }

    #[test]
    fn binlogs_to_replay_missing_start_file_with_later_files_is_a_gap() {
        // The full backup's own coordinate file is absent from the archive
        // while LATER binlogs exist: those are unreachable without it — the
        // exact silent-loss shape, reported as a gap at the start.
        let disk = files(&["binlog.000001", "binlog.000004", "binlog.000005"]);
        let plan = binlogs_to_replay(disk, "binlog.000003");
        assert_eq!(plan.run, Vec::<String>::new());
        assert_eq!(
            plan.gap,
            Some(BinlogGap {
                after: String::new(),
                next_present: "binlog.000004".to_string(),
            })
        );
    }

    #[test]
    fn binlog_runs_past_gap_splits_at_every_further_hole() {
        // Past the gap at 000004, 000004-000005 are one run and 000007 —
        // behind a second hole — another: each replays as its own stream,
        // and every hole it skips must show up in gtid_executed afterwards.
        let disk = files(&[
            "binlog.000001",
            "binlog.000002",
            "binlog.000004",
            "binlog.000005",
            "binlog.000007",
        ]);
        assert_eq!(
            binlog_runs_past_gap(disk, "binlog.000004"),
            vec![
                vec!["binlog.000004", "binlog.000005"],
                vec!["binlog.000007"]
            ]
        );
    }

    #[test]
    fn binlog_runs_past_gap_ignores_files_before_the_gap_and_unordered_input() {
        let disk = files(&[
            "binlog.000007",
            "binlog.000004",
            "binlog.000001",
            "binlog.000005",
        ]);
        assert_eq!(
            binlog_runs_past_gap(disk, "binlog.000004"),
            vec![
                vec!["binlog.000004", "binlog.000005"],
                vec!["binlog.000007"]
            ]
        );
    }

    #[test]
    fn binlog_runs_past_gap_with_the_far_side_missing_is_empty() {
        let disk = files(&["binlog.000001", "binlog.000002"]);
        assert_eq!(
            binlog_runs_past_gap(disk, "binlog.000004"),
            Vec::<Vec<String>>::new()
        );
    }

    #[test]
    fn binlogs_to_replay_handles_no_gap_at_all() {
        let disk = files(&["binlog.000001", "binlog.000002", "binlog.000003"]);
        let plan = binlogs_to_replay(disk, "binlog.000001");
        assert_eq!(
            plan.run,
            vec!["binlog.000001", "binlog.000002", "binlog.000003"]
        );
        assert_eq!(plan.gap, None);
    }

    #[test]
    fn binlogs_to_replay_tolerates_unordered_input() {
        let disk = files(&["binlog.000003", "binlog.000001", "binlog.000002"]);
        let plan = binlogs_to_replay(disk, "binlog.000001");
        assert_eq!(
            plan.run,
            vec!["binlog.000001", "binlog.000002", "binlog.000003"]
        );
        assert_eq!(plan.gap, None);
    }

    #[test]
    fn purge_cut_skips_a_lost_file_so_it_cannot_pin_the_cut_forever() {
        let disk = files(&[
            "binlog.000001",
            "binlog.000002",
            "binlog.000003",
            "binlog.000004",
        ]);
        // 000002 is LOST (gone from disk before upload — reported where it
        // was detected): it can never ship, so it must not hold the boundary;
        // uploaded 000003 may still be reclaimed behind the active file.
        let uploaded = set(&["binlog.000001", "binlog.000003"]);
        let lost = set(&["binlog.000002"]);
        assert_eq!(
            purge_cut(&disk, "binlog.000004", &uploaded, &lost),
            Some("binlog.000004".to_string())
        );
        // The same shape WITHOUT the lost marker still refuses to cross the
        // unuploaded file — losing must be an explicit, recorded state.
        assert_eq!(
            purge_cut(&disk, "binlog.000004", &uploaded, &set(&[])),
            Some("binlog.000002".to_string())
        );
    }

    #[test]
    fn purge_cut_retaining_recent_keeps_uploaded_files_inside_the_recovery_window() {
        let day = std::time::Duration::from_secs(86_400);
        let disk = files(&[
            "binlog.000001",
            "binlog.000002",
            "binlog.000003",
            "binlog.000004",
        ]);
        let uploaded = set(&["binlog.000001", "binlog.000002", "binlog.000003"]);
        // 000001 is four days old, 000002 two days, 000003 one hour: with a
        // three-day window only the first may go — the cut stops at the
        // first uploaded file a rejoining peer might still need.
        let age = |name: &str| -> Option<std::time::Duration> {
            match name {
                "binlog.000001" => Some(4 * day),
                "binlog.000002" => Some(2 * day),
                "binlog.000003" => Some(std::time::Duration::from_secs(3_600)),
                _ => Some(std::time::Duration::ZERO),
            }
        };
        assert_eq!(
            purge_cut_retaining_recent(&disk, "binlog.000004", &uploaded, &set(&[]), &age, 3 * day),
            Some("binlog.000002".to_string())
        );
        // Everything old enough: the cut advances to the active file, exactly
        // like the standalone rule.
        let all_old = |_: &str| Some(10 * day);
        assert_eq!(
            purge_cut_retaining_recent(
                &disk,
                "binlog.000004",
                &uploaded,
                &set(&[]),
                &all_old,
                3 * day
            ),
            Some("binlog.000004".to_string())
        );
    }

    #[test]
    fn purge_cut_retaining_recent_never_crosses_an_unuploaded_file_however_old() {
        let disk = files(&["binlog.000001", "binlog.000002", "binlog.000003"]);
        // 000002 never shipped — age is irrelevant, it pins the cut. This is
        // the rule mysqld's own expiry cannot honor, and the reason the
        // primary takes expiry over.
        let uploaded = set(&["binlog.000001"]);
        let ancient = |_: &str| Some(std::time::Duration::from_secs(30 * 86_400));
        assert_eq!(
            purge_cut_retaining_recent(
                &disk,
                "binlog.000003",
                &uploaded,
                &set(&[]),
                &ancient,
                std::time::Duration::from_secs(86_400)
            ),
            Some("binlog.000002".to_string())
        );
    }

    #[test]
    fn purge_cut_retaining_recent_treats_an_unreadable_age_as_recent() {
        let disk = files(&["binlog.000001", "binlog.000002"]);
        let uploaded = set(&["binlog.000001"]);
        let unknown = |_: &str| None;
        assert_eq!(
            purge_cut_retaining_recent(
                &disk,
                "binlog.000002",
                &uploaded,
                &set(&[]),
                &unknown,
                std::time::Duration::from_secs(1)
            ),
            None
        );
    }

    // --- retention ----------------------------------------------------------

    /// A full with an explicit binlog coordinate, so retention's
    /// keep-binlogs-at-or-after-the-floor rule can be exercised precisely.
    fn full_at(server_uuid: &str, taken_at: &str, coord: &str) -> FullBackupRef {
        let mut f = full(server_uuid, taken_at);
        f.meta.binlog_file = coord.to_string();
        f
    }

    fn lineage(server_uuid: &str, fulls: Vec<FullBackupRef>, binlogs: &[&str]) -> LineageObjects {
        let full_objects_seen = fulls.len();
        LineageObjects {
            server_uuid: server_uuid.to_string(),
            fulls,
            full_objects_seen,
            orphan_dumps: Vec::new(),
            binlogs: binlogs.iter().map(|s| s.to_string()).collect(),
            binlog_ages: BTreeMap::new(),
        }
    }

    /// A full taken from a GTID-enabled source — what marks an archive as one
    /// shared history (`archive_shares_history`).
    fn gtid_full(server_uuid: &str, taken_at: &str, coord: &str, purged: &str) -> FullBackupRef {
        let mut f = full_at(server_uuid, taken_at, coord);
        f.meta.gtid_purged = Some(purged.to_string());
        f
    }

    /// A lineage whose binlogs carry upload times — `(name, uploaded_at)`.
    fn aged_lineage(
        server_uuid: &str,
        fulls: Vec<FullBackupRef>,
        binlogs: &[(&str, &str)],
    ) -> LineageObjects {
        let mut l = lineage(
            server_uuid,
            fulls,
            &binlogs.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        );
        l.binlog_ages = binlogs
            .iter()
            .map(|(n, ts)| (n.to_string(), at(ts)))
            .collect();
        l
    }

    fn at(ts: &str) -> DateTime<Utc> {
        parse_target_time(ts).unwrap()
    }

    fn input(
        lineages: Vec<LineageObjects>,
        active: Option<&str>,
        now: &str,
        days: i64,
    ) -> RetentionInput {
        RetentionInput {
            lineages,
            active_server_uuid: active.map(|s| s.to_string()),
            shared_history_marker: false,
            now: at(now),
            horizon: chrono::Duration::days(days),
        }
    }

    #[test]
    fn retention_keeps_everything_inside_the_horizon() {
        let l = lineage(
            "a",
            vec![
                full_at("a", "2026-08-25T00:00:00.000Z", "binlog.000010"),
                full_at("a", "2026-08-26T00:00:00.000Z", "binlog.000020"),
                full_at("a", "2026-08-27T00:00:00.000Z", "binlog.000030"),
            ],
            &["binlog.000010", "binlog.000020", "binlog.000030"],
        );
        let plan = plan_retention(&input(vec![l], Some("a"), "2026-08-27T12:00:00.000Z", 7));
        assert!(plan.is_empty(), "nothing is past a 7d horizon: {plan:?}");
    }

    #[test]
    fn retention_expires_fulls_past_the_horizon_and_binlogs_below_the_floor() {
        // 3d horizon at 2026-08-27T12:00Z -> cutoff 2026-08-24T12:00Z.
        // The 08-20 and 08-22 fulls are outside; 08-25 and 08-26 inside.
        // The 08-22 full is the boundary — the base restore needs for targets
        // before 08-25 — so it stays and the floor moves down to it. Its
        // coordinate is binlog.000020, so binlog 10 goes and 20/30/40 stay.
        let l = lineage(
            "a",
            vec![
                full_at("a", "2026-08-20T00:00:00.000Z", "binlog.000010"),
                full_at("a", "2026-08-22T00:00:00.000Z", "binlog.000020"),
                full_at("a", "2026-08-25T00:00:00.000Z", "binlog.000030"),
                full_at("a", "2026-08-26T00:00:00.000Z", "binlog.000040"),
            ],
            &[
                "binlog.000010",
                "binlog.000020",
                "binlog.000030",
                "binlog.000040",
            ],
        );
        let plan = plan_retention(&input(vec![l], Some("a"), "2026-08-27T12:00:00.000Z", 3));
        assert_eq!(
            plan.expired_full_keys,
            vec![
                "server-a/full/2026-08-20T00:00:00.000Z.sql.gz".to_string(),
                "server-a/full/2026-08-20T00:00:00.000Z.meta.json".to_string(),
            ]
        );
        let expired: Vec<&str> = plan
            .expired_binlogs
            .iter()
            .map(|(_, n)| n.as_str())
            .collect();
        assert_eq!(expired, vec!["binlog.000010"]);
    }

    #[test]
    fn retention_keeps_the_boundary_full_every_in_window_target_restores_from() {
        // The window's promise is any point in the last N days. A target
        // between the cutoff and the oldest in-horizon full restores from the
        // boundary full — the newest one at-or-before it — so deleting that
        // base strands the earliest sliver of the window. Walk the whole
        // window hourly, through restore's own selection rule, against only
        // what retention left behind.
        let all_binlogs: Vec<String> = (10..=28).map(|n| format!("binlog.{n:06}")).collect();
        let names: Vec<&str> = all_binlogs.iter().map(|s| s.as_str()).collect();
        let l = lineage(
            "a",
            vec![
                full_at("a", "2026-08-20T00:00:00.000Z", "binlog.000010"),
                full_at("a", "2026-08-22T00:00:00.000Z", "binlog.000015"),
                full_at("a", "2026-08-25T00:00:00.000Z", "binlog.000021"),
                full_at("a", "2026-08-26T00:00:00.000Z", "binlog.000027"),
            ],
            &names,
        );
        let all_fulls = l.fulls.clone();
        let now = at("2026-08-27T12:00:00.000Z");
        let plan = plan_retention(&input(vec![l], Some("a"), "2026-08-27T12:00:00.000Z", 3));
        let cutoff = now - chrono::Duration::days(3);

        // Only the pre-boundary full expires, with the binlogs its dump covers.
        assert_eq!(
            plan.expired_full_keys,
            vec![
                "server-a/full/2026-08-20T00:00:00.000Z.sql.gz".to_string(),
                "server-a/full/2026-08-20T00:00:00.000Z.meta.json".to_string(),
            ]
        );
        let expired_binlogs: Vec<&str> = plan
            .expired_binlogs
            .iter()
            .map(|(_, n)| n.as_str())
            .collect();
        assert_eq!(
            expired_binlogs,
            vec![
                "binlog.000010",
                "binlog.000011",
                "binlog.000012",
                "binlog.000013",
                "binlog.000014"
            ]
        );

        let surviving_fulls: Vec<FullBackupRef> = all_fulls
            .iter()
            .filter(|f| !plan.expired_full_keys.contains(&f.dump_key))
            .cloned()
            .collect();
        let surviving_binlogs: Vec<String> = all_binlogs
            .iter()
            .filter(|n| !expired_binlogs.contains(&n.as_str()))
            .cloned()
            .collect();

        let mut t = cutoff;
        while t <= now {
            let base = newest_qualifying_full(&surviving_fulls, t).unwrap_or_else(|| {
                panic!("in-window target {t} lost its only qualifying full to retention")
            });
            let replay = binlogs_to_replay(surviving_binlogs.clone(), &base.meta.binlog_file);
            assert!(
                replay.gap.is_none(),
                "retention left a gap above the base for target {t}: {replay:?}"
            );
            assert_eq!(
                replay.run.first(),
                Some(&base.meta.binlog_file),
                "the base's own coordinate must survive with it: {replay:?}"
            );
            t += chrono::Duration::hours(1);
        }

        // The specific regression: the earliest sliver of the window restores
        // from the boundary full, not from nothing.
        let earliest = newest_qualifying_full(&surviving_fulls, cutoff).unwrap();
        assert_eq!(
            earliest.meta.taken_at,
            at("2026-08-22T00:00:00.000Z"),
            "the boundary full must survive as the base for pre-08-25 targets"
        );
    }

    #[test]
    fn retention_never_expires_the_floor_fulls_own_coordinate_file() {
        // The coordinate file is where replay STARTS (binlogs_to_replay treats
        // it as missing -> empty run + a gap), so it must survive even though
        // nothing older than it is retained. The floor here is a boundary
        // full — outside the horizon, kept as the base for in-window targets —
        // which is the usual shape in a steady cadence. Sequence numbers are
        // consecutive, as real binlogs are: retention deletes a strict PREFIX
        // below the floor coordinate, which is exactly why what remains is
        // still gap-free from the floor.
        let all = [
            "binlog.000001",
            "binlog.000002",
            "binlog.000003",
            "binlog.000004",
        ];
        let l = lineage(
            "a",
            vec![
                full_at("a", "2026-08-20T00:00:00.000Z", "binlog.000002"),
                full_at("a", "2026-08-26T00:00:00.000Z", "binlog.000003"),
                full_at("a", "2026-08-27T00:00:00.000Z", "binlog.000004"),
            ],
            &all,
        );
        let plan = plan_retention(&input(vec![l], Some("a"), "2026-08-27T12:00:00.000Z", 3));
        let expired: Vec<&str> = plan
            .expired_binlogs
            .iter()
            .map(|(_, n)| n.as_str())
            .collect();
        assert_eq!(
            expired,
            vec!["binlog.000001"],
            "only files strictly below the floor coordinate expire"
        );
        assert!(
            !expired.contains(&"binlog.000002"),
            "the floor full's own coordinate file must never expire: {expired:?}"
        );

        // The surviving archive must still replay, gap-free, from the floor.
        let survivors: Vec<String> = all
            .iter()
            .filter(|n| !expired.contains(n))
            .map(|n| n.to_string())
            .collect();
        let replay = binlogs_to_replay(survivors.clone(), "binlog.000002");
        assert!(
            replay.gap.is_none(),
            "retention left a gap in the retained chain: {replay:?}"
        );
        assert_eq!(replay.run, survivors);
    }

    #[test]
    fn retention_count_floor_saves_the_only_fulls_when_archiving_has_been_broken() {
        // Every full is far outside the horizon — a naive time sweep would
        // delete all of them and leave the service unrestorable. The active
        // lineage's count floor is what prevents that.
        let l = lineage(
            "a",
            vec![
                full_at("a", "2026-07-01T00:00:00.000Z", "binlog.000010"),
                full_at("a", "2026-07-02T00:00:00.000Z", "binlog.000020"),
                full_at("a", "2026-07-03T00:00:00.000Z", "binlog.000030"),
            ],
            &["binlog.000010", "binlog.000020", "binlog.000030"],
        );
        let plan = plan_retention(&input(vec![l], Some("a"), "2026-08-27T12:00:00.000Z", 7));
        // Newest MIN_ACTIVE_FULLS_KEPT survive; only the oldest expires.
        assert_eq!(MIN_ACTIVE_FULLS_KEPT, 2);
        assert_eq!(
            plan.expired_full_keys,
            vec![
                "server-a/full/2026-07-01T00:00:00.000Z.sql.gz".to_string(),
                "server-a/full/2026-07-01T00:00:00.000Z.meta.json".to_string(),
            ]
        );
        let expired: Vec<&str> = plan
            .expired_binlogs
            .iter()
            .map(|(_, n)| n.as_str())
            .collect();
        assert_eq!(expired, vec!["binlog.000010"]);
    }

    #[test]
    fn retention_never_touches_a_lineage_with_a_single_full() {
        let l = lineage(
            "a",
            vec![full_at("a", "2026-01-01T00:00:00.000Z", "binlog.000005")],
            &["binlog.000005", "binlog.000006"],
        );
        let plan = plan_retention(&input(vec![l], Some("a"), "2026-08-27T12:00:00.000Z", 1));
        assert!(
            plan.is_empty(),
            "a lone full and its chain must survive any horizon: {plan:?}"
        );
    }

    #[test]
    fn retention_retires_a_dead_lineage_wholly_past_the_horizon() {
        let dead = lineage(
            "old",
            vec![
                full_at("old", "2026-08-01T00:00:00.000Z", "binlog.000010"),
                full_at("old", "2026-08-02T00:00:00.000Z", "binlog.000020"),
            ],
            &["binlog.000010", "binlog.000020"],
        );
        let live = lineage(
            "new",
            vec![
                full_at("new", "2026-08-26T00:00:00.000Z", "binlog.000001"),
                full_at("new", "2026-08-27T00:00:00.000Z", "binlog.000002"),
            ],
            &["binlog.000001", "binlog.000002"],
        );
        let plan = plan_retention(&input(
            vec![dead, live],
            Some("new"),
            "2026-08-27T12:00:00.000Z",
            7,
        ));
        assert_eq!(plan.retired_lineages, vec!["old".to_string()]);
        assert_eq!(
            plan.expired_full_keys.len(),
            4,
            "both of old's fulls, dump+meta"
        );
        assert!(
            plan.expired_binlogs.iter().all(|(uuid, _)| uuid == "old"),
            "the live lineage must be untouched: {:?}",
            plan.expired_binlogs
        );
    }

    #[test]
    fn retention_keeps_a_dead_lineage_still_inside_the_horizon() {
        // The volume was reset an hour ago. Restoring to before the reset is
        // exactly what the window promises, so the dead lineage stays.
        let dead = lineage(
            "old",
            vec![full_at("old", "2026-08-27T09:00:00.000Z", "binlog.000010")],
            &["binlog.000010", "binlog.000011"],
        );
        let live = lineage(
            "new",
            vec![full_at("new", "2026-08-27T11:00:00.000Z", "binlog.000001")],
            &["binlog.000001"],
        );
        let plan = plan_retention(&input(
            vec![dead, live],
            Some("new"),
            "2026-08-27T12:00:00.000Z",
            7,
        ));
        assert!(
            plan.is_empty(),
            "a fresh dead lineage must survive: {plan:?}"
        );
        assert!(plan.retired_lineages.is_empty());
    }

    #[test]
    fn retention_refuses_to_act_without_an_active_lineage() {
        // Cannot tell a dead lineage from this server's own yet: do nothing.
        let l = lineage(
            "a",
            vec![full_at("a", "2026-01-01T00:00:00.000Z", "binlog.000010")],
            &["binlog.000010"],
        );
        let plan = plan_retention(&input(vec![l], None, "2026-08-27T12:00:00.000Z", 1));
        assert!(plan.is_empty());
        assert!(plan.notes.iter().any(|n| n.contains("no active lineage")));
    }

    #[test]
    fn retention_is_inert_on_a_non_positive_horizon() {
        let l = lineage(
            "a",
            vec![
                full_at("a", "2026-01-01T00:00:00.000Z", "binlog.000010"),
                full_at("a", "2026-01-02T00:00:00.000Z", "binlog.000020"),
                full_at("a", "2026-01-03T00:00:00.000Z", "binlog.000030"),
            ],
            &["binlog.000010"],
        );
        let mut inp = input(vec![l], Some("a"), "2026-08-27T12:00:00.000Z", 0);
        inp.horizon = chrono::Duration::zero();
        assert!(plan_retention(&inp).is_empty());
    }

    #[test]
    fn retention_expires_orphan_dumps_only_past_the_grace() {
        let mut l = lineage(
            "a",
            vec![full_at("a", "2026-08-27T00:00:00.000Z", "binlog.000010")],
            &["binlog.000010"],
        );
        l.orphan_dumps = vec![
            // In flight 10 minutes ago — must be spared.
            (
                "server-a/full/2026-08-27T11:50:00.000Z.sql.gz".to_string(),
                at("2026-08-27T11:50:00.000Z"),
            ),
            // Wreckage from yesterday — unrestorable, expire it.
            (
                "server-a/full/2026-08-26T00:00:00.000Z.sql.gz".to_string(),
                at("2026-08-26T00:00:00.000Z"),
            ),
        ];
        let plan = plan_retention(&input(vec![l], Some("a"), "2026-08-27T12:00:00.000Z", 7));
        assert_eq!(
            plan.orphan_dump_keys,
            vec!["server-a/full/2026-08-26T00:00:00.000Z.sql.gz".to_string()]
        );
    }

    #[test]
    fn retention_expires_binlogs_of_a_dead_lineage_that_never_got_a_full() {
        let orphaned = lineage("stale", vec![], &["binlog.000001", "binlog.000002"]);
        let live = lineage(
            "new",
            vec![full_at("new", "2026-08-27T00:00:00.000Z", "binlog.000001")],
            &["binlog.000001"],
        );
        let plan = plan_retention(&input(
            vec![orphaned, live],
            Some("new"),
            "2026-08-27T12:00:00.000Z",
            7,
        ));
        assert_eq!(plan.expired_binlogs.len(), 2);
        assert!(plan.expired_binlogs.iter().all(|(u, _)| u == "stale"));
    }

    #[test]
    fn retention_spares_the_active_lineage_before_its_first_full_lands() {
        // Binlogs are shipping but the initial full has not finished; expiring
        // them here would punch a hole the first full can never cover.
        let l = lineage("a", vec![], &["binlog.000001", "binlog.000002"]);
        let plan = plan_retention(&input(vec![l], Some("a"), "2026-08-27T12:00:00.000Z", 7));
        assert!(plan.is_empty(), "must spare the active lineage: {plan:?}");
    }

    #[test]
    fn retention_keeps_every_binlog_when_the_floor_coordinate_is_unparseable() {
        let l = lineage(
            "a",
            vec![
                full_at("a", "2026-08-01T00:00:00.000Z", "weird-name"),
                full_at("a", "2026-08-26T00:00:00.000Z", "also-weird"),
                full_at("a", "2026-08-27T00:00:00.000Z", "binlog.000030"),
            ],
            &["binlog.000010", "binlog.000030"],
        );
        let plan = plan_retention(&input(vec![l], Some("a"), "2026-08-27T12:00:00.000Z", 3));
        assert!(
            plan.expired_binlogs.is_empty(),
            "an unparseable floor coordinate must not expire any binlog: {:?}",
            plan.expired_binlogs
        );
        assert!(plan
            .notes
            .iter()
            .any(|n| n.contains("no parseable sequence")));
    }

    #[test]
    fn retention_does_not_wipe_a_lineage_whose_fulls_merely_could_not_be_read() {
        // The dangerous shape: a dead lineage that HAS good fulls in the
        // bucket, but whose meta.json objects could not be read this pass (an
        // S3 blip, or a corrupt/unparseable meta). The caller cannot express
        // "keeping this full" by simply omitting it — an empty `fulls` list is
        // indistinguishable from a lineage that never had one, and expiring
        // its binlogs would make the surviving fulls unrestorable past their
        // own coordinates. Irreversibly.
        let mut dead = lineage("old", vec![], &["binlog.000001", "binlog.000002"]);
        dead.full_objects_seen = 2; // two meta.json objects exist, unread
        let live = lineage(
            "new",
            vec![full_at("new", "2026-08-27T00:00:00.000Z", "binlog.000001")],
            &["binlog.000001"],
        );
        let plan = plan_retention(&input(
            vec![dead, live],
            Some("new"),
            "2026-08-27T12:00:00.000Z",
            7,
        ));
        assert!(
            plan.expired_binlogs.is_empty(),
            "unreadable fulls must never be treated as absent fulls: {:?}",
            plan.expired_binlogs
        );
        assert!(plan.retired_lineages.is_empty());
        assert!(plan.notes.iter().any(|n| n.contains("could not be read")));
    }

    #[test]
    fn retention_waits_for_the_active_lineage_to_have_a_full_before_expiring_anything() {
        // A fresh volume: the new server has archived nothing yet, and the
        // only fulls in the bucket belong to the dead lineage it replaced.
        // Retiring that lineage now — even though it is past the horizon —
        // would leave the bucket with nothing restorable at all until the
        // first new full lands. Wait for the replacement to exist.
        let dead = lineage(
            "old",
            vec![full_at("old", "2026-08-01T00:00:00.000Z", "binlog.000010")],
            &["binlog.000010"],
        );
        let fresh = lineage("new", vec![], &[]);
        let plan = plan_retention(&input(
            vec![dead, fresh],
            Some("new"),
            "2026-08-27T12:00:00.000Z",
            7,
        ));
        assert!(
            plan.is_empty(),
            "must not expire the bucket's last fulls before the active lineage has one: {plan:?}"
        );
        assert!(plan
            .notes
            .iter()
            .any(|n| n.contains("active lineage has no complete full")));
    }

    #[test]
    fn meta_key_for_dump_pairs_the_sidecar() {
        assert_eq!(
            meta_key_for_dump("binlog/server-a/full/2026-08-27T00:00:00.000Z.sql.gz"),
            "binlog/server-a/full/2026-08-27T00:00:00.000Z.meta.json"
        );
    }

    // --- GTID: dump set parsing, set holes, key-encoded taken_at ----------------

    #[test]
    fn gtid_purged_parses_the_8_0_form_across_lines() {
        let head = "-- MySQL dump 8.4.3\n\
                    SET @@SESSION.SQL_LOG_BIN= 0;\n\
                    --\n-- GTID state at the beginning of the backup\n--\n\
                    SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '8f0e1c2a-0000-0000-0000-000000000001:1-13,\n\
                    9a110000-0000-0000-0000-000000000002:1-2';\n\
                    -- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='binlog.000004', SOURCE_LOG_POS=197;\n";
        assert_eq!(
            parse_gtid_purged(head).as_deref(),
            Some(
                "8f0e1c2a-0000-0000-0000-000000000001:1-13,9a110000-0000-0000-0000-000000000002:1-2"
            )
        );
        // The coordinate parser is untouched by the GTID line ahead of it.
        assert_eq!(
            parse_change_master_coords(head),
            Some(("binlog.000004".to_string(), 197))
        );
    }

    #[test]
    fn gtid_purged_parses_the_bare_form_and_the_empty_set() {
        assert_eq!(
            parse_gtid_purged("SET @@GLOBAL.GTID_PURGED='aaaa:1-5';\n").as_deref(),
            Some("aaaa:1-5")
        );
        // A GTID server with no history yet: present, empty — still a GTID
        // source, which restore must treat differently from an anonymous one.
        assert_eq!(
            parse_gtid_purged("SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '';\n").as_deref(),
            Some("")
        );
    }

    #[test]
    fn gtid_purged_is_absent_on_an_anonymous_dump() {
        let head = "-- MySQL dump\n-- CHANGE MASTER TO MASTER_LOG_FILE='binlog.000003', MASTER_LOG_POS=157;\n";
        assert_eq!(parse_gtid_purged(head), None);
    }

    fn binlog_event(timestamp: u32, type_code: u8, payload: &[u8]) -> Vec<u8> {
        let mut event = Vec::new();
        event.extend_from_slice(&timestamp.to_le_bytes());
        event.push(type_code);
        event.extend_from_slice(&1u32.to_le_bytes()); // server_id
        let len = (19 + payload.len()) as u32;
        event.extend_from_slice(&len.to_le_bytes());
        event.extend_from_slice(&0u32.to_le_bytes()); // next_position (unused here)
        event.extend_from_slice(&0u16.to_le_bytes()); // flags
        event.extend_from_slice(payload);
        event
    }

    fn encoded_gtid_set(sids: &[([u8; 16], &[(u64, u64)])]) -> Vec<u8> {
        let mut out = (sids.len() as u64).to_le_bytes().to_vec();
        for (uuid, intervals) in sids {
            out.extend_from_slice(uuid);
            out.extend_from_slice(&(intervals.len() as u64).to_le_bytes());
            for (start, end) in intervals.iter() {
                out.extend_from_slice(&start.to_le_bytes());
                out.extend_from_slice(&end.to_le_bytes());
            }
        }
        // The event's CRC32 trails the set; the decoder must not read it.
        out.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        out
    }

    const UUID_A: [u8; 16] = [
        0x8e, 0x2f, 0x4a, 0x10, 0x9c, 0x3b, 0x11, 0xef, 0xa1, 0xb2, 0x02, 0x42, 0xac, 0x12, 0x00,
        0x02,
    ];
    const UUID_B: [u8; 16] = [0x01; 16];

    fn binlog_bytes(created: u32, set: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0xfe, b'b', b'i', b'n'];
        // Format_description payload: binlog_version(2) + server_version(50) +
        // create_timestamp(4) + header_len(1) + post-header lengths + checksum
        // alg — opaque here, the parser only reads its header.
        bytes.extend(binlog_event(created, 15, &[0u8; 80]));
        bytes.extend(binlog_event(created, 35, set));
        bytes
    }

    #[test]
    fn binlog_head_reads_creation_time_and_previous_gtids() {
        let set = encoded_gtid_set(&[(UUID_A, &[(1, 6)]), (UUID_B, &[(1, 2), (8, 10)])]);
        let head = parse_binlog_head(&binlog_bytes(1_700_000_000, &set)).unwrap();
        assert_eq!(head.created_at.timestamp(), 1_700_000_000);
        assert_eq!(
            head.previous_gtids,
            "8e2f4a10-9c3b-11ef-a1b2-0242ac120002:1-5,01010101-0101-0101-0101-010101010101:1:8-9"
        );
    }

    #[test]
    fn binlog_head_of_a_server_with_no_gtid_history_is_empty() {
        let set = encoded_gtid_set(&[]);
        let head = parse_binlog_head(&binlog_bytes(1_700_000_000, &set)).unwrap();
        assert_eq!(head.previous_gtids, "");
    }

    #[test]
    fn binlog_head_ignores_bytes_past_the_two_events() {
        let set = encoded_gtid_set(&[(UUID_A, &[(1, 2)])]);
        let mut bytes = binlog_bytes(1_700_000_000, &set);
        bytes.extend(binlog_event(1_700_000_001, 33, &[7u8; 40])); // a Gtid event
        bytes.extend_from_slice(&[0xff; 100]); // whatever follows, unread
        let head = parse_binlog_head(&bytes).unwrap();
        assert_eq!(head.previous_gtids, "8e2f4a10-9c3b-11ef-a1b2-0242ac120002:1");
    }

    #[test]
    fn binlog_head_refuses_what_it_cannot_read() {
        // Wrong magic: an encrypted binlog, a relay log, or not a binlog.
        assert!(parse_binlog_head(b"\xfdbin\0\0\0\0").is_err());
        // Truncated inside the Previous_gtids event.
        let set = encoded_gtid_set(&[(UUID_A, &[(1, 6)])]);
        let bytes = binlog_bytes(1_700_000_000, &set);
        assert!(parse_binlog_head(&bytes[..bytes.len() - 30]).is_err());
        // Tagged-GTID encoding (format byte 1 in n_sids' high byte).
        let mut tagged = encoded_gtid_set(&[(UUID_A, &[(1, 6)])]);
        tagged[7] = 1;
        let err = parse_binlog_head(&binlog_bytes(1_700_000_000, &tagged)).unwrap_err();
        assert!(err.to_string().contains("tagged"), "{err}");
        // A file whose second event is not Previous_gtids, within budget.
        let mut no_pg = vec![0xfe, b'b', b'i', b'n'];
        no_pg.extend(binlog_event(1, 15, &[0u8; 80]));
        for _ in 0..BINLOG_HEAD_EVENT_BUDGET {
            no_pg.extend(binlog_event(1, 4, &[0u8; 8])); // Rotate events
        }
        assert!(parse_binlog_head(&no_pg).is_err());
    }

    #[test]
    fn a_binlog_vouches_only_when_opened_before_the_targets_second() {
        let target = parse_target_time("2026-09-10T12:00:05.700Z").unwrap();
        let opened = |s: &str| parse_target_time(s).unwrap();
        assert!(binlog_opened_before_cutoff(opened("2026-09-10T12:00:04.999Z"), target));
        // Same second as the target: the replay cut events stamped 12:00:05,
        // which this file's Previous_gtids may include.
        assert!(!binlog_opened_before_cutoff(opened("2026-09-10T12:00:05.000Z"), target));
        assert!(!binlog_opened_before_cutoff(opened("2026-09-10T12:00:05.900Z"), target));
        assert!(!binlog_opened_before_cutoff(opened("2026-09-10T12:00:06.000Z"), target));
    }

    fn owner_now() -> DateTime<Utc> {
        parse_target_time("2026-09-10T00:00:00.000Z").unwrap()
    }

    fn owner(env: &str, history: &str, ids: &[&str]) -> ArchiveOwner {
        ArchiveOwner {
            environment_id: env.to_string(),
            history: history.to_string(),
            service_ids: ids.iter().map(|s| s.to_string()).collect(),
            claimed_at: owner_now(),
        }
    }

    fn claimant<'a>(
        env: &'a str,
        service: &'a str,
        group_primary: bool,
        executed: &'a str,
    ) -> ArchiveClaimant<'a> {
        ArchiveClaimant {
            environment_id: env,
            service_id: service,
            history: "11111111-2222-3333-4444-555555555555",
            group_primary,
            executed_gtid_set: executed,
        }
    }

    #[test]
    fn an_unclaimed_root_is_claimed_by_whoever_archives_first() {
        let me = claimant("env-a", "svc-1", false, "");
        assert_eq!(
            archive_ownership_verdict("binlog", None, &me, owner_now()),
            OwnershipVerdict::Write(owner(
                "env-a",
                "11111111-2222-3333-4444-555555555555",
                &["svc-1"]
            ))
        );
    }

    #[test]
    fn a_root_claimed_by_another_environment_is_refused_in_every_mode() {
        let existing = owner("env-a", "11111111-2222-3333-4444-555555555555", &["svc-1"]);
        for group_primary in [false, true] {
            let me = claimant("env-fork", "svc-1", group_primary, "");
            match archive_ownership_verdict("binlog", Some(&existing), &me, owner_now()) {
                OwnershipVerdict::Refuse(reason) => {
                    assert!(reason.contains("belongs to environment env-a"), "{reason}");
                    assert!(reason.contains("binlog/owner.json"), "{reason}");
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_groups_members_share_the_root_and_each_primary_records_itself() {
        let existing = owner("env-a", "11111111-2222-3333-4444-555555555555", &["svc-1"]);
        let me = claimant("env-a", "svc-2", true, "");
        let OwnershipVerdict::Write(updated) =
            archive_ownership_verdict("binlog", Some(&existing), &me, owner_now())
        else {
            panic!("a same-environment group primary is recorded");
        };
        assert_eq!(updated.service_ids, vec!["svc-1", "svc-2"]);
        let again = claimant("env-a", "svc-2", true, "");
        assert_eq!(
            archive_ownership_verdict("binlog", Some(&updated), &again, owner_now()),
            OwnershipVerdict::Keep
        );
    }

    #[test]
    fn a_recorded_standalone_service_keeps_its_root() {
        let existing = owner("env-a", "11111111-2222-3333-4444-555555555555", &["svc-1"]);
        let me = claimant("env-a", "svc-1", false, "");
        assert_eq!(
            archive_ownership_verdict("binlog", Some(&existing), &me, owner_now()),
            OwnershipVerdict::Keep
        );
    }

    #[test]
    fn a_duplicated_standalone_service_in_the_same_environment_is_refused() {
        let existing = owner("env-a", "11111111-2222-3333-4444-555555555555", &["svc-1"]);
        // Fresh data (no history), another service id.
        let me = claimant("env-a", "svc-copy", false, "");
        match archive_ownership_verdict("binlog", Some(&existing), &me, owner_now()) {
            OwnershipVerdict::Refuse(reason) => {
                assert!(reason.contains("belongs to service svc-1"), "{reason}");
                assert!(reason.contains("svc-copy"), "{reason}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        // Its own GTID history under another UUID is not the root's history.
        let foreign = claimant(
            "env-a",
            "svc-copy",
            false,
            "99999999-2222-3333-4444-555555555555:1-40",
        );
        assert!(matches!(
            archive_ownership_verdict("binlog", Some(&existing), &foreign, owner_now()),
            OwnershipVerdict::Refuse(_)
        ));
    }

    #[test]
    fn a_member_reverted_to_standalone_carries_the_roots_history_and_is_recorded() {
        let existing = owner("env-a", "11111111-2222-3333-4444-555555555555", &["svc-1"]);
        let me = claimant(
            "env-a",
            "svc-3",
            false,
            "aaaaaaaa-0000-0000-0000-000000000000:1-3,\n11111111-2222-3333-4444-555555555555:1-900",
        );
        let OwnershipVerdict::Write(updated) =
            archive_ownership_verdict("binlog", Some(&existing), &me, owner_now())
        else {
            panic!("a reverted member carries the group's UUID and is recorded");
        };
        assert_eq!(updated.service_ids, vec!["svc-1", "svc-3"]);
    }

    #[test]
    fn owner_key_sits_at_the_archive_root() {
        let mut loc = S3Location {
            bucket: "b".to_string(),
            access_key: "k".to_string(),
            secret_key: "s".to_string(),
            region: "r".to_string(),
            endpoint: "http://minio:9000".to_string(),
            path: "/binlog".to_string(),
        };
        assert_eq!(owner_key(&loc), "binlog/owner.json");
        loc.path = String::new();
        assert_eq!(owner_key(&loc), "owner.json");
    }

    #[test]
    fn full_taken_at_is_read_from_either_full_object_name() {
        let t = at("2026-08-27T00:00:00.000Z");
        assert_eq!(
            full_taken_at_from_key("binlog/server-a/full/2026-08-27T00:00:00.000Z.sql.gz"),
            Some(t)
        );
        assert_eq!(
            full_taken_at_from_key("binlog/server-a/full/2026-08-27T00:00:00.000Z.meta.json"),
            Some(t)
        );
        assert_eq!(
            full_taken_at_from_key("binlog/server-a/binlog/binlog.000001"),
            None
        );
        assert_eq!(
            full_taken_at_from_key("binlog/server-a/full/not-a-time.sql.gz"),
            None
        );
    }

    // --- retention over one shared history (Group Replication) -----------------

    #[test]
    fn shared_history_is_detected_from_any_gtid_full() {
        let anon = vec![lineage(
            "a",
            vec![full("a", "2026-08-20T00:00:00.000Z")],
            &[],
        )];
        assert!(!archive_shares_history(&anon));
        let shared = vec![
            lineage("a", vec![full("a", "2026-08-20T00:00:00.000Z")], &[]),
            lineage(
                "b",
                vec![gtid_full(
                    "b",
                    "2026-08-21T00:00:00.000Z",
                    "binlog.000001",
                    "g:1-9",
                )],
                &[],
            ),
        ];
        assert!(archive_shares_history(&shared));
    }

    #[test]
    fn shared_history_keeps_a_full_less_primary_lineage_whole() {
        // X was primary and took the only full; Y took over (no full of its
        // own — the archive-wide cadence said none was due) and has archived
        // its tenure since. Every one of Y's binlogs is the only carrier of
        // the group's transactions after the handoff; none may expire, and
        // Y — the active lineage — has no full to be "waiting for".
        let x = aged_lineage(
            "x",
            vec![gtid_full(
                "x",
                "2026-08-20T00:00:00.000Z",
                "binlog.000003",
                "g:1-10",
            )],
            &[
                ("binlog.000001", "2026-08-19T00:00:00.000Z"),
                ("binlog.000002", "2026-08-19T12:00:00.000Z"),
                ("binlog.000003", "2026-08-20T00:01:00.000Z"),
                ("binlog.000004", "2026-08-20T06:00:00.000Z"),
            ],
        );
        let y = aged_lineage(
            "y",
            vec![],
            &[
                ("binlog.000001", "2026-08-20T06:05:00.000Z"),
                ("binlog.000002", "2026-08-21T00:00:00.000Z"),
            ],
        );
        let plan = plan_retention(&input(vec![x, y], Some("y"), "2026-08-22T00:00:00.000Z", 7));
        assert!(plan.expired_full_keys.is_empty(), "{plan:?}");
        // Only X's files strictly below its own full's coordinate expire.
        assert_eq!(
            plan.expired_binlogs,
            vec![
                ("x".to_string(), "binlog.000001".to_string()),
                ("x".to_string(), "binlog.000002".to_string()),
            ]
        );
        assert!(plan.retired_lineages.is_empty(), "{plan:?}");
        assert!(
            !plan.notes.iter().any(|n| n.contains("unrestorable")),
            "a full-less lineage is not unrestorable in a shared history: {plan:?}"
        );
    }

    #[test]
    fn shared_history_ranks_fulls_archive_wide_and_expires_by_age_against_the_floor() {
        // Fulls alternate between lineages as the primary moved around. With a
        // 2-day horizon at 08-27 the cutoff is 08-25: fulls on 08-26 (y) and
        // 08-25T12 (x) are inside it, and the boundary rule keeps the 08-24 (y)
        // full as the floor. Everything older than the floor expires — x's
        // 08-22 full, and every binlog UPLOADED before 08-24T00:00 in lineages
        // other than the floor's (which trims by sequence instead).
        let x = aged_lineage(
            "x",
            vec![
                gtid_full("x", "2026-08-22T00:00:00.000Z", "binlog.000002", "g:1-100"),
                gtid_full("x", "2026-08-25T12:00:00.000Z", "binlog.000009", "g:1-500"),
            ],
            &[
                ("binlog.000001", "2026-08-21T00:00:00.000Z"),
                ("binlog.000002", "2026-08-22T00:01:00.000Z"),
                ("binlog.000005", "2026-08-23T23:59:00.000Z"),
                ("binlog.000006", "2026-08-24T00:30:00.000Z"),
                ("binlog.000009", "2026-08-25T12:01:00.000Z"),
            ],
        );
        let y = aged_lineage(
            "y",
            vec![
                gtid_full("y", "2026-08-24T00:00:00.000Z", "binlog.000004", "g:1-300"),
                gtid_full("y", "2026-08-26T00:00:00.000Z", "binlog.000012", "g:1-900"),
            ],
            &[
                ("binlog.000001", "2026-08-23T00:00:00.000Z"),
                ("binlog.000003", "2026-08-23T23:00:00.000Z"),
                ("binlog.000004", "2026-08-24T00:01:00.000Z"),
                ("binlog.000012", "2026-08-26T00:01:00.000Z"),
            ],
        );
        let oldest_x_dump = x.fulls[0].dump_key.clone();
        let plan = plan_retention(&input(vec![x, y], Some("y"), "2026-08-27T00:00:00.000Z", 2));
        assert_eq!(
            plan.expired_full_keys,
            vec![oldest_x_dump.clone(), meta_key_for_dump(&oldest_x_dump)]
        );
        let mut expired = plan.expired_binlogs.clone();
        expired.sort();
        assert_eq!(
            expired,
            vec![
                ("x".to_string(), "binlog.000001".to_string()),
                ("x".to_string(), "binlog.000002".to_string()),
                ("x".to_string(), "binlog.000005".to_string()),
                // y (the floor's lineage) trims by sequence below 000004.
                ("y".to_string(), "binlog.000001".to_string()),
                ("y".to_string(), "binlog.000003".to_string()),
            ]
        );
        assert!(plan.retired_lineages.is_empty(), "{plan:?}");
        assert!(plan
            .notes
            .iter()
            .any(|n| n.contains("floor full") && n.contains("predates")));
    }

    #[test]
    fn shared_history_retires_a_dead_lineage_only_once_nothing_of_it_survives_the_floor() {
        // z was primary long ago; everything it holds predates the floor.
        let z = aged_lineage(
            "z",
            vec![gtid_full(
                "z",
                "2026-08-01T00:00:00.000Z",
                "binlog.000001",
                "g:1-5",
            )],
            &[
                ("binlog.000001", "2026-08-01T00:01:00.000Z"),
                ("binlog.000002", "2026-08-02T00:00:00.000Z"),
            ],
        );
        let x = aged_lineage(
            "x",
            vec![
                gtid_full("x", "2026-08-20T00:00:00.000Z", "binlog.000001", "g:1-50"),
                gtid_full("x", "2026-08-26T00:00:00.000Z", "binlog.000007", "g:1-90"),
            ],
            &[
                ("binlog.000001", "2026-08-20T00:01:00.000Z"),
                ("binlog.000007", "2026-08-26T00:01:00.000Z"),
            ],
        );
        let z_dump = z.fulls[0].dump_key.clone();
        let plan = plan_retention(&input(vec![z, x], Some("x"), "2026-08-27T00:00:00.000Z", 2));
        assert_eq!(plan.retired_lineages, vec!["z".to_string()]);
        assert!(plan.expired_full_keys.contains(&z_dump), "{plan:?}");
        assert!(plan
            .expired_binlogs
            .contains(&("z".to_string(), "binlog.000002".to_string())));

        // Same z, but one of its binlogs was uploaded AFTER the floor full was
        // taken: it may carry transactions the floor dump lacks. z stays.
        let mut z2 = aged_lineage(
            "z",
            vec![gtid_full(
                "z",
                "2026-08-01T00:00:00.000Z",
                "binlog.000001",
                "g:1-5",
            )],
            &[
                ("binlog.000001", "2026-08-01T00:01:00.000Z"),
                ("binlog.000002", "2026-08-20T00:30:00.000Z"),
            ],
        );
        z2.server_uuid = "z".to_string();
        let x2 = aged_lineage(
            "x",
            vec![
                gtid_full("x", "2026-08-20T00:00:00.000Z", "binlog.000001", "g:1-50"),
                gtid_full("x", "2026-08-26T00:00:00.000Z", "binlog.000007", "g:1-90"),
            ],
            &[
                ("binlog.000001", "2026-08-20T00:01:00.000Z"),
                ("binlog.000007", "2026-08-26T00:01:00.000Z"),
            ],
        );
        let plan = plan_retention(&input(
            vec![z2, x2],
            Some("x"),
            "2026-08-27T00:00:00.000Z",
            2,
        ));
        assert!(plan.retired_lineages.is_empty(), "{plan:?}");
        assert!(!plan
            .expired_binlogs
            .contains(&("z".to_string(), "binlog.000002".to_string())));
    }

    #[test]
    fn shared_history_keeps_binlogs_with_unknown_age_and_honors_the_count_floor() {
        // Archiving broke weeks ago: both fulls are past a 2-day horizon, yet
        // MIN_ACTIVE_FULLS_KEPT keeps them (archive-wide), so the floor is the
        // OLDEST full and nothing in its lineage is below its coordinate; the
        // age-less binlog in the other lineage is kept because nothing proves
        // it old.
        let x = aged_lineage(
            "x",
            vec![
                gtid_full("x", "2026-08-01T00:00:00.000Z", "binlog.000001", "g:1-5"),
                gtid_full("x", "2026-08-02T00:00:00.000Z", "binlog.000003", "g:1-9"),
            ],
            &[
                ("binlog.000001", "2026-08-01T00:01:00.000Z"),
                ("binlog.000003", "2026-08-02T00:01:00.000Z"),
            ],
        );
        let y = lineage("y", vec![], &["binlog.000001"]);
        let plan = plan_retention(&input(vec![x, y], Some("y"), "2026-08-27T00:00:00.000Z", 2));
        assert!(plan.expired_full_keys.is_empty(), "{plan:?}");
        assert!(plan.expired_binlogs.is_empty(), "{plan:?}");
        assert!(plan
            .notes
            .iter()
            .any(|n| n.contains("upload time is unknown")));

        // A third, in-horizon full moves the floor up to the 08-02 full: now
        // x's 000001 (below coordinate 000003) expires, and y's age-less
        // binlog is STILL kept.
        let x = aged_lineage(
            "x",
            vec![
                gtid_full("x", "2026-08-01T00:00:00.000Z", "binlog.000001", "g:1-5"),
                gtid_full("x", "2026-08-02T00:00:00.000Z", "binlog.000003", "g:1-9"),
                gtid_full("x", "2026-08-26T00:00:00.000Z", "binlog.000009", "g:1-50"),
            ],
            &[
                ("binlog.000001", "2026-08-01T00:01:00.000Z"),
                ("binlog.000003", "2026-08-02T00:01:00.000Z"),
                ("binlog.000009", "2026-08-26T00:01:00.000Z"),
            ],
        );
        let y = lineage("y", vec![], &["binlog.000001"]);
        let plan = plan_retention(&input(vec![x, y], Some("y"), "2026-08-27T00:00:00.000Z", 2));
        assert_eq!(
            plan.expired_binlogs,
            vec![("x".to_string(), "binlog.000001".to_string())]
        );
        assert_eq!(
            plan.expired_full_keys.len(),
            2,
            "the 08-01 full (dump + meta) is below the floor and expires: {plan:?}"
        );
        assert!(plan.expired_full_keys[0].contains("2026-08-01T00:00:00.000Z"));
    }

    #[test]
    fn shared_history_marker_alone_selects_the_shared_rules() {
        // A standalone server converted to HA: its only full is anonymous
        // (pre-conversion), the new primary has archived GTID binlogs since,
        // and a peer's lineage (no full) carries the tenure after a failover.
        // Under the independent-histories rules that peer lineage would be
        // "unrestorable" and expire; the marker says otherwise.
        let x = aged_lineage(
            "x",
            vec![full_at("x", "2026-08-20T00:00:00.000Z", "binlog.000002")],
            &[
                ("binlog.000002", "2026-08-20T00:01:00.000Z"),
                ("binlog.000003", "2026-08-21T00:00:00.000Z"),
            ],
        );
        let y = aged_lineage(
            "y",
            vec![],
            &[("binlog.000001", "2026-08-22T00:00:00.000Z")],
        );
        // Active = x (it holds the full) so the independent rules get past
        // their own "active lineage has a full" gate and reach y.
        let mut in_ = input(
            vec![x.clone(), y.clone()],
            Some("x"),
            "2026-08-27T00:00:00.000Z",
            7,
        );
        assert!(!archive_shares_history(&in_.lineages));
        let plan = plan_retention(&in_);
        assert!(
            plan.expired_binlogs
                .contains(&("y".to_string(), "binlog.000001".to_string())),
            "without the marker the independent rules retire the full-less lineage: {plan:?}"
        );
        in_.shared_history_marker = true;
        let plan = plan_retention(&in_);
        assert!(
            plan.is_empty(),
            "with the marker nothing after the floor expires: {plan:?}"
        );
    }

    #[test]
    fn shared_history_is_inert_while_any_full_is_unreadable() {
        let mut x = aged_lineage(
            "x",
            vec![gtid_full(
                "x",
                "2026-08-20T00:00:00.000Z",
                "binlog.000003",
                "g:1-10",
            )],
            &[("binlog.000001", "2026-08-19T00:00:00.000Z")],
        );
        x.full_objects_seen = 2; // one more meta exists than could be read
        let plan = plan_retention(&input(vec![x], Some("x"), "2026-08-27T00:00:00.000Z", 2));
        assert!(plan.is_empty(), "{plan:?}");
        assert!(plan.notes.iter().any(|n| n.contains("could not be read")));
    }
}
