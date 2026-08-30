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

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

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

/// Parse an operator-supplied ISO-8601 UTC timestamp (`MYSQL_RECOVERY_TARGET_TIME`).
pub fn parse_target_time(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| {
            format!("MYSQL_RECOVERY_TARGET_TIME {s:?} is not a valid ISO-8601 timestamp")
        })
}

/// A full backup's sidecar metadata (`<...>.meta.json`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FullBackupMeta {
    pub taken_at: DateTime<Utc>,
    pub binlog_file: String,
    pub binlog_pos: u64,
    pub server_uuid: String,
    pub mysql_version: String,
}

/// One full backup discovered in the bucket, with enough to select it and
/// then locate its dump object and lineage's binlogs.
#[derive(Debug, Clone, PartialEq)]
pub struct FullBackupRef {
    pub server_uuid: String,
    pub dump_key: String,
    pub meta: FullBackupMeta,
}

/// The newest full backup, across every lineage, whose `taken_at` does not
/// exceed the recovery target — restore's core selection rule. Ties (same
/// instant, different lineages — vanishingly unlikely but not impossible)
/// break on `server_uuid` so the choice is deterministic.
pub fn newest_qualifying_full(
    fulls: &[FullBackupRef],
    target: DateTime<Utc>,
) -> Option<&FullBackupRef> {
    fulls
        .iter()
        .filter(|f| f.meta.taken_at <= target)
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
        }
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
}
