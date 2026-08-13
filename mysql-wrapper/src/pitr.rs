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
        .with_context(|| format!("MYSQL_RECOVERY_TARGET_TIME {s:?} is not a valid ISO-8601 timestamp"))
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
) -> Option<String> {
    let boundary = files_oldest_first
        .iter()
        .find(|f| f.as_str() == active || !uploaded.contains(f.as_str()))?;
    if files_oldest_first.first() == Some(boundary) {
        return None;
    }
    Some(boundary.clone())
}

/// Given the lineage's binlog files on disk (any order) and the coordinate
/// where replay must start, the ordered, gap-free run of files to replay:
/// the file the full backup's own coordinate names, then every following
/// file with no missing sequence number, stopping (and logging, at the
/// caller) at the first gap.
pub fn binlogs_to_replay(mut files: Vec<String>, start_file: &str) -> Vec<String> {
    files.sort_by(|a, b| binlog_name_cmp(a, b));
    let Some(start_idx) = files.iter().position(|f| f == start_file) else {
        return Vec::new();
    };
    let mut run = Vec::new();
    let mut prev_seq = None;
    for name in &files[start_idx..] {
        if let (Some(prev), Some(cur)) = (prev_seq, binlog_seq(name)) {
            if cur != prev + 1 {
                break;
            }
        }
        run.push(name.clone());
        prev_seq = binlog_seq(name);
    }
    run
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
        assert_eq!(picked.meta.taken_at, parse_target_time("2026-08-13T13:00:00.000Z").unwrap());
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
        let fulls = vec![full("z", "2026-08-13T10:00:00.000Z"), full("a", "2026-08-13T10:00:00.000Z")];
        let target = parse_target_time("2026-08-13T10:00:00.000Z").unwrap();
        // Deterministic: same instant, "z" wins the lexicographic tie-break —
        // pinned here so the rule can't silently flip between runs.
        assert_eq!(newest_qualifying_full(&fulls, target).unwrap().server_uuid, "z");
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
        assert_eq!(parse_change_master_coords("-- just a regular header\n"), None);
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
        let disk = files(&["binlog.000001", "binlog.000002", "binlog.000003", "binlog.000004"]);
        // Everything but the active file is uploaded: cut right at active.
        let uploaded = set(&["binlog.000001", "binlog.000002", "binlog.000003"]);
        assert_eq!(
            purge_cut(&disk, "binlog.000004", &uploaded),
            Some("binlog.000004".to_string())
        );
    }

    #[test]
    fn purge_cut_never_crosses_an_unuploaded_gap() {
        let disk = files(&["binlog.000001", "binlog.000002", "binlog.000003", "binlog.000004"]);
        // 000002 failed to upload (backoff in progress): nothing at or after
        // it may be reclaimed, even though 000001 (older) is safe.
        let uploaded = set(&["binlog.000001", "binlog.000003"]);
        assert_eq!(
            purge_cut(&disk, "binlog.000004", &uploaded),
            Some("binlog.000002".to_string())
        );
    }

    #[test]
    fn purge_cut_none_when_the_oldest_file_is_already_the_boundary() {
        let disk = files(&["binlog.000001", "binlog.000002"]);
        // The very first file on disk is itself unuploaded/active: nothing
        // precedes it, so there is nothing to purge yet.
        assert_eq!(purge_cut(&disk, "binlog.000001", &set(&[])), None);
        let uploaded = set(&[]);
        assert_eq!(purge_cut(&disk, "binlog.000002", &uploaded), None);
    }

    #[test]
    fn purge_cut_empty_disk_list_is_none() {
        assert_eq!(purge_cut(&[], "binlog.000001", &set(&[])), None);
    }

    #[test]
    fn binlogs_to_replay_runs_from_the_start_file_until_a_gap() {
        let disk = files(&["binlog.000001", "binlog.000002", "binlog.000003", "binlog.000005"]);
        assert_eq!(
            binlogs_to_replay(disk.clone(), "binlog.000002"),
            vec!["binlog.000002", "binlog.000003"]
        );
        // Starting file missing entirely -> nothing to replay.
        assert_eq!(binlogs_to_replay(disk, "binlog.000099"), Vec::<String>::new());
    }

    #[test]
    fn binlogs_to_replay_handles_no_gap_at_all() {
        let disk = files(&["binlog.000001", "binlog.000002", "binlog.000003"]);
        assert_eq!(
            binlogs_to_replay(disk, "binlog.000001"),
            vec!["binlog.000001", "binlog.000002", "binlog.000003"]
        );
    }

    #[test]
    fn binlogs_to_replay_tolerates_unordered_input() {
        let disk = files(&["binlog.000003", "binlog.000001", "binlog.000002"]);
        assert_eq!(
            binlogs_to_replay(disk, "binlog.000001"),
            vec!["binlog.000001", "binlog.000002", "binlog.000003"]
        );
    }
}
