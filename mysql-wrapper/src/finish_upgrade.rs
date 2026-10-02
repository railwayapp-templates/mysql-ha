//! Finish a server upgrade that a previous boot was killed in the middle of.
//!
//! A mysqld whose binary is newer than the datadir upgrades it on boot in two
//! steps: the data-dictionary step, then the server step (system tables, sys
//! schema, help tables). When the container dies during the server step —
//! app sleep, an OOM kill, a redeploy — every later boot of that binary fails
//! the upgrade and aborts, forever. Two ways, both reproduced on 9.4.0 -> 9.7.2:
//!
//!   1. The killed step left a recovered, uncommitted transaction. InnoDB
//!      rolls recovered transactions back in the background only once the
//!      server step has finished, but the server step needs the rows that
//!      transaction holds: `Lock wait timeout` (1205), `Failed to upgrade
//!      server`, abort. More CPU or memory changes nothing.
//!   2. The kill landed inside an ALTER of a non-atomic (CSV) system table
//!      and left a `mysql.#sql-…` intermediate table in the data dictionary
//!      with only an `.sdi` file on disk: `Table 'mysql.#sql-4_4' requires
//!      repair`, `Failed to upgrade server`, abort.
//!
//! The pass below runs before the real mysqld: boot with `--upgrade=MINIMAL` (the dictionary step
//! runs, the server step is skipped, so the background rollback can start),
//! wait for every recovered transaction to finish rolling back, drop the
//! orphaned intermediate tables, shut down cleanly. The normal boot that
//! follows runs the whole server step and serves.
//!
//! Whether a server step is still owed cannot be read from the datadir:
//! `mysql_upgrade_history` gains the new version's entry when the dictionary
//! step runs, BEFORE the server step starts (verified on mysql:9.7.2 by
//! SIGKILLing it a few seconds after `Server upgrade from '90400' to '90702'
//! started`: the history already listed 9.7.2). MySQL's own record of the
//! completed server step (`MYSQLD_VERSION_UPGRADED` in `mysql.dd_properties`)
//! lives inside `mysql.ibd`. So the wrapper keeps its own record: the server
//! version that last reached accepting connections on this datadir, which
//! only happens after the server step has completed. A datadir whose history
//! shows it was upgraded to this binary's version, without that version on
//! record as having served, owes a server step.
//!
//! Every failure in here is logged and falls through to the normal boot: the
//! pass exists to un-wedge a datadir, never to add a way for one to refuse.

use crate::config::Config;
use crate::process_manager;
use anyhow::{anyhow, Context, Result};
use common::{Telemetry, TelemetryEvent};
use mysql_async::prelude::Queryable;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use serde::Deserialize;
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::process::Child;
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tracing::{error, info, warn};

/// Written by the wrapper, read only by this module.
pub const SERVED_VERSION_MARKER: &str = ".railway_served_server_version";
/// Written by mysqld (8.0.35+/8.1+): one entry per server version that opened
/// the datadir, appended when the data-dictionary step runs.
const UPGRADE_HISTORY_FILE: &str = "mysql_upgrade_history";
/// Its own socket, so nothing that dials the serving socket (health server,
/// pin resolver, orchestrator) can reach a half-upgraded server.
pub const PRIVATE_SOCKET: &str = "/var/run/mysqld/finish-upgrade.sock";

const POLL: Duration = Duration::from_secs(1);
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const PROGRESS_LOG_EVERY: Duration = Duration::from_secs(30);
/// A rollback that is still moving is the fastest way to a serving database —
/// the normal boot would only fail the upgrade on the same rows — so the wait
/// is bounded on lack of progress, not on total time.
const ROLLBACK_STALL_LIMIT: Duration = Duration::from_secs(600);
/// A clean shutdown flushes the buffer pool; past this the server is killed
/// and the next boot's crash recovery picks up where it stopped.
const SHUTDOWN_LIMIT: Duration = Duration::from_secs(300);
const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServerVersion {
    major: u32,
    minor: u32,
    patch: u32,
}

impl ServerVersion {
    /// The leading `X.Y.Z` of a version string: `9.7.2`, the official image's
    /// `MYSQL_VERSION=9.7.2-1.el9`, or `@@version`-style `8.4.11-log`.
    pub fn parse(raw: &str) -> Option<Self> {
        let mut parts = raw.trim().splitn(3, '.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        let rest = parts.next()?;
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        let patch = digits.parse().ok()?;
        Some(Self {
            major,
            minor,
            patch,
        })
    }

    /// `/usr/sbin/mysqld  Ver 9.7.2 for Linux on aarch64 (MySQL Community Server - GPL)`
    fn from_mysqld_version_output(out: &str) -> Option<Self> {
        let after = out.split(" Ver ").nth(1)?;
        Self::parse(after.split_whitespace().next()?)
    }
}

impl std::fmt::Display for ServerVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// The version of the mysqld this container will run. `mysqld --version` is
/// the binary itself; the official image's `MYSQL_VERSION` is the fallback.
pub async fn binary_version() -> Option<ServerVersion> {
    let probe = tokio::process::Command::new("mysqld")
        .arg("--version")
        .kill_on_drop(true)
        .output();
    if let Ok(Ok(out)) = tokio::time::timeout(VERSION_PROBE_TIMEOUT, probe).await {
        if let Some(v) =
            ServerVersion::from_mysqld_version_output(&String::from_utf8_lossy(&out.stdout))
        {
            return Some(v);
        }
    }
    std::env::var("MYSQL_VERSION")
        .ok()
        .and_then(|v| ServerVersion::parse(&v))
}

#[derive(Deserialize)]
struct HistoryFile {
    upgrade_history: Vec<HistoryEntry>,
}

#[derive(Deserialize)]
struct HistoryEntry {
    version: String,
    #[serde(default)]
    initialize: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum History {
    /// No file: a fresh datadir, or one only ever opened by a server that
    /// does not keep the history (8.0 before 8.0.35).
    Absent,
    Unreadable(String),
    /// The most recent entry — the last server version that opened the
    /// datadir, and whether it was the one that initialized it.
    Newest {
        version: ServerVersion,
        initialized_by_it: bool,
    },
}

fn parse_history(raw: &str) -> History {
    let file: HistoryFile = match serde_json::from_str(raw) {
        Ok(f) => f,
        Err(e) => return History::Unreadable(e.to_string()),
    };
    let Some(last) = file.upgrade_history.last() else {
        return History::Unreadable("upgrade_history is empty".to_string());
    };
    match ServerVersion::parse(&last.version) {
        Some(version) => History::Newest {
            version,
            initialized_by_it: last.initialize,
        },
        None => History::Unreadable(format!("unparseable version {:?}", last.version)),
    }
}

fn read_history(data_dir: &str) -> History {
    match std::fs::read_to_string(Path::new(data_dir).join(UPGRADE_HISTORY_FILE)) {
        Ok(raw) => parse_history(&raw),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => History::Absent,
        Err(e) => History::Unreadable(e.to_string()),
    }
}

fn read_served_version(data_dir: &str) -> Option<ServerVersion> {
    std::fs::read_to_string(Path::new(data_dir).join(SERVED_VERSION_MARKER))
        .ok()
        .and_then(|s| ServerVersion::parse(&s))
}

fn write_served_version(data_dir: &str, version: ServerVersion) -> Result<()> {
    let path = Path::new(data_dir).join(SERVED_VERSION_MARKER);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, format!("{version}\n"))
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("publishing {}", path.display()))
}

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Skip(&'static str),
    Pending(&'static str),
}

fn decide(binary: ServerVersion, history: &History, served: Option<ServerVersion>) -> Verdict {
    let (newest, initialized_by_it) = match history {
        History::Absent => return Verdict::Skip("the datadir records no upgrade history"),
        History::Unreadable(_) => return Verdict::Skip("the upgrade history is unreadable"),
        History::Newest {
            version,
            initialized_by_it,
        } => (*version, *initialized_by_it),
    };
    if newest < binary {
        return Verdict::Skip(
            "this server version has not opened the datadir yet; this boot runs the whole upgrade",
        );
    }
    if newest > binary {
        return Verdict::Skip("a newer server version already opened the datadir");
    }
    if initialized_by_it {
        return Verdict::Skip("this server version initialized the datadir");
    }
    match served {
        Some(s) if s >= binary => Verdict::Skip("this server version already served this datadir"),
        Some(_) => Verdict::Pending(
            "the data dictionary was upgraded to this version, but no boot of it has served yet",
        ),
        // Upgraded by an image that kept no served record: a completed server
        // step and an interrupted one look identical from here. Running the
        // pass on a completed upgrade costs one short extra boot, once (the
        // normal boot records the version); skipping it on an interrupted one
        // leaves the database down for good.
        None => Verdict::Pending(
            "the data dictionary was upgraded to this version and no wrapper has recorded it \
             serving; an interrupted server upgrade cannot be ruled out",
        ),
    }
}

/// `@0023sql@002d4_4_566.sdi` -> `#sql-4_4`: the `.sdi` file name minus the
/// extension and the trailing `_<sdi id>`, with MySQL's `@XXXX` filename
/// escapes decoded. None for anything that is not an intermediate table.
fn orphan_table_name(file_name: &str) -> Option<String> {
    let stem = file_name.strip_suffix(".sdi")?;
    let (encoded, sdi_id) = stem.rsplit_once('_')?;
    if sdi_id.is_empty() || !sdi_id.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let name = decode_filename(encoded)?;
    name.starts_with("#sql").then_some(name)
}

fn decode_filename(encoded: &str) -> Option<String> {
    let mut out = String::with_capacity(encoded.len());
    let mut chars = encoded.chars();
    while let Some(c) = chars.next() {
        if c == '@' {
            let hex: String = chars.by_ref().take(4).collect();
            if hex.len() != 4 {
                return None;
            }
            let code = u32::from_str_radix(&hex, 16).ok()?;
            out.push(char::from_u32(code)?);
        } else {
            out.push(c);
        }
    }
    Some(out)
}

fn orphan_tables(data_dir: &str) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(Path::new(data_dir).join("mysql")) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .filter_map(|e| e.ok())
        .filter_map(|e| orphan_table_name(&e.file_name().to_string_lossy()))
        .collect();
    names.sort();
    names.dedup();
    names
}

fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// What the caller does after the pass.
#[derive(Debug, PartialEq, Eq)]
pub enum PassOutcome {
    /// Start the serving mysqld, as on any boot.
    Continue,
    /// The container was asked to stop while the pass ran; the minimal server
    /// is already down.
    ShutdownRequested,
}

enum DriveError {
    Signaled,
    Failed(anyhow::Error),
}

impl From<anyhow::Error> for DriveError {
    fn from(e: anyhow::Error) -> Self {
        DriveError::Failed(e)
    }
}

struct Signals {
    term: tokio::signal::unix::Signal,
    int: tokio::signal::unix::Signal,
}

impl Signals {
    async fn recv(&mut self) {
        tokio::select! {
            _ = self.term.recv() => {}
            _ = self.int.recv() => {}
        }
    }
}

/// Run the pass when this boot owes a server step a previous boot started,
/// before the serving mysqld spawns. `args` are the passthrough mysqld args;
/// the pass appends its own after them so they win.
pub async fn run_if_pending(
    config: &Config,
    args: &[String],
    binary: Option<ServerVersion>,
    telemetry: &Telemetry,
) -> PassOutcome {
    if !config.datadir_is_initialized() {
        return PassOutcome::Continue;
    }
    let Some(binary) = binary else {
        warn!("finish-upgrade: could not determine the server binary's version; skipping the interrupted-upgrade check");
        return PassOutcome::Continue;
    };
    let history = read_history(&config.data_dir);
    if let History::Unreadable(reason) = &history {
        warn!(reason = %reason, "finish-upgrade: {UPGRADE_HISTORY_FILE} is unreadable");
    }
    let served = read_served_version(&config.data_dir);
    let reason = match decide(binary, &history, served) {
        Verdict::Skip(reason) => {
            info!(binary = %binary, served = ?served.map(|v| v.to_string()), reason, "finish-upgrade: no interrupted server upgrade");
            return PassOutcome::Continue;
        }
        Verdict::Pending(reason) => reason,
    };

    let candidates = root_candidates(config);
    if candidates.is_empty() {
        let message = "finish-upgrade: a server upgrade may be unfinished, but no root password \
                       is available to finish it with; booting normally";
        error!(binary = %binary, reason, "{message}");
        report(telemetry, message.to_string());
        return PassOutcome::Continue;
    }

    let mut signals = match (
        unix_signal(SignalKind::terminate()),
        unix_signal(SignalKind::interrupt()),
    ) {
        (Ok(term), Ok(int)) => Signals { term, int },
        (Err(e), _) | (_, Err(e)) => {
            error!(error = %e, "finish-upgrade: could not install signal handlers; booting normally");
            return PassOutcome::Continue;
        }
    };

    info!(binary = %binary, served = ?served.map(|v| v.to_string()), reason, socket = PRIVATE_SOCKET, "finish-upgrade: minimal boot");
    process_manager::clear_stale_socket_locks(PRIVATE_SOCKET);
    let mut child = match process_manager::spawn_mysqld(&minimal_boot_args(args)).await {
        Ok(child) => child,
        Err(e) => {
            error!(error = %e, "finish-upgrade: could not start the minimal boot; booting normally");
            report(
                telemetry,
                format!("could not start the minimal boot: {e:#}"),
            );
            return PassOutcome::Continue;
        }
    };

    let started = Instant::now();
    let result = drive(config, &mut child, &candidates, &mut signals).await;
    stop(&mut child).await;
    match result {
        Ok(()) => {
            info!(
                elapsed_seconds = started.elapsed().as_secs(),
                "finish-upgrade: normal boot"
            );
            PassOutcome::Continue
        }
        Err(DriveError::Signaled) => {
            info!("finish-upgrade: stop requested during the minimal boot; it was shut down");
            PassOutcome::ShutdownRequested
        }
        Err(DriveError::Failed(e)) => {
            error!(error = %format!("{e:#}"), "finish-upgrade: the pass failed; booting normally");
            report(telemetry, format!("{e:#}"));
            PassOutcome::Continue
        }
    }
}

/// Record the binary version once the serving mysqld accepts connections —
/// the server step, if one was owed, has completed by then.
pub async fn record_served_version(
    data_dir: String,
    sql: crate::sql::Sql,
    binary: Option<ServerVersion>,
) {
    let Some(binary) = binary else { return };
    loop {
        let answered = match sql.is_init_temp_server().await {
            Ok(is_init_temp_server) => !is_init_temp_server,
            Err(e) => crate::sql::is_access_denied(&e),
        };
        if answered {
            if read_served_version(&data_dir) != Some(binary) {
                match write_served_version(&data_dir, binary) {
                    Ok(()) => {
                        info!(version = %binary, "finish-upgrade: recorded the server version serving this datadir")
                    }
                    Err(e) => {
                        warn!(error = %e, "finish-upgrade: could not record the serving server version")
                    }
                }
            }
            return;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn report(telemetry: &Telemetry, error: String) {
    telemetry.send(TelemetryEvent::ComponentError {
        component: "mysql-wrapper".to_string(),
        error,
        context: "finish_interrupted_upgrade".to_string(),
    });
}

/// Same order the password-pin resolver probes in: an in-flight rotation's
/// new password, then the pin, then the environment.
fn root_candidates(config: &Config) -> Vec<String> {
    let pin = crate::password_pin::read_pin(&config.data_dir);
    let mut list = Vec::new();
    if let Some(pending) = crate::credentials::pending_password(&config.data_dir) {
        list.push(pending);
    }
    for (_, password) in
        crate::password_pin::candidates(&config.mysql_root_password, pin.as_deref())
    {
        if !list.contains(&password) {
            list.push(password);
        }
    }
    list.retain(|p| !p.is_empty());
    list
}

fn minimal_boot_args(args: &[String]) -> Vec<String> {
    let mut out = args.to_vec();
    out.extend(
        [
            // Dictionary step only: the server step is what fails until the
            // rollback has run.
            "--upgrade=MINIMAL",
            "--skip-networking",
            &format!("--socket={PRIVATE_SOCKET}"),
            "--mysqlx=OFF",
            // A group member's or replica's data must not move while it is
            // offline from its topology: no customer events, no replication
            // channel applying.
            "--event-scheduler=DISABLED",
            "--skip-replica-start",
        ]
        .map(str::to_string),
    );
    out
}

async fn connect(password: &str) -> Result<mysql_async::Conn> {
    let opts = crate::sql::root_opts(PRIVATE_SOCKET, password);
    tokio::time::timeout(QUERY_TIMEOUT, mysql_async::Conn::new(opts))
        .await
        .map_err(|_| anyhow!("connect timed out"))?
        .map_err(Into::into)
}

async fn with_timeout<T>(
    fut: impl std::future::Future<Output = std::result::Result<T, mysql_async::Error>>,
) -> Result<T> {
    tokio::time::timeout(QUERY_TIMEOUT, fut)
        .await
        .map_err(|_| anyhow!("query timed out after {QUERY_TIMEOUT:?}"))?
        .map_err(Into::into)
}

async fn child_exited(child: &mut Child) -> anyhow::Error {
    match child.wait().await {
        Ok(status) => anyhow!("the minimal boot exited on its own ({status})"),
        Err(e) => anyhow!("waiting for the minimal boot: {e}"),
    }
}

async fn drive(
    config: &Config,
    child: &mut Child,
    candidates: &[String],
    signals: &mut Signals,
) -> std::result::Result<(), DriveError> {
    // Same budget the boot-loop accounting gives a serving boot: the minimal
    // boot runs InnoDB crash recovery first, which is the slow part of both.
    let budget = Duration::from_secs(config.boot_ready_budget_seconds);
    let started = Instant::now();
    let mut conn = loop {
        tokio::select! {
            e = child_exited(child) => return Err(e.into()),
            _ = signals.recv() => return Err(DriveError::Signaled),
            _ = tokio::time::sleep(POLL) => {}
        }
        let mut denied = 0;
        let mut connected = None;
        for password in candidates {
            match connect(password).await {
                Ok(conn) => {
                    connected = Some(conn);
                    break;
                }
                Err(e) if crate::sql::is_access_denied(&e) => denied += 1,
                Err(_) => {}
            }
        }
        if let Some(conn) = connected {
            break conn;
        }
        if denied == candidates.len() {
            return Err(
                anyhow!("the minimal boot refused every root password the wrapper holds").into(),
            );
        }
        if started.elapsed() >= budget {
            return Err(
                anyhow!("the minimal boot did not accept connections within {budget:?}").into(),
            );
        }
    };
    info!(
        elapsed_seconds = started.elapsed().as_secs(),
        "finish-upgrade: minimal boot accepting connections"
    );

    // Recovered transactions roll back in the background now that the server
    // step is out of the way.
    let mut initial: Option<(u64, u64)> = None;
    let mut last: Option<(u64, u64)> = None;
    let mut last_progress = Instant::now();
    let mut last_log = Instant::now();
    loop {
        match with_timeout(conn.query_first::<(u64, u64), _>(
            "SELECT COUNT(*), CAST(COALESCE(SUM(trx_rows_modified), 0) AS UNSIGNED) \
             FROM information_schema.innodb_trx",
        ))
        .await
        {
            Ok(Some((0, _))) => {
                let (transactions, rows) = initial.unwrap_or((0, 0));
                info!(
                    transactions,
                    rows, "finish-upgrade: rolled back {transactions} recovered transaction(s)"
                );
                break;
            }
            Ok(Some(state)) => {
                if initial.is_none() {
                    initial = Some(state);
                    info!(
                        transactions = state.0,
                        rows = state.1,
                        "finish-upgrade: waiting for the background rollback of recovered transactions"
                    );
                }
                if last != Some(state) {
                    last = Some(state);
                    last_progress = Instant::now();
                }
                if last_log.elapsed() >= PROGRESS_LOG_EVERY {
                    last_log = Instant::now();
                    info!(
                        transactions = state.0,
                        rows = state.1,
                        "finish-upgrade: rollback still running"
                    );
                }
            }
            Ok(None) => {}
            Err(e) => {
                warn!(error = %e, "finish-upgrade: could not read information_schema.innodb_trx");
                if let Some(password) = candidates.first() {
                    if let Ok(fresh) = connect(password).await {
                        conn = fresh;
                    }
                }
            }
        }
        if last_progress.elapsed() >= ROLLBACK_STALL_LIMIT {
            return Err(anyhow!(
                "the rollback of recovered transactions made no progress for {ROLLBACK_STALL_LIMIT:?} (last: {last:?})"
            )
            .into());
        }
        tokio::select! {
            e = child_exited(child) => return Err(e.into()),
            _ = signals.recv() => return Err(DriveError::Signaled),
            _ = tokio::time::sleep(POLL) => {}
        }
    }

    let orphans = orphan_tables(&config.data_dir);
    if !orphans.is_empty() {
        // On a group member a binlogged DROP would be a transaction the rest
        // of the group never had.
        with_timeout(conn.query_drop("SET SESSION sql_log_bin = 0"))
            .await
            .context("disabling the binary log for the orphan cleanup")?;
        let mut failed = Vec::new();
        for name in &orphans {
            let statement = format!("DROP TABLE IF EXISTS mysql.{}", quote_identifier(name));
            match with_timeout(conn.query_drop(statement)).await {
                Ok(()) => info!(table = %name, "finish-upgrade: dropped orphan mysql.{name}"),
                Err(e) => {
                    error!(table = %name, error = %e, "finish-upgrade: could not drop orphan mysql.{name}");
                    failed.push(name.clone());
                }
            }
        }
        if !failed.is_empty() {
            return Err(anyhow!("could not drop orphan intermediate table(s) {failed:?}").into());
        }
    }
    let _ = conn.disconnect().await;
    Ok(())
}

/// SIGTERM is mysqld's clean shutdown. The child is the entrypoint, which
/// execs into mysqld, so its pid is mysqld's.
async fn stop(child: &mut Child) {
    if let Ok(Some(_)) = child.try_wait() {
        return;
    }
    if let Some(pid) = child.id() {
        let _ = signal::kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
    }
    match tokio::time::timeout(SHUTDOWN_LIMIT, child.wait()).await {
        Ok(status) => info!(status = ?status.ok(), "finish-upgrade: minimal boot shut down"),
        Err(_) => {
            warn!(limit = ?SHUTDOWN_LIMIT, "finish-upgrade: minimal boot did not shut down in time; killing it");
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> ServerVersion {
        ServerVersion::parse(s).unwrap()
    }

    // Verbatim from the official images (2026-10-01).
    const HISTORY_940_INIT: &str = r#"{"file_format":"1","upgrade_history":[{"date":"2026-10-01 15:52:24","version":"9.4.0","maturity":"INNOVATION","initialize":true}]}"#;
    const HISTORY_84_INIT: &str = r#"{"file_format":"1","upgrade_history":[{"date":"2026-10-01 15:52:33","version":"8.4.11","maturity":"LTS","initialize":true}]}"#;
    // mysql:9.7.2 on the 9.4.0 datadir, SIGKILLed 3s after `Server upgrade
    // from '90400' to '90702' started.` — the server step never completed,
    // and the 9.7.2 entry is already there.
    const HISTORY_940_TO_972_KILLED: &str = r#"{"file_format":"1","upgrade_history":[{"date":"2026-10-01 15:52:24","version":"9.4.0","maturity":"INNOVATION","initialize":true},{"date":"2026-10-01 16:08:27","version":"9.7.2","maturity":"LTS"}]}"#;

    #[test]
    fn versions_parse_from_every_place_they_are_read() {
        assert_eq!(v("9.7.2"), v("9.7.2-1.el9"));
        assert_eq!(v("8.4.11-log").to_string(), "8.4.11");
        assert_eq!(
            ServerVersion::from_mysqld_version_output(
                "/usr/sbin/mysqld  Ver 9.7.2 for Linux on aarch64 (MySQL Community Server - GPL)\n"
            ),
            Some(v("9.7.2"))
        );
        assert!(ServerVersion::parse("9.7").is_none());
        assert!(ServerVersion::parse("").is_none());
        assert!(v("9.4.0") < v("9.7.2"));
        assert!(v("8.4.11") < v("9.4.0"));
        assert!(v("8.4.9") < v("8.4.11"));
    }

    #[test]
    fn history_parses_the_official_file_format() {
        assert_eq!(
            parse_history(HISTORY_940_INIT),
            History::Newest {
                version: v("9.4.0"),
                initialized_by_it: true
            }
        );
        assert_eq!(
            parse_history(HISTORY_940_TO_972_KILLED),
            History::Newest {
                version: v("9.7.2"),
                initialized_by_it: false
            }
        );
        assert!(matches!(parse_history("{"), History::Unreadable(_)));
        assert!(matches!(
            parse_history(r#"{"file_format":"1","upgrade_history":[]}"#),
            History::Unreadable(_)
        ));
    }

    #[test]
    fn a_killed_server_upgrade_is_pending() {
        let history = parse_history(HISTORY_940_TO_972_KILLED);
        assert!(matches!(
            decide(v("9.7.2"), &history, Some(v("9.4.0"))),
            Verdict::Pending(_)
        ));
        // Upgraded under an image that kept no served record.
        assert!(matches!(
            decide(v("9.7.2"), &history, None),
            Verdict::Pending(_)
        ));
    }

    #[test]
    fn healthy_restarts_do_not_run_the_pass() {
        let history = parse_history(HISTORY_940_TO_972_KILLED);
        assert!(matches!(
            decide(v("9.7.2"), &history, Some(v("9.7.2"))),
            Verdict::Skip(_)
        ));
        // Initialized on this version: there was never an upgrade.
        assert!(matches!(
            decide(v("8.4.11"), &parse_history(HISTORY_84_INIT), None),
            Verdict::Skip(_)
        ));
        // First boot of a newer binary: the history has not moved yet, and
        // this boot runs the full upgrade on its own.
        assert!(matches!(
            decide(
                v("9.7.2"),
                &parse_history(HISTORY_940_INIT),
                Some(v("9.4.0"))
            ),
            Verdict::Skip(_)
        ));
        // Older binary than the datadir: mysqld's own refusal, not ours.
        assert!(matches!(
            decide(v("9.4.0"), &history, None),
            Verdict::Skip(_)
        ));
        assert!(matches!(
            decide(v("9.7.2"), &History::Absent, None),
            Verdict::Skip(_)
        ));
        assert!(matches!(
            decide(v("9.7.2"), &History::Unreadable("x".into()), None),
            Verdict::Skip(_)
        ));
    }

    #[test]
    fn orphan_names_decode_like_the_prod_sdi_file() {
        assert_eq!(
            orphan_table_name("@0023sql@002d4_4_566.sdi").as_deref(),
            Some("#sql-4_4")
        );
        assert_eq!(
            orphan_table_name("@0023sql@002dib1234@002d567_89.sdi").as_deref(),
            Some("#sql-ib1234-567")
        );
        // Real system tables and other files are never candidates.
        assert_eq!(orphan_table_name("general_log_213.sdi"), None);
        assert_eq!(orphan_table_name("slow_log.CSV"), None);
        assert_eq!(orphan_table_name("@0023sql@002d4_4.CSV"), None);
        assert_eq!(orphan_table_name("@0023sql@002d4_4_abc.sdi"), None);
        assert_eq!(orphan_table_name("@00.sdi"), None);
        assert_eq!(quote_identifier("#sql-4_4"), "`#sql-4_4`");
        assert_eq!(quote_identifier("a`b"), "`a``b`");
    }

    fn temp_datadir(tag: &str) -> tempfile::TempDir {
        let dir = tempfile::Builder::new()
            .prefix(&format!("finish-upgrade-{tag}"))
            .tempdir()
            .unwrap();
        std::fs::create_dir(dir.path().join("mysql")).unwrap();
        dir
    }

    #[test]
    fn orphans_are_found_in_the_mysql_schema_directory() {
        let dir = temp_datadir("orphans");
        for f in [
            "@0023sql@002d4_4_566.sdi",
            "general_log_213.sdi",
            "general_log.CSV",
        ] {
            std::fs::write(dir.path().join("mysql").join(f), "").unwrap();
        }
        assert_eq!(
            orphan_tables(dir.path().to_str().unwrap()),
            vec!["#sql-4_4".to_string()]
        );
    }

    #[test]
    fn served_version_round_trips_and_missing_reads_as_none() {
        let dir = temp_datadir("served");
        let d = dir.path().to_str().unwrap();
        assert_eq!(read_served_version(d), None);
        write_served_version(d, v("9.7.2")).unwrap();
        assert_eq!(read_served_version(d), Some(v("9.7.2")));
        assert_eq!(read_history(d), History::Absent);
        std::fs::write(
            dir.path().join(UPGRADE_HISTORY_FILE),
            HISTORY_940_TO_972_KILLED,
        )
        .unwrap();
        assert!(matches!(
            decide(v("9.7.2"), &read_history(d), read_served_version(d)),
            Verdict::Skip(_)
        ));
    }

    #[test]
    fn minimal_boot_args_come_after_the_passthrough_args() {
        let args = minimal_boot_args(&["--upgrade=AUTO".to_string()]);
        assert_eq!(args[0], "--upgrade=AUTO");
        assert!(args.contains(&"--upgrade=MINIMAL".to_string()));
        assert!(args.contains(&"--skip-networking".to_string()));
        assert!(args.contains(&format!("--socket={PRIVATE_SOCKET}")));
        let minimal = args.iter().position(|a| a == "--upgrade=MINIMAL").unwrap();
        assert!(minimal > 0);
    }

    fn config_with(data_dir: &str, root_password: &str) -> Config {
        let mut config = crate::gr::tests::test_config();
        config.data_dir = data_dir.to_string();
        config.mysql_root_password = root_password.to_string();
        config
    }

    #[test]
    fn no_credential_means_no_candidates() {
        let dir = temp_datadir("creds");
        let d = dir.path().to_str().unwrap();
        assert!(root_candidates(&config_with(d, "")).is_empty());
        assert_eq!(
            root_candidates(&config_with(d, "pw")),
            vec!["pw".to_string()]
        );
        crate::password_pin::write_pin(d, "pinned").unwrap();
        assert_eq!(
            root_candidates(&config_with(d, "pw")),
            vec!["pinned".to_string(), "pw".to_string()]
        );
    }

    #[test]
    fn fall_through_paths_never_block_the_boot() {
        // Telemetry owns a blocking HTTP client, which may not be dropped
        // inside a runtime: built (and dropped) outside it.
        let telemetry = Telemetry::from_env("mysql-ha-test");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let run = |config: Config, binary: Option<ServerVersion>| {
            rt.block_on(async { run_if_pending(&config, &[], binary, &telemetry).await })
        };
        // Fresh datadir.
        let fresh = tempfile::tempdir().unwrap();
        assert_eq!(
            run(
                config_with(fresh.path().to_str().unwrap(), "pw"),
                Some(v("9.7.2"))
            ),
            PassOutcome::Continue
        );
        // Pending upgrade, but no root credential: logs and boots normally
        // without ever starting a server.
        let dir = temp_datadir("nocreds");
        std::fs::write(
            dir.path().join(UPGRADE_HISTORY_FILE),
            HISTORY_940_TO_972_KILLED,
        )
        .unwrap();
        let d = dir.path().to_str().unwrap();
        assert_eq!(
            run(config_with(d, ""), Some(v("9.7.2"))),
            PassOutcome::Continue
        );
        // Unknown binary version.
        assert_eq!(run(config_with(d, "pw"), None), PassOutcome::Continue);
    }
}
