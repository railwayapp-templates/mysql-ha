//! Root-password pin — a MYSQL_ROOT_PASSWORD variable edit must never take
//! the cluster down.
//!
//! mysqld only applies MYSQL_ROOT_PASSWORD when the datadir is initialized;
//! on every later boot the password enforced is whatever the datadir carries,
//! and docker-entrypoint ignores the variable. Without a pin, an operator
//! editing the variable and redeploying locks the wrapper out of its own
//! mysqld on EVERY node at once (the same edit lands cluster-wide): /health
//! and /role both fail, HAProxy drops the entire backend set, and the outage
//! is total while mysqld itself is perfectly healthy.
//!
//! The contract (mirrors redis-ha's password pin, and standalone MySQL's own
//! image behavior): the ACTIVE password is the one that authenticates, the
//! variable is aspirational. On boot the resolver probes the candidates —
//! the pin persisted on the volume from the previous boot, then the
//! environment — and whichever one mysqld accepts becomes the pool credential
//! and the new pin. A drifted variable is warned about (log + telemetry),
//! never obeyed.
//!
//! Known edge, deliberately out of scope: CLONE INSTANCE replaces the datadir
//! and wipes the pin file, so a node provisioned by clone WHILE the variable
//! is drifted boots with only the (wrong) env candidate and parks in the
//! all-denied loop below, loudly. Restoring the variable to the active
//! password and redeploying recovers it. Solving it fully would mean
//! replicating the active password inside the dataset itself, which is a
//! bigger decision than this fix.

use crate::config::Config;
use crate::sql::{probe_root_password, RootPasswordProbe, Sql};
use anyhow::{Context, Result};
use common::{Telemetry, TelemetryEvent};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

const PIN_FILE: &str = ".railway_active_root_password";

/// How many consecutive all-candidates-denied rounds before alerting: a fresh
/// datadir legitimately denies the env password while docker-entrypoint's
/// init phase is still running its setup SQL, so denial only becomes an
/// incident once it has clearly outlived any init (rounds are ~1s apart).
const DENIED_ROUNDS_BEFORE_ALERT: u32 = 120;

pub fn pin_path(data_dir: &str) -> PathBuf {
    PathBuf::from(data_dir).join(PIN_FILE)
}

/// The pinned active password from the previous boot, if any. Unreadable or
/// empty pins are treated as absent — the resolver then has only the env
/// candidate, which is exactly the pre-pin behavior.
pub fn read_pin(data_dir: &str) -> Option<String> {
    let content = std::fs::read_to_string(pin_path(data_dir)).ok()?;
    let trimmed = content.trim_end_matches(['\r', '\n']);
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Persist the proven-active password, 0600, via temp-file + rename so a
/// crash mid-write can't leave a truncated pin. Only ever called after mysqld
/// has answered a query — the datadir is guaranteed non-empty by then, so
/// this never trips `mysqld --initialize`'s empty-datadir requirement.
pub fn write_pin(data_dir: &str, password: &str) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let path = pin_path(data_dir);
    let tmp = PathBuf::from(data_dir).join(format!("{PIN_FILE}.tmp"));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .with_context(|| format!("opening {}", tmp.display()))?;
    file.write_all(password.as_bytes())
        .and_then(|()| file.write_all(b"\n"))
        .and_then(|()| file.sync_all())
        .with_context(|| format!("writing {}", tmp.display()))?;
    drop(file);
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("renaming {} into place", path.display()))?;
    Ok(())
}

/// The password the boot-time pool should be built with, before mysqld is up
/// to arbitrate: the pin when it exists and disagrees with the environment
/// (the drift case this module exists for — starting on the pin means zero
/// failed authentications on the happy path), the environment otherwise.
pub fn initial_password(env_password: &str, pin: Option<&str>) -> String {
    match pin {
        Some(p) if p != env_password => p.to_string(),
        _ => env_password.to_string(),
    }
}

/// Candidate passwords in probe order. At most two, pin first — see
/// `initial_password` for why.
fn candidates(env_password: &str, pin: Option<&str>) -> Vec<(&'static str, String)> {
    let mut list = Vec::new();
    if let Some(p) = pin {
        if p != env_password {
            list.push(("pin", p.to_string()));
        }
    }
    list.push(("env", env_password.to_string()));
    list
}

/// Wait for mysqld, prove which candidate password it enforces, swap the
/// shared pool onto it, and persist it as the new pin. Runs once per boot,
/// in both HA and standalone mode. Never gives up: while every candidate is
/// denied it keeps probing (docker-entrypoint init, or an operator fixing
/// the variable live, both resolve without a restart).
pub async fn resolve_and_apply(config: Arc<Config>, sql: Sql, telemetry: Arc<Telemetry>) {
    let env_password = config.mysql_root_password.clone();
    let boot_pin = read_pin(&config.data_dir);
    let boot_password = initial_password(&env_password, boot_pin.as_deref());

    let mut denied_rounds = 0u32;
    let mut alerted = false;

    loop {
        // Re-read each round: the file is tiny, and a concurrent fix (or a
        // clone finishing) should be picked up without a restart.
        let pin = read_pin(&config.data_dir);
        let mut all_denied = true;

        for (source, password) in candidates(&env_password, pin.as_deref()) {
            match probe_root_password(&config.socket_path, &password).await {
                RootPasswordProbe::Works => {
                    finalize(
                        &config,
                        &sql,
                        &telemetry,
                        source,
                        &password,
                        &env_password,
                        &boot_password,
                        pin.as_deref(),
                    )
                    .await;
                    return;
                }
                RootPasswordProbe::AccessDenied => {}
                RootPasswordProbe::NotReady(reason) => {
                    all_denied = false;
                    if denied_rounds % 30 == 0 {
                        info!(reason, "password resolver waiting for mysqld");
                    }
                    break;
                }
            }
        }

        if all_denied {
            denied_rounds += 1;
            if denied_rounds >= DENIED_ROUNDS_BEFORE_ALERT && !alerted {
                alerted = true;
                error!(
                    "mysqld denies every known root password (pin and environment); \
                     the wrapper cannot manage this node until the variable is \
                     restored to the active password"
                );
                telemetry.send(TelemetryEvent::ComponentError {
                    component: "mysql-wrapper".to_string(),
                    error: "all root password candidates denied".to_string(),
                    context: "root-password-pin".to_string(),
                });
            }
        } else {
            denied_rounds = 0;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn finalize(
    config: &Config,
    sql: &Sql,
    telemetry: &Telemetry,
    source: &str,
    active: &str,
    env_password: &str,
    boot_password: &str,
    pin: Option<&str>,
) {
    if active != env_password {
        // The drift this module exists for: keep the cluster on the active
        // password and say, loudly, that the variable is lying.
        warn!(
            "MYSQL_ROOT_PASSWORD differs from the active root password; keeping the \
             pinned active password — variable edits do not rotate the live credential"
        );
        telemetry.send(TelemetryEvent::ComponentError {
            component: "mysql-wrapper".to_string(),
            error: "MYSQL_ROOT_PASSWORD drifted from the active root password; pinned".to_string(),
            context: "root-password-pin".to_string(),
        });
    } else if source == "env" && pin.is_some_and(|p| p != env_password) {
        // The database itself was rotated (out-of-band ALTER USER) and the
        // variable already matches — converge the stale pin quietly.
        info!("active root password rotated out of band; refreshing the pin");
    }

    if pin != Some(active) {
        if let Err(e) = write_pin(&config.data_dir, active) {
            warn!(error = %e, "could not persist the root password pin");
            telemetry.send(TelemetryEvent::ComponentError {
                component: "mysql-wrapper".to_string(),
                error: e.to_string(),
                context: "root-password-pin-write".to_string(),
            });
        }
    }

    if active != boot_password {
        sql.swap_root_password(active).await;
    }
    info!(source, "root password resolved");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "mysql-wrapper-pin-test-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    #[test]
    fn pin_roundtrip_and_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp_dir("roundtrip");
        assert_eq!(read_pin(&dir), None);
        write_pin(&dir, "s3cret").unwrap();
        assert_eq!(read_pin(&dir), Some("s3cret".to_string()));
        let mode = std::fs::metadata(pin_path(&dir))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        // Overwrite converges.
        write_pin(&dir, "rotated").unwrap();
        assert_eq!(read_pin(&dir), Some("rotated".to_string()));
    }

    #[test]
    fn empty_or_missing_pin_reads_as_absent() {
        let dir = temp_dir("empty");
        std::fs::write(pin_path(&dir), "\n").unwrap();
        assert_eq!(read_pin(&dir), None);
    }

    #[test]
    fn trailing_newline_is_not_part_of_the_password() {
        let dir = temp_dir("newline");
        write_pin(&dir, "pw-with-no-newline").unwrap();
        let raw = std::fs::read_to_string(pin_path(&dir)).unwrap();
        assert!(raw.ends_with('\n'));
        assert_eq!(read_pin(&dir).unwrap(), "pw-with-no-newline");
    }

    #[test]
    fn initial_password_prefers_a_disagreeing_pin() {
        assert_eq!(initial_password("env-pw", None), "env-pw");
        assert_eq!(initial_password("env-pw", Some("env-pw")), "env-pw");
        // The drift case: the pin is what the datadir actually enforces.
        assert_eq!(initial_password("edited-pw", Some("real-pw")), "real-pw");
    }

    #[test]
    fn candidate_order_probes_pin_before_env_only_on_drift() {
        let c = candidates("env-pw", None);
        assert_eq!(c, vec![("env", "env-pw".to_string())]);

        let c = candidates("env-pw", Some("env-pw"));
        assert_eq!(c, vec![("env", "env-pw".to_string())]);

        let c = candidates("edited-pw", Some("real-pw"));
        assert_eq!(
            c,
            vec![
                ("pin", "real-pw".to_string()),
                ("env", "edited-pw".to_string()),
            ]
        );
    }
}
