//! A member that was not running while the group rotated its password.
//!
//! The control plane rotates credentials on the members that are running.
//! A member whose container was down takes part in no step: its datadir, its
//! pin and its local `gr_recovery` still carry the previous password, and it
//! comes back with MYSQL_ROOT_PASSWORD (and the GR_REPLICATION_PASSWORD that
//! references it) already on the new one. The pin keeps it on the previous
//! password, which is right for an edited variable and wrong here: every
//! donor refuses the previous password since the rotation's finalize, so the
//! member could never rejoin.
//!
//! The group says which case it is. When a live member of the group accepts
//! the variable's password for root and refuses the pinned one, the group
//! rotated while this member was away: the member presents the variable's
//! password to the group (its local recovery account and recovery channel),
//! joins, and receives the rotation's account changes through distributed
//! recovery like any other transaction. Once its own root carries the new
//! password, the pin and the pool follow. An edited variable is refused by
//! the group and changes nothing.

use crate::config::Config;
use crate::gr::{recovery_credential, RECOVERY_USER};
use crate::password_pin;
use crate::sql::{peer_accepts_root, probe_root_password, RootPasswordProbe, Sql};
use common::{Telemetry, TelemetryEvent};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// The variable's password, once the group proved it is the one it runs with.
static ADOPTED: Mutex<Option<String>> = Mutex::new(None);
/// When the group last refused the variable's password: an edited variable
/// stays edited, so the group is not asked again on every join pass.
static REFUSED_AT: Mutex<Option<Instant>> = Mutex::new(None);
const ASK_AGAIN_AFTER_REFUSAL: Duration = Duration::from_secs(300);
const FOLLOW_POLL: Duration = Duration::from_secs(5);

/// The password this member presents to the group when the group proved it
/// rotated while the member was away; `None` otherwise.
pub fn adopted() -> Option<String> {
    ADOPTED.lock().unwrap().clone()
}

/// What the live members said about the variable's and the pin's passwords.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupAnswer {
    /// A member accepts the variable's password and refuses the pinned one.
    Rotated,
    /// A member refuses the variable's password: it was edited.
    Refused,
    /// Nothing conclusive: no member answered, or one accepts both (a
    /// rotation in flight retains the previous password until its finalize).
    Undecided,
}

/// One live member's answer, from its root logins with the variable's
/// password and the pinned one (`None` when the login could not be tried).
fn member_answer(variable: Option<bool>, pinned: Option<bool>) -> GroupAnswer {
    match (variable, pinned) {
        (Some(true), Some(false)) => GroupAnswer::Rotated,
        (Some(false), _) => GroupAnswer::Refused,
        _ => GroupAnswer::Undecided,
    }
}

/// Asked once per join pass, before anything authenticates to the group.
/// `live_peers` are the hosts that advertise a live group. Acts at most once
/// per boot; a no-op when the variable matches the pin.
pub async fn adopt_if_the_group_rotated(
    config: &Arc<Config>,
    sql: &Sql,
    telemetry: &Arc<Telemetry>,
    live_peers: &[String],
) {
    let variable = config.mysql_root_password.clone();
    let Some(pinned) = password_pin::read_pin(&config.data_dir) else {
        return;
    };
    if pinned == variable || adopted().is_some() || live_peers.is_empty() {
        return;
    }
    if REFUSED_AT
        .lock()
        .unwrap()
        .is_some_and(|at| at.elapsed() < ASK_AGAIN_AFTER_REFUSAL)
    {
        return;
    }
    let mut answer = GroupAnswer::Undecided;
    for host in live_peers {
        let accepts_variable = peer_accepts_root(host, config.mysql_port, &variable).await;
        let accepts_pinned = peer_accepts_root(host, config.mysql_port, &pinned).await;
        answer = member_answer(accepts_variable, accepts_pinned);
        if answer != GroupAnswer::Undecided {
            break;
        }
    }
    match answer {
        GroupAnswer::Undecided => return,
        GroupAnswer::Refused => {
            *REFUSED_AT.lock().unwrap() = Some(Instant::now());
            return;
        }
        GroupAnswer::Rotated => {}
    }
    let env_recovery = config
        .gr_replication_password
        .clone()
        .unwrap_or_else(|| variable.clone());
    let credential = recovery_credential(&env_recovery, &variable, &variable);
    let applied = async {
        sql.set_recovery_password_outside_group(RECOVERY_USER, &credential)
            .await?;
        sql.configure_recovery_channel(RECOVERY_USER, &credential)
            .await
    }
    .await;
    if let Err(e) = applied {
        warn!(error = %e, "the group rotated its password while this member was away, and the recovery credential could not be moved yet; retrying");
        telemetry.send(TelemetryEvent::ComponentError {
            component: "mysql-wrapper".to_string(),
            error: e.to_string(),
            context: "rotated-password-adoption".to_string(),
        });
        return;
    }
    *ADOPTED.lock().unwrap() = Some(variable.clone());
    info!(
        "the group accepts the password in MYSQL_ROOT_PASSWORD and refuses the pinned one: it \
         rotated while this member was away. Joining with the new password; the rotation's \
         account changes arrive through distributed recovery"
    );
    tokio::spawn(follow_replicated_password(
        config.clone(),
        sql.clone(),
        variable,
    ));
}

/// Wait for the rotation's account changes to replay on this member, then
/// move the pin and the pool to the password the group runs with.
async fn follow_replicated_password(config: Arc<Config>, sql: Sql, password: String) {
    loop {
        tokio::time::sleep(FOLLOW_POLL).await;
        if password_pin::read_pin(&config.data_dir).as_deref() == Some(password.as_str()) {
            return;
        }
        if !matches!(
            probe_root_password(&config.socket_path, &password).await,
            RootPasswordProbe::Works
        ) {
            continue;
        }
        if let Err(e) = password_pin::write_pin(&config.data_dir, &password) {
            warn!(error = %e, "could not persist the root password pin; retrying");
            continue;
        }
        sql.swap_root_password(&password).await;
        info!("this member's root now carries the group's password; pin and pool follow it");
        return;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_member_that_refuses_the_pin_and_takes_the_variable_proves_a_rotation() {
        assert_eq!(member_answer(Some(true), Some(false)), GroupAnswer::Rotated);
        // The variable was edited: the group runs on the pin.
        assert_eq!(member_answer(Some(false), Some(true)), GroupAnswer::Refused);
        assert_eq!(member_answer(Some(false), None), GroupAnswer::Refused);
        // A rotation in flight: the previous password is retained until the
        // finalize, so both log in and nothing is decided yet.
        assert_eq!(
            member_answer(Some(true), Some(true)),
            GroupAnswer::Undecided
        );
        // The member could not be asked.
        assert_eq!(member_answer(None, None), GroupAnswer::Undecided);
        assert_eq!(member_answer(Some(true), None), GroupAnswer::Undecided);
    }
}
