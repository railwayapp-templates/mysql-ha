//! The platform orders these idempotent phases across the cluster.
use crate::{health_server::AppState, password_pin, sql::RootPasswordProbe};
use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    Json,
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

static ROTATION: Mutex<()> = Mutex::const_new(());
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Rotation {
    operation: Operation,
    new_password: String,
    current_password: String,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Preflight,
    Prepare,
    Database,
    Member,
    Verify,
}

pub async fn rotate(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<Rotation>,
) -> (StatusCode, Json<Value>) {
    let _guard = ROTATION.lock().await;
    let Some(active) = password_pin::read_pin(&state.data_dir) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"credential pin not ready"})),
        );
    };
    let expected = format!("railway:{active}");
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("basic"))
        .and_then(|(_, token)| base64::engine::general_purpose::STANDARD.decode(token).ok());
    if !supplied.is_some_and(|s| bool::from(s.as_slice().ct_eq(expected.as_bytes()))) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"unauthorized"})),
        );
    }
    if request.new_password.is_empty() || request.new_password.len() > 1024 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid password"})),
        );
    }
    match tokio::time::timeout(std::time::Duration::from_secs(35), apply(&state, request)).await {
        Ok(Ok(value)) => (StatusCode::OK, Json(value)),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"credential rotation could not be verified"})),
        ),
    }
}
async fn apply(state: &AppState, request: Rotation) -> anyhow::Result<Value> {
    match request.operation {
        Operation::Preflight => {
            anyhow::ensure!(
                matches!(
                    state.sql.probe_password(&request.new_password).await,
                    RootPasswordProbe::Works
                ),
                "current password refused"
            );
            let members = state.sql.group_members().await?;
            anyhow::ensure!(
                members.len() >= 3 && members.iter().all(|m| m.state == "ONLINE"),
                "group not healthy"
            );
        }
        Operation::Prepare => {
            save_pending(&state.data_dir, &request)?;
            return Ok(json!({"version": 1, "leader": false}));
        }
        Operation::Database => {
            if !matches!(
                state.sql.probe_password(&request.new_password).await,
                RootPasswordProbe::Works
            ) {
                anyhow::ensure!(
                    matches!(
                        state.sql.probe_password(&request.current_password).await,
                        RootPasswordProbe::Works
                    ),
                    "current password refused"
                );
                state
                    .sql
                    .swap_root_password(&request.current_password)
                    .await;
                anyhow::ensure!(!state.sql.super_read_only().await?, "not primary");
                state
                    .sql
                    .rotate_cluster_accounts(&request.new_password)
                    .await?;
            }
            state.sql.swap_root_password(&request.new_password).await;
        }
        Operation::Member => {
            anyhow::ensure!(
                matches!(
                    state.sql.probe_password(&request.new_password).await,
                    RootPasswordProbe::Works
                ),
                "password not replicated"
            );
            state.sql.swap_root_password(&request.new_password).await;
            state
                .sql
                .configure_recovery_channel("gr_recovery", &request.new_password)
                .await?;
            password_pin::write_pin(&state.data_dir, &request.new_password)?;
        }
        Operation::Verify => {
            let members = state.sql.group_members().await?;
            anyhow::ensure!(
                members.len() >= 3 && members.iter().all(|m| m.state == "ONLINE"),
                "group not healthy"
            );
            if request.current_password != request.new_password {
                anyhow::ensure!(
                    matches!(
                        state.sql.probe_password(&request.current_password).await,
                        RootPasswordProbe::AccessDenied
                    ),
                    "previous password still accepted"
                );
            }
            anyhow::ensure!(
                matches!(
                    state.sql.probe_password(&request.new_password).await,
                    RootPasswordProbe::Works
                ),
                "password refused"
            );
            anyhow::ensure!(
                password_pin::read_pin(&state.data_dir).as_deref() == Some(&request.new_password),
                "pin differs"
            );
            match std::fs::remove_file(format!("{}/.railway_rotation", state.data_dir)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    let leader = !state.sql.super_read_only().await?;
    Ok(json!({"version": 1, "leader": leader}))
}

fn save_pending(data_dir: &str, request: &Rotation) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let path = format!("{data_dir}/.railway_rotation");
    let tmp = format!("{path}.tmp");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)?;
    file.write_all(&serde_json::to_vec(request)?)?;
    file.sync_all()?;
    std::fs::rename(tmp, path)?;
    std::fs::File::open(data_dir)?.sync_all()?;
    Ok(())
}
pub fn pending_password(data_dir: &str) -> Option<String> {
    let request: Rotation =
        serde_json::from_slice(&std::fs::read(format!("{data_dir}/.railway_rotation")).ok()?)
            .ok()?;
    Some(request.new_password)
}
pub async fn reconcile(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let _lock = ROTATION.lock().await;
        let attempt = async {
            let path = format!("{}/.railway_rotation", state.data_dir);
            anyhow::ensure!(
                std::fs::metadata(&path)?.modified()?.elapsed()?.as_secs() >= 30,
                "normal ordered phase"
            );
            let mut request: Rotation = serde_json::from_slice(&std::fs::read(path)?)?;
            request.operation = Operation::Member;
            apply(&state, request).await?;
            Ok::<(), anyhow::Error>(())
        };
        let _ = tokio::time::timeout(std::time::Duration::from_secs(25), attempt).await;
    }
}
