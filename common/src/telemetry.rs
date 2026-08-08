use crate::config::RailwayEnv;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tracing::{info, warn};

/// Engine-neutral telemetry events. This enum used to carry Redis/Sentinel-
/// specific variants (Failover, LinkHealTriggered, ...) — those live in each
/// image's own crate now (if they need them at all), since `common` is
/// reused verbatim across postgres-ha, redis-ha and mysql-ha and the wire
/// contract only needs the shape every HA image shares: a node starting up,
/// its role changing, a supervised process dying, the edge losing its only
/// writable backend, and a generic component error.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TelemetryEvent {
    /// A data node started successfully.
    NodeStarted { node: String, role: String },

    /// Node role changed (e.g. replica → primary, or primary → replica).
    RoleChanged {
        node: String,
        old_role: String,
        new_role: String,
    },

    /// A supervised process died unexpectedly.
    ProcessDied {
        node: String,
        process: String,
        exit_code: Option<i32>,
    },

    /// The edge (HAProxy) has no healthy primary/write backend.
    NoPrimary { backends: Vec<String> },

    /// Generic error during startup or operation.
    ComponentError {
        component: String,
        error: String,
        context: String,
    },
}

impl TelemetryEvent {
    /// The event's kind. Sent as `command` — the field backboard groups by.
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::NodeStarted { .. } => "mysql_ha.node_started",
            Self::RoleChanged { .. } => "mysql_ha.role_changed",
            Self::ProcessDied { .. } => "mysql_ha.process_died",
            Self::NoPrimary { .. } => "mysql_ha.no_primary",
            Self::ComponentError { .. } => "mysql_ha.component_error",
        }
    }

    /// One-line human summary, sent as `error`. The mutation's shape is the
    /// CLI's (`command`/`error`/`stacktrace`), not a generic event envelope,
    /// so the structured payload rides along in `stacktrace` as JSON.
    pub fn message(&self) -> String {
        match self {
            Self::NodeStarted { node, role } => format!("{node} started as {role}"),
            Self::RoleChanged {
                node,
                old_role,
                new_role,
            } => format!("{node} changed role from {old_role} to {new_role}"),
            Self::ProcessDied {
                node,
                process,
                exit_code,
            } => format!(
                "{node}: {process} died with exit code {}",
                exit_code
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            ),
            Self::NoPrimary { backends } => {
                format!("no healthy primary backend among [{}]", backends.join(", "))
            }
            Self::ComponentError {
                component,
                error,
                context,
            } => format!("{component} failed during {context}: {error}"),
        }
    }
}

#[derive(Clone)]
pub struct Telemetry {
    client: Client,
    endpoint: String,
    service_id: String,
    project_id: String,
    environment_id: String,
    component: String,
    /// Off-Railway (local dev, the e2e harness) telemetry is a no-op: the
    /// default endpoint is production backboard, and test events with empty
    /// resource ids are pure noise there.
    enabled: bool,
}

impl Telemetry {
    pub fn from_env(component: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("HTTP client creation should not fail");

        let enabled = RailwayEnv::is_railway();
        info!(component, enabled, "telemetry initialized");

        Self {
            client,
            enabled,
            endpoint: RailwayEnv::graphql_endpoint(),
            service_id: RailwayEnv::service_id(),
            project_id: RailwayEnv::project_id(),
            environment_id: RailwayEnv::environment_id(),
            component: component.to_string(),
        }
    }

    /// The GraphQL request body. Split out so a test can pin the contract —
    /// the shape is what broke in redis-ha, and it broke silently.
    fn build_payload(&self, event: &TelemetryEvent) -> serde_json::Value {
        // `telemetrySend`/`TelemetrySendInput` — the mutation backboard
        // actually serves. An earlier image called `sendTelemetry(input:
        // TelemetryInput!)`, which exists nowhere in backboard's schema, so
        // every event it produced was rejected. Nobody noticed because the
        // send path only inspected the TRANSPORT result: a 400 "unknown
        // field" was logged as "telemetry sent".
        json!({
            "query": "mutation telemetrySend($input: TelemetrySendInput!) { telemetrySend(input: $input) }",
            "variables": {
                "input": {
                    "command": event.event_type(),
                    "error": event.message(),
                    "stacktrace": serde_json::to_string(&event).unwrap_or_default(),
                    "projectId": self.project_id,
                    "environmentId": self.environment_id,
                    "serviceId": self.service_id,
                    "version": self.component,
                }
            }
        })
    }

    pub fn send(&self, event: TelemetryEvent) {
        if !self.enabled {
            tracing::debug!(event = event.event_type(), "telemetry disabled off-Railway");
            return;
        }
        let payload = self.build_payload(&event);

        // The first event a node ever sends is NodeStarted, ~100ms into the
        // container's life — and in production that one consistently fails
        // with "error sending request": the container's egress isn't ready
        // yet. Since most nodes never emit a second event, that made the
        // whole telemetry stream empty in practice even after the contract
        // was fixed. One delayed retry is enough to clear the startup race.
        //
        // ONLY transport errors are retried. A rejection is deterministic —
        // retrying a bad contract just doubles the noise — and the retry is
        // bounded to one because `send` is synchronous by design: callers
        // like the boot guards emit an event and then exit(1), so the event
        // has to be on the wire before the process goes away.
        for attempt in 1..=SEND_ATTEMPTS {
            match self
                .client
                .post(&self.endpoint)
                .header("Content-Type", "application/json")
                .json(&payload)
                .send()
            {
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().unwrap_or_default();
                    match classify(status.as_u16(), &body) {
                        SendOutcome::Sent => {
                            info!(event = %event.event_type(), attempt, "telemetry sent")
                        }
                        SendOutcome::Rejected(why) => {
                            warn!(event = %event.event_type(), %status, reason = %why, body = %truncate(&body), "telemetry rejected")
                        }
                    }
                    return;
                }
                Err(e) if attempt < SEND_ATTEMPTS => {
                    warn!(event = %event.event_type(), attempt, error = %e, "telemetry send failed, retrying");
                    std::thread::sleep(RETRY_DELAY);
                }
                Err(e) => {
                    warn!(event = %event.event_type(), attempt, error = %e, "telemetry send failed")
                }
            }
        }
    }
}

/// One retry: the failure this exists for is a startup race, not a flaky
/// endpoint, and `send` blocks the caller.
const SEND_ATTEMPTS: u32 = 2;
const RETRY_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, PartialEq, Eq)]
enum SendOutcome {
    Sent,
    Rejected(&'static str),
}

/// A GraphQL error is a 200 with an `errors` array, so the status alone
/// cannot tell success from a rejected request — reading only the status is
/// what let a nonexistent mutation look like a successful send for months.
fn classify(status: u16, body: &str) -> SendOutcome {
    if !(200..300).contains(&status) {
        return SendOutcome::Rejected("http status");
    }
    if body.contains("\"errors\"") {
        return SendOutcome::Rejected("graphql errors");
    }
    SendOutcome::Sent
}

/// Keep a rejection body loggable without dumping a whole HTML error page.
fn truncate(body: &str) -> String {
    const MAX: usize = 300;
    if body.len() <= MAX {
        return body.to_string();
    }
    format!("{}…", &body[..MAX])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn telemetry_for_tests() -> Telemetry {
        Telemetry {
            client: Client::builder().build().unwrap(),
            endpoint: "http://localhost:0/graphql/internal".to_string(),
            service_id: "svc-1".to_string(),
            project_id: "proj-1".to_string(),
            environment_id: "env-1".to_string(),
            component: "mysql-wrapper".to_string(),
            enabled: true,
        }
    }

    fn all_events() -> Vec<TelemetryEvent> {
        vec![
            TelemetryEvent::NodeStarted {
                node: "node-1".into(),
                role: "primary".into(),
            },
            TelemetryEvent::RoleChanged {
                node: "node-1".into(),
                old_role: "primary".into(),
                new_role: "secondary".into(),
            },
            TelemetryEvent::ProcessDied {
                node: "node-1".into(),
                process: "mysqld".into(),
                exit_code: Some(1),
            },
            TelemetryEvent::NoPrimary {
                backends: vec!["node-1".into(), "node-2".into()],
            },
            TelemetryEvent::ComponentError {
                component: "mysql-wrapper".into(),
                error: "boom".into(),
                context: "startup".into(),
            },
        ]
    }

    /// The regression this module's transport code is written to avoid: an
    /// image that calls `sendTelemetry(input: TelemetryInput!)`, which
    /// backboard does not serve, has every event silently rejected because
    /// only the transport result is checked. Pin the mutation backboard
    /// actually has.
    #[test]
    fn payload_targets_the_mutation_backboard_serves() {
        let telemetry = telemetry_for_tests();
        let payload = telemetry.build_payload(&all_events()[0]);
        let query = payload["query"].as_str().unwrap();

        assert!(query.contains("telemetrySend("));
        assert!(query.contains("$input: TelemetrySendInput!"));
        assert!(!query.contains("sendTelemetry"));
        assert!(!query.contains("TelemetryInput!"));
    }

    /// `command`, `error` and `stacktrace` are non-null in TelemetrySendInput:
    /// an empty one is a rejected request, and every event must fill them.
    #[test]
    fn every_event_fills_the_required_input_fields() {
        let telemetry = telemetry_for_tests();
        for event in all_events() {
            let payload = telemetry.build_payload(&event);
            let input = &payload["variables"]["input"];
            for field in ["command", "error", "stacktrace"] {
                let value = input[field].as_str().unwrap_or_else(|| {
                    panic!("{field} missing for {:?}", event.event_type())
                });
                assert!(!value.is_empty(), "{field} empty for {}", event.event_type());
            }
            assert_eq!(input["projectId"], "proj-1");
            assert_eq!(input["environmentId"], "env-1");
            assert_eq!(input["serviceId"], "svc-1");
            assert_eq!(input["version"], "mysql-wrapper");
        }
    }

    /// Every variant needs its own type string — a duplicate would silently
    /// merge two event kinds in whatever groups by `command`.
    #[test]
    fn event_types_are_distinct_and_namespaced() {
        let mut seen = std::collections::HashSet::new();
        for event in all_events() {
            let ty = event.event_type();
            assert!(ty.starts_with("mysql_ha."), "{ty} is not namespaced");
            assert!(seen.insert(ty), "duplicate event_type {ty}");
        }
    }

    /// The structured payload still rides along, so nothing is lost by the
    /// CLI-shaped envelope.
    #[test]
    fn stacktrace_carries_the_structured_event() {
        let telemetry = telemetry_for_tests();
        let event = TelemetryEvent::RoleChanged {
            node: "node-1".into(),
            old_role: "secondary".into(),
            new_role: "primary".into(),
        };
        let payload = telemetry.build_payload(&event);
        let raw = payload["variables"]["input"]["stacktrace"].as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(raw).unwrap();

        assert_eq!(parsed["type"], "RoleChanged");
        assert_eq!(parsed["node"], "node-1");
        assert_eq!(parsed["new_role"], "primary");
    }

    /// The exact confusion that hid a broken contract in redis-ha: GraphQL
    /// answers a rejected request with HTTP 200 and an `errors` array.
    #[test]
    fn a_200_with_graphql_errors_is_a_rejection() {
        assert_eq!(
            classify(200, r#"{"errors":[{"message":"Cannot query field"}]}"#),
            SendOutcome::Rejected("graphql errors")
        );
        assert_eq!(
            classify(200, r#"{"data":{"telemetrySend":true}}"#),
            SendOutcome::Sent
        );
        assert_eq!(classify(400, "bad request"), SendOutcome::Rejected("http status"));
        assert_eq!(classify(503, ""), SendOutcome::Rejected("http status"));
    }

    /// A body that merely mentions the word must not be read as a rejection.
    #[test]
    fn success_bodies_are_not_misread_as_rejections() {
        assert_eq!(
            classify(200, r#"{"data":{"telemetrySend":true},"note":"no errors here"}"#),
            SendOutcome::Sent
        );
    }

    /// Retries exist for the boot-time egress race only, and `send` blocks
    /// callers that exit(1) right after — so the budget stays at one retry.
    #[test]
    fn retry_budget_stays_bounded() {
        assert_eq!(SEND_ATTEMPTS, 2);
        assert!(RETRY_DELAY <= Duration::from_secs(3));
    }

    #[test]
    fn truncate_keeps_bodies_loggable() {
        assert_eq!(truncate("short"), "short");
        let long = "x".repeat(400);
        let out = truncate(&long);
        assert!(out.len() < 400 && out.ends_with('…'));
    }
}
