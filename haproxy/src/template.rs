//! HAProxy configuration generator for MySQL HA.
//!
//! Architecture (v1 — write frontend only, no read port):
//!   - Port 3306 (writes): HTTP health check on each node's /role endpoint.
//!     Only the node that returns 200 (the current Group Replication
//!     single-primary) is marked UP.
//!   - Port 8404: stats page for observability. Open on loopback (the
//!     in-container monitor and the healthcheck); any other client presents
//!     HTTP Basic auth (HAPROXY_STATS_USER / HAPROXY_STATS_PASSWORD, default
//!     the MYSQLUSER / MYSQLPASSWORD account the edge carries). Without a
//!     credential, remote access is denied.
//!
//! The health check hits the Rust health server running on each mysql-wrapper
//! container (HEALTH_CHECK_PORT, default 8080), not MySQL directly. This
//! eliminates the need for raw tcp-check sequences in the MySQL protocol.
//!
//! There is no read frontend/backend in v1: this image is scoped to failover
//! for the write path, matching Railway's single-click MySQL HA template.

use crate::config::Config;
use crate::nodes::MySqlNode;

fn server_entries(nodes: &[MySqlNode], health_port: u16, config: &Config) -> String {
    nodes
        .iter()
        .map(|n| {
            format!(
                "    server {} {}:{} check port {} resolvers railway inter {} fastinter {} downinter {}",
                n.name, n.host, n.mysql_port, health_port,
                config.check_interval, config.check_fastinter, config.check_downinter
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn generate_config(config: &Config, nodes: &[MySqlNode]) -> String {
    let servers = server_entries(nodes, config.health_port, config);

    format!(
        r#"global
    maxconn {max_conn}
    log stdout format raw local0

defaults
    log global
    mode tcp
    option tcpka
    option clitcpka
    option srvtcpka
    option redispatch
    retries 3
    timeout connect {timeout_connect}
    timeout client {timeout_client}
    timeout server {timeout_server}
    timeout check {timeout_check}

resolvers railway
    parse-resolv-conf
    resolve_retries 3
    timeout resolve 1s
    timeout retry   1s
    hold other      10s
    hold refused    10s
    hold nx         10s
    hold timeout    10s
    hold valid      10s
    hold obsolete   10s

{stats}
# Write traffic — routed exclusively to the current Group Replication
# primary. The /role health check returns 200 only on the primary node.
frontend mysql_writes
    bind :::{mysql_port} v4v6
    default_backend mysql_primary_backend

backend mysql_primary_backend
    option httpchk
    http-check send meth GET uri /role
    http-check expect status 200
    # fall 2 + fastinter 500ms: the first failed /role check switches the
    # probe to the fast interval, so a real demotion is confirmed and the
    # server pulled ~500ms after the first failure — but ONE slow or dropped
    # check can no longer RST every client connection on a healthy primary.
    # /role runs two SQL reads (2s timeout each) against `timeout check 3s`,
    # so a single blip under load is expected, and with no secondary passing
    # /role a false mark-down is a self-inflicted write outage until `rise 2`
    # readmits the primary. shutdown-sessions RSTs every open client
    # connection the moment the server is genuinely marked down, forcing
    # clients to reconnect and land on the new primary.
    default-server fall 2 rise 2 on-marked-down shutdown-sessions
{servers}
"#,
        max_conn = config.max_conn,
        timeout_connect = config.timeout_connect,
        timeout_client = config.timeout_client,
        timeout_server = config.timeout_server,
        timeout_check = config.timeout_check,
        mysql_port = config.mysql_port,
        stats = generate_stats_listener(config),
        servers = servers,
    )
}

/// The stats listener. Loopback clients (the in-container monitor and the
/// healthcheck) are always allowed. Anyone else must present the stats
/// credential; without a credential configured, remote access is denied.
///
/// The credential is read by haproxy from the environment at parse time
/// (`"${HAPROXY_STATS_USER}"` / `"${HAPROXY_STATS_PASSWORD}"`) so the
/// rendered config — which is logged at startup — never contains it.
fn generate_stats_listener(config: &Config) -> String {
    let (userlist, remote_rule) = if config.stats_auth.is_some() {
        (
            "userlist stats_users\n    user \"${HAPROXY_STATS_USER}\" insecure-password \"${HAPROXY_STATS_PASSWORD}\"\n\n",
            "http-request auth unless { http_auth(stats_users) }",
        )
    } else {
        ("", "http-request deny")
    };
    format!(
        r#"{userlist}# Stats page for monitoring
listen stats
    bind :::8404 v4v6
    mode http
    # This proxy's own traffic is not worth logging: the in-container
    # monitoring loop scrapes /stats every few seconds and each scrape opens
    # two connections. Carried over from redis-ha, where inheriting `log
    # global` here made self-traffic ~99% of the service's entire log volume,
    # burying the lines an operator actually needs (backend UP/DOWN, DNS
    # re-resolution, client connects).
    no log
    acl LOCALHOST src 127.0.0.1 ::1 ::ffff:127.0.0.1
    http-request allow if LOCALHOST
    {remote_rule}
    stats enable
    stats uri /stats
    stats refresh 10s
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_for_tests() -> Config {
        Config {
            mysql_nodes: "mysql-1.railway.internal:3306,mysql-2.railway.internal:3306".to_string(),
            health_port: 8080,
            mysql_port: 3306,
            max_conn: "1000".to_string(),
            timeout_connect: "5s".to_string(),
            timeout_client: "30m".to_string(),
            timeout_server: "30m".to_string(),
            timeout_check: "3s".to_string(),
            check_interval: "3s".to_string(),
            check_fastinter: "500ms".to_string(),
            check_downinter: "500ms".to_string(),
            stats_auth: None,
        }
    }

    fn section<'a>(conf: &'a str, header: &str) -> &'a str {
        // Anchor to line start: a bare `find("backend x")` would match the
        // substring inside the frontend's `default_backend x` line.
        let needle = format!("\n{header}");
        let start = conf.find(&needle).expect("section header not found") + 1;
        let rest = &conf[start..];
        // A section runs until the next blank line followed by a non-indented
        // line — good enough for this fixed template.
        match rest.find("\n\n") {
            Some(end) => &rest[..end],
            None => rest,
        }
    }

    /// The stats listener must not log: the in-container monitoring loop
    /// scrapes it every few seconds, and inheriting `log global` made that
    /// self-traffic the bulk of this service's log volume in redis-ha.
    #[test]
    fn stats_listener_does_not_log_its_own_traffic() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.mysql_nodes).unwrap();
        let conf = generate_config(&config, &nodes);

        assert!(section(&conf, "listen stats").contains("no log"));
    }

    /// ...and silencing it must not silence the proxy that carries real
    /// traffic: it still inherits `log global` from defaults.
    #[test]
    fn write_frontend_still_logs() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.mysql_nodes).unwrap();
        let conf = generate_config(&config, &nodes);

        assert!(conf.contains("defaults\n    log global"));
        assert!(!section(&conf, "frontend mysql_writes").contains("no log"));
    }

    #[test]
    fn stats_page_requires_auth_for_remote_clients_when_a_credential_is_set() {
        let mut config = config_for_tests();
        config.stats_auth = Some(crate::config::StatsAuth {
            user: "root".to_string(),
            password: "s3cret".to_string(),
        });
        let nodes = crate::nodes::parse_nodes(&config.mysql_nodes).unwrap();
        let conf = generate_config(&config, &nodes);

        assert!(conf.contains("userlist stats_users\n    user \"${HAPROXY_STATS_USER}\" insecure-password \"${HAPROXY_STATS_PASSWORD}\""));
        let stats = section(&conf, "listen stats");
        assert!(stats.contains("acl LOCALHOST src 127.0.0.1 ::1 ::ffff:127.0.0.1"));
        assert!(stats.contains(
            "http-request allow if LOCALHOST\n    http-request auth unless { http_auth(stats_users) }\n    stats enable"
        ));
        assert!(!stats.contains("http-request deny"));
        // The secret itself never lands in the rendered file (it is logged at
        // boot): haproxy expands it from its environment at parse time.
        assert!(!conf.contains("s3cret"));
    }

    #[test]
    fn stats_page_denies_remote_clients_without_a_credential() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.mysql_nodes).unwrap();
        let conf = generate_config(&config, &nodes);

        assert!(!conf.contains("userlist"));
        let stats = section(&conf, "listen stats");
        assert!(stats
            .contains("http-request allow if LOCALHOST\n    http-request deny\n    stats enable"));
    }

    /// The loopback allow rule must come BEFORE the auth/deny rule: the
    /// in-container monitor (`localhost:8404/stats;csv`) and the Dockerfile
    /// HEALTHCHECK (`127.0.0.1:8404/stats`) carry no credential.
    #[test]
    fn stats_listener_keeps_its_bind_uri_and_loopback_exemption_first() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.mysql_nodes).unwrap();
        let conf = generate_config(&config, &nodes);
        let stats = section(&conf, "listen stats");

        assert!(stats.starts_with("listen stats\n    bind :::8404 v4v6\n    mode http\n"));
        assert!(stats.contains("stats uri /stats\n    stats refresh 10s"));
        let allow = stats.find("http-request allow if LOCALHOST").unwrap();
        let gate = stats.find("http-request deny").unwrap();
        assert!(allow < gate);
    }

    /// v1 has no read port — the read frontend/backend must not exist at all.
    #[test]
    fn there_is_no_read_frontend_or_backend() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.mysql_nodes).unwrap();
        let conf = generate_config(&config, &nodes);

        assert!(!conf.contains("frontend mysql_reads"));
        assert!(!conf.contains("mysql_replica_backend"));
        assert!(!conf.contains(":6380"));
    }

    /// The write frontend binds the configured MYSQL_PORT (default 3306),
    /// and routes to the single primary backend via the /role health check.
    #[test]
    fn write_frontend_binds_mysql_port_and_checks_role() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.mysql_nodes).unwrap();
        let conf = generate_config(&config, &nodes);

        let frontend = section(&conf, "frontend mysql_writes");
        assert!(frontend.contains("bind :::3306 v4v6"));
        assert!(frontend.contains("default_backend mysql_primary_backend"));

        let backend = section(&conf, "backend mysql_primary_backend");
        assert!(backend.contains("http-check send meth GET uri /role"));
        assert!(backend.contains("http-check expect status 200"));
        // fall 2, not 1: one slow /role check on a healthy primary must not
        // RST every client connection (fastinter re-probes 500ms later, so a
        // real demotion is still confirmed almost immediately).
        assert!(backend.contains("default-server fall 2 rise 2 on-marked-down shutdown-sessions"));
    }

    /// The resolvers block must be present with the same tunables redis-ha
    /// shipped — Railway's private network DNS needs re-resolution on
    /// redeploy, and this is what makes HAProxy pick up an IP change.
    #[test]
    fn resolvers_block_is_present() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.mysql_nodes).unwrap();
        let conf = generate_config(&config, &nodes);

        assert!(conf.contains("resolvers railway"));
        assert!(conf.contains("parse-resolv-conf"));
    }

    /// Every declared node must appear as a `server` line in the primary
    /// backend, health-checked against the wrapper's health port.
    #[test]
    fn every_node_gets_a_server_line() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.mysql_nodes).unwrap();
        let conf = generate_config(&config, &nodes);
        let backend = section(&conf, "backend mysql_primary_backend");

        assert!(backend.contains("server mysql-1 mysql-1.railway.internal:3306 check port 8080"));
        assert!(backend.contains("server mysql-2 mysql-2.railway.internal:3306 check port 8080"));
    }
}
