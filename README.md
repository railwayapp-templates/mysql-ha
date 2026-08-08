# MySQL High Availability Template for Railway

MySQL images for Railway's single-click HA template: MySQL Group Replication
(single-primary mode) behind an HAProxy edge, following the same shape as
[`redis-ha`](https://github.com/railwayapp-templates/redis-ha) and
[`postgres-ha`](https://github.com/railwayapp-templates/postgres-ha) — a thin
Rust wrapper around the upstream database image handles config rendering,
process supervision, and health serving; HAProxy routes client traffic based
on what those wrappers report.

**Status: WIP scaffold.** See [Status](#status) before relying on anything
here — most of the actual Group Replication behavior is not implemented yet.

## Topology

```
Application
    ↓
MySQL HA (HAProxy)
    └─ :3306 (write) → current Group Replication primary only
    ↓
MySQL Group Replication cluster
    ├─ MySQL-1 (root)      ← initial primary
    ├─ MySQL-2 (secondary) ← replicates via GR, failover-ready
    └─ MySQL-3 (secondary) ← replicates via GR, failover-ready
```

- **MySQL-1** is the root service — the node the template deploys first, and
  the initial Group Replication primary.
- **MySQL-2** / **MySQL-3** join the same GR group as secondaries.
- **MySQL HA** is the HAProxy edge — the only thing clients should connect
  to. It exposes a single write port, `:3306`, health-checked against each
  node's `/role` endpoint so writes always land on whichever node is
  currently the GR primary.
- **v1 has no read port.** This template version is scoped to failover for
  the write path; a read-only load-balanced port (the `:6380` equivalent in
  redis-ha) is a future addition, not part of this scaffold.

Minimum group size for Group Replication to tolerate a node loss is 3 —
identical reasoning to redis-ha's Sentinel quorum: a 2-node group can't
distinguish "the other node died" from "I'm the one partitioned away."

## The `/role` / `/health` contract

Every data node runs an HTTP server (the Rust wrapper) on port 8080 with two
endpoints, and HAProxy never talks to MySQL's wire protocol directly to make
routing decisions:

- `GET /health` — liveness. 200 if MySQL is up and answering, 503 otherwise.
- `GET /role` — the routing signal. 200 **only** when this node is the
  current Group Replication primary; 503 in every other case, including when
  the node cannot confirm its own status.

HAProxy's write frontend (`mysql_primary_backend`) marks a node UP only while
its `/role` returns 200 (`http-check send meth GET uri /role` / `http-check
expect status 200`), with `default-server fall 1 rise 2 on-marked-down
shutdown-sessions` — one failed check pulls a node out of rotation
immediately, and `shutdown-sessions` forces every open client connection to
reconnect and land on the new primary.

**This is the split-brain fence.** A primary that loses contact with the rest
of the group must answer 503, not 200, even if MySQL locally still believes
it's the primary — exactly the pattern redis-ha's `/role` uses Sentinel
confirmation for. Fail-closed is the contract: an uncertain answer is a
non-primary answer.

## Wrapper responsibilities

The `mysql-wrapper` binary (one per data node) is the analogue of redis-ha's
`redis-wrapper`. Its job, once fully implemented, is:

- **Config rendering.** Render a my.cnf carrying Group Replication in
  single-primary mode, with `group_replication_start_on_boot=OFF` — GR is
  joined or started explicitly by the wrapper's own logic, never
  automatically as part of mysqld startup.
- **Bootstrap guard.** Before a node ever issues `START GROUP_REPLICATION`
  with `group_replication_bootstrap_group=ON`, it queries its declared peers
  for an already-live group. A booting node must join an existing group
  whenever one exists among its declared peers — it may only bootstrap a
  brand new group when none of them answer with one. Without this, a node
  restarting after a network partition heals could start a second, competing
  group instead of rejoining the real one.
- **Clone-plugin provisioning.** A new or rejoining peer provisions its
  dataset via MySQL's Clone plugin against a healthy group member, instead of
  requiring an operator to seed it manually — the GR equivalent of a Redis
  replica's full sync.
- **Automated total-outage recovery.** If every declared peer is down at
  once, nothing may unilaterally pick a dataset to resume from. Recovery
  works by exchanging each candidate's executed-GTID set through their
  `/health` servers; the node with the most-advanced set bootstraps after a
  dwell period (giving slower-to-report nodes a chance to be seen), and the
  number of automatic bootstrap attempts is capped so a flapping network
  can't repeatedly re-elect different nodes as the source of truth.
- **`super_read_only` on every secondary, always.** Secondaries never accept
  direct writes, independent of what HAProxy is doing — a second fence
  against a client that somehow bypasses the edge.
- **Errant-GTID detection.** A node whose GTID set contains transactions the
  rest of the group never saw (e.g. it took a local write while partitioned,
  or was promoted-then-demoted in a way that left orphaned transactions)
  cannot safely rejoin the group as-is; the wrapper needs to detect that
  condition rather than let GR fail cryptically or, worse, admit a node
  carrying diverged data.

## Conversion notes

Railway's standalone `mysql` template runs `mysql:9.4` with
`--disable-log-bin`, `--performance_schema=0`, and a fixed 1G buffer pool —
none of which Group Replication can work with. Converting a standalone
service into this HA template's root node means the rendered my.cnf has to
flip all three:

- **Binlog re-enabled** — GR replicates via the binary log; the standalone
  template turns it off entirely to save disk and I/O.
- **`performance_schema=ON`** — required to read
  `performance_schema.replication_group_members`, which is what `/role`'s
  primary-and-quorum check queries.
- **`gtid_mode=ON` / `enforce_gtid_consistency=ON`** — GR requires GTIDs; the
  standalone template has no opinion on them either way.
- **`innodb_buffer_pool_size`** sized from the container's actual memory
  limit instead of the standalone template's fixed 1G.

The datadir is the volume root in both the standalone template and this
image — `/var/lib/mysql` — so no data migration step is needed on adoption,
only the config change above.

## Images

| Image | GHCR path | Base |
|---|---|---|
| `mysql-wrapper` | `ghcr.io/railwayapp-templates/mysql-ha/mysql-wrapper:8.4` | `mysql:8.4` |
| `mysql-wrapper` | `ghcr.io/railwayapp-templates/mysql-ha/mysql-wrapper:9.4` | `mysql:9.4` |
| `haproxy` | `ghcr.io/railwayapp-templates/mysql-ha/haproxy:3.2` | `haproxy:3.2-alpine` |

No image carries a floating `:latest` tag — every published tag pins an
exact MySQL/HAProxy version or commit SHA.

## Development

### Prerequisites

- Rust (stable)
- Docker + Docker Buildx

### Build locally

```bash
# Build mysql-wrapper
docker build -f mysql-wrapper/Dockerfile -t mysql-wrapper:local .

# Build haproxy
docker build -f haproxy/Dockerfile -t mysql-ha-haproxy:local .
```

### Test

```bash
cargo test --locked
```

## Status

This repository is a **scaffold**, seeded from redis-ha's workspace shape.
What's real:

- `common/` — engine-neutral config-parsing helpers, logging, and telemetry
  transport. Copied from redis-ha as-is (it never mentioned Redis to begin
  with, past the telemetry event set, which has been trimmed to
  engine-neutral variants).
- `haproxy/` — the config generator, node parsing, and monitoring loop are
  fully adapted and tested: a single write frontend on `:3306` against
  `mysql_primary_backend`, no read frontend/backend, stats on `:8404`.
- `mysql-wrapper/` — **stub only.** Config parsing works
  (`Config::from_env`), and the process supervision skeleton (spawn
  `docker-entrypoint.sh mysqld`, forward signals, exit with the child's code)
  is real. Everything Group-Replication-specific is not:
  - `/health` and `/role` both unconditionally return 503 — fail-closed by
    design, since a stub that answered 200 would tell HAProxy every node is
    a valid write target simultaneously.
  - No my.cnf rendering — the container currently boots mysqld with
    whatever config the base image ships, not a Group Replication config.
  - No bootstrap guard, no Clone-plugin provisioning, no total-outage
    recovery, no errant-GTID detection.

  Search `mysql-wrapper/src` for `TODO` to find every specific gap.

See `test/e2e.sh` for the planned end-to-end scenario list — none of it runs
yet.
