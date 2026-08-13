# MySQL High Availability Template for Railway

MySQL images for Railway's single-click HA template: MySQL Group Replication
(single-primary mode) behind an HAProxy edge, following the same shape as
[`redis-ha`](https://github.com/railwayapp-templates/redis-ha) and
[`postgres-ha`](https://github.com/railwayapp-templates/postgres-ha) — a thin
Rust wrapper around the upstream database image handles config rendering,
process supervision, and health serving; HAProxy routes client traffic based
on what those wrappers report.

**Status: functional.** Group formation, failover, conversion of a standalone
volume (clone-first provisioning), scale-up, partition fencing, cross-version
conversion, patch-skew survival, and total-outage recovery are all implemented
and covered by `test/e2e.sh`. See [Status](#status) for what remains scoped
out of v1.

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
  the write path; a read-only load-balanced port is a future addition.

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
expect status 200`), with `default-server fall 2 rise 2 on-marked-down
shutdown-sessions` — the first failed check switches probing to the fast
interval (500ms), so a real demotion pulls the node out ~500ms later, while a
single slow check on a healthy primary no longer severs every client
connection. `shutdown-sessions` forces every open client connection to
reconnect and land on the new primary once a node is genuinely marked down.

**This is the split-brain fence.** A primary that loses contact with the rest
of the group must answer 503, not 200, even if MySQL locally still believes
it's the primary — exactly the pattern redis-ha's `/role` uses Sentinel
confirmation for. Fail-closed is the contract: an uncertain answer is a
non-primary answer.

## Wrapper responsibilities

The `mysql-wrapper` binary (one per data node) is the analogue of redis-ha's
`redis-wrapper`. Its job:

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
- **Automated total-outage recovery, with dynamic candidacy.** If every
  declared peer is down at once, nothing may unilaterally pick a dataset to
  resume from. Recovery works by exchanging each node's executed-GTID set
  through the `/gr/state` health endpoints; a node bootstraps only when
  every peer answers, none reports a live group, and every reported set is a
  subset of its own — and only after that verdict holds through a dwell
  period (giving slower-to-report nodes a window to contradict it). Any node
  can be the one to bootstrap — candidacy follows the data, not a fixed seed
  (a fixed candidate deadlocks the group whenever it is behind, as after any
  failover, or permanently gone). Identical sets tie-break on pre-GTID data
  (an adopted standalone volume outranks fresh nodes) and then declared seed
  order, both of which every node computes identically.
- **`super_read_only` on every secondary, always.** Secondaries never accept
  direct writes, independent of what HAProxy is doing — a second fence
  against a client that somehow bypasses the edge.
- **Diverged-history freeze.** When two nodes each hold transactions the
  other never saw (e.g. one took writes while partitioned), no automatic
  bootstrap choice is safe — picking a side would silently discard the other
  side's committed writes. The wrapper detects the divergence at
  bootstrap-decision time, refuses to proceed anywhere, and pages through
  telemetry instead of letting GR fail cryptically.
- **Self-heal for unconnectable members.** A member that provably cannot
  come back on its own — mysqld stuck in an InnoDB crash-recovery boot loop
  on a corrupted datadir, or a live member wedged in ERROR /
  RECOVERING-without-progress past a dwell — discards its local copy and
  reprovisions from the group, with no operator action. Strictly gated: it
  only ever fires while a peer answers `/role` 200 (a quorum-confirmed
  primary, whose side is guaranteed to hold every committed transaction);
  with the whole group down it fails closed and never destroys what may be
  the best surviving copy. Attempts are capped and backed off, persisted on
  the volume. Thresholds: `BOOT_LOOP_THRESHOLD`, `BOOT_READY_BUDGET_SECONDS`,
  `STUCK_MEMBER_DWELL_SECONDS`, `SELF_HEAL_ATTEMPT_CAP`,
  `SELF_HEAL_BACKOFF_BASE_SECONDS`.

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
| `mysql-wrapper` | `ghcr.io/railwayapp-templates/mysql-ha/mysql:<major.minor>` (every `X.Y` series Docker Hub publishes for majors 8 and 9) | `mysql:<major.minor>` |
| `haproxy` | `ghcr.io/railwayapp-templates/mysql-ha/haproxy:3.2` | `haproxy:3.2-alpine` |

No image carries a floating `:latest` tag — every published tag pins an
exact MySQL/HAProxy version or commit SHA.

Every `major.minor` tag is a real, continuously rebuilt build line (daily +
on every wrapper change), not a frozen alias: a MySQL data dir cannot be
downgraded and series upgrades are one-way, so the platform's HA conversion
pins a converted service to its own series, and that pin must keep receiving
upstream patch, base-image and wrapper updates for its whole life. The
series list is discovered from Docker Hub on every run;
`MYSQL_SUPPORTED_MAJORS` in `.github/workflows/build-and-push.yml` is the
only policy knob.

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

Implemented and e2e-covered (`test/e2e.sh`, one scenario each): group
formation and replication with the write fence, failover with rejoin, cold
restart, conversion of a never-binlogged standalone volume (clone-first),
scale-up 3→5, minority-partition fencing, patch-skew on redeploy (including
the rollback refusal), total-outage recovery with the first seed behind, loss
of the first seed's volume, a volume backup of one node restored onto every
node (identical datadirs — each joiner regenerates its server_uuid instead of
being refused forever), cross-version conversion (previous LTS → wrapper
series), and the unconnectable-member self-heal (a boot-wedged corrupted
datadir reprovisions from the group; an applier-wedged ERROR member reclones;
and the negative guard — no quorum-confirmed donor, no wipe, ever).

Deliberately out of scope for v1:

- **No read port.** The edge exposes only the write frontend; a load-balanced
  read port is a future addition.
- **Rolling upgrades are not coordinated — and don't need to be, within a
  series.** Data nodes carry a series tag with no auto-update; any redeploy
  re-pulls the tag's current patch. This is safe by the LTS model: Group
  Replication tolerates the skew, clone works across patch releases of the
  same series, and a rollback of an upgraded member performs MySQL's
  automatic in-place downgrade on boot ("Server downgrade from X to Y") and
  rejoins with its data — the e2e locks both directions. Cross-SERIES moves
  (8.4 → 9.x) remain one-way (dump/reload only), which is exactly why the
  tags pin the series and conversions match the source's major.
- **Runs as root** — same posture (and same deferred fix) as redis-ha; see
  the Dockerfile TODO.
