#!/usr/bin/env bash
# End-to-end tests for the mysql-ha images. Pure docker CLI, one host, no
# compose — same harness style as redis-ha/postgres-ha. Every resource is
# labeled mysql-ha-e2e=1 and cleaned up on exit.
#
# Usage: ./test/e2e.sh [t_name ...]      (default: all tests)
#   MYSQL_VERSION=8.4 ./test/e2e.sh      (default 8.4)
#   KEEP=1 ./test/e2e.sh t_group_forms   (skip cleanup for debugging)

set -u

cd "$(dirname "$0")/.."

MYSQL_VERSION="${MYSQL_VERSION:-8.4}"
IMAGE="mysql-ha-e2e:${MYSQL_VERSION}"
NET="mysql-ha-e2e-net"
LABEL="mysql-ha-e2e=1"
ROOT_PW="e2e-root-pw"
REPL_PW="e2e-repl-pw"
SEEDS="mysql-1:3306,mysql-2:3306,mysql-3:3306"

# PITR scenario: a minio container stands in for the S3-compatible bucket.
# MINIO_HOST_PORT is set by start_minio() itself (docker-assigned at random).
MINIO_ROOT_USER="e2e-minio-user"
MINIO_ROOT_PASSWORD="e2e-minio-password"
PITR_BUCKET="mysql-pitr-e2e"
MINIO_HOST_PORT=""

PASS=0
FAIL=0
FAILED_TESTS=()

log()  { printf '\033[1;34m[e2e]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '\033[1;31m[fail]\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); FAILED_TESTS+=("$*"); }

cleanup() {
  [ "${KEEP:-0}" = "1" ] && { log "KEEP=1 — leaving resources up"; return; }
  docker ps -aq --filter "label=$LABEL" | xargs -r docker rm -f >/dev/null 2>&1
  docker volume ls -q --filter "label=$LABEL" | xargs -r docker volume rm >/dev/null 2>&1
  docker network rm "$NET" >/dev/null 2>&1
}
trap cleanup EXIT

ensure_image() {
  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    log "building $IMAGE"
    docker build -t "$IMAGE" -f mysql-wrapper/Dockerfile \
      --build-arg MYSQL_VERSION="$MYSQL_VERSION" . || { echo "image build failed"; exit 1; }
  fi
}

ensure_network() {
  docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
}

# start_node <n> [extra docker args...] — boots mysql-N with the same env
# shape the Railway template stamps. NODE_IMAGE overrides the image for this
# one call (patch-skew scenarios boot members on different patch levels, the
# way a redeploy re-pulling the moving :8.4 tag does in production).
# NODE_SUFFIX appends to every hostname/alias/container name — the deletion
# scenarios park their nodes under the reserved `.invalid` TLD so a removed
# container's name resolves as authoritative NXDOMAIN on any resolver (bare
# names get environment-dependent answers once the container is gone).
start_node() {
  local n="$1"; shift
  local image="${NODE_IMAGE:-$IMAGE}"
  local host="mysql-$n${NODE_SUFFIX:-}"
  docker volume create --label "$LABEL" "mysql-ha-e2e-vol-$n" >/dev/null
  # --restart unless-stopped mirrors Railway's restart policy — and the clone
  # provisioning path DEPENDS on a restart: the clone recipient replaces its
  # datadir and shuts down, expecting the platform to boot it back up.
  docker run -d --label "$LABEL" --restart unless-stopped \
    --name "$host" --hostname "$host" \
    --network "$NET" --network-alias "$host" \
    -v "mysql-ha-e2e-vol-$n:/var/lib/mysql" \
    -e MYSQL_ROOT_PASSWORD="$ROOT_PW" \
    -e GR_REPLICATION_PASSWORD="$REPL_PW" \
    -e GR_SEEDS="$SEEDS" \
    -e RAILWAY_PRIVATE_DOMAIN="$host" \
    -e RAILWAY_ENVIRONMENT_ID="e2e-env" \
    -e RAILWAY_VOLUME_MOUNT_PATH="/var/lib/mysql" \
    -e BOOTSTRAP_DWELL_SECONDS=5 \
    "$@" \
    "$image" >/dev/null
}

start_trio() { start_node 1; start_node 2; start_node 3; }

teardown_trio() {
  local s="${NODE_SUFFIX:-}"
  docker rm -f "mysql-1$s" "mysql-2$s" "mysql-3$s" >/dev/null 2>&1
  docker volume rm mysql-ha-e2e-vol-1 mysql-ha-e2e-vol-2 mysql-ha-e2e-vol-3 >/dev/null 2>&1
}

# start_standalone <name> [extra docker args...] — boots a standalone
# (no GR_SEEDS) wrapper node under the given name/hostname. Used by the PITR
# scenario: archiving and restore are standalone-only in this version.
start_standalone() {
  local name="$1"; shift
  docker volume create --label "$LABEL" "mysql-ha-e2e-vol-$name" >/dev/null
  docker run -d --label "$LABEL" --restart unless-stopped \
    --name "$name" --hostname "$name" \
    --network "$NET" --network-alias "$name" \
    -v "mysql-ha-e2e-vol-$name:/var/lib/mysql" \
    -e MYSQL_ROOT_PASSWORD="$ROOT_PW" \
    -e RAILWAY_PRIVATE_DOMAIN="$name" \
    -e RAILWAY_ENVIRONMENT_ID="e2e-env" \
    -e RAILWAY_VOLUME_MOUNT_PATH="/var/lib/mysql" \
    "$@" \
    "$IMAGE" >/dev/null
}

# start_minio — a minio container standing in for the S3-compatible bucket
# the PITR env contract points at, on the shared e2e network, with
# PITR_BUCKET pre-created via the mc client. The wrapper containers reach it
# over the network alias (never the host port); the host port is ONLY for
# this function's own readiness probe, and is docker-assigned at random
# (`-p 9000` with no host part) rather than a fixed guess, which collided
# with an unrelated local listener in practice.
start_minio() {
  docker volume create --label "$LABEL" mysql-ha-e2e-minio-data >/dev/null
  docker run -d --label "$LABEL" --name mysql-ha-e2e-minio --hostname mysql-ha-e2e-minio \
    --network "$NET" --network-alias mysql-ha-e2e-minio \
    -p 9000 \
    -v mysql-ha-e2e-minio-data:/data \
    -e MINIO_ROOT_USER="$MINIO_ROOT_USER" -e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
    minio/minio server /data >/dev/null

  MINIO_HOST_PORT="$(docker port mysql-ha-e2e-minio 9000/tcp | head -1 | awk -F: '{print $NF}')"
  [ -n "$MINIO_HOST_PORT" ] || { log "could not determine minio's assigned host port"; return 1; }

  wait_until 60 "minio up" \
    bash -c "curl -sf http://localhost:$MINIO_HOST_PORT/minio/health/live >/dev/null 2>&1" \
    || return 1

  # minio/mc's entrypoint is `mc` itself, not a shell — override it to chain
  # the alias-set and bucket-create in one container.
  docker run --rm --label "$LABEL" --network "$NET" --entrypoint sh minio/mc \
    -c "mc alias set e2e http://mysql-ha-e2e-minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD >/dev/null && mc mb --ignore-existing e2e/$PITR_BUCKET >/dev/null" \
    >/dev/null 2>&1
}

# mc_rm_key <bucket-relative-key> — deletes exactly one object from the e2e
# minio bucket via a throwaway mc container (same alias-then-act pattern as
# start_minio's own bucket-create, since the alias config lives only inside
# that one throwaway container). Used by the PITR adversarial scenarios below
# to force a shipped binlog out of the archive after the fact — standing in
# for an object a bucket lifecycle rule expired, or one that was simply
# corrupted/dropped — which is a deterministic way to punch a hole in the
# archive without racing the archiver's own ~10s ship-poll timing.
mc_rm_key() {
  docker run --rm --label "$LABEL" --network "$NET" --entrypoint sh minio/mc \
    -c "mc alias set e2e http://mysql-ha-e2e-minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD >/dev/null && mc rm e2e/$PITR_BUCKET/$1" \
    >/dev/null 2>&1
}

# sql <node> <statement> — root SQL over the node's local socket.
sql() {
  local node="$1"; shift
  docker exec "$node" mysql -uroot -p"$ROOT_PW" --batch --skip-column-names -e "$1" 2>/dev/null
}

# role_code <from-node> <target-node> — HTTP status class of /role (200|503).
role_code() {
  if docker exec "$1" wget -q -O /dev/null "http://$2:8080/role" 2>/dev/null; then
    echo 200
  else
    echo 503
  fi
}

online_members() {
  sql "$1" "SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_STATE='ONLINE'" || echo 0
}

has_n_online() { [ "$(online_members "$1" | tr -d '[:space:]')" = "$2" ]; }

# group_online_excluding <probe-node> <n> <excluded-host> — exactly <n>
# members ONLINE and <excluded-host> is not one of them. Plain "N online"
# can't tell a bad 2-node group (the fresh pair, without the adopted node)
# from a GOOD partial convergence (the adopted node plus one fresh node,
# the other still mid-clone) — both report the same count.
group_online_excluding() {
  local probe="$1" n="$2" excluded="$3"
  [ "$(online_members "$probe" | tr -d '[:space:]')" = "$n" ] || return 1
  ! sql "$probe" \
    "SELECT MEMBER_HOST FROM performance_schema.replication_group_members WHERE MEMBER_STATE='ONLINE'" \
    | grep -qx "$excluded"
}

# wait_until <timeout-s> <description> <command...>
wait_until() {
  local timeout="$1" desc="$2"; shift 2
  local waited=0
  until "$@"; do
    sleep 3
    waited=$((waited+3))
    if [ "$waited" -ge "$timeout" ]; then
      log "TIMEOUT ($timeout s) waiting for: $desc"
      return 1
    fi
  done
}

group_is_fully_online() { has_n_online "$1" 3; }

# current_primary — prints the node name (mysql-N) whose /role answers 200,
# probing from mysql-2 (any live node works as probe origin).
current_primary() {
  local probe="$1"; shift
  local n
  for n in "$@"; do
    if [ "$(role_code "$probe" "$n")" = "200" ]; then
      echo "$n"
      return 0
    fi
  done
  return 1
}

# ---------------------------------------------------------------------------

t_group_forms_and_replicates() {
  log "t_group_forms_and_replicates"
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  ok "group formed with 3 ONLINE members"

  # Exactly the bootstrap candidate answers /role 200.
  local codes
  codes="$(role_code mysql-2 mysql-1)/$(role_code mysql-2 mysql-2)/$(role_code mysql-2 mysql-3)"
  if [ "$codes" = "200/503/503" ]; then
    ok "/role fence: only the primary answers 200 ($codes)"
  else
    bad "/role fence wrong: $codes (want 200/503/503)"
  fi

  sql mysql-1 "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (1,'from-primary') ON DUPLICATE KEY UPDATE v='from-primary';"
  wait_until 60 "row replicated to a secondary" \
    bash -c '[ "$(docker exec mysql-3 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=1" 2>/dev/null)" = "from-primary" ]' \
    || { bad "write did not replicate to mysql-3"; return; }
  ok "write on primary visible on secondary"

  if sql mysql-2 "INSERT INTO t.kv VALUES (99,'rogue')" >/dev/null 2>&1; then
    bad "secondary accepted a direct write (super_read_only fence broken)"
  else
    ok "secondary refuses direct writes"
  fi
}

t_failover_on_primary_pause() {
  log "t_failover_on_primary_pause (reuses the running trio)"
  group_is_fully_online mysql-1 || { start_trio; wait_until 300 "3 ONLINE" group_is_fully_online mysql-1 || { bad "no group to fail over"; return; }; }

  docker pause mysql-1 >/dev/null
  log "primary paused; waiting for election"

  wait_until 120 "a new primary among mysql-2/3" \
    bash -c '[ "$(docker exec mysql-2 wget -q -O /dev/null http://mysql-2:8080/role 2>/dev/null && echo 200 || echo 503)" = "200" ] || [ "$(docker exec mysql-2 wget -q -O /dev/null http://mysql-3:8080/role 2>/dev/null && echo 200 || echo 503)" = "200" ]' \
    || { bad "no new primary elected after pause"; docker unpause mysql-1 >/dev/null; return; }

  local new_primary
  new_primary="$(current_primary mysql-2 mysql-2 mysql-3)"
  ok "new primary elected: $new_primary"

  sql "$new_primary" "INSERT INTO t.kv VALUES (2,'post-failover') ON DUPLICATE KEY UPDATE v='post-failover';" \
    && ok "write accepted by new primary" \
    || bad "new primary refused a write"

  # Bring the old primary back the way Railway would: the container restarts
  # and the boot orchestration joins the existing group.
  docker unpause mysql-1 >/dev/null
  docker restart mysql-1 >/dev/null
  wait_until 300 "old primary rejoined (3 ONLINE)" group_is_fully_online mysql-2 \
    || { bad "old primary did not rejoin"; return; }
  ok "old primary rejoined the group"

  wait_until 60 "post-failover row visible on rejoined node" \
    bash -c '[ "$(docker exec mysql-1 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=2" 2>/dev/null)" = "post-failover" ]' \
    || { bad "rejoined node missing post-failover write"; return; }
  ok "rejoined node caught up"

  if [ "$(role_code mysql-2 mysql-1)" = "503" ]; then
    ok "rejoined ex-primary is a secondary (/role 503)"
  else
    bad "rejoined ex-primary still answers /role 200"
  fi
}

t_cold_restart_preserves_group() {
  log "t_cold_restart_preserves_group (reuses the running trio)"
  group_is_fully_online mysql-2 || { bad "no group to cold-restart"; return; }

  # Stop secondaries first so the candidate (mysql-1) holds every transaction.
  docker stop mysql-2 mysql-3 >/dev/null
  docker stop mysql-1 >/dev/null
  log "all nodes stopped; starting them back up"
  docker start mysql-1 mysql-2 mysql-3 >/dev/null

  wait_until 300 "group reformed after cold restart" group_is_fully_online mysql-1 \
    || { bad "group did not reform after cold restart"; return; }
  ok "group reformed after full outage"

  local v
  v="$(sql mysql-1 "SELECT v FROM t.kv WHERE k=2")"
  if [ "$v" = "post-failover" ]; then
    ok "data survived the cold restart"
  else
    bad "data lost after cold restart (got: '$v')"
  fi

  local codes
  codes="$(role_code mysql-2 mysql-1)/$(role_code mysql-2 mysql-2)/$(role_code mysql-2 mysql-3)"
  local twohundreds
  twohundreds="$(echo "$codes" | tr '/' '\n' | grep -c 200)"
  if [ "$twohundreds" = "1" ]; then
    ok "exactly one primary after cold restart ($codes)"
  else
    bad "expected exactly one 200 after cold restart, got $codes"
  fi
}

t_conversion_adopts_standalone_volume() {
  log "t_conversion_adopts_standalone_volume (fresh environment)"
  teardown_trio

  # Seed a volume the way Railway's standalone mysql template leaves it:
  # official image, binlog disabled, performance_schema off — so the data has
  # NO GTID history at all.
  docker volume create --label "$LABEL" mysql-ha-e2e-vol-1 >/dev/null
  docker run -d --label "$LABEL" --name seed-mysql --network "$NET" \
    -v mysql-ha-e2e-vol-1:/var/lib/mysql \
    -e MYSQL_ROOT_PASSWORD="$ROOT_PW" -e MYSQL_DATABASE=railway \
    "mysql:${MYSQL_VERSION}" \
    mysqld --disable-log-bin --performance_schema=0 >/dev/null

  wait_until 240 "standalone seed mysqld up" \
    bash -c 'docker exec seed-mysql mysql -uroot -p'"$ROOT_PW"' -e "SELECT 1" >/dev/null 2>&1' \
    || { bad "standalone seed never came up"; return; }

  docker exec seed-mysql mysql -uroot -p"$ROOT_PW" -e \
    "CREATE TABLE railway.legacy (id INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO railway.legacy VALUES (1, 'pre-conversion');" 2>/dev/null
  docker stop seed-mysql >/dev/null && docker rm seed-mysql >/dev/null
  ok "standalone volume seeded (binlog off, 1 row of base data)"

  # Convert: node 1 adopts the volume, nodes 2/3 are fresh. The fresh nodes
  # must CLONE (binlog recovery cannot reconstruct never-binlogged data) —
  # each clone shuts its server down and rides the restart policy back up.
  start_node 1
  start_node 2
  start_node 3

  wait_until 420 "converted group fully ONLINE" group_is_fully_online mysql-1 \
    || { bad "converted group never reached 3 ONLINE"; return; }
  ok "adopted volume bootstrapped a group; fresh nodes provisioned"

  for n in mysql-2 mysql-3; do
    local v
    v="$(sql "$n" "SELECT v FROM railway.legacy WHERE id=1")"
    if [ "$v" = "pre-conversion" ]; then
      ok "base (never-binlogged) data present on $n"
    else
      bad "base data MISSING on $n (got: '$v') — clone path failed"
    fi
  done

  sql mysql-1 "INSERT INTO railway.legacy VALUES (2, 'post-conversion');"
  wait_until 60 "post-conversion write replicated" \
    bash -c '[ "$(docker exec mysql-3 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM railway.legacy WHERE id=2" 2>/dev/null)" = "post-conversion" ]' \
    || { bad "post-conversion write did not replicate"; return; }
  ok "post-conversion writes replicate"

  if [ "$(role_code mysql-2 mysql-1)" = "200" ]; then
    ok "adopting node is the primary (/role 200)"
  else
    bad "adopting node is not the primary"
  fi
}

# gr_state_field <from-node> <target-node> <json-field> — value of one field
# from the target's /gr/state, probed from another node's container (no host
# networking assumed). Empty string on any non-200/unreachable/unparseable
# answer.
gr_state_field() {
  local body
  body="$(docker exec "$1" wget -q -O - "http://$2:8080/gr/state" 2>/dev/null)" || return 0
  echo "$body" | grep -o "\"$3\":[a-zA-Z0-9]*" | head -1 | cut -d: -f2
}

# gr_state_code <from-node> <target-node> — like role_code, for /gr/state:
# "200" or "not-200" (connection-refused, not-yet-listening, and 503 are all
# indistinguishable via wget's exit code alone, and all equally count as
# "did not leak a premature answer" for this test's purposes).
gr_state_code() {
  if docker exec "$1" wget -q -O /dev/null "http://$2:8080/gr/state" 2>/dev/null; then
    echo 200
  else
    echo not-200
  fi
}

# t_adoption_survives_seed_disadvantaged_race — the adopted node is placed
# LAST in seed order (worst possible tie-break priority) and its adoption
# detection is artificially stalled well past the fresh pair's poll cadence
# and bootstrap dwell. Reproduces the exact race the pre-GTID-data tie-break
# exists to prevent: an about-to-be-adopted node's own /gr/state answering
# "empty dataset, no pre-GTID data" (true, until its one-time detection step
# runs) instead of "not ready" — which used to let the fresh nodes, seeing
# every peer "answer", form a group before ever comparing against the real
# data. The fix (adoption_checked gating /gr/state) refuses to answer AT ALL
# until detection completes, forcing the fresh pair through Undecidable
# instead. Assertion is against the PRIMARY specifically — that's the node
# every client actually talks to.
t_adoption_survives_seed_disadvantaged_race() {
  local t=t_adoption_survives_seed_disadvantaged_race
  teardown_trio
  # Adopted node (mysql-1) declared LAST — seed_rank 2, the worst priority.
  # A tie-break that (incorrectly) fell back to seed order alone would hand
  # bootstrap candidacy to mysql-2 (rank 0), never to the adopted node.
  local SEEDS="mysql-2:3306,mysql-3:3306,mysql-1:3306"

  docker volume create --label "$LABEL" mysql-ha-e2e-vol-1 >/dev/null
  docker run -d --label "$LABEL" --name seed-mysql --network "$NET" \
    -v mysql-ha-e2e-vol-1:/var/lib/mysql \
    -e MYSQL_ROOT_PASSWORD="$ROOT_PW" -e MYSQL_DATABASE=railway \
    "mysql:${MYSQL_VERSION}" \
    mysqld --disable-log-bin --performance_schema=0 >/dev/null
  wait_until 240 "standalone seed mysqld up" \
    bash -c 'docker exec seed-mysql mysql -uroot -p'"$ROOT_PW"' -e "SELECT 1" >/dev/null 2>&1' \
    || { bad "$t" "standalone seed never came up"; return; }
  docker exec seed-mysql mysql -uroot -p"$ROOT_PW" -e \
    "CREATE TABLE railway.legacy (id INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO railway.legacy VALUES (1, 'pre-conversion');" 2>/dev/null
  docker stop seed-mysql >/dev/null && docker rm seed-mysql >/dev/null
  ok "standalone volume seeded (binlog off, 1 row of base data)"

  # Stall mysql-1's adoption detection well past BOOTSTRAP_DWELL_SECONDS (5s)
  # and several POLL_INTERVAL (3s) cycles, so the fresh pair has every
  # opportunity to observe a stable (wrong, pre-fix) verdict and act on it.
  start_node 1 -e RAILWAY_TEST_ADOPTION_DETECTION_DELAY_MS=60000
  start_node 2
  start_node 3

  # Mechanism-level check, direct: mysql-1's own /gr/state must never answer
  # 200 for the whole stall — the gate this test exists to pin. Bounded to
  # 45s (comfortably inside the 60s stall, wide margin on both sides) so it
  # can only observe pre-gate behavior, not the legitimate 200 that follows
  # once the stall elapses.
  local leaked_early_answer=1 elapsed=0
  while [ "$elapsed" -lt 45 ]; do
    if [ "$(gr_state_code mysql-2 mysql-1)" = "200" ]; then
      leaked_early_answer=0
      break
    fi
    sleep 2
    elapsed=$((elapsed+2))
  done
  if [ "$leaked_early_answer" -eq 0 ]; then
    bad "$t" "mysql-1's /gr/state answered 200 during the stall — adoption_checked gate did not hold"
  else
    ok "mysql-1's /gr/state stayed non-200 for the whole probed stall window"
  fi

  # Wide margin on purpose: mysqld for the fresh pair is typically reachable
  # within ~10s of container start, leaving the rest of this 45s budget for
  # several full POLL_INTERVAL (3s) cycles to satisfy BOOTSTRAP_DWELL_SECONDS
  # (5s) well before the 60s stall lifts — no ambiguity between "the race
  # didn't have time to manifest" and "the fix actually holds".
  wait_until 45 "a 2-node group forms from the fresh pair, excluding mysql-1, while it is stalled" \
    group_online_excluding mysql-2 2 mysql-1
  local fresh_pair_bootstrapped=$?
  if [ "$fresh_pair_bootstrapped" -eq 0 ]; then
    bad "$t" "mysql-2/mysql-3 formed a 2-node group WITHOUT the adopted node while it was still stalled"
  fi

  # Generous on purpose: the stall alone is 60s, and if the fresh pair did
  # bootstrap early, correcting requires the adopted node to bootstrap ITS
  # OWN group after the stall and both fresh nodes to clone off it
  # sequentially (MySQL caps concurrent clones per donor at one) — full
  # datadir replace + container restart per node. The existing (unstalled,
  # single-clone-donor) conversion test already budgets 420s for one clone;
  # this one can need two, after a 60s head start.
  wait_until 500 "converged group fully ONLINE" group_is_fully_online mysql-2 \
    || { bad "$t" "group never reached 3 ONLINE after the stall elapsed"; return; }

  local primary
  primary="$(current_primary mysql-2 mysql-1 mysql-2 mysql-3)"
  [ -n "$primary" ] || { bad "$t" "no primary elected"; return; }

  local v
  v="$(sql "$primary" "SELECT v FROM railway.legacy WHERE id=1" 2>/dev/null)"
  if [ "$v" = "pre-conversion" ]; then
    ok "pre-conversion data present on the primary ($primary)"
  else
    bad "$t" "pre-conversion data MISSING on the primary ($primary) (got: '$v') — adopted data orphaned or lost"
  fi
}

t_scale_up_to_five() {
  log "t_scale_up_to_five (reuses the converted trio — new nodes must clone)"
  has_n_online mysql-1 3 || { bad "no 3-node group to scale"; return; }

  # New nodes get the SAME template-stamped seed list (the original trio) —
  # matching Railway's scale flow, where existing nodes are not re-stamped.
  start_node 4
  start_node 5

  wait_until 420 "5 ONLINE members" has_n_online mysql-1 5 \
    || { bad "scale-up never reached 5 ONLINE"; return; }
  ok "scaled 3 → 5 (fresh nodes provisioned by clone)"

  local v
  v="$(sql mysql-4 "SELECT v FROM railway.legacy WHERE id=1")"
  if [ "$v" = "pre-conversion" ]; then
    ok "never-binlogged base data present on scale-up node"
  else
    bad "scale-up node missing base data (got: '$v')"
  fi

  sql mysql-1 "INSERT INTO railway.legacy VALUES (3, 'post-scale') ON DUPLICATE KEY UPDATE v='post-scale';"
  wait_until 60 "write replicated to node 5" \
    bash -c '[ "$(docker exec mysql-5 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM railway.legacy WHERE id=3" 2>/dev/null)" = "post-scale" ]' \
    || { bad "write did not reach node 5"; return; }
  ok "writes replicate across all 5"
}

t_minority_partition_write_fence() {
  log "t_minority_partition_write_fence (reuses the 5-node group)"
  has_n_online mysql-1 5 || { bad "no 5-node group to partition"; return; }

  # Isolate the current primary: 1 vs 4 — the isolated side must fence.
  local primary
  primary="$(current_primary mysql-2 mysql-1 mysql-2 mysql-3 mysql-4 mysql-5)"
  [ -n "$primary" ] || { bad "no primary found pre-partition"; return; }
  log "partitioning primary $primary away from the group"
  docker network disconnect "$NET" "$primary"

  # The isolated ex-primary must fail its own /role (fence) — probed from
  # inside itself, since the network path to it is gone.
  wait_until 120 "isolated primary fences itself" \
    bash -c '! docker exec '"$primary"' wget -q -O /dev/null http://localhost:8080/role 2>/dev/null' \
    || { bad "isolated primary still answers /role 200 (split-brain window)"; docker network connect "$NET" "$primary"; return; }
  ok "isolated primary fenced (/role 503 on itself)"

  # The majority side must elect a replacement.
  local survivors=()
  for n in mysql-1 mysql-2 mysql-3 mysql-4 mysql-5; do
    [ "$n" = "$primary" ] || survivors+=("$n")
  done
  wait_until 120 "majority side elects a new primary" \
    bash -c 'for n in '"${survivors[*]}"'; do docker exec '"${survivors[0]}"' wget -q -O /dev/null "http://$n:8080/role" 2>/dev/null && exit 0; done; exit 1' \
    || { bad "majority side never elected a primary"; docker network connect "$NET" "$primary"; return; }
  local new_primary
  new_primary="$(current_primary "${survivors[0]}" "${survivors[@]}")"
  ok "majority elected new primary: $new_primary"

  sql "$new_primary" "INSERT INTO railway.legacy VALUES (4, 'during-partition') ON DUPLICATE KEY UPDATE v='during-partition';" \
    && ok "majority side accepts writes during the partition" \
    || bad "majority side refused a write during the partition"

  # Heal: reconnect + restart (Railway-style) — the ex-primary must come back
  # as a secondary with the partition-era write present.
  docker network connect "$NET" "$primary"
  docker restart "$primary" >/dev/null
  wait_until 420 "partitioned node rejoined (5 ONLINE)" has_n_online "$new_primary" 5 \
    || { bad "partitioned ex-primary did not rejoin"; return; }
  ok "partitioned ex-primary rejoined"

  wait_until 60 "partition-era write visible on rejoined node" \
    bash -c '[ "$(docker exec '"$primary"' mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM railway.legacy WHERE id=4" 2>/dev/null)" = "during-partition" ]' \
    || { bad "rejoined node missing partition-era write"; return; }
  ok "rejoined node caught up on partition-era writes"
}

# The LTS one step below the wrapper's series — the version a real customer's
# standalone is likely to be on when they convert. Seeding with THIS and
# booting the wrapper forces the cross-version InnoDB data-dictionary upgrade.
seed_prev_version() {
  case "$MYSQL_VERSION" in
    8.4) echo "8.0" ;;
    9.4) echo "8.4" ;;
    *)   echo "8.0" ;;
  esac
}

t_conversion_cross_version_upgrade() {
  local prev; prev="$(seed_prev_version)"
  log "t_conversion_cross_version_upgrade (seed mysql:$prev -> wrapper $MYSQL_VERSION)"
  teardown_trio

  # The failure mode this guards is the MySQL analogue of redis-ha's RDB
  # discovery: persistence formats change across versions, and a single-
  # version test never exercises the upgrade path. Railway's standalone
  # template floats forward, but a customer converting from an OLDER series
  # boots the (newer) HA wrapper against an older datadir — the adopting node
  # must run the InnoDB data-dictionary upgrade on start, and the pre-upgrade
  # data must survive it and clone-propagate to the fresh members.
  docker volume create --label "$LABEL" mysql-ha-e2e-vol-1 >/dev/null
  docker run -d --label "$LABEL" --name seed-mysql --network "$NET" \
    -v mysql-ha-e2e-vol-1:/var/lib/mysql \
    -e MYSQL_ROOT_PASSWORD="$ROOT_PW" -e MYSQL_DATABASE=railway \
    "mysql:$prev" \
    mysqld --disable-log-bin --performance_schema=0 >/dev/null

  wait_until 240 "old-version seed mysqld up" \
    bash -c 'docker exec seed-mysql mysql -uroot -p'"$ROOT_PW"' -e "SELECT 1" >/dev/null 2>&1' \
    || { bad "old-version seed never came up"; return; }

  local seeded_ver
  seeded_ver="$(docker exec seed-mysql mysql -uroot -p"$ROOT_PW" --batch --skip-column-names -e "SELECT @@version" 2>/dev/null)"
  docker exec seed-mysql mysql -uroot -p"$ROOT_PW" -e \
    "CREATE TABLE railway.legacy (id INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO railway.legacy VALUES (1, 'pre-upgrade');" 2>/dev/null
  docker stop seed-mysql >/dev/null && docker rm seed-mysql >/dev/null
  ok "seeded a mysql:$prev standalone volume (server $seeded_ver)"

  start_node 1
  start_node 2
  start_node 3

  wait_until 480 "cross-version converted group fully ONLINE" group_is_fully_online mysql-1 \
    || { bad "cross-version conversion never reached 3 ONLINE (data-dictionary upgrade may have wedged the adopting node)"; return; }
  ok "old-version volume upgraded in place + bootstrapped a group"

  # The upgrade must have actually run — the adopting node now reports the
  # wrapper's series, not the seeded one.
  local now_ver
  now_ver="$(sql mysql-1 "SELECT @@version")"
  if printf '%s' "$now_ver" | grep -q "^$MYSQL_VERSION"; then
    ok "adopting node upgraded $seeded_ver -> $now_ver"
  else
    bad "adopting node did not upgrade (still $now_ver, expected $MYSQL_VERSION.x)"
  fi

  # The pre-upgrade row must survive the data-dictionary upgrade AND clone to
  # the fresh members.
  local v1
  v1="$(sql mysql-1 "SELECT v FROM railway.legacy WHERE id=1")"
  [ "$v1" = "pre-upgrade" ] \
    && ok "pre-upgrade data survived the in-place upgrade on the primary" \
    || bad "pre-upgrade data LOST on the primary (got: '$v1')"

  for n in mysql-2 mysql-3; do
    local v
    v="$(sql "$n" "SELECT v FROM railway.legacy WHERE id=1")"
    [ "$v" = "pre-upgrade" ] \
      && ok "pre-upgrade data cloned to $n across the version bump" \
      || bad "pre-upgrade data MISSING on $n (got: '$v')"
  done

  sql mysql-1 "INSERT INTO railway.legacy VALUES (2, 'post-upgrade');"
  wait_until 60 "post-upgrade write replicated" \
    bash -c '[ "$(docker exec mysql-3 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM railway.legacy WHERE id=2" 2>/dev/null)" = "post-upgrade" ]' \
    && ok "writes replicate on the upgraded cluster" \
    || bad "post-upgrade write did not replicate"
}

t_patch_skew_on_redeploy() {
  local old_image="mysql-ha-e2e:8.4.0"
  log "t_patch_skew_on_redeploy (group on 8.4.0; one member redeploys onto $IMAGE)"
  docker image inspect "$old_image" >/dev/null 2>&1 || {
    log "building $old_image"
    docker build -t "$old_image" -f mysql-wrapper/Dockerfile \
      --build-arg MYSQL_VERSION=8.4.0 . || { bad "old-patch image build failed"; return; }
  }
  teardown_trio

  # Production reality this reproduces: data nodes carry the moving :8.4 tag
  # with NO auto-update, but ANY redeploy re-pulls the tag — a user redeploy,
  # a scale-up node, or the fleet monitor's crashed-node deploy-latest
  # self-heal — so one member lands on the newest patch while its siblings
  # stay on whatever they pulled at deploy time.
  NODE_IMAGE="$old_image" start_node 1
  NODE_IMAGE="$old_image" start_node 2
  NODE_IMAGE="$old_image" start_node 3

  wait_until 300 "3 ONLINE on the old patch" group_is_fully_online mysql-1 \
    || { bad "old-patch group never formed"; return; }
  local before_ver
  before_ver="$(sql mysql-1 "SELECT @@version")"
  sql mysql-1 "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (1,'pre-skew') ON DUPLICATE KEY UPDATE v='pre-skew';"
  ok "group formed on $before_ver with data"

  # "Redeploy" mysql-2: same volume, image = the current tag. The datadir
  # patch-upgrades in place, then the higher-patch member rejoins the group.
  docker rm -f mysql-2 >/dev/null 2>&1
  start_node 2

  wait_until 420 "skewed member rejoined (3 ONLINE)" group_is_fully_online mysql-1 \
    || { bad "higher-patch member did not rejoin a lower-patch group"; return; }
  # Membership is observed from mysql-1; mysql-2's own SQL port lags ONLINE by
  # a few seconds (clone/upgrade restart window), so poll it before asserting.
  wait_until 120 "skewed member answers SQL locally" \
    bash -c '[ -n "$(docker exec mysql-2 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT 1" 2>/dev/null)" ]' \
    || { bad "rejoined member never accepted local SQL"; return; }
  local v2_ver
  v2_ver="$(sql mysql-2 "SELECT @@version")"
  ok "mixed-patch group healthy: mysql-2 on $v2_ver, siblings on $before_ver"

  local v
  v="$(sql mysql-2 "SELECT v FROM t.kv WHERE k=1")"
  [ "$v" = "pre-skew" ] \
    && ok "data intact on the patch-upgraded member" \
    || bad "data missing on patch-upgraded member (got '$v')"

  sql mysql-1 "INSERT INTO t.kv VALUES (2,'during-skew') ON DUPLICATE KEY UPDATE v='during-skew';"
  # A just-rejoined higher-patch member finishes distributed recovery and
  # applies this write within seconds normally, but under the full suite's
  # CPU contention certification/apply can lag — a tight window here used to
  # time out and then cascade into the rollback probe below (which fails on a
  # still-stabilizing group). return on failure so one slow apply can't be
  # read as two separate regressions.
  wait_until 240 "write replicates across the patch skew" \
    bash -c '[ "$(docker exec mysql-2 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=2" 2>/dev/null)" = "during-skew" ]' \
    && ok "writes replicate in the mixed-patch group" \
    || { bad "replication broken across patch skew"; return; }

  # Rollback probe — a deployment ROLLBACK boots the older binary against
  # the now-upgraded datadir. Within an LTS series this is SUPPORTED: the
  # server performs an automatic in-place downgrade on boot ("[MY-014064]
  # Server downgrade from X to Y"), rejoins, and keeps its data — verified
  # 2026-08-10 for both clean-shutdown and kill (rm -f, crash-recovery)
  # paths. An earlier revision of this probe waited only 20s, read the
  # mid-downgrade member as a refusal, and "documented" a hazard that does
  # not exist in the LTS model — this locks the REAL behavior instead.
  # (Cross-SERIES moves remain one-way — dump/reload only — but the image
  # tags pin the series, so no redeploy ever crosses that boundary.)
  docker rm -f mysql-2 >/dev/null 2>&1
  NODE_IMAGE="$old_image" start_node 2
  # Order matters: check mysql-2's OWN socket first, not mysql-1's group
  # view. group_is_fully_online reads mysql-1's membership table, which can
  # read stale-ONLINE for an instant right after `docker rm -f` — GR's
  # failure detection on mysql-1 hasn't necessarily flagged the removed peer
  # UNREACHABLE yet, so `wait_until`'s un-slept first check can pass before
  # mysql-2 has even started booting, let alone reached the downgrade step.
  # Waiting on mysql-2 answering SQL directly is real: it can only happen
  # after mysqld finishes init (downgrade included).
  wait_until 120 "rolled-back member answers SQL locally" \
    bash -c '[ -n "$(docker exec mysql-2 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT 1" 2>/dev/null)" ]' \
    || { bad "rolled-back member never booted (automatic in-place downgrade may have failed)"; return; }
  if docker logs mysql-2 2>&1 | grep -q "Server downgrade from"; then
    ok "server performed the automatic in-place downgrade on boot"
  else
    bad "member booted but no 'Server downgrade' log line — behavior changed, investigate"
  fi
  wait_until 420 "rolled-back member rejoined the group (3 ONLINE)" group_is_fully_online mysql-1 \
    || { bad "rolled-back member did not rejoin the group"; return; }
  local back_ver
  back_ver="$(sql mysql-2 "SELECT @@version")"
  printf '%s' "$back_ver" | grep -q "^8.4.0" \
    && ok "member is back on the rolled-back patch ($back_ver)" \
    || bad "expected 8.4.0 after rollback, got '$back_ver'"
  local v
  v="$(sql mysql-2 "SELECT v FROM t.kv WHERE k=2")"
  [ "$v" = "during-skew" ] \
    && ok "data intact across upgrade + rollback" \
    || bad "data missing after rollback (got '$v')"

  # And rolling forward again onto the current tag must also work.
  docker rm -f mysql-2 >/dev/null 2>&1
  start_node 2
  wait_until 420 "member re-upgraded onto the current patch" group_is_fully_online mysql-1 \
    && ok "re-redeploy onto the current patch works after a rollback" \
    || bad "member did not recover after returning to the current patch"
}

t_total_outage_after_failover() {
  log "t_total_outage_after_failover (first seed is BEHIND at the outage; ex-primary must bootstrap)"
  teardown_trio
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  sql mysql-1 "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (1,'pre-failover') ON DUPLICATE KEY UPDATE v='pre-failover';"

  # Fail over away from the first seed, then write — mysql-1 is now STRICTLY
  # behind the surviving members.
  docker stop mysql-1 >/dev/null
  wait_until 120 "failover away from the first seed" \
    bash -c 'docker exec mysql-2 wget -q -O /dev/null http://mysql-2:8080/role 2>/dev/null || docker exec mysql-2 wget -q -O /dev/null http://mysql-3:8080/role 2>/dev/null' \
    || { bad "no failover after stopping the first seed"; return; }
  local new_primary
  new_primary="$(current_primary mysql-2 mysql-2 mysql-3)"
  sql "$new_primary" "INSERT INTO t.kv VALUES (2,'post-failover') ON DUPLICATE KEY UPDATE v='post-failover';"
  ok "failed over to $new_primary and wrote while the first seed was down"

  # Total outage with that skew in place. With a FIXED bootstrap candidate
  # this deadlocks on restart: only the first seed may bootstrap, but it
  # refuses (its peers are ahead) — dynamic candidacy must let the
  # most-advanced node bootstrap instead, with no operator action.
  docker stop mysql-2 mysql-3 >/dev/null
  docker start mysql-1 mysql-2 mysql-3 >/dev/null

  wait_until 300 "group reformed with the first seed behind" group_is_fully_online mysql-2 \
    || { bad "group did not re-form after a total outage with a behind first seed (bootstrap deadlock)"; return; }
  ok "group re-formed without operator action"

  wait_until 60 "post-failover write visible on the caught-up first seed" \
    bash -c '[ "$(docker exec mysql-1 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=2" 2>/dev/null)" = "post-failover" ]' \
    || { bad "first seed missing the post-failover write after recovery"; return; }
  ok "first seed caught up on the writes it missed"

  local codes
  codes="$(role_code mysql-2 mysql-1)/$(role_code mysql-2 mysql-2)/$(role_code mysql-2 mysql-3)"
  local twohundreds
  twohundreds="$(echo "$codes" | tr '/' '\n' | grep -c 200)"
  [ "$twohundreds" = "1" ] \
    && ok "exactly one primary after recovery ($codes)" \
    || bad "expected exactly one primary after recovery, got $codes"
}

t_first_seed_permanent_loss() {
  log "t_first_seed_permanent_loss (volume destroyed; survivors re-form once a fresh node answers)"
  teardown_trio
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  sql mysql-1 "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (1,'survives-loss') ON DUPLICATE KEY UPDATE v='survives-loss';"
  wait_until 60 "seed write replicated" \
    bash -c '[ "$(docker exec mysql-3 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=1" 2>/dev/null)" = "survives-loss" ]' \
    || { bad "seed write never replicated"; return; }

  # Total outage during which the first seed's VOLUME is destroyed — the
  # permanent-loss worst case: the survivors hold quorum and all the data,
  # but with a fixed candidate nothing may ever bootstrap again.
  docker stop mysql-2 mysql-3 >/dev/null
  docker rm -f mysql-1 >/dev/null 2>&1
  docker volume rm mysql-ha-e2e-vol-1 >/dev/null 2>&1
  docker start mysql-2 mysql-3 >/dev/null

  # Fail-closed while the lost peer is unreachable: its dataset can't be
  # compared, so nobody may bootstrap yet.
  sleep 30
  local codes
  codes="$(role_code mysql-2 mysql-2)/$(role_code mysql-2 mysql-3)"
  [ "$codes" = "503/503" ] \
    && ok "survivors hold fail-closed while the lost peer is unreachable ($codes)" \
    || bad "a survivor bootstrapped past an unreachable peer ($codes)"

  # The platform (a redeploy, or the fleet monitor's crashed-node self-heal)
  # brings a FRESH first seed back. It answers "empty dataset, no group" —
  # now every peer is comparable and a data-holding survivor must bootstrap.
  start_node 1
  wait_until 420 "group re-formed around the surviving data" group_is_fully_online mysql-2 \
    || { bad "survivors never re-formed after the fresh node answered (bootstrap deadlock)"; return; }
  ok "survivors re-formed the group"

  wait_until 120 "data recovered onto the fresh first seed" \
    bash -c '[ "$(docker exec mysql-1 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=1" 2>/dev/null)" = "survives-loss" ]' \
    || { bad "fresh first seed did not recover the surviving data"; return; }
  ok "fresh first seed recovered the dataset"

  local all_codes
  all_codes="$(role_code mysql-2 mysql-1)/$(role_code mysql-2 mysql-2)/$(role_code mysql-2 mysql-3)"
  local n200
  n200="$(echo "$all_codes" | tr '/' '\n' | grep -c 200)"
  [ "$n200" = "1" ] \
    && ok "exactly one primary after the loss recovery ($all_codes)" \
    || bad "expected exactly one primary, got $all_codes"
}

t_password_variable_edit_does_not_rotate() {
  log "t_password_variable_edit_does_not_rotate (env drift must not lock the wrapper out)"
  teardown_trio
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  sql mysql-1 "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (10,'before-drift') ON DUPLICATE KEY UPDATE v='before-drift';"

  # A fresh cluster must have persisted the pin — that is what survives the
  # variable edit below.
  docker exec mysql-1 test -f /var/lib/mysql/.railway_active_root_password \
    && ok "active-password pin persisted on the volume" \
    || bad "pin file missing after a healthy boot"

  # "Edit the variable": every node redeploys with a NEW env password while
  # the datadir keeps enforcing the real one (docker-entrypoint only applies
  # MYSQL_ROOT_PASSWORD at first init). Pre-pin, this locked the wrapper out
  # of its own mysqld on all three nodes at once: /health and /role both
  # 503'd everywhere and HAProxy dropped the whole backend set.
  docker rm -f mysql-1 mysql-2 mysql-3 >/dev/null 2>&1
  local real_pw="$ROOT_PW"
  ROOT_PW="rotated-by-variable-edit"
  start_trio
  ROOT_PW="$real_pw"

  # The wait itself is the assertion: online_members authenticates with the
  # ORIGINAL password, and /role can only answer 200 if the wrapper's own
  # connection (pinned) works.
  wait_until 300 "group re-forms on the pinned password" group_is_fully_online mysql-1 \
    || { bad "group never re-formed after the variable edit (wrapper locked out?)"; return; }

  local primary
  primary="$(current_primary mysql-2 mysql-1 mysql-2 mysql-3)" \
    && ok "a writable primary is being served ($primary)" \
    || { bad "no node answers /role 200 after the variable edit"; return; }

  [ "$(sql mysql-1 "SELECT v FROM t.kv WHERE k=10")" = "before-drift" ] \
    && ok "original password still authenticates and the data is intact" \
    || bad "original password no longer reads the dataset"

  # The drifted value must NOT have become the live credential.
  if docker exec mysql-1 mysql -uroot -p"rotated-by-variable-edit" -e "SELECT 1" >/dev/null 2>&1; then
    bad "the drifted env password authenticates — the edit rotated the live credential"
  else
    ok "the drifted env password does not authenticate (edit was a no-op)"
  fi

  # Writes still flow and replicate on the pinned credential.
  sql "$primary" "INSERT INTO t.kv VALUES (11,'after-drift') ON DUPLICATE KEY UPDATE v='after-drift';"
  wait_until 60 "post-drift write replicated" \
    bash -c '[ "$(docker exec mysql-3 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=11" 2>/dev/null)" = "after-drift" ]' \
    || { bad "post-drift write never replicated"; return; }
  ok "writes replicate after the drifted redeploy"

  docker logs mysql-1 2>&1 | grep -q "MYSQL_ROOT_PASSWORD differs from the active root password" \
    && ok "wrapper warned about the drifted variable" \
    || bad "no drift warning in the wrapper log"
}

t_sigterm_primary_demotes_before_exit() {
  log "t_sigterm_primary_demotes_before_exit (planned shutdown = switchover, not timeout failover)"
  teardown_trio
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  local old_primary
  old_primary="$(current_primary mysql-2 mysql-1 mysql-2 mysql-3)" \
    || { bad "no primary to stop"; return; }
  sql "$old_primary" "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (30,'before-demote') ON DUPLICATE KEY UPDATE v='before-demote';"

  # Planned shutdown: SIGTERM with a stop budget above the demote deadline
  # (20s) plus mysqld's own exit (docker's default 10s would SIGKILL through
  # the handoff).
  docker stop -t 60 "$old_primary" >/dev/null

  docker logs "$old_primary" 2>&1 | grep -q "demoted before shutdown: primary handed off" \
    && ok "outgoing primary handed the role off before exiting" \
    || bad "no demote-on-shutdown log line — shutdown paid a timeout failover"

  # The handoff is synchronous inside the stop: a survivor must already be
  # (or within seconds become) the writable primary.
  local survivors=()
  local n
  for n in mysql-1 mysql-2 mysql-3; do
    [ "$n" != "$old_primary" ] && survivors+=("$n")
  done
  new_primary_is_serving() { current_primary "${survivors[0]}" "${survivors[@]}" >/dev/null; }
  wait_until 60 "a survivor serves as primary" new_primary_is_serving \
    || { bad "no survivor became primary after the planned shutdown"; return; }
  local new_primary
  new_primary="$(current_primary "${survivors[0]}" "${survivors[@]}")"
  ok "survivor $new_primary is the writable primary"

  sql "$new_primary" "INSERT INTO t.kv VALUES (31,'after-demote') ON DUPLICATE KEY UPDATE v='after-demote';"

  # The old primary rejoins as a SECONDARY — no failback, and the data
  # written while it was away reaches it.
  docker start "$old_primary" >/dev/null
  wait_until 300 "full group re-forms" group_is_fully_online "$new_primary" \
    || { bad "old primary never rejoined"; return; }
  [ "$(role_code "$new_primary" "$old_primary")" = "503" ] \
    && ok "old primary rejoined as a secondary (no failback)" \
    || bad "old primary reclaimed the primary role on rejoin"
  wait_until 60 "away-write replicated to the rejoined node" \
    bash -c '[ "$(docker exec '"$old_primary"' mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=31" 2>/dev/null)" = "after-demote" ]' \
    || { bad "write from the away window never reached the rejoined node"; return; }
  ok "rejoined node caught up with the away-window write"
}

# The platform's graceful double failure (issue #31): a secondary and then
# the primary stop the way removeDeployment stops them (SIGTERM). The
# survivor must fence every write while it stands alone — and once both
# members RETURN, the wrapper's majority-loss recovery must force the
# blocked view down and let them rejoin with no human step. Born RED on the
# pre-fix wrapper: the survivor waits forever for a majority that can never
# form, the joiners loop on failed joins, and only a manual
# group_replication_force_members unwedges it.
t_graceful_double_stop_reforms() {
  log "t_graceful_double_stop_reforms (survivor fences alone, group self-reforms on return)"
  teardown_trio
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  local primary
  primary="$(current_primary mysql-2 mysql-1 mysql-2 mysql-3)" \
    || { bad "no primary to stop"; return; }
  local victim survivor
  case "$primary" in
    mysql-1) victim=mysql-2; survivor=mysql-3 ;;
    mysql-2) victim=mysql-1; survivor=mysql-3 ;;
    *)       victim=mysql-1; survivor=mysql-2 ;;
  esac
  sql "$primary" "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (40,'pre-double-stop') ON DUPLICATE KEY UPDATE v='pre-double-stop';"
  log "primary=$primary victim=$victim survivor=$survivor — stopping the victim, then the primary"

  # Stagger like the production flow: the secondary first (the live majority
  # expels it cleanly), the primary second. The primary goes down HARD
  # (SIGKILL): the platform's stop grace is short enough that the GR leave
  # handshake doesn't complete, so the survivor keeps the dead primary in
  # its view as UNREACHABLE and loses majority — the exact live-probed shape
  # of issue #31. (A stop with generous grace instead completes a clean
  # leave, the view shrinks to one, and the survivor becomes a legitimately
  # writable single-member group — a DIFFERENT contract gap, tracked
  # separately: the platform fence must hold there too.)
  docker stop -t 60 "$victim" >/dev/null
  wait_until 120 "victim's departure settles to a 2-member view" \
    bash -c '[ "$(docker exec '"$survivor"' mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT COUNT(*) FROM performance_schema.replication_group_members" 2>/dev/null | tr -d "[:space:]")" = "2" ]' \
    || { bad "double-stop: victim was never expelled from the view"; return; }
  docker kill "$primary" >/dev/null

  # Alone, the survivor must refuse primacy and writes for as long as the
  # peers stay gone (this is t_paused_peer_keeps_the_fence's property, held
  # here through a DEPARTURE instead of a pause).
  sleep 10
  local i
  for i in 1 2 3; do
    if [ "$(role_code "$survivor" "$survivor")" = "200" ]; then
      bad "double-stop: survivor answered /role 200 while standing alone"
      return
    fi
    sleep 5
  done
  if timeout 15 docker exec "$survivor" mysql -uroot -p"$ROOT_PW" \
      -e "INSERT INTO t.kv VALUES (41,'must-not-land')" >/dev/null 2>&1; then
    bad "double-stop: survivor ACCEPTED a write while standing alone"
    return
  fi
  ok "survivor stayed fenced while both peers were gone"

  # Both return. The survivor's majority_watch needs each returning wrapper
  # to answer /gr/state under this group's identity, dwells on the proof,
  # forces the view down, and the joiners then rejoin normally. Budget:
  # wrapper boot + proof dwell (30s) + join/recovery.
  docker start "$victim" "$primary" >/dev/null
  wait_until 420 "group self-reforms to 3 ONLINE with no manual step" group_is_fully_online "$survivor" \
    || { bad "double-stop: group never reformed after both members returned (issue #31 wedge)"; return; }
  docker logs "$survivor" 2>&1 | grep -q "re-forming the group from this lone survivor" \
    && ok "survivor's majority-loss recovery forced the blocked view down" \
    || bad "group reformed but not through majority_watch (no force log line) — the wedge fix was not what resolved it"

  # Data written before the double stop survives, and writes flow again.
  writable_primary_appears() { current_primary "$survivor" mysql-1 mysql-2 mysql-3 >/dev/null; }
  wait_until 90 "a writable primary after the fence lifts" writable_primary_appears \
    || { bad "double-stop: no writable primary after reform"; return; }
  local new_primary
  new_primary="$(current_primary "$survivor" mysql-1 mysql-2 mysql-3)"
  [ "$(sql "$new_primary" "SELECT v FROM t.kv WHERE k=40")" = "pre-double-stop" ] \
    || { bad "double-stop: pre-failure row lost across the reform"; return; }
  sql "$new_primary" "INSERT INTO t.kv VALUES (42,'post-reform') ON DUPLICATE KEY UPDATE v='post-reform';" \
    || { bad "double-stop: post-reform write failed on $new_primary"; return; }
  ok "graceful double stop: fence held alone, group self-reformed, data intact"
}

# The clean-leave sibling of the scenario above (issue #33): both departures
# get a FULL stop grace, so mysqld completes each Group Replication leave
# handshake and the view legitimately shrinks 3 -> 2 -> 1. Without the
# membership fence the lone survivor is then a writable single-member group
# at 1 of 3 declared members — /role 200 and direct writes landing. The
# fence must hold writes however the peers left, and lift on its own once
# the group is back above a declared majority.
t_clean_double_stop_keeps_fence() {
  log "t_clean_double_stop_keeps_fence (clean leaves shrink the view; the membership fence must still hold)"
  teardown_trio
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  local primary
  primary="$(current_primary mysql-2 mysql-1 mysql-2 mysql-3)" \
    || { bad "no primary to stop"; return; }
  local victim survivor
  case "$primary" in
    mysql-1) victim=mysql-2; survivor=mysql-3 ;;
    mysql-2) victim=mysql-1; survivor=mysql-3 ;;
    *)       victim=mysql-1; survivor=mysql-2 ;;
  esac
  sql "$primary" "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (50,'pre-clean-stop') ON DUPLICATE KEY UPDATE v='pre-clean-stop';"
  log "primary=$primary victim=$victim survivor=$survivor — stopping both with full grace"

  # Full grace on BOTH: each departure is a completed clean leave, the shape
  # that shrinks the survivor's view instead of blocking it.
  docker stop -t 60 "$victim" >/dev/null
  docker stop -t 60 "$primary" >/dev/null

  # The fence engages on the watch round after the survivor's election —
  # wait for it (the contract is engage-promptly-then-HOLD, same as the
  # production battery's fence poll), then require it to hold every sample.
  fence_engaged() { [ "$(role_code "$survivor" "$survivor")" != "200" ]; }
  wait_until 60 "membership fence engages on the lone survivor" fence_engaged \
    || { bad "clean-stop: survivor kept answering /role 200 alone (issue #33 gap)"; return; }
  local i
  for i in 1 2 3 4; do
    sleep 5
    if [ "$(role_code "$survivor" "$survivor")" = "200" ]; then
      bad "clean-stop: fence did not HOLD — /role went back to 200 while alone"
      return
    fi
  done
  if timeout 15 docker exec "$survivor" mysql -uroot -p"$ROOT_PW" \
      -e "INSERT INTO t.kv VALUES (51,'must-not-land')" >/dev/null 2>&1; then
    bad "clean-stop: survivor ACCEPTED a direct write while standing alone (issue #33 gap)"
    return
  fi
  docker logs "$survivor" 2>&1 | grep -q "engaging the membership write fence" \
    || { bad "clean-stop: fence held but not through the membership fence (no engage log line)"; return; }
  ok "membership fence held the lone survivor after clean leaves"

  # Both return and simply REJOIN the live (fenced) group — no recovery
  # needed here; the fence must lift on its own at declared majority.
  docker start "$victim" "$primary" >/dev/null
  wait_until 420 "group back to 3 ONLINE" group_is_fully_online "$survivor" \
    || { bad "clean-stop: group did not reform after both members returned"; return; }
  fence_lift_logged() { docker logs "$survivor" 2>&1 | grep -q "membership majority restored"; }
  wait_until 60 "the membership fence reports lifting" fence_lift_logged \
    || { bad "clean-stop: group reformed but the fence never lifted"; return; }

  clean_writable_primary() { current_primary "$survivor" mysql-1 mysql-2 mysql-3 >/dev/null; }
  wait_until 90 "a writable primary after the fence lifts" clean_writable_primary \
    || { bad "clean-stop: no writable primary after the fence lifted"; return; }
  local newp
  newp="$(current_primary "$survivor" mysql-1 mysql-2 mysql-3)"
  [ "$(sql "$newp" "SELECT v FROM t.kv WHERE k=50")" = "pre-clean-stop" ] \
    || { bad "clean-stop: pre-stop row lost"; return; }
  sql "$newp" "INSERT INTO t.kv VALUES (52,'post-lift') ON DUPLICATE KEY UPDATE v='post-lift';" \
    || { bad "clean-stop: write failed after the fence lifted"; return; }
  ok "clean double stop: fence held alone, lifted at majority, data intact"
}

# Both waiver scenarios run their trios under the reserved `.invalid` TLD
# (see start_node's NODE_SUFFIX comment) with a short PEER_GONE_DWELL so the
# dwell fits a test run. They restore the globals and tear down their
# suffixed containers on exit.

t_deleted_peer_unfences_bootstrap() {
  log "t_deleted_peer_unfences_bootstrap (scale-down must not wedge total-outage recovery)"
  # The previous scenario in the chain leaves its unsuffixed trio running
  # (several scenarios end without a teardown, by design, so the next one
  # can reuse a live group). Switching NODE_SUFFIX below would make
  # teardown_trio target the WRONG names and leave that trio running
  # alongside this one — six live mysqld processes fighting over CPU/RAM
  # instead of three, which is exactly what timed the group formation out
  # in CI. Tear it down under its real identity first.
  teardown_trio
  local old_suffix="${NODE_SUFFIX:-}" old_seeds="$SEEDS"
  NODE_SUFFIX=".wv.e2e.invalid"
  SEEDS="mysql-1$NODE_SUFFIX:3306,mysql-2$NODE_SUFFIX:3306,mysql-3$NODE_SUFFIX:3306"
  local n1="mysql-1$NODE_SUFFIX" n2="mysql-2$NODE_SUFFIX" n3="mysql-3$NODE_SUFFIX"

  teardown_trio
  start_node 1 -e PEER_GONE_DWELL_SECONDS=30
  start_node 2 -e PEER_GONE_DWELL_SECONDS=30
  start_node 3 -e PEER_GONE_DWELL_SECONDS=30

  if ! wait_until 300 "3 ONLINE members" group_is_fully_online "$n1"; then
    bad "group never formed"; teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"; return
  fi
  sql "$n1" "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (20,'survives-scale-down') ON DUPLICATE KEY UPDATE v='survives-scale-down';"

  # Scale-down by deletion: the platform removes the service AND its volume;
  # GR_SEEDS on the survivors is never restamped.
  docker rm -f "$n3" >/dev/null 2>&1
  docker volume rm mysql-ha-e2e-vol-3 >/dev/null 2>&1
  wait_until 120 "group settles at 2 members" has_n_online "$n1" 2 \
    || { bad "group never expelled the deleted member"; teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"; return; }

  # Total outage of the survivors. On restart their bootstrap guard queries
  # the deleted peer forever — pre-waiver this deadlocked here for good.
  docker stop "$n1" "$n2" >/dev/null
  docker start "$n1" "$n2" >/dev/null

  # Fail-closed while the deletion proof accumulates (dwell 30s).
  sleep 15
  local codes
  codes="$(role_code "$n1" "$n1")/$(role_code "$n1" "$n2")"
  [ "$codes" = "503/503" ] \
    && ok "survivors hold fail-closed inside the deletion dwell ($codes)" \
    || bad "a survivor bootstrapped before the deletion was proven ($codes)"

  # Past the dwell the waiver drops the deleted peer from the round and the
  # data-holding survivor bootstraps.
  wait_until 240 "survivors re-form past the deleted peer" has_n_online "$n1" 2 \
    || { bad "survivors never re-formed (deleted-peer wedge)"; teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"; return; }
  ok "survivors re-formed the group without the deleted peer"

  codes="$(role_code "$n1" "$n1")/$(role_code "$n1" "$n2")"
  local n200
  n200="$(echo "$codes" | tr '/' '\n' | grep -c 200)"
  [ "$n200" = "1" ] \
    && ok "exactly one primary after the waiver recovery ($codes)" \
    || bad "expected exactly one primary, got $codes"

  [ "$(sql "$n1" "SELECT v FROM t.kv WHERE k=20")" = "survives-scale-down" ] \
    && ok "dataset survived the scale-down outage recovery" \
    || bad "dataset missing after the waiver recovery"

  docker logs "$n1" 2>&1 | grep -q "no longer waiting on them" \
    || docker logs "$n2" 2>&1 | grep -q "no longer waiting on them" \
    && ok "a survivor logged the deletion waiver" \
    || bad "no waiver log line on either survivor"

  teardown_trio
  NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"
}

t_paused_peer_keeps_the_fence() {
  log "t_paused_peer_keeps_the_fence (a partitioned peer must never read as deleted)"
  # Defensive, same reasoning as t_deleted_peer_unfences_bootstrap: never
  # switch NODE_SUFFIX before clearing out whatever identity is currently
  # live, or teardown_trio below targets the wrong names.
  teardown_trio
  local old_suffix="${NODE_SUFFIX:-}" old_seeds="$SEEDS"
  NODE_SUFFIX=".pf.e2e.invalid"
  SEEDS="mysql-1$NODE_SUFFIX:3306,mysql-2$NODE_SUFFIX:3306,mysql-3$NODE_SUFFIX:3306"
  local n1="mysql-1$NODE_SUFFIX" n2="mysql-2$NODE_SUFFIX" n3="mysql-3$NODE_SUFFIX"

  teardown_trio
  start_node 1 -e PEER_GONE_DWELL_SECONDS=30
  start_node 2 -e PEER_GONE_DWELL_SECONDS=30
  start_node 3 -e PEER_GONE_DWELL_SECONDS=30

  if ! wait_until 300 "3 ONLINE members" group_is_fully_online "$n1"; then
    bad "group never formed"; teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"; return
  fi
  sql "$n1" "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (21,'survives-pause') ON DUPLICATE KEY UPDATE v='survives-pause';"

  # Pause = partition with the name still registered: the container stays on
  # the network, so its name resolves (ExistsOrUnknown) while every probe to
  # it times out. The waiver must never arm on this.
  docker pause "$n3" >/dev/null
  docker stop "$n1" "$n2" >/dev/null
  docker start "$n1" "$n2" >/dev/null

  # Well past the 30s dwell: the fence must still hold — the paused peer's
  # dataset can't be compared and its name never proves deletion.
  sleep 75
  local codes
  codes="$(role_code "$n1" "$n1")/$(role_code "$n1" "$n2")"
  [ "$codes" = "503/503" ] \
    && ok "fence held past the dwell for a paused (partitioned) peer ($codes)" \
    || bad "a survivor bootstrapped past a merely-partitioned peer ($codes)"

  # Unpausing alone does not heal this: node3's in-memory view is frozen at
  # "2 peers UNREACHABLE", and expelling them is itself a membership change
  # that requires a majority to commit — which a lone member can never
  # reach on its own. This is Group Replication's real, documented safety
  # property (losing majority needs explicit reconfiguration), not a bug in
  # the waiver this scenario exists to test. A restart is what actually
  # recovers it: node3 drops its stale view and goes through the same
  # dynamic-candidacy bootstrap guard as node1/node2 already did, discovers
  # no live group anywhere, and the group re-forms via the tie-break
  # already proven by t_group_forms_and_replicates.
  docker unpause "$n3" >/dev/null
  docker restart "$n3" >/dev/null
  wait_until 300 "full group re-forms after the partition heals" group_is_fully_online "$n1" \
    || { bad "group never re-formed after unpause"; teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"; return; }
  ok "full group re-formed once the partition healed"

  [ "$(sql "$n1" "SELECT v FROM t.kv WHERE k=21")" = "survives-pause" ] \
    && ok "dataset survived the partition round-trip" \
    || bad "dataset missing after the partition healed"

  teardown_trio
  NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"
}

# switchover_code <from-node> <target-node> — HTTP status class of POST
# /switchover (200|503). wget's --post-data with an empty body issues the
# POST the endpoint expects.
switchover_code() {
  if docker exec "$1" wget -q -O /dev/null --post-data="" "http://$2:8080/switchover" 2>/dev/null; then
    echo 200
  else
    echo 503
  fi
}

t_switchover_promotes_requested_node() {
  log "t_switchover_promotes_requested_node (the marquee button must move the primary where asked)"
  teardown_trio
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  sql mysql-1 "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (40,'pre-switchover') ON DUPLICATE KEY UPDATE v='pre-switchover';"

  local old_primary
  old_primary="$(current_primary mysql-2 mysql-1 mysql-2 mysql-3)" || { bad "no primary"; return; }
  local target
  for target in mysql-1 mysql-2 mysql-3; do
    [ "$target" != "$old_primary" ] && break
  done

  # The REQUESTED node — not merely some node — must win.
  [ "$(switchover_code mysql-2 "$target")" = "200" ] \
    && ok "switchover to $target answered 200" \
    || { bad "switchover to $target refused"; return; }
  [ "$(role_code mysql-2 "$target")" = "200" ] \
    && ok "requested node $target is the writable primary" \
    || bad "requested node $target did not become primary"
  [ "$(role_code mysql-2 "$old_primary")" = "503" ] \
    && ok "outgoing primary $old_primary demoted" \
    || bad "outgoing primary $old_primary still answers as primary (split view)"

  # Idempotence: asking the current primary again is a 200 no-op.
  [ "$(switchover_code mysql-2 "$target")" = "200" ] \
    && ok "switchover to the current primary is a 200 no-op" \
    || bad "switchover to the current primary errored"

  # Writes land on the new primary and replicate; then a switchover back
  # proves no state leaked from the first one.
  sql "$target" "INSERT INTO t.kv VALUES (41,'post-switchover') ON DUPLICATE KEY UPDATE v='post-switchover';"
  wait_until 60 "post-switchover write replicated" \
    bash -c '[ "$(docker exec '"$old_primary"' mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=41" 2>/dev/null)" = "post-switchover" ]' \
    || { bad "post-switchover write never replicated"; return; }
  ok "writes flow on the promoted node"

  [ "$(switchover_code mysql-2 "$old_primary")" = "200" ] && [ "$(role_code mysql-2 "$old_primary")" = "200" ] \
    && ok "switchover back to $old_primary works (no leaked state)" \
    || bad "second switchover back to $old_primary failed"
}

t_wiped_primary_volume_rejoins_fresh() {
  log "t_wiped_primary_volume_rejoins_fresh (losing the primary's volume must not lose the cluster)"
  teardown_trio
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  local primary
  primary="$(current_primary mysql-2 mysql-1 mysql-2 mysql-3)" || { bad "no primary"; return; }
  sql "$primary" "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (50,'survives-wipe') ON DUPLICATE KEY UPDATE v='survives-wipe';"
  wait_until 60 "seed write replicated" \
    bash -c '[ "$(docker exec mysql-2 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=50" 2>/dev/null)" = "survives-wipe" ]' \
    || { bad "seed write never replicated"; return; }

  # The primary's container AND volume disappear while the group is live —
  # a dead-disk replacement. The survivors keep quorum (2 of 3).
  local n; n="${primary#mysql-}"
  docker rm -f "$primary" >/dev/null 2>&1
  docker volume rm "mysql-ha-e2e-vol-$n" >/dev/null 2>&1

  local probe
  probe=$([ "$primary" = "mysql-2" ] && echo mysql-3 || echo mysql-2)
  wait_until 120 "survivors elect a new primary" \
    bash -c 'docker exec '"$probe"' wget -q -O /dev/null http://mysql-1:8080/role 2>/dev/null || docker exec '"$probe"' wget -q -O /dev/null http://mysql-2:8080/role 2>/dev/null || docker exec '"$probe"' wget -q -O /dev/null http://mysql-3:8080/role 2>/dev/null' \
    || { bad "no survivor took over as primary"; return; }
  ok "survivors kept serving through the volume loss"

  # A fresh replacement boots at the same name with an empty volume and must
  # come back as a SECONDARY holding the data (recovery/clone), never as a
  # competing empty primary.
  start_node "$n"
  wait_until 420 "replacement rejoins the group" group_is_fully_online "$probe" \
    || { bad "fresh replacement never rejoined"; return; }
  wait_until 120 "data recovered onto the replacement" \
    bash -c '[ "$(docker exec '"$primary"' mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=50" 2>/dev/null)" = "survives-wipe" ]' \
    || { bad "replacement did not recover the dataset"; return; }
  ok "replacement recovered the dataset"

  local codes
  codes="$(role_code "$probe" mysql-1)/$(role_code "$probe" mysql-2)/$(role_code "$probe" mysql-3)"
  local n200
  n200="$(echo "$codes" | tr '/' '\n' | grep -c 200)"
  [ "$n200" = "1" ] \
    && ok "exactly one primary after the replacement ($codes)" \
    || bad "expected exactly one primary, got $codes"
}

t_split_brain_fork_self_heals() {
  log "t_split_brain_fork_self_heals (a stale fork after a waiver bootstrap must self-heal, never merge silently)"
  # Runs under .invalid so a STOPPED node's name resolves NXDOMAIN exactly
  # like a deleted one — the condition under which the deletion waiver fires
  # on a merely-stopped (data-intact) peer. Short dwells so the waiver arms
  # within the test.
  local old_suffix="${NODE_SUFFIX:-}" old_seeds="$SEEDS"
  teardown_trio
  NODE_SUFFIX=".sb.e2e.invalid"
  SEEDS="mysql-1$NODE_SUFFIX:3306,mysql-2$NODE_SUFFIX:3306,mysql-3$NODE_SUFFIX:3306"
  local n1="mysql-1$NODE_SUFFIX" n2="mysql-2$NODE_SUFFIX" n3="mysql-3$NODE_SUFFIX"

  start_node 1 -e PEER_GONE_DWELL_SECONDS=30
  start_node 2 -e PEER_GONE_DWELL_SECONDS=30
  start_node 3 -e PEER_GONE_DWELL_SECONDS=30
  if ! wait_until 300 "3 ONLINE members" group_is_fully_online "$n1"; then
    bad "group never formed"; teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"; return
  fi

  # W1 (k=1) reaches all three.
  sql "$n1" "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (1,'w1');"
  wait_until 60 "W1 replicated to the future-behind node" \
    bash -c '[ "$(docker exec '"$n3"' mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=1" 2>/dev/null)" = "w1" ]' \
    || { bad "W1 never replicated"; teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"; return; }
  local derived_name
  derived_name="$(sql "$n1" "SELECT @@group_replication_group_name")"

  # Take the behind node out, then write W2 (k=2='w2-ahead') to the survivors
  # only. n1/n2 now hold {W1,W2}; n3 holds {W1}. W2 occupies the exact GTID
  # coordinate n3's post-bootstrap write will reuse.
  docker stop "$n3" >/dev/null
  wait_until 90 "group drops to 2 members" has_n_online "$n1" 2 \
    || { bad "group never expelled the stopped node"; teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"; return; }
  local ahead_primary
  ahead_primary="$(current_primary "$n1" "$n1" "$n2")" || { bad "no primary among survivors"; teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"; return; }
  sql "$ahead_primary" "INSERT INTO t.kv VALUES (2,'w2-ahead');"

  # Total outage: stop the survivors too (volumes intact — the bug's premise),
  # then bring back ONLY the behind node. Its stopped peers resolve NXDOMAIN,
  # so past the dwell it waiver-bootstraps — now under a FRESH identity.
  docker stop "$n1" "$n2" >/dev/null
  docker start "$n3" >/dev/null
  wait_until 240 "behind node waiver-bootstraps a fresh group" \
    bash -c '[ "$(docker exec '"$n3"' wget -q -O /dev/null http://'"$n3"':8080/role 2>/dev/null && echo 200 || echo 503)" = "200" ]' \
    || { bad "behind node never re-formed a group"; teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"; return; }

  local fresh_name
  fresh_name="$(sql "$n3" "SELECT @@group_replication_group_name")"
  [ -n "$fresh_name" ] && [ "$fresh_name" != "$derived_name" ] \
    && ok "waiver bootstrap minted a fresh group identity ($fresh_name != $derived_name)" \
    || bad "waiver bootstrap reused the derived identity ($fresh_name) — the fence did not arm"

  # W3 (k=2='w3-reformed') on the reformed primary — SAME key, different value,
  # SAME GTID coordinate W2 got. Under the old shared identity this is the
  # silent collision; under the fresh identity it is a distinct transaction.
  sql "$n3" "INSERT INTO t.kv VALUES (2,'w3-reformed') ON DUPLICATE KEY UPDATE v='w3-reformed';"

  # Bring the stale-ahead nodes back. They must self-heal — with NO human
  # action — by discarding their orphaned W2 and recloning from n3, never
  # silently readmitting their forked history.
  docker start "$n1" "$n2" >/dev/null
  wait_until 420 "full group re-forms after the fork heals" group_is_fully_online "$n3" \
    || { bad "group never re-formed 3/3 (stale nodes did not self-heal)"; teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"; return; }
  ok "all three nodes re-formed the group without intervention"

  # The decisive assertion: k=2 is IDENTICAL on every node (no split brain),
  # and it is the reformed primary's value — the stale W2 was discarded.
  local v1 v2 v3
  v1="$(sql "$n1" "SELECT v FROM t.kv WHERE k=2")"
  v2="$(sql "$n2" "SELECT v FROM t.kv WHERE k=2")"
  v3="$(sql "$n3" "SELECT v FROM t.kv WHERE k=2")"
  [ "$v1" = "w3-reformed" ] && [ "$v2" = "w3-reformed" ] && [ "$v3" = "w3-reformed" ] \
    && ok "k=2 converged to the reformed primary's value on all nodes (no fork: $v1/$v2/$v3)" \
    || bad "SPLIT BRAIN: k=2 differs across nodes or kept the stale value ($v1/$v2/$v3)"

  # The pre-fork data survived everywhere.
  [ "$(sql "$n1" "SELECT v FROM t.kv WHERE k=1")" = "w1" ] \
    && ok "pre-fork data (k=1) intact on the healed nodes" \
    || bad "pre-fork data lost on a healed node"

  # And it was a genuine self-heal, logged as a discard — not a silent merge.
  docker logs "$n1" 2>&1 | grep -q "discarding the orphaned transactions and recloning" \
    || docker logs "$n2" 2>&1 | grep -q "discarding the orphaned transactions and recloning" \
    && ok "a stale node logged the divergence self-heal (orphaned tail discarded, not merged)" \
    || bad "no divergence self-heal log line — convergence may have been a silent merge"

  teardown_trio; NODE_SUFFIX="$old_suffix"; SEEDS="$old_seeds"
}

# ---------------------------------------------------------------------------

t_restore_identical_datadirs() {
  log "t_restore_identical_datadirs (a volume backup of one node restored onto every node must reform)"
  teardown_trio
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  local primary
  primary="$(current_primary mysql-2 mysql-1 mysql-2 mysql-3)" || { bad "no primary"; return; }
  sql "$primary" "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (60,'survives-restore') ON DUPLICATE KEY UPDATE v='survives-restore';"
  wait_until 60 "seed write replicated" \
    bash -c '[ "$(docker exec mysql-2 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=60" 2>/dev/null)" = "survives-restore" ]' \
    || { bad "seed write never replicated"; return; }

  # Simulate a platform volume-backup restore onto the whole cluster: stop
  # the trio and replace every node's datadir with a byte copy of node 1's —
  # auto.cnf included, which is exactly what leaves every node with the SAME
  # server_uuid. Without the identity-regeneration path the first node up
  # bootstraps and the other two are refused forever ("There is already a
  # member with server_uuid ..."), a permanent 1-of-3 group.
  docker rm -f mysql-1 mysql-2 mysql-3 >/dev/null 2>&1
  local n
  for n in 2 3; do
    docker run --rm --label "$LABEL" \
      -v "mysql-ha-e2e-vol-1:/src:ro" -v "mysql-ha-e2e-vol-$n:/dst" \
      alpine sh -c 'find /dst -mindepth 1 -delete && cp -a /src/. /dst/' >/dev/null \
      || { bad "datadir copy onto node $n failed"; return; }
  done
  start_trio

  wait_until 600 "group reforms with 3 ONLINE members" group_is_fully_online mysql-1 \
    || { bad "group never reformed from identical datadirs"; return; }
  ok "group reformed from identical datadirs (3/3 ONLINE)"

  # The uuids must have diverged again — three distinct identities.
  local uuids
  uuids="$( { sql mysql-1 "SELECT @@server_uuid"; sql mysql-2 "SELECT @@server_uuid"; sql mysql-3 "SELECT @@server_uuid"; } | sort -u | wc -l | tr -d '[:space:]')"
  [ "$uuids" = "3" ] \
    && ok "every node minted a distinct server_uuid ($uuids unique)" \
    || bad "expected 3 distinct server_uuids, got $uuids unique"

  # The restored dataset is served, and the reformed group takes writes.
  local v
  v="$(sql mysql-2 "SELECT v FROM t.kv WHERE k=60")"
  [ "$v" = "survives-restore" ] \
    && ok "restored dataset intact on a replica" \
    || bad "restored dataset missing on a replica (got '$v')"
  primary="$(current_primary mysql-2 mysql-1 mysql-2 mysql-3)" || { bad "no primary after restore"; return; }
  sql "$primary" "INSERT INTO t.kv VALUES (61,'post-restore') ON DUPLICATE KEY UPDATE v='post-restore';"
  wait_until 60 "post-restore write replicated" \
    bash -c '[ "$(docker exec mysql-3 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=61" 2>/dev/null)" = "post-restore" ]' \
    && ok "post-restore write replicated group-wide" \
    || bad "post-restore write never replicated"
}

# ---------------------------------------------------------------------------

# corrupt_datadir <n> — garbage over node N's InnoDB system tablespace header
# and redo logs while its container is gone, so mysqld aborts crash recovery
# on every start. This is the unrecoverable-datadir failure mode (a crash
# mid-write, bad blocks) — nothing short of reprovisioning fixes it.
corrupt_datadir() {
  docker run --rm --label "$LABEL" -v "mysql-ha-e2e-vol-$1:/d" alpine sh -c \
    'dd if=/dev/urandom of=/d/ibdata1 bs=4096 count=4 conv=notrunc 2>/dev/null;
     for f in /d/#innodb_redo/*; do dd if=/dev/urandom of="$f" bs=4096 count=4 conv=notrunc 2>/dev/null; done;
     true'
}

t_boot_wedged_member_self_heals() {
  log "t_boot_wedged_member_self_heals (a datadir mysqld cannot recover must reprovision from the group on its own)"
  teardown_trio
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  sql mysql-1 "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (80,'pre-corruption') ON DUPLICATE KEY UPDATE v='pre-corruption';"
  wait_until 60 "seed write replicated" \
    bash -c '[ "$(docker exec mysql-3 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=80" 2>/dev/null)" = "pre-corruption" ]' \
    || { bad "seed write never replicated"; return; }

  # Corrupt mysql-3's datadir while its container is gone, then bring it back
  # with the self-heal thresholds shrunk to test scale (the container is
  # recreated because env vars are fixed at create time). The survivors keep
  # quorum, so a /role-200 donor exists the whole time.
  docker rm -f mysql-3 >/dev/null 2>&1
  corrupt_datadir 3 || { bad "could not corrupt the datadir"; return; }
  start_node 3 -e BOOT_LOOP_THRESHOLD=2 -e SELF_HEAL_BACKOFF_BASE_SECONDS=1

  # Wait for the DROP before waiting for the rejoin: right after `docker
  # rm -f`, mysql-1's membership view can read stale-3-ONLINE for a moment
  # (failure detection hasn't expelled the removed member yet), so an
  # immediate 3-ONLINE wait passes spuriously before the heal even starts —
  # the same stale-view race the patch-skew rollback probe documents. The
  # drop to 2 can only reflect the real expulsion.
  wait_until 120 "group drops to 2 while the wedged member crash-loops" has_n_online mysql-1 2 \
    || { bad "group never expelled the wedged member"; return; }

  # Everything from here is the node's own doing: failed boots accumulate on
  # the volume marker, the donor gate confirms a quorum-backed primary, the
  # wedged datadir is discarded, and the fresh boot provisions from the group.
  wait_until 600 "wedged member healed itself back to 3 ONLINE" group_is_fully_online mysql-1 \
    || { bad "corrupted member never healed (still stranded)"; return; }
  ok "corrupted member wiped, reprovisioned, and rejoined with no external action"

  docker logs mysql-3 2>&1 | grep -q "discarding local state to reprovision from the group" \
    && ok "the heal logged its evidence before discarding" \
    || bad "no boot-loop self-heal log line on the healed member"

  wait_until 120 "healed member answers SQL locally" \
    bash -c '[ -n "$(docker exec mysql-3 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT 1" 2>/dev/null)" ]' \
    || { bad "healed member never accepted local SQL"; return; }
  [ "$(sql mysql-3 "SELECT v FROM t.kv WHERE k=80")" = "pre-corruption" ] \
    && ok "pre-corruption data recovered onto the healed member" \
    || bad "healed member is missing the dataset"

  sql mysql-1 "INSERT INTO t.kv VALUES (81,'post-heal') ON DUPLICATE KEY UPDATE v='post-heal';"
  wait_until 60 "post-heal write replicated" \
    bash -c '[ "$(docker exec mysql-3 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=81" 2>/dev/null)" = "post-heal" ]' \
    && ok "post-heal writes replicate to the healed member" \
    || bad "post-heal write never reached the healed member"

  [ "$(role_code mysql-1 mysql-3)" = "503" ] \
    && ok "healed member rejoined as a secondary (/role 503)" \
    || bad "healed member answers /role 200"
}

t_stuck_error_member_self_heals() {
  log "t_stuck_error_member_self_heals (an applier-wedged ERROR member must reclone on its own)"
  teardown_trio
  # The stuck dwell is shrunk to test scale (production default is minutes);
  # all three nodes get it so the scenario doesn't depend on which node ends
  # up the victim.
  start_node 1 -e STUCK_MEMBER_DWELL_SECONDS=15 -e SELF_HEAL_BACKOFF_BASE_SECONDS=1
  start_node 2 -e STUCK_MEMBER_DWELL_SECONDS=15 -e SELF_HEAL_BACKOFF_BASE_SECONDS=1
  start_node 3 -e STUCK_MEMBER_DWELL_SECONDS=15 -e SELF_HEAL_BACKOFF_BASE_SECONDS=1

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  local primary
  primary="$(current_primary mysql-2 mysql-1 mysql-2 mysql-3)" || { bad "no primary"; return; }
  local victim
  for victim in mysql-3 mysql-2 mysql-1; do
    [ "$victim" != "$primary" ] && break
  done

  sql "$primary" "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (90,'pre-wedge') ON DUPLICATE KEY UPDATE v='pre-wedge';"
  wait_until 60 "seed write replicated" \
    bash -c '[ "$(docker exec '"$victim"' mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=90" 2>/dev/null)" = "pre-wedge" ]' \
    || { bad "seed write never replicated"; return; }

  # Wedge the victim's applier: plant an UNLOGGED local row (sql_log_bin=0 —
  # no GTID minted, so this is not divergence and the divergence self-heal
  # stays out of it), then write the same key on the primary. The replicated
  # INSERT hits a duplicate key on the victim, its applier errors, and the
  # member drops to ERROR — the state it would otherwise sit in forever
  # (auto-rejoin covers expulsion, not applier failures).
  sql "$victim" "SET GLOBAL super_read_only=OFF; SET SESSION sql_log_bin=0; INSERT INTO t.kv VALUES (91,'local-orphan'); SET SESSION sql_log_bin=1; SET GLOBAL super_read_only=ON;" \
    || { bad "could not plant the conflicting local row"; return; }
  sql "$primary" "INSERT INTO t.kv VALUES (91,'from-primary');"

  wait_until 120 "victim drops to ERROR" \
    bash -c '[ "$(docker exec '"$victim"' mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid" 2>/dev/null)" = "ERROR" ]' \
    || { bad "victim never hit ERROR (wedge did not take)"; return; }
  ok "victim wedged in ERROR"

  # Wait for the DROP before waiting for the rejoin: the primary's view can
  # still read stale-3-ONLINE for a moment after the victim's applier dies
  # (the errored member leaves the group via a view change that takes a few
  # seconds to land), so an immediate 3-ONLINE wait passes spuriously before
  # the heal even starts. The drop to 2 can only reflect the real leave.
  wait_until 120 "group drops to 2 while the victim sits in ERROR" has_n_online "$primary" 2 \
    || { bad "group never registered the errored member's leave"; return; }

  # Autonomous from here: dwell, donor gate, stop-plugin, clone, restart,
  # rejoin — no docker/exec intervention.
  wait_until 420 "wedged member recloned and rejoined (3 ONLINE)" group_is_fully_online "$primary" \
    || { bad "ERROR member never healed (still stranded)"; return; }
  ok "ERROR member healed back to 3 ONLINE with no external action"

  docker logs "$victim" 2>&1 | grep -q "provably stuck while the group is healthy" \
    && ok "the heal logged its evidence before discarding" \
    || bad "no stuck-member self-heal log line on the victim"

  wait_until 120 "healed member answers SQL locally" \
    bash -c '[ -n "$(docker exec '"$victim"' mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT 1" 2>/dev/null)" ]' \
    || { bad "healed member never accepted local SQL"; return; }
  [ "$(sql "$victim" "SELECT v FROM t.kv WHERE k=91")" = "from-primary" ] \
    && ok "conflicting local row was discarded; the group's value won" \
    || bad "victim still carries the wedging local row (or lost the group write)"
}

t_no_quorum_no_wipe() {
  log "t_no_quorum_no_wipe (with no quorum-confirmed donor anywhere, a wedged member must never discard its data)"
  teardown_trio
  start_trio

  wait_until 300 "3 ONLINE members" group_is_fully_online mysql-1 || { bad "group never formed"; return; }
  sql mysql-1 "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE IF NOT EXISTS t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (95,'must-survive') ON DUPLICATE KEY UPDATE v='must-survive';"
  wait_until 60 "seed write replicated" \
    bash -c '[ "$(docker exec mysql-3 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=95" 2>/dev/null)" = "must-survive" ]' \
    || { bad "seed write never replicated"; return; }

  # Whole-group outage, then the same corruption as the positive scenario —
  # but now NOTHING answers /role 200, so the wedged copy may be the best one
  # left and the heal must hold no matter how many boots fail. A sentinel
  # file marks the datadir so a wipe cannot be missed.
  docker rm -f mysql-3 >/dev/null 2>&1
  docker stop mysql-1 mysql-2 >/dev/null
  docker run --rm --label "$LABEL" -v mysql-ha-e2e-vol-3:/d alpine \
    touch /d/e2e-corruption-sentinel || { bad "could not plant the sentinel"; return; }
  corrupt_datadir 3 || { bad "could not corrupt the datadir"; return; }
  start_node 3 -e BOOT_LOOP_THRESHOLD=2 -e SELF_HEAL_BACKOFF_BASE_SECONDS=1

  # Let it crash-loop well past the threshold (RestartCount is the ground
  # truth for how many boots have failed; the threshold arms on the third).
  wait_until 300 "wedged member boot-looped past the threshold" \
    bash -c '[ "$(docker inspect --format "{{.RestartCount}}" mysql-3 2>/dev/null)" -ge 4 ]' \
    || { bad "member never accumulated enough failed boots"; return; }

  # The decisive assertion: the datadir is untouched — sentinel and the
  # table's tablespace still present — and the wrapper said why it held.
  docker run --rm --label "$LABEL" -v mysql-ha-e2e-vol-3:/d alpine sh -c \
    'test -f /d/e2e-corruption-sentinel && test -e /d/t/kv.ibd' \
    && ok "datadir survived the whole boot-loop window (fail closed, invariant held)" \
    || bad "datadir was discarded with no healthy donor anywhere (invariant broken)"

  docker logs mysql-3 2>&1 | grep -q "refusing to discard the local datadir" \
    && ok "the wrapper logged the fail-closed hold" \
    || bad "no fail-closed log line on the wedged member"

  # This scenario intentionally strands the trio (that is the point); clear
  # everything so the next scenario starts clean.
  teardown_trio
}

# PITR: standalone-only in this version, so this scenario never touches the
# GR trio at all — a fresh pair of standalone (non-GR) nodes plus a minio
# container standing in for the S3-compatible bucket. Self-contained: no
# teardown_trio, no shared GR state, safe to run anywhere in the list.
t_pitr_archive_and_restore_to_point_in_time() {
  log "t_pitr_archive_and_restore_to_point_in_time"
  docker rm -f mysql-pitr-src mysql-pitr-restore mysql-ha-e2e-minio >/dev/null 2>&1
  docker volume rm mysql-ha-e2e-vol-mysql-pitr-src mysql-ha-e2e-vol-mysql-pitr-restore mysql-ha-e2e-minio-data >/dev/null 2>&1

  start_minio || { bad "minio never became healthy"; return; }
  ok "minio up with bucket $PITR_BUCKET"

  local archive_env=(
    -e "BINLOG_ARCHIVE_BUCKET=$PITR_BUCKET"
    -e "BINLOG_ARCHIVE_KEY=$MINIO_ROOT_USER"
    -e "BINLOG_ARCHIVE_SECRET=$MINIO_ROOT_PASSWORD"
    -e "BINLOG_ARCHIVE_REGION=us-east-1"
    -e "BINLOG_ARCHIVE_ENDPOINT=http://mysql-ha-e2e-minio:9000"
    -e "BINLOG_ARCHIVE_PATH=/e2e-pitr"
  )
  start_standalone mysql-pitr-src "${archive_env[@]}"

  wait_until 120 "PITR source node healthy" \
    bash -c 'docker exec mysql-pitr-src wget -q -O /dev/null http://localhost:8080/health 2>/dev/null' \
    || { bad "PITR source node never became healthy"; return; }
  ok "PITR source node up with archiving enabled"

  wait_until 120 "initial full backup completed" \
    bash -c 'docker logs mysql-pitr-src 2>&1 | grep -q "initial full backup completed"' \
    || { bad "initial full backup never completed"; docker logs mysql-pitr-src 2>&1 | tail -40; return; }
  ok "initial full backup completed"

  sql mysql-pitr-src "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (1,'before-t1');"
  # Second-granularity separation on both sides of T1 — mysqlbinlog's
  # --stop-datetime is second-granular, so the marker rows must not share a
  # wall-clock second with the writes they need to be distinguished from.
  sleep 2
  local t1
  t1="$(date -u +'%Y-%m-%dT%H:%M:%S.000Z')"
  log "captured T1=$t1"
  sleep 2
  sql mysql-pitr-src "INSERT INTO t.kv VALUES (2,'after-t1');"
  sql mysql-pitr-src "DROP TABLE t.kv;"

  # Force rotation so the pre-drop data closes into a shippable binlog file
  # instead of waiting out BINLOG_ROTATE_INTERVAL_SECONDS.
  sql mysql-pitr-src "FLUSH BINARY LOGS;"

  wait_until 60 "binlog shipped" \
    bash -c 'docker logs mysql-pitr-src 2>&1 | grep -q "binlog uploaded"' \
    || { bad "binlog was never shipped to the bucket"; docker logs mysql-pitr-src 2>&1 | tail -40; return; }
  ok "binlog shipped to the bucket"

  local recover_env=(
    -e "BINLOG_RECOVER_FROM_BUCKET=$PITR_BUCKET"
    -e "BINLOG_RECOVER_FROM_KEY=$MINIO_ROOT_USER"
    -e "BINLOG_RECOVER_FROM_SECRET=$MINIO_ROOT_PASSWORD"
    -e "BINLOG_RECOVER_FROM_REGION=us-east-1"
    -e "BINLOG_RECOVER_FROM_ENDPOINT=http://mysql-ha-e2e-minio:9000"
    -e "BINLOG_RECOVER_FROM_PATH=/e2e-pitr"
    -e "MYSQL_RECOVERY_TARGET_TIME=$t1"
  )
  start_standalone mysql-pitr-restore "${recover_env[@]}"

  wait_until 180 "restore completed and serving" \
    bash -c 'docker exec mysql-pitr-restore wget -q -O /dev/null http://localhost:8080/health 2>/dev/null' \
    || { bad "restored node never became healthy (restore failed?)"; docker logs mysql-pitr-restore 2>&1 | tail -60; return; }
  ok "restored node completed PITR and is serving"

  docker logs mysql-pitr-restore 2>&1 | grep -q "point-in-time restore completed" \
    && ok "restore log confirms completion" \
    || bad "no restore-completed log line found"

  local v1
  v1="$(sql mysql-pitr-restore "SELECT v FROM t.kv WHERE k=1")"
  [ "$v1" = "before-t1" ] \
    && ok "pre-T1 row present after restore" \
    || bad "pre-T1 row missing after restore (got: '$v1')"

  local v2
  v2="$(sql mysql-pitr-restore "SELECT v FROM t.kv WHERE k=2")"
  [ -z "$v2" ] \
    && ok "post-T1 row correctly absent after restore (restored exactly to T1, not past it)" \
    || bad "post-T1 row present after restore (got: '$v2') — restored past the target time"

  # A restore that crashed mid-way must self-heal on the next boot: the
  # datadir holds only derived state, so the wrapper wipes it and re-runs
  # the restore instead of crash-looping. Simulate the crash by pre-seeding
  # a fresh volume with an in-progress marker AND a junk mysql/ schema dir
  # (so the datadir reads as initialized — proving the wipe actually ran,
  # not just the uninitialized-dir path).
  docker rm -f mysql-pitr-crash >/dev/null 2>&1
  docker volume rm mysql-ha-e2e-vol-mysql-pitr-crash >/dev/null 2>&1
  docker volume create --label "$LABEL" mysql-ha-e2e-vol-mysql-pitr-crash >/dev/null
  docker run --rm -v mysql-ha-e2e-vol-mysql-pitr-crash:/var/lib/mysql --entrypoint sh "$IMAGE" -c \
    'printf "%s" "{\"status\":\"in_progress\",\"target_time\":\"2020-01-01T00:00:00.000Z\",\"updated_at\":\"2020-01-01T00:00:00.000Z\"}" > /var/lib/mysql/.pitr_restore_state.json && mkdir -p /var/lib/mysql/mysql && echo junk > /var/lib/mysql/mysql/ibdata1' \
    || { bad "could not pre-seed the crashed-restore volume"; return; }
  start_standalone mysql-pitr-crash "${recover_env[@]}"

  wait_until 180 "crashed-restore boot self-heals and serves" \
    bash -c 'docker exec mysql-pitr-crash wget -q -O /dev/null http://localhost:8080/health 2>/dev/null' \
    || { bad "crashed-restore boot never became healthy (should have wiped and retried)"; docker logs mysql-pitr-crash 2>&1 | tail -60; return; }
  docker logs mysql-pitr-crash 2>&1 | grep -q "wiping the partially-restored data directory" \
    && ok "crashed mid-restore boot wiped the partial datadir and retried" \
    || bad "no wipe-and-retry log line on the crashed-restore boot"
  local v3
  v3="$(sql mysql-pitr-crash "SELECT v FROM t.kv WHERE k=1")"
  [ "$v3" = "before-t1" ] \
    && ok "self-healed restore serves the pre-T1 row" \
    || bad "self-healed restore missing the pre-T1 row (got: '$v3')"

  docker rm -f mysql-pitr-src mysql-pitr-restore mysql-pitr-crash mysql-ha-e2e-minio >/dev/null 2>&1
  docker volume rm mysql-ha-e2e-vol-mysql-pitr-src mysql-ha-e2e-vol-mysql-pitr-restore mysql-ha-e2e-vol-mysql-pitr-crash mysql-ha-e2e-minio-data >/dev/null 2>&1
}

# ADVERSARIAL (expected RED): the two scenarios below prove known,
# confirmed-but-not-yet-fixed gaps in the PITR path. They are intentionally
# NOT expected to pass — they document the defect precisely enough that
# fixing it means making them go green, not deleting or loosening them.

t_pitr_restore_silently_stops_short_of_target() {
  log "t_pitr_restore_silently_stops_short_of_target (a lineage gap must fail the restore loudly, never replay short)"
  # Regression for the silent-stop-short bug: pitr.rs's binlogs_to_replay
  # used to silently truncate at the FIRST sequence-number gap in the
  # lineage's binlog files — indistinguishable from legitimately running out
  # of binlogs — and restore.rs reported complete success, so a single hole
  # anywhere in the archive silently discarded every good, fully-uploaded
  # binlog past it. Fixed: a gap with binlogs still present beyond it now
  # FAILS the restore outright, with a structured log line naming the hole
  # (see BinlogReplayPlan in pitr.rs and the bail in replay_binlogs).
  docker rm -f mysql-pitr-gap-src mysql-pitr-gap-restore mysql-ha-e2e-minio >/dev/null 2>&1
  docker volume rm mysql-ha-e2e-vol-mysql-pitr-gap-src mysql-ha-e2e-vol-mysql-pitr-gap-restore mysql-ha-e2e-minio-data >/dev/null 2>&1

  start_minio || { bad "minio never became healthy"; return; }
  ok "minio up with bucket $PITR_BUCKET"

  local archive_env=(
    -e "BINLOG_ARCHIVE_BUCKET=$PITR_BUCKET"
    -e "BINLOG_ARCHIVE_KEY=$MINIO_ROOT_USER"
    -e "BINLOG_ARCHIVE_SECRET=$MINIO_ROOT_PASSWORD"
    -e "BINLOG_ARCHIVE_REGION=us-east-1"
    -e "BINLOG_ARCHIVE_ENDPOINT=http://mysql-ha-e2e-minio:9000"
    -e "BINLOG_ARCHIVE_PATH=/e2e-pitr-gap"
    # The auto-rotate loop must stay out of the way — this scenario drives
    # every rotation with explicit FLUSH BINARY LOGS calls and identifies
    # "binlog A/B/C" purely by their POSITION in the shipping order; an
    # uncontrolled auto-rotation landing between two of those FLUSHes would
    # throw that ordering off.
    -e "BINLOG_ROTATE_INTERVAL_SECONDS=3600"
  )
  start_standalone mysql-pitr-gap-src "${archive_env[@]}"

  wait_until 120 "PITR source node healthy" \
    bash -c 'docker exec mysql-pitr-gap-src wget -q -O /dev/null http://localhost:8080/health 2>/dev/null' \
    || { bad "PITR source node never became healthy"; return; }
  wait_until 120 "initial full backup completed" \
    bash -c 'docker logs mysql-pitr-gap-src 2>&1 | grep -q "initial full backup completed"' \
    || { bad "initial full backup never completed"; docker logs mysql-pitr-gap-src 2>&1 | tail -40; return; }
  ok "source node archiving, initial full backup completed"

  # Each row's binlog is identified DIRECTLY (SHOW BINARY LOG STATUS names
  # the active file the row just landed in, same technique as the expiry
  # scenario) and every ship-wait matches that exact file name in the
  # archiver's structured log. Never by upload COUNT or upload ORDER: the
  # first ship cycle uploads every binlog the first-boot init left behind
  # (docker-entrypoint's temp-server/final-server restarts each rotate), so
  # count-based waits pass early and ordinal picks grab an init-era file —
  # exactly the harness bug that made the first run of this scenario punch
  # its "gap" BEFORE the full backup's own coordinate, where a gap is
  # legitimately invisible (everything before the dump coordinate is in the
  # dump itself; see binlogs_to_replay).

  # Row A ships normally — never touched again. It is the control: if it
  # went missing too, that would be a harness/setup bug, not the one under
  # test.
  sql mysql-pitr-gap-src "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (1,'row-a');"
  local binlog_a
  binlog_a="$(sql mysql-pitr-gap-src "SHOW BINARY LOG STATUS" | awk '{print $1}')"
  [ -n "$binlog_a" ] || { bad "could not determine row A's active binlog"; return; }
  sql mysql-pitr-gap-src "FLUSH BINARY LOGS;"
  wait_until 60 "binlog A ($binlog_a) shipped" \
    bash -c 'docker logs mysql-pitr-gap-src 2>&1 | grep "\"message\":\"binlog uploaded\"" | grep -q "\"file\":\"'"$binlog_a"'\""' \
    || { bad "binlog A ($binlog_a) was never shipped"; return; }
  ok "binlog A ($binlog_a) shipped"

  # Row B's binlog ships normally too, then its object is deleted straight
  # out of the bucket — simulating a binlog that made it out but was then
  # lost (an object a lifecycle rule expired, or a corrupted/dropped upload —
  # see Bug 2 below for the "never even shipped" variant of this same loss).
  # Deleting an already-confirmed upload is deterministic and immune to the
  # archiver's ~10s ship-poll timing, where racing the LOCAL file against
  # ship_once would flake under CI load.
  sql mysql-pitr-gap-src "INSERT INTO t.kv VALUES (2,'row-b');"
  local binlog_b
  binlog_b="$(sql mysql-pitr-gap-src "SHOW BINARY LOG STATUS" | awk '{print $1}')"
  [ -n "$binlog_b" ] || { bad "could not determine row B's active binlog"; return; }
  sql mysql-pitr-gap-src "FLUSH BINARY LOGS;"
  wait_until 60 "binlog B ($binlog_b) shipped" \
    bash -c 'docker logs mysql-pitr-gap-src 2>&1 | grep "\"message\":\"binlog uploaded\"" | grep -q "\"file\":\"'"$binlog_b"'\""' \
    || { bad "binlog B ($binlog_b) was never shipped"; return; }
  local server_uuid
  server_uuid="$(sql mysql-pitr-gap-src "SELECT @@server_uuid")"
  [ -n "$server_uuid" ] || { bad "could not read the source node's server_uuid"; return; }
  mc_rm_key "e2e-pitr-gap/server-$server_uuid/binlog/$binlog_b" \
    || { bad "could not delete binlog B ($binlog_b) from the bucket"; return; }
  ok "binlog B ($binlog_b) shipped, then deleted from the bucket to punch a gap"

  # Row C's binlog closes well after a captured T_target — and ships
  # normally (nothing touches it). Second-granularity separation on both
  # sides, same as t_pitr_archive_and_restore_to_point_in_time:
  # mysqlbinlog's --stop-datetime is second-granular.
  sql mysql-pitr-gap-src "INSERT INTO t.kv VALUES (3,'row-c');"
  local binlog_c
  binlog_c="$(sql mysql-pitr-gap-src "SHOW BINARY LOG STATUS" | awk '{print $1}')"
  [ -n "$binlog_c" ] || { bad "could not determine row C's active binlog"; return; }
  sleep 2
  local t_target
  t_target="$(date -u +'%Y-%m-%dT%H:%M:%S.000Z')"
  log "captured T_target=$t_target (after rows B and C, past the punched-out gap)"
  sleep 2
  sql mysql-pitr-gap-src "FLUSH BINARY LOGS;"
  wait_until 60 "binlog C ($binlog_c) shipped" \
    bash -c 'docker logs mysql-pitr-gap-src 2>&1 | grep "\"message\":\"binlog uploaded\"" | grep -q "\"file\":\"'"$binlog_c"'\""' \
    || { bad "binlog C ($binlog_c) was never shipped"; return; }
  ok "binlog C ($binlog_c) shipped normally — the archive now holds [.., $binlog_a, <gap: $binlog_b>, $binlog_c]"

  local recover_env=(
    -e "BINLOG_RECOVER_FROM_BUCKET=$PITR_BUCKET"
    -e "BINLOG_RECOVER_FROM_KEY=$MINIO_ROOT_USER"
    -e "BINLOG_RECOVER_FROM_SECRET=$MINIO_ROOT_PASSWORD"
    -e "BINLOG_RECOVER_FROM_REGION=us-east-1"
    -e "BINLOG_RECOVER_FROM_ENDPOINT=http://mysql-ha-e2e-minio:9000"
    -e "BINLOG_RECOVER_FROM_PATH=/e2e-pitr-gap"
    -e "MYSQL_RECOVERY_TARGET_TIME=$t_target"
  )
  start_standalone mysql-pitr-gap-restore "${recover_env[@]}"

  # THE decisive assertions. Row B is unrecoverable by construction (its
  # binlog was deleted from the bucket), so "reach the target in full" is
  # impossible — the only correct behavior is the one restore.rs now
  # implements: detect the hole, name it in a structured log line, and FAIL
  # the restore outright rather than serving a green-looking database that
  # silently lost row B AND row C (binlog C shipped intact but sits past the
  # hole). Before that fix, this scenario failed here: the node came up
  # healthy, logged "point-in-time restore completed", and was missing both
  # rows.
  wait_until 180 "restore refused loudly on the lineage gap" \
    bash -c 'docker logs mysql-pitr-gap-restore 2>&1 | grep "\"message\":" | grep -qi "binlog lineage has a gap"' \
    || { bad "the restore never logged the lineage gap — did it silently replay short of the target again?"; docker logs mysql-pitr-gap-restore 2>&1 | tail -60; return; }
  ok "restore detected and named the lineage gap"

  if docker logs mysql-pitr-gap-restore 2>&1 | grep -q "point-in-time restore completed"; then
    bad "restore claimed completion despite the lineage gap"
  else
    ok "restore never claimed completion"
  fi

  # Fail-closed: the wrapper must not bring the health server up over a
  # datadir whose restore refused — nothing may route to partial data.
  if docker exec mysql-pitr-gap-restore wget -q -O /dev/null http://localhost:8080/health 2>/dev/null; then
    bad "health endpoint is serving on a node whose restore refused"
  else
    ok "health endpoint is not serving — the refused restore stays fail-closed"
  fi

  docker rm -f mysql-pitr-gap-src mysql-pitr-gap-restore mysql-ha-e2e-minio >/dev/null 2>&1
  docker volume rm mysql-ha-e2e-vol-mysql-pitr-gap-src mysql-ha-e2e-vol-mysql-pitr-gap-restore mysql-ha-e2e-minio-data >/dev/null 2>&1
}

t_binlog_expiry_silently_loses_unshipped_data() {
  log "t_binlog_expiry_silently_loses_unshipped_data (a binlog lost before upload must be reported, never skipped silently)"
  # Regression for the silent-loss bug pair: mysql_conf.rs used to hardcode
  # binlog_expire_logs_seconds to 604800 (7 days) on an archiving standalone
  # node, so mysqld's OWN auto-expiry could reclaim a binlog the archiver had
  # not shipped yet — and archiver.rs's ship_once skipped the missing file
  # with a bare `continue`: zero log line, zero telemetry. Fixed twice over:
  # auto-expiry is now DISABLED on archiving nodes (only the archiver purges,
  # and only files it confirmed uploaded), and a closed, never-uploaded
  # binlog found missing from disk is reported once with a structured
  # error naming the file and recorded in the upload-state file as a
  # permanent lineage gap.
  #
  # Forcing the REAL 7-day expiry deterministically inside a short e2e run
  # isn't practical, and there is no override to shrink it: a live `SET
  # GLOBAL binlog_expire_logs_seconds` would still only be proving the same
  # underlying condition ("the file is gone from disk before ship_once gets
  # to it"), contingent on mysqld's own internal purge-check timing, which
  # runs on rotation/flush events rather than a clock this test controls.
  # So this scenario removes the just-rotated file directly off the
  # container's datadir, immediately after the FLUSH that closes it and
  # comfortably inside the archiver's 10s SHIP_POLL — functionally identical
  # to what the expiry would eventually do (the file disappearing off disk
  # before it is ever shipped), without depending on mysqld's internal purge
  # scheduling.
  docker rm -f mysql-pitr-expiry-src mysql-ha-e2e-minio >/dev/null 2>&1
  docker volume rm mysql-ha-e2e-vol-mysql-pitr-expiry-src mysql-ha-e2e-minio-data >/dev/null 2>&1

  start_minio || { bad "minio never became healthy"; return; }
  ok "minio up with bucket $PITR_BUCKET"

  local archive_env=(
    -e "BINLOG_ARCHIVE_BUCKET=$PITR_BUCKET"
    -e "BINLOG_ARCHIVE_KEY=$MINIO_ROOT_USER"
    -e "BINLOG_ARCHIVE_SECRET=$MINIO_ROOT_PASSWORD"
    -e "BINLOG_ARCHIVE_REGION=us-east-1"
    -e "BINLOG_ARCHIVE_ENDPOINT=http://mysql-ha-e2e-minio:9000"
    -e "BINLOG_ARCHIVE_PATH=/e2e-pitr-expiry"
    -e "BINLOG_ROTATE_INTERVAL_SECONDS=3600"
  )
  start_standalone mysql-pitr-expiry-src "${archive_env[@]}"

  wait_until 120 "PITR source node healthy" \
    bash -c 'docker exec mysql-pitr-expiry-src wget -q -O /dev/null http://localhost:8080/health 2>/dev/null' \
    || { bad "PITR source node never became healthy"; return; }
  wait_until 120 "initial full backup completed" \
    bash -c 'docker logs mysql-pitr-expiry-src 2>&1 | grep -q "initial full backup completed"' \
    || { bad "initial full backup never completed"; docker logs mysql-pitr-expiry-src 2>&1 | tail -40; return; }
  ok "source node archiving, initial full backup completed"

  sql mysql-pitr-expiry-src "CREATE DATABASE IF NOT EXISTS t; CREATE TABLE t.kv (k INT PRIMARY KEY, v VARCHAR(64)); INSERT INTO t.kv VALUES (1,'lost-row');"

  # Identify the binlog this write lands in BEFORE rotating, so exactly that
  # file (and nothing else) gets removed.
  local victim
  victim="$(sql mysql-pitr-expiry-src "SHOW BINARY LOG STATUS" | awk '{print $1}')"
  [ -n "$victim" ] || { bad "could not determine the active binlog file"; return; }

  sql mysql-pitr-expiry-src "FLUSH BINARY LOGS;"
  docker exec mysql-pitr-expiry-src rm -f "/var/lib/mysql/$victim" \
    || { bad "could not remove the victim binlog from the container's datadir"; return; }
  ok "binlog $victim closed and removed from disk before the archiver could ship it"

  # Several ship-poll cycles (SHIP_POLL=10s in archiver.rs) to let it notice.
  sleep 45

  local logs
  logs="$(docker logs mysql-pitr-expiry-src 2>&1)"

  if printf '%s' "$logs" | grep '"message":"binlog uploaded"' | grep -q "\"file\":\"$victim\""; then
    bad "the archiver claims it shipped $victim after it was removed from disk — false success logged"
  else
    ok "the archiver never falsely claims to have shipped the removed binlog"
  fi

  # The decisive assertion — expected to FAIL today. Scoped to the wrapper's
  # own structured (JSON, "message"-bearing) log lines so this can't be
  # satisfied by unrelated raw mysqld chatter — it must be a signal FROM the
  # archiver naming the lost file.
  if printf '%s' "$logs" | grep '"message":' | grep -iE '(missing|lost|purged|gap)' | grep -q "$victim"; then
    ok "the archiver logged a signal naming the lost binlog $victim"
  else
    bad "a binlog ($victim) was purged/lost before upload and the archiver logged nothing — see archiver.rs's ship_once"
  fi

  docker rm -f mysql-pitr-expiry-src mysql-ha-e2e-minio >/dev/null 2>&1
  docker volume rm mysql-ha-e2e-vol-mysql-pitr-expiry-src mysql-ha-e2e-minio-data >/dev/null 2>&1
}

# t_adoption_survives_seed_disadvantaged_race is also self-contained (its own
# teardown_trio + fresh 'pre-conversion' seed, same shape as the
# cross-version/outage/self-heal tests below) and for the same reason must
# not sit inside the adopts→scale→partition chain — it previously sat between
# t_scale_up_to_five and t_minority_partition_write_fence, tearing the
# chain's 5-node group down to its own fresh 3-node trio and never scaling
# back up, so the very next test (which reuses that 5-node group) failed
# with "no 5-node group to partition". Sits right before the chain starts
# instead, alongside the other early self-contained tests.
#
# t_conversion_cross_version_upgrade runs LAST: it teardown_trio's at the start
# and seeds its own 'pre-upgrade' dataset, so it must not sit inside the
# adopts→scale→partition chain that reuses one shared trio + 'pre-conversion'
# row. The two outage-recovery tests teardown and seed their own trios, so
# they sit safely between the chain and the cross-version finale. The three
# self-heal scenarios each teardown and seed their own trio too, and
# t_no_quorum_no_wipe deliberately wrecks one member's datadir and stops the
# rest — it sits second to last. t_pitr_archive_and_restore_to_point_in_time
# is fully self-contained (its own standalone nodes + minio, no GR trio at
# all) and sits after it, followed by the two adversarial PITR scenarios
# (t_pitr_restore_silently_stops_short_of_target,
# t_binlog_expiry_silently_loses_unshipped_data — same self-contained shape;
# born RED to prove the silent-loss bugs, green since the gap-refusal and
# lost-binlog-signal fixes), with only the cross-version finale last.
ALL_TESTS=(t_group_forms_and_replicates t_failover_on_primary_pause t_cold_restart_preserves_group t_adoption_survives_seed_disadvantaged_race t_conversion_adopts_standalone_volume t_scale_up_to_five t_minority_partition_write_fence t_patch_skew_on_redeploy t_total_outage_after_failover t_first_seed_permanent_loss t_password_variable_edit_does_not_rotate t_sigterm_primary_demotes_before_exit t_graceful_double_stop_reforms t_clean_double_stop_keeps_fence t_deleted_peer_unfences_bootstrap t_paused_peer_keeps_the_fence t_split_brain_fork_self_heals t_switchover_promotes_requested_node t_wiped_primary_volume_rejoins_fresh t_restore_identical_datadirs t_boot_wedged_member_self_heals t_stuck_error_member_self_heals t_no_quorum_no_wipe t_pitr_archive_and_restore_to_point_in_time t_pitr_restore_silently_stops_short_of_target t_binlog_expiry_silently_loses_unshipped_data t_conversion_cross_version_upgrade)

main() {
  ensure_image
  ensure_network

  local tests=("$@")
  [ ${#tests[@]} -eq 0 ] && tests=("${ALL_TESTS[@]}")

  for t in "${tests[@]}"; do
    "$t"
  done

  echo
  log "PASS=$PASS FAIL=$FAIL"
  if [ "$FAIL" -gt 0 ]; then
    for f in "${FAILED_TESTS[@]}"; do log "  failed: $f"; done
  fi
  exit "$FAIL"
}

main "$@"
