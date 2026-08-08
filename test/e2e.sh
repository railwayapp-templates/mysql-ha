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
# shape the Railway template stamps.
start_node() {
  local n="$1"; shift
  docker volume create --label "$LABEL" "mysql-ha-e2e-vol-$n" >/dev/null
  # --restart unless-stopped mirrors Railway's restart policy — and the clone
  # provisioning path DEPENDS on a restart: the clone recipient replaces its
  # datadir and shuts down, expecting the platform to boot it back up.
  docker run -d --label "$LABEL" --restart unless-stopped \
    --name "mysql-$n" --hostname "mysql-$n" \
    --network "$NET" --network-alias "mysql-$n" \
    -v "mysql-ha-e2e-vol-$n:/var/lib/mysql" \
    -e MYSQL_ROOT_PASSWORD="$ROOT_PW" \
    -e GR_REPLICATION_PASSWORD="$REPL_PW" \
    -e GR_SEEDS="$SEEDS" \
    -e RAILWAY_PRIVATE_DOMAIN="mysql-$n" \
    -e RAILWAY_ENVIRONMENT_ID="e2e-env" \
    -e RAILWAY_VOLUME_MOUNT_PATH="/var/lib/mysql" \
    -e BOOTSTRAP_DWELL_SECONDS=5 \
    "$@" \
    "$IMAGE" >/dev/null
}

start_trio() { start_node 1; start_node 2; start_node 3; }

teardown_trio() {
  docker rm -f mysql-1 mysql-2 mysql-3 >/dev/null 2>&1
  docker volume rm mysql-ha-e2e-vol-1 mysql-ha-e2e-vol-2 mysql-ha-e2e-vol-3 >/dev/null 2>&1
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

group_is_fully_online() { [ "$(online_members "$1" | tr -d '[:space:]')" = "3" ]; }

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

# ---------------------------------------------------------------------------

ALL_TESTS=(t_group_forms_and_replicates t_failover_on_primary_pause t_cold_restart_preserves_group t_conversion_adopts_standalone_volume)

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
