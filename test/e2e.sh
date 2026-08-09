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
# shape the Railway template stamps. NODE_IMAGE overrides the image for this
# one call (patch-skew scenarios boot members on different patch levels, the
# way a redeploy re-pulling the moving :8.4 tag does in production).
start_node() {
  local n="$1"; shift
  local image="${NODE_IMAGE:-$IMAGE}"
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
    "$image" >/dev/null
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

has_n_online() { [ "$(online_members "$1" | tr -d '[:space:]')" = "$2" ]; }

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

  wait_until 300 "skewed member rejoined (3 ONLINE)" group_is_fully_online mysql-1 \
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
  wait_until 60 "write replicates across the patch skew" \
    bash -c '[ "$(docker exec mysql-2 mysql -uroot -p'"$ROOT_PW"' --batch --skip-column-names -e "SELECT v FROM t.kv WHERE k=2" 2>/dev/null)" = "during-skew" ]' \
    && ok "writes replicate in the mixed-patch group" \
    || bad "replication broken across patch skew"

  # Rollback probe — the sharp edge of moving tags: a deployment ROLLBACK
  # boots the older binary against the now-upgraded datadir. MySQL does not
  # support downgrades (even patch-level), so this member must fail loudly
  # while the group survives on the remaining majority. This documents the
  # hazard rather than pretending it away.
  docker rm -f mysql-2 >/dev/null 2>&1
  NODE_IMAGE="$old_image" start_node 2
  sleep 20
  local online
  online="$(online_members mysql-1 | tr -d '[:space:]')"
  if [ "$online" = "2" ]; then
    ok "rollback of a patch-upgraded member refuses to rejoin (2/3 survive) — downgrade unsupported, as documented"
  elif [ "$online" = "3" ]; then
    # If MySQL ever tolerates this it's strictly better than documented;
    # record it rather than failing.
    ok "rollback member unexpectedly rejoined (3/3) — better than MySQL's documented no-downgrade stance"
  else
    bad "group degraded past the rollback member (online=$online)"
  fi

  # Heal: put the member back on the current image; must rejoin.
  docker rm -f mysql-2 >/dev/null 2>&1
  start_node 2
  wait_until 300 "healed member rejoined" group_is_fully_online mysql-1 \
    && ok "re-redeploy onto the current patch heals the rolled-back member" \
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

# ---------------------------------------------------------------------------

# t_conversion_cross_version_upgrade runs LAST: it teardown_trio's at the start
# and seeds its own 'pre-upgrade' dataset, so it must not sit inside the
# adopts→scale→partition chain that reuses one shared trio + 'pre-conversion'
# row. The two outage-recovery tests teardown and seed their own trios, so
# they sit safely between the chain and the cross-version finale.
ALL_TESTS=(t_group_forms_and_replicates t_failover_on_primary_pause t_cold_restart_preserves_group t_conversion_adopts_standalone_volume t_scale_up_to_five t_minority_partition_write_fence t_patch_skew_on_redeploy t_total_outage_after_failover t_first_seed_permanent_loss t_conversion_cross_version_upgrade)

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
