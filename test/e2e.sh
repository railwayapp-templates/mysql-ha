#!/usr/bin/env bash
# test/e2e.sh — end-to-end harness for the mysql-ha image family.
#
# PLACEHOLDER. Modeled on redis-ha's test/e2e.sh (itself modeled on
# postgres-ssl / postgres-ha's e2e harnesses): each scenario will be a
# `t_*` function with its own docker volumes/network, a label so the exit
# trap can clean up whatever a failed run leaves behind, and a final exit
# code equal to the count of failed tests. None of that exists yet — this
# file only records the scenario list this image's guarantees will need to
# be checked against once mysql-wrapper's Group Replication logic is real.
#
# Planned scenarios:
#
#   - fresh boot + GR bootstrap guard
#       A brand-new 3-node group boots with no peer already live; exactly one
#       node bootstraps the group (group_replication_bootstrap_group=ON) and
#       the other two join it. No node should ever bootstrap a second,
#       competing group.
#
#   - failover via docker pause
#       Pause the current primary (pause, not kill — see redis-ha's e2e.sh
#       for why pause is the realistic failure: a dead node's private domain
#       keeps resolving on Railway). A secondary must be promoted and /role
#       must flip within the expected window.
#
#   - cold restart -> automated total-outage recovery
#       Stop every node in the group, then restart them all. No node may
#       silently self-elect; the total-outage recovery flow (executed-GTID
#       exchange over /health, highest set bootstraps after a dwell) must be
#       what selects who resumes, and it must pick the node with the
#       most-advanced GTID set.
#
#   - minority-partition write fence
#       Partition a minority of nodes (including a former primary) away from
#       the majority. The minority side must not accept writes — /role must
#       return non-200 there even if MySQL locally still thinks it's the
#       primary.
#
#   - standalone volume adoption
#       Point this image at a volume created by Railway's standalone mysql
#       template (binlog off, performance_schema off, fixed buffer pool) and
#       confirm the rendered config flips binlog/GTID/performance_schema on
#       without losing the existing dataset.
#
#   - revert to standalone
#       The reverse of adoption: converting an HA root back to a standalone
#       deploy must leave the data intact and boot cleanly with GR disabled.
#
#   - Clone provisioning of a new peer
#       A newly added node with an empty datadir must provision itself via
#       the Clone plugin against a healthy group member, not require manual
#       seeding.
#
#   - scale 3 -> 5
#       Adding two more nodes to a healthy group must not disrupt the
#       existing primary/secondaries, and both new nodes must successfully
#       join via Clone provisioning.
#
#   - errant-GTID detection
#       A node carrying a GTID the rest of the group never saw (e.g. it took
#       a local write while partitioned) must be detected and must not be
#       allowed to rejoin the group as-is.
#
# Run (once implemented): ./test/e2e.sh
# Or:                     ./test/e2e.sh t_fresh_boot t_failover   # subset

echo "test/e2e.sh: not implemented yet — see the scenario list in this file's header"
exit 0
