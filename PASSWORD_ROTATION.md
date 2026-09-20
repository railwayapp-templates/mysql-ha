# Coordinated password rotation

The platform owns cluster ordering through authenticated `POST /credentials/rotate` requests to each data member's health server. Requests use Basic authentication (`railway`, current pinned password) and a JSON body with `operation`, `newPassword` and `currentPassword`. Successful responses contain `{ "version": 1, "leader": true|false }`. Phases are `preflight`, `prepare`, `database`, `member` and `verify`; each tolerates a repeated request after a lost response. Engine error bodies and credentials are never returned.

`database` runs one binlogged multi-account ALTER USER on the elected primary for root, gr_recovery and the configured application user, retaining grants. `member` proves the replicated password, swaps the shared SQL pool, updates the Group Replication recovery channel and persists the pin. Existing pooled callers and startup recovery can adopt the staged credential only after successful authentication. Verification requires all Group Replication members ONLINE and rejects the old root password.

The platform preflights every member and writes private durable intent everywhere before changing the primary. It commits the root variable only after member adoption, then rolls coordination/proxy services and verifies every member. Pre-commit compensation uses the same protocol with the two passwords reversed. Journals are removed only after verification. A manual environment-variable edit alone is not evidence that database credentials changed.

Publish this image before enabling the companion backend workflow, and upgrade all data members of existing clusters. Templates and their variable references stay unchanged. Existing images without this endpoint are refused during platform preflight.
