ARG MYSQL_VERSION=8.4

# ============================================================
# Rust builder stage (statically linked with musl)
# ============================================================
FROM rust:1.90-alpine AS rust-builder

RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY common ./common
COPY mysql-wrapper ./mysql-wrapper
COPY haproxy ./haproxy

RUN cargo build --release -p mysql-wrapper

FROM scratch AS rust-builder-local-output
COPY --from=rust-builder /build/target/release/mysql-wrapper /mysql-wrapper

FROM scratch AS rust-binaries
COPY --from=rust-builder-local-output / /

# ============================================================
# Final stage: MySQL with Rust entrypoint
# ============================================================
FROM mysql:${MYSQL_VERSION}

USER root

# Official mysql images (8.0.31+/8.4) are Oracle Linux 9-based and use
# microdnf, not apt/apk. wget is needed for the HEALTHCHECK below — unlike
# the redis:bookworm base redis-ha wraps, this image doesn't bundle it.
RUN microdnf install -y wget && microdnf clean all

COPY --from=rust-binaries --chmod=755 /mysql-wrapper /usr/local/bin/mysql-wrapper

# TODO(security): runs as root, mirroring redis-sentinel's current posture —
# see that Dockerfile's TODO for why (Railway's runtime-mounted volume has
# its own ownership independent of a build-time chown). The proper fix is
# the same one deferred there: start as root, chown /var/lib/mysql to the
# mysql user at runtime, then drop privileges before exec'ing mysqld.

VOLUME ["/var/lib/mysql"]

# 3306   = MySQL client port (write traffic in front of HAProxy)
# 33061  = Group Replication's own communication port (xcom)
# 8080   = wrapper health server (/health, /role)
EXPOSE 3306 33061 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["mysql-wrapper"]

# mysql-wrapper always invokes `docker-entrypoint.sh mysqld` itself and
# appends CMD's args after `mysqld` — so CMD here carries only EXTRA mysqld
# flags, never the `mysqld` token itself (that would duplicate it).
CMD []
