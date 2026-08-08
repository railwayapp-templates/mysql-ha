ARG HAPROXY_VERSION=3.2

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

RUN cargo build --release -p haproxy-entrypoint

FROM scratch AS rust-builder-local-output
COPY --from=rust-builder /build/target/release/haproxy-entrypoint /haproxy-entrypoint

FROM scratch AS rust-binaries
COPY --from=rust-builder-local-output / /

# ============================================================
# Final stage: HAProxy with Rust entrypoint
# ============================================================
FROM haproxy:${HAPROXY_VERSION}-alpine

USER root

# mysql-client for Railway's Database UI SSH tunnel
RUN apk add --no-cache mysql-client bash

COPY --from=rust-binaries --chmod=755 /haproxy-entrypoint /entrypoint

RUN chown -R haproxy:haproxy /usr/local/etc/haproxy

USER haproxy

# 3306 = write port (primary only, v1 has no read port)
# 8404 = stats page
EXPOSE 3306 8404

HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8404/stats || exit 1

ENTRYPOINT ["/entrypoint"]
