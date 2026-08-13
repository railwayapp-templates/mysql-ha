ARG MYSQL_VERSION=8.4

# ============================================================
# Rust builder stage (statically linked with musl)
# ============================================================
# 1.94 (bumped from 1.90): the PITR archiver/restore path's aws-sdk-s3 +
# aws-config dependencies carry an MSRV that creeps forward on ordinary
# releases and currently sits at 1.94.1 — see the workspace Cargo.toml.
FROM rust:1.94-alpine AS rust-builder

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
# the redis:bookworm base redis-ha wraps, this image doesn't bundle it. gzip
# is needed for PITR archiving/restore (mysqldump | gzip on the way out,
# gunzip | mysql on the way back in) — mysql/mysqldump/mysqladmin already
# ship in this image's "server-minimal" package, but NOT mysqlbinlog (also
# needed for restore's binlog replay); that one only comes with the full
# mysql-community-client package, installed separately below.
# CI passes its content key here (see the build workflow's content gate):
# the value only changes when the repo code, the upstream base, or the
# base's pending-security-fix set moved, so this layer re-runs exactly when
# there is something to pick up — and stays byte-cached (stable digest)
# otherwise. A superseded series' base never re-mints upstream, so the
# microdnf upgrade below is the only way OS fixes reach a pinned series.
ARG PKG_REFRESH=none
# exclude=mysql*: the OS upgrade must never touch the packages that define
# the bundled MySQL series — the image's yum repo carries the cross-series
# upgrade path, so an unrestricted `microdnf upgrade` turned a pinned :8.3
# image into one bundling mysqld 8.4.0 (caught by the bundled-series guard
# in CI, which refused to publish). microdnf has no --exclude flag; the
# glob goes through dnf.conf (empty in this image) and is restored after.
# Older bases (8.1–8.3, Oracle Linux 8 era) ship no /etc/dnf/dnf.conf at
# all — creating it works the same (microdnf honors it either way), it just
# has to be REMOVED again instead of restored.
RUN echo "refresh key: ${PKG_REFRESH}" \
    && if [ -f /etc/dnf/dnf.conf ]; then cp /etc/dnf/dnf.conf /tmp/dnf.conf.orig; fi \
    && mkdir -p /etc/dnf \
    && printf '[main]\nexclude=mysql*\n' >> /etc/dnf/dnf.conf \
    && microdnf upgrade -y \
    && if [ -f /tmp/dnf.conf.orig ]; then mv /tmp/dnf.conf.orig /etc/dnf/dnf.conf; else rm -f /etc/dnf/dnf.conf; fi \
    && microdnf install -y wget gzip && microdnf clean all

# mysqlbinlog (PITR restore's binlog replay) isn't in the "server-minimal"
# package above — it ships in the full mysql-community-client package,
# hosted at the SAME repo family the image's own minimal repo already points
# at, one path segment up (no "/docker/"). Deriving that URL from the
# already-baked-in minimal repo file — instead of hardcoding a series name —
# is what makes this work across the whole build matrix unchanged: numbered
# series (8.0, 8.4, ...) and the 9.x "innovation" line both follow the same
# docker-vs-full split, just under different repo ids.
# --replacefiles/--replacepkgs: several files (mysql, mysqldump, mysqladmin,
# a few auth plugins) are byte-identical between server-minimal and the full
# client package at the same version — rpm's own conflict check would
# otherwise refuse that overlap, which microdnf's transaction planner can't
# route around on its own.
RUN minimal_repo="$(grep -l '/docker/el/' /etc/yum.repos.d/*.repo | head -1)" \
    && full_baseurl="$(grep '^baseurl=' "$minimal_repo" | sed 's#/docker/el/#/el/#')" \
    && printf '[mysql-full-client]\nname=MySQL Full Client\n%s\nenabled=1\ngpgcheck=1\ngpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-mysql\nmodule_hotfixes=true\n' "$full_baseurl" > /etc/yum.repos.d/mysql-full-client.repo \
    && mkdir -p /tmp/mysql-client-rpms && cd /tmp/mysql-client-rpms \
    && microdnf download mysql-community-client mysql-community-client-plugins mysql-community-common mysql-community-libs \
    && rpm -Uvh --replacefiles --replacepkgs ./*.rpm \
    && cd / && rm -rf /tmp/mysql-client-rpms /etc/yum.repos.d/mysql-full-client.repo

COPY --from=rust-binaries --chmod=755 /mysql-wrapper /usr/local/bin/mysql-wrapper

# TODO(security): runs as root, mirroring redis-sentinel's current posture —
# see that Dockerfile's TODO for why (Railway's runtime-mounted volume has
# its own ownership independent of a build-time chown). The proper fix is
# the same one deferred there: start as root, chown /var/lib/mysql to the
# mysql user at runtime, then drop privileges before exec'ing mysqld.

VOLUME ["/var/lib/mysql"]

# 3306   = MySQL client port — also carries Group Replication traffic
#          (group_replication_communication_stack = MYSQL; no XCom port)
# 8080   = wrapper health server (/health, /role, /gr/state)
EXPOSE 3306 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["mysql-wrapper"]

# mysql-wrapper always invokes `docker-entrypoint.sh mysqld` itself and
# appends CMD's args after `mysqld` — so CMD here carries only EXTRA mysqld
# flags, never the `mysqld` token itself (that would duplicate it).
CMD []
