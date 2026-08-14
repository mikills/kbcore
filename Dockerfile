# syntax=docker/dockerfile:1.7

FROM golang:1.24-bookworm AS build
WORKDIR /src
ENV GOWORK=off

COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/minnow .

FROM caddy:2.10 AS caddy

FROM debian:bookworm-slim AS runtime-base
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 minnow \
    && useradd --system --uid 10001 --gid minnow --home /var/lib/minnow minnow \
    && mkdir -p /etc/minnow /var/lib/minnow/blobs /var/lib/minnow/cache /opt/minnow/extensions \
    && chown -R minnow:minnow /etc/minnow /var/lib/minnow

COPY --from=build /out/minnow /usr/local/bin/minnow
COPY --chown=minnow:minnow extensions/v1.5.0/linux_amd64/ /opt/minnow/extensions/v1.5.0/linux_amd64/
COPY --chown=minnow:minnow deploy/docker/minnow.yaml /etc/minnow/minnow.yaml

USER minnow
ENV MINNOW_CONFIG=/etc/minnow/minnow.yaml \
    MINNOW_LOG_FORMAT=json \
    GOMEMLIMIT=768MiB
EXPOSE 8080
VOLUME ["/var/lib/minnow"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl --fail --silent http://127.0.0.1:8080/healthz >/dev/null || exit 1

FROM runtime-base AS fly
USER root
COPY --from=caddy /usr/bin/caddy /usr/bin/caddy
COPY --chown=minnow:minnow deploy/fly/minnow.yaml /etc/minnow/minnow.yaml
COPY deploy/fly/Caddyfile /etc/caddy/Caddyfile
COPY deploy/fly/start.sh /usr/local/bin/start-minnow
RUN chmod 0555 /usr/local/bin/start-minnow \
    && chown -R minnow:minnow /etc/caddy
ENTRYPOINT ["/usr/local/bin/start-minnow"]

FROM runtime-base AS runtime
ENTRYPOINT ["/usr/local/bin/minnow"]
