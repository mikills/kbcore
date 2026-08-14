#!/bin/sh
set -eu

case "${MINNOW_TOKEN:-}" in
  ""|*[!0-9a-fA-F]*)
    echo "MINNOW_TOKEN must be exactly 64 hexadecimal characters" >&2
    exit 1
    ;;
esac
if [ "${#MINNOW_TOKEN}" -ne 64 ]; then
  echo "MINNOW_TOKEN must be exactly 64 hexadecimal characters" >&2
  exit 1
fi

# Fly mounts a new volume as root. Avoid recursively walking a large dataset on
# every restart; only initialize the mount root and required directories.
chown minnow:minnow /var/lib/minnow
install -d -o minnow -g minnow -m 0750 \
  /var/lib/minnow/blobs /var/lib/minnow/cache /var/lib/minnow/caddy-config
export HOME=/var/lib/minnow
export XDG_CONFIG_HOME=/var/lib/minnow/caddy-config

setpriv --reuid=minnow --regid=minnow --init-groups /usr/local/bin/minnow &
minnow_pid=$!

setpriv --reuid=minnow --regid=minnow --init-groups \
  /usr/bin/caddy run --config /etc/caddy/Caddyfile --adapter caddyfile &
caddy_pid=$!

shutdown() {
  kill -TERM "$minnow_pid" "$caddy_pid" 2>/dev/null || true
  wait "$minnow_pid" 2>/dev/null || true
  wait "$caddy_pid" 2>/dev/null || true
}
trap 'shutdown; exit 0' TERM INT

while kill -0 "$minnow_pid" 2>/dev/null && kill -0 "$caddy_pid" 2>/dev/null; do
  sleep 1
done
shutdown
exit 1
