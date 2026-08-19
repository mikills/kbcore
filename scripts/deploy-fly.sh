#!/usr/bin/env bash
# Provision a Minnow deployment on Fly.io and point codeindex at it.
#
#   OPENAI_API_KEY=sk-... scripts/deploy-fly.sh --app my-minnow --region ams
#
# Safe to re-run: existing apps, volumes and secrets are left alone.
set -euo pipefail

APP=""
REGION=""
VOLUME_SIZE=10
CONFIG_DIR=""
ROTATE_TOKEN=0
SKIP_CODEINDEX=0

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
log() { printf '==> %s\n' "$*"; }

usage() {
	cat >&2 <<'USAGE'
usage: scripts/deploy-fly.sh --app NAME --region REGION [options]

  --app NAME          Fly app name (globally unique)
  --region REGION     Fly region, e.g. ams, fra, iad
  --volume-size GB    Persistent volume size (default: 10)
  --config-dir DIR    Where to keep the generated fly.toml
                      (default: ~/.config/<app>)
  --rotate-token      Generate a new MINNOW_TOKEN even if one is set
  --skip-codeindex    Do not install or configure codeindex
USAGE
}

while [ $# -gt 0 ]; do
	case "$1" in
		--app) APP="${2:-}"; shift 2 ;;
		--region) REGION="${2:-}"; shift 2 ;;
		--volume-size) VOLUME_SIZE="${2:-}"; shift 2 ;;
		--config-dir) CONFIG_DIR="${2:-}"; shift 2 ;;
		--rotate-token) ROTATE_TOKEN=1; shift ;;
		--skip-codeindex) SKIP_CODEINDEX=1; shift ;;
		-h|--help) usage; exit 0 ;;
		*) usage; die "unknown argument: $1" ;;
	esac
done

[ -n "$APP" ] || { usage; die "--app is required"; }
[ -n "$REGION" ] || { usage; die "--region is required"; }
CONFIG_DIR="${CONFIG_DIR:-$HOME/.config/$APP}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TEMPLATE="$REPO_ROOT/deploy/fly/fly.toml"
[ -f "$REPO_ROOT/Dockerfile" ] || die "run this from a Minnow checkout (no Dockerfile at $REPO_ROOT)"
[ -f "$TEMPLATE" ] || die "missing $TEMPLATE"

for cmd in fly go curl openssl; do
	command -v "$cmd" >/dev/null 2>&1 || die "$cmd is not installed"
done
fly auth whoami >/dev/null 2>&1 || die "not logged in to Fly; run: fly auth login"
[ -n "${OPENAI_API_KEY:-}" ] || die "OPENAI_API_KEY must be set; the deployed config uses it to embed"

TOKEN_SERVICE="MINNOW_TOKEN_$APP"

store_token() {
	if command -v security >/dev/null 2>&1; then
		security add-generic-password -a "$USER" -s "$TOKEN_SERVICE" -w "$1" -U
		log "token stored in the login keychain as $TOKEN_SERVICE"
	else
		mkdir -p "$CONFIG_DIR"
		umask 077
		printf '%s' "$1" > "$CONFIG_DIR/token"
		log "token written to $CONFIG_DIR/token (mode 600)"
	fi
}

read_token() {
	if command -v security >/dev/null 2>&1; then
		security find-generic-password -a "$USER" -s "$TOKEN_SERVICE" -w 2>/dev/null || true
	elif [ -f "$CONFIG_DIR/token" ]; then
		cat "$CONFIG_DIR/token"
	fi
}

log "app: $APP   region: $REGION"

if fly status --app "$APP" >/dev/null 2>&1; then
	log "app already exists"
else
	log "creating app"
	fly apps create "$APP"
fi

if fly volumes list --app "$APP" 2>/dev/null | grep -q "minnow_data"; then
	log "volume minnow_data already exists"
else
	log "creating ${VOLUME_SIZE}GB volume in $REGION"
	fly volumes create minnow_data --region "$REGION" --size "$VOLUME_SIZE" \
		--scheduled-snapshots --snapshot-retention 14 --app "$APP" --yes
fi

# Fly secrets are write-only, so the local copy is the only readable one.
TOKEN="$(read_token)"
if [ "$ROTATE_TOKEN" = "1" ] || [ -z "$TOKEN" ]; then
	# start.sh rejects anything that is not exactly 64 hex characters.
	TOKEN="$(openssl rand -hex 32)"
	store_token "$TOKEN"
fi
log "setting secrets"
fly secrets set OPENAI_API_KEY="$OPENAI_API_KEY" MINNOW_TOKEN="$TOKEN" --app "$APP" --stage >/dev/null

mkdir -p "$CONFIG_DIR"
CONFIG="$CONFIG_DIR/fly.toml"
# The template points at ../../Dockerfile, correct for an in-repo config. This
# copy lives outside the repo, so the key is dropped and --dockerfile is passed.
sed -e "s/^app = .*/app = \"$APP\"/" \
    -e "s/^primary_region = .*/primary_region = \"$REGION\"/" \
    -e '/^  dockerfile = /d' \
    "$TEMPLATE" > "$CONFIG"

log "config written to $CONFIG"

log "deploying (this builds the image; it takes a few minutes)"
( cd "$REPO_ROOT" && fly deploy --config "$CONFIG" --dockerfile Dockerfile --ha=false )

URL="https://$APP.fly.dev"
log "verifying $URL"

code="$(curl -s -o /dev/null -w '%{http_code}' "$URL/healthz" || true)"
[ "$code" = "200" ] || die "healthz returned $code, expected 200"

code="$(curl -s -o /dev/null -w '%{http_code}' "$URL/mcp" || true)"
[ "$code" = "401" ] || die "/mcp returned $code without a token, expected 401"

code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$URL/mcp" \
	-H "Authorization: Bearer $TOKEN" \
	-H 'Content-Type: application/json' \
	-H 'Accept: application/json, text/event-stream' \
	-d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"deploy-fly","version":"1"}}}' || true)"
[ "$code" = "200" ] || die "MCP initialize returned $code, expected 200"
log "healthz 200, unauthenticated /mcp 401, authenticated MCP handshake 200"

if [ "$SKIP_CODEINDEX" = "0" ]; then
	log "installing codeindex"
	( cd "$HOME" && go install github.com/mikills/minnow/codeindex@latest )
	CODEINDEX="$(command -v codeindex || echo "$(go env GOPATH)/bin/codeindex")"
	MINNOW_TOKEN="$TOKEN" "$CODEINDEX" setup --minnow-url "$URL" --token-env MINNOW_TOKEN
fi

cat <<DONE

Deployed: $URL
Config:   $CONFIG

Export the token in your shell profile (~/.zshenv so git hooks see it too):

  export MINNOW_TOKEN=\$(security find-generic-password -a \$USER -s $TOKEN_SERVICE -w)

Then index a repository:

  cd /path/to/repo && codeindex codebase && codeindex hooks install

Redeploy later with:

  fly deploy --config $CONFIG --dockerfile Dockerfile --ha=false
DONE
