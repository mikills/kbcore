#!/usr/bin/env bash
# Provision a Minnow deployment on Fly.io and point codeindex at it.
#
#   OPENAI_API_KEY=sk-... scripts/deploy-fly.sh --app my-minnow --region ams
#
# Re-running is safe: an existing app, volume and MINNOW_TOKEN are reused, and
# the script refuses to act when reusing them would break existing clients.
set -euo pipefail

APP=""
REGION=""
VOLUME_SIZE=10
CONFIG_DIR=""
ROTATE_TOKEN=0
SKIP_CODEINDEX=0
WHOAMI="${USER:-$(id -un)}"

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
  --rotate-token      Replace MINNOW_TOKEN. Every existing client breaks
                      until it is given the new token.
  --skip-codeindex    Do not install or configure codeindex
USAGE
}

need_value() { [ $# -ge 2 ] || { usage; die "$1 needs a value"; }; }

while [ $# -gt 0 ]; do
	case "$1" in
		--app) need_value "$@"; APP="$2"; shift 2 ;;
		--region) need_value "$@"; REGION="$2"; shift 2 ;;
		--volume-size) need_value "$@"; VOLUME_SIZE="$2"; shift 2 ;;
		--config-dir) need_value "$@"; CONFIG_DIR="$2"; shift 2 ;;
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

for cmd in fly curl openssl python3; do
	command -v "$cmd" >/dev/null 2>&1 || die "$cmd is not installed"
done
[ "$SKIP_CODEINDEX" = "1" ] || command -v go >/dev/null 2>&1 || die "go is not installed"
fly auth whoami >/dev/null 2>&1 || die "not logged in to Fly; run: fly auth login"
[ -n "${OPENAI_API_KEY:-}" ] || die "OPENAI_API_KEY must be set; the deployed config uses it to embed"

TOKEN_SERVICE="MINNOW_TOKEN_$APP"
TOKEN_FILE="$CONFIG_DIR/token"
USES_KEYCHAIN=0
command -v security >/dev/null 2>&1 && USES_KEYCHAIN=1

store_token() {
	if [ "$USES_KEYCHAIN" = "1" ]; then
		security add-generic-password -a "$WHOAMI" -s "$TOKEN_SERVICE" -w "$1" -U \
			|| die "could not write to the login keychain (is it unlocked?)"
		log "token stored in the login keychain as $TOKEN_SERVICE"
	else
		mkdir -p "$CONFIG_DIR"
		install -m 600 /dev/null "$TOKEN_FILE"
		printf '%s' "$1" > "$TOKEN_FILE"
		log "token written to $TOKEN_FILE (mode 600)"
	fi
}

read_token() {
	if [ "$USES_KEYCHAIN" = "1" ]; then
		security find-generic-password -a "$WHOAMI" -s "$TOKEN_SERVICE" -w 2>/dev/null || true
	elif [ -f "$TOKEN_FILE" ]; then
		cat "$TOKEN_FILE"
	fi
}

log "app: $APP   region: $REGION"

APP_EXISTS=0
if fly status --app "$APP" >/dev/null 2>&1; then
	APP_EXISTS=1
	log "app already exists"
else
	log "creating app"
	fly apps create "$APP"
fi

# Reusing an app while minting a fresh token would 401 every existing client.
TOKEN="$(read_token)"
REMOTE_HAS_TOKEN=0
if [ "$APP_EXISTS" = "1" ] && fly secrets list --app "$APP" 2>/dev/null | grep -q '^ *MINNOW_TOKEN '; then
	REMOTE_HAS_TOKEN=1
fi
if [ "$REMOTE_HAS_TOKEN" = "1" ] && [ -z "$TOKEN" ] && [ "$ROTATE_TOKEN" = "0" ]; then
	die "$APP already has a MINNOW_TOKEN but no local copy was found.
Fly secrets cannot be read back. Copy the token from the machine that has it,
or re-run with --rotate-token and update every client afterwards."
fi

VOLUME_JSON="$(fly volumes list --app "$APP" --json 2>/dev/null || echo FAILED)"
[ "$VOLUME_JSON" != "FAILED" ] || die "could not list volumes for $APP"
VOLUME_REGION="$(printf '%s' "$VOLUME_JSON" | python3 -c '
import json,sys
try:
    vols = json.load(sys.stdin)
except Exception:
    vols = []
for v in vols:
    if v.get("name") == "minnow_data":
        print(v.get("region", ""))
        break
')"
if [ -n "$VOLUME_REGION" ]; then
	[ "$VOLUME_REGION" = "$REGION" ] || die "volume minnow_data is in $VOLUME_REGION but --region is $REGION.
Deploying would start the app on a new, empty volume and orphan the existing one."
	log "volume minnow_data already exists in $VOLUME_REGION"
else
	log "creating ${VOLUME_SIZE}GB volume in $REGION"
	fly volumes create minnow_data --region "$REGION" --size "$VOLUME_SIZE" \
		--scheduled-snapshots --snapshot-retention 14 --app "$APP" --yes
fi

if [ "$ROTATE_TOKEN" = "1" ] || [ -z "$TOKEN" ]; then
	# start.sh rejects anything that is not exactly 64 hex characters.
	TOKEN="$(openssl rand -hex 32)"
	store_token "$TOKEN"
fi
case "${#TOKEN}:$TOKEN" in
	64:*[!0-9a-f]*|64:) die "stored MINNOW_TOKEN is not 64 hex characters" ;;
	64:*) ;;
	*) die "stored MINNOW_TOKEN is ${#TOKEN} characters, expected 64" ;;
esac

log "setting secrets"
# Passed on stdin so neither secret appears in the process table.
printf 'OPENAI_API_KEY=%s\nMINNOW_TOKEN=%s\n' "$OPENAI_API_KEY" "$TOKEN" \
	| fly secrets import --app "$APP" --stage >/dev/null

mkdir -p "$CONFIG_DIR"
CONFIG="$CONFIG_DIR/fly.toml"
# The dockerfile key is dropped because this copy lives outside the repo, where
# the template's ../../Dockerfile does not resolve; --dockerfile is used below.
sed -e "s/^app = .*/app = \"$APP\"/" \
    -e "s/^primary_region = .*/primary_region = \"$REGION\"/" \
    -e "s/^  initial_size = .*/  initial_size = \"${VOLUME_SIZE}gb\"/" \
    -e '/^  dockerfile = /d' \
    "$TEMPLATE" > "$CONFIG"
grep -q "^app = \"$APP\"\$" "$CONFIG" || die "generated config does not name $APP; the template layout changed"
grep -q "^primary_region = \"$REGION\"\$" "$CONFIG" || die "generated config does not set $REGION"
! grep -q "^  dockerfile = " "$CONFIG" || die "generated config still carries a dockerfile key"
log "config written to $CONFIG"

log "deploying (this builds the image; it takes a few minutes)"
( cd "$REPO_ROOT" && fly deploy --config "$CONFIG" --dockerfile Dockerfile --ha=false )

URL="https://$APP.fly.dev"
log "verifying $URL"

CURL_CFG="$(mktemp)"
trap 'rm -f "$CURL_CFG"' EXIT
chmod 600 "$CURL_CFG"
printf 'header = "Authorization: Bearer %s"\n' "$TOKEN" > "$CURL_CFG"

code="$(curl -s -o /dev/null -w '%{http_code}' "$URL/healthz" || true)"
[ "$code" = "200" ] || die "healthz returned $code, expected 200"

code="$(curl -s -o /dev/null -w '%{http_code}' "$URL/mcp" || true)"
[ "$code" = "401" ] || die "/mcp returned $code without a token, expected 401"

code="$(curl -s -o /dev/null -w '%{http_code}' --config "$CURL_CFG" -X POST "$URL/mcp" \
	-H 'Content-Type: application/json' \
	-H 'Accept: application/json, text/event-stream' \
	-d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"deploy-fly","version":"1"}}}' || true)"
[ "$code" = "200" ] || die "MCP initialize returned $code, expected 200"
log "healthz 200, unauthenticated /mcp 401, authenticated MCP handshake 200"

if [ "$SKIP_CODEINDEX" = "0" ]; then
	log "installing codeindex"
	( cd "$HOME" && go install github.com/mikills/minnow/codeindex@latest )
	CODEINDEX="${GOBIN:-$(go env GOPATH)/bin}/codeindex"
	# codeindex keeps one config per machine, so --force is required to repoint it.
	"$CODEINDEX" setup --minnow-url "$URL" --token-env MINNOW_TOKEN --force
fi

if [ "$USES_KEYCHAIN" = "1" ]; then
	TOKEN_HINT="export MINNOW_TOKEN=\$(security find-generic-password -a $WHOAMI -s $TOKEN_SERVICE -w)"
else
	TOKEN_HINT="export MINNOW_TOKEN=\$(cat $TOKEN_FILE)"
fi

cat <<DONE

Deployed: $URL
Config:   $CONFIG

Export the token in your shell profile (~/.zshenv so git hooks see it too):

  $TOKEN_HINT

Then index a repository:

  cd /path/to/repo && codeindex codebase && codeindex hooks install

Redeploy later with:

  fly deploy --config $CONFIG --dockerfile Dockerfile --ha=false
DONE
