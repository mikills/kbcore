#!/bin/sh
# Installs the codeindex CLI and starts a local Minnow.
#
#   curl -fsSL https://raw.githubusercontent.com/mikills/minnow/main/install.sh | sh
#
# Environment:
#   CODEINDEX_VERSION      release tag to install, default the latest codeindex/v*
#   CODEINDEX_INSTALL_DIR  where the binary goes, default ~/.local/bin
#   MINNOW_EMBEDDER        ollama (default) or openai; openai needs OPENAI_API_KEY
#   MINNOW_PORT            host port for Minnow, default 8080
#   SKIP_SERVER            set to 1 to install the CLI only

set -eu

REPO="mikills/minnow"
INSTALL_DIR="${CODEINDEX_INSTALL_DIR:-$HOME/.local/bin}"
PORT="${MINNOW_PORT:-8080}"
EMBEDDER="${MINNOW_EMBEDDER:-ollama}"
MINNOW_URL="http://127.0.0.1:$PORT"

log() { printf '%s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

have curl || die "curl is required"
have tar || die "tar is required"

detect_platform() {
	os=$(uname -s | tr '[:upper:]' '[:lower:]')
	arch=$(uname -m)
	case "$os" in
		darwin | linux) ;;
		*) die "unsupported OS: $os. Build from source with: go install github.com/$REPO/codeindex@latest" ;;
	esac
	case "$arch" in
		x86_64 | amd64) arch=amd64 ;;
		arm64 | aarch64) arch=arm64 ;;
		*) die "unsupported architecture: $arch" ;;
	esac
	printf '%s_%s' "$os" "$arch"
}

latest_version() {
	curl -fsSL "https://api.github.com/repos/$REPO/releases?per_page=50" |
		grep '"tag_name":' |
		sed -e 's/.*"tag_name": *"//' -e 's/".*//' |
		grep '^codeindex/v' |
		head -n 1
}

verify_checksum() {
	# archive, checksums file, expected name
	if have sha256sum; then
		actual=$(sha256sum "$1" | cut -d' ' -f1)
	elif have shasum; then
		actual=$(shasum -a 256 "$1" | cut -d' ' -f1)
	else
		log "warning: no sha256 tool found, skipping checksum verification"
		return 0
	fi
	expected=$(grep " $3\$" "$2" | cut -d' ' -f1)
	[ -n "$expected" ] || die "no checksum listed for $3"
	[ "$actual" = "$expected" ] || die "checksum mismatch for $3"
}

install_codeindex() {
	platform=$(detect_platform)
	version="${CODEINDEX_VERSION:-$(latest_version)}"
	[ -n "$version" ] || die "no codeindex release found"
	short="${version#codeindex/}"
	asset="codeindex_${short}_${platform}.tar.gz"
	base="https://github.com/$REPO/releases/download/$version"

	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT

	log "Downloading codeindex $short for $platform"
	curl -fsSL -o "$tmp/$asset" "$base/$asset" ||
		die "no binary for $platform in $version. Build it with: go install github.com/$REPO/codeindex@latest"
	curl -fsSL -o "$tmp/checksums.txt" "$base/checksums.txt" ||
		die "could not download checksums.txt"
	verify_checksum "$tmp/$asset" "$tmp/checksums.txt" "$asset"

	tar -xzf "$tmp/$asset" -C "$tmp"
	mkdir -p "$INSTALL_DIR"
	mv "$tmp/codeindex" "$INSTALL_DIR/codeindex"
	chmod +x "$INSTALL_DIR/codeindex"
	log "Installed $INSTALL_DIR/codeindex"

	case ":$PATH:" in
		*":$INSTALL_DIR:"*) ;;
		*) log "Add it to your PATH:  export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
	esac
}

start_minnow() {
	if ! have docker; then
		log "Docker not found, skipping the Minnow server."
		log "Start one yourself, then run: codeindex setup --minnow-url URL"
		return 1
	fi
	if curl -fsS "$MINNOW_URL/healthz" >/dev/null 2>&1; then
		log "Minnow already running on $MINNOW_URL"
		return 0
	fi

	dir="${XDG_CONFIG_HOME:-$HOME/.config}/minnow"
	mkdir -p "$dir"
	curl -fsSL -o "$dir/compose.yaml" \
		"https://raw.githubusercontent.com/$REPO/main/compose.yaml" ||
		die "could not download compose.yaml"

	if [ "$EMBEDDER" = ollama ] && ! curl -fsS http://127.0.0.1:11434/api/tags >/dev/null 2>&1; then
		log "Ollama is not answering on port 11434. Install it from https://ollama.com,"
		log "then run: ollama pull all-minilm"
		log "Or use OpenAI instead: MINNOW_EMBEDDER=openai OPENAI_API_KEY=sk-... $0"
	fi

	log "Starting Minnow on $MINNOW_URL (embedder: $EMBEDDER)"
	MINNOW_EMBEDDER="$EMBEDDER" MINNOW_PORT="$PORT" \
		docker compose -f "$dir/compose.yaml" -p minnow up -d ||
		die "docker compose up failed"

	i=0
	while [ "$i" -lt 60 ]; do
		if curl -fsS "$MINNOW_URL/healthz" >/dev/null 2>&1; then
			log "Minnow is healthy"
			return 0
		fi
		i=$((i + 1))
		sleep 1
	done
	log "Minnow did not become healthy in 60s. Check: docker compose -p minnow logs"
	return 1
}

install_codeindex

server_ready=0
if [ "${SKIP_SERVER:-0}" != 1 ]; then
	start_minnow && server_ready=1
fi

if [ "$server_ready" = 1 ]; then
	# No --force: an existing config may point at a hosted Minnow.
	if "$INSTALL_DIR/codeindex" setup --minnow-url "$MINNOW_URL" >/dev/null 2>&1; then
		log "Wrote the codeindex config pointing at $MINNOW_URL"
	else
		log "Kept your existing codeindex config. To repoint it:"
		log "  codeindex setup --minnow-url $MINNOW_URL --force"
	fi
fi

cat >&2 <<EOF

Next, from inside a Git repository:

  codeindex codebase          index the current branch
  codeindex hooks install     reindex after checkouts and commits

Then register the MCP server with your agent:

  Claude Code   claude mcp add codeindex -- codeindex mcp --root "\$PWD"
  Codex         add codeindex to mcp_servers in ~/.codex/config.toml
  OpenCode      add codeindex to mcp in opencode.jsonc

A remote Minnow needs a token. Forward it in the agent's own server entry,
not just your shell: Claude Code takes -e MINNOW_TOKEN=..., Codex uses
env_vars, OpenCode uses an environment map. See docs/mcp.md.
EOF
