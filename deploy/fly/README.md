# Fly.io

This deploys one always-running x86-64 Minnow Machine with a persistent Fly
Volume. Fly Proxy terminates HTTPS, and an in-Machine Caddy process requires a
static bearer token on every route except `/healthz`.

The default Ashburn deployment is estimated at approximately **$12.89/month**
before embeddings and variable usage. See [deployment pricing](../pricing.md).

## Deploy

From the repository root:

```bash
fly auth login
# Edit `app` and `primary_region` in deploy/fly/fly.toml, then use that same
# app name and region below.
fly apps create YOUR_APP_NAME
fly volumes create minnow_data --region YOUR_REGION --size 10 \
  --scheduled-snapshots --snapshot-retention 14 --app YOUR_APP_NAME
fly secrets set OPENAI_API_KEY=sk-... \
  MINNOW_TOKEN="$(openssl rand -hex 32)" --app YOUR_APP_NAME
fly deploy --config deploy/fly/fly.toml --ha=false
fly scale count 1 --app YOUR_APP_NAME
fly checks list --app YOUR_APP_NAME
curl https://YOUR_APP_NAME.fly.dev/healthz
```

If `fly launch` already created the app, skip `fly apps create`. Keep one
Machine in the same region as the volume. Fly Volumes attach to one Machine,
exist in one region, and do not replicate automatically; create snapshots or a
second backup strategy for important data. Without MongoDB, a restart can lose
queued/in-flight operations even though published blobs and manifests persist.

The Docker image currently targets `linux/amd64` because the repository ships
DuckDB extensions for Linux x86-64. Do not change `architecture` to arm64 until
matching extensions are included and tested.

## Connect clients

Use the same token supplied with `fly secrets set`:

```bash
export MINNOW_TOKEN='the-generated-token'
codeindex setup --minnow-url https://YOUR_APP_NAME.fly.dev \
  --token-env MINNOW_TOKEN
codex mcp add minnow --url https://YOUR_APP_NAME.fly.dev/mcp \
  --bearer-token-env-var MINNOW_TOKEN
```

This is a shared deployment token, not per-user authorization. Rotate it with
`fly secrets set MINNOW_TOKEN=...` and update every client.
