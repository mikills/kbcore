# Deployment examples

Minnow can run as a single container wherever it has persistent storage and an
embeddings provider.

| Target | Files | Best for | Important limitation |
|---|---|---|---|
| Docker Compose | [`../compose.yaml`](../compose.yaml) | Laptop, home server, VPS | Binds to loopback; add authenticated HTTPS for remote access |
| Fly.io | [`fly/`](fly/) | Fast managed HTTPS deployment | Fly Volume is single-region/single-Machine; included bearer auth is one shared token |
| AWS | [`aws/`](aws/) | Durable VM with EBS, DNS, and bearer-protected HTTPS | Terraform example is single-instance, not HA |

See the [deployment pricing comparison](pricing.md) for baseline Fly.io, AWS,
storage, network, snapshot, and embedding API estimates with source links.

## Container quick start

```bash
export OPENAI_API_KEY=sk-...
docker compose up --build -d
curl http://127.0.0.1:8080/healthz
```

The named `minnow-data` volume contains the blob store and DuckDB query cache.
Back it up before removing volumes. Without MongoDB, queued/in-flight operations
and operation history are in memory and may be lost during restarts; published
manifests and shards remain on the volume. The image runs as UID/GID `10001`, bundles
the tested Linux x86-64 DuckDB extensions, and sets conservative defaults for a
2 GiB memory limit.

The image is currently **linux/amd64 only**. It can run under emulation on an
Apple Silicon development machine, but hosted deployments should select x86-64.

## Configuration

The image includes [`docker/minnow.yaml`](docker/minnow.yaml). To customize it,
mount your own file read-only:

```yaml
services:
  minnow:
    volumes:
      - ./minnow.yaml:/etc/minnow/minnow.yaml:ro
      - minnow-data:/var/lib/minnow
```

Keep secrets in environment variables referenced as `${VAR}` in YAML. The
provided configuration requires `OPENAI_API_KEY`. You may replace the embedder
with Ollama or Minnow's deterministic local embedder.

## Security

Minnow v0.2.2 does not validate bearer tokens. The root Compose file therefore
publishes port 8080 only on `127.0.0.1`. For remote clients, use Tailscale or put
an HTTPS reverse proxy/API gateway that validates credentials in front. The AWS
and Fly examples include a Caddy bearer-token boundary. It is one shared static
token—not user identity, fine-grained authorization, or native Minnow auth.
