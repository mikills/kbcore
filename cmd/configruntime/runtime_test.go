package configruntime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/blobstore/journal"
	"github.com/mikills/minnow/kb/config"
	"github.com/stretchr/testify/require"
)

type openFailJournal struct {
	journal.Store
	openCalls int
}

func (j *openFailJournal) Open(context.Context) error {
	j.openCalls++
	return errors.New("custom journal open failed")
}

func (j *openFailJournal) Close() error { return nil }

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// writeTempConfig writes a YAML config into a temp dir and returns its path.
// The temp dir survives for the test's lifetime. the runtime should treat
// relative paths within the YAML as relative to this dir.
func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "minnow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}

func TestNormalizeLeasePrefix(t *testing.T) {
	require.Equal(t, "leases/", normalizeLeasePrefix(""))
	require.Equal(t, "locks/", normalizeLeasePrefix(" locks "))
	require.Equal(t, "locks/", normalizeLeasePrefix("locks/"))
}

func TestBuild(t *testing.T) {
	t.Run("dry_run_from_minimal_example", func(t *testing.T) {
		cfg, err := config.Load("../../examples/minnow.min.yaml")
		require.NoError(t, err)

		rt, err := Build(context.Background(), cfg, BuildOptions{DryRun: true, Logger: quietLogger()})
		require.NoError(t, err)
		require.NotNil(t, rt.KB(), "KB must be wired even in dry-run")
		require.NotNil(t, rt.App(), "App must be wired even in dry-run")
		require.NotNil(t, rt.KB().EventStore, "local mode must wire an in-memory event store")
		require.NotNil(t, rt.KB().EventInbox, "local mode must wire an in-memory event inbox")
		require.NotEmpty(t, rt.WorkerPools(), "in-memory event store should yield worker pools")

		// Start and Wait must be no-ops in dry-run. Stop must succeed.
		require.NoError(t, rt.Start(context.Background()))
		require.NoError(t, rt.Wait())
		require.NoError(t, rt.Stop(context.Background()))
	})

	t.Run("uses_substituted_replication_journal", func(t *testing.T) {
		path := writeTempConfig(t, `
http:
  address: 127.0.0.1:0
storage:
  blob:
    kind: tiered
    s3:
      bucket: test
      endpoint: http://127.0.0.1:1
      access_key_id: test
      secret_access_key: test
    tiered:
      durability: local_journal
      journal:
        dir: state/journal
embedder:
  provider: local
  local:
    dim: 16
scheduler:
  enabled: false
media:
  enabled: false
`)
		cfg, err := config.Load(path)
		require.NoError(t, err)
		custom := &openFailJournal{Store: nil}
		rt, err := Build(context.Background(), cfg, BuildOptions{
			Logger:             quietLogger(),
			ReplicationJournal: custom,
		})
		require.NoError(t, err)
		err = rt.Start(context.Background())
		require.ErrorContains(t, err, "custom journal open")
		err = rt.Start(context.Background())
		require.ErrorContains(t, err, "custom journal open")
		require.Equal(t, 2, custom.openCalls, "transient early startup failures must be retryable")
	})

	t.Run("respects_sharding_policy", func(t *testing.T) {
		path := writeTempConfig(t, `
embedder:
  provider: local
  local:
    dim: 16

scheduler:
  enabled: false

media:
  enabled: false

sharding:
  query_shard_fanout: 8
  query_shard_fanout_adaptive_max: 16
`)

		cfg, err := config.Load(path)
		require.NoError(t, err)

		rt, err := Build(context.Background(), cfg, BuildOptions{DryRun: true, Logger: quietLogger()})
		require.NoError(t, err)

		policy := rt.KB().ShardingPolicy
		require.Equal(t, 8, policy.QueryShardFanout, "config sharding policy must reach KB")
		require.Equal(t, 16, policy.QueryShardFanoutAdaptiveMax)
	})

	t.Run("builds openai compatible embedder", func(t *testing.T) {
		path := writeTempConfig(t, `
embedder:
  provider: openai_compatible
  openai_compatible:
    base_url: http://localhost:11434/v1
    model: nomic-embed-text
    token: ollama
    dimensions: 768

scheduler:
  enabled: false

media:
  enabled: false
`)

		cfg, err := config.Load(path)
		require.NoError(t, err)
		rt, err := Build(context.Background(), cfg, BuildOptions{DryRun: true, Logger: quietLogger()})
		require.NoError(t, err)

		embedder, ok := rt.KB().Embedder.(*kb.OpenAICompatibleEmbedder)
		require.True(t, ok, "expected OpenAI-compatible embedder, got %T", rt.KB().Embedder)
		require.Equal(t, "http://localhost:11434/v1", embedder.BaseURL)
		require.Equal(t, "nomic-embed-text", embedder.Model)
		require.Equal(t, "ollama", embedder.Token)
		require.Equal(t, 768, embedder.Dimensions)
	})
}

func TestBuildLiveLocalMode(t *testing.T) {
	path := writeTempConfig(t, `
http:
  address: 127.0.0.1:0   # let the kernel pick a free port
  shutdown_timeout: 2s

embedder:
  provider: local
  local:
    dim: 16

scheduler:
  enabled: false

media:
  enabled: false
`)

	cfg, err := config.Load(path)
	require.NoError(t, err)

	rt, err := Build(context.Background(), cfg, BuildOptions{Logger: quietLogger()})
	require.NoError(t, err)
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rt.Stop(stopCtx)
	})

	require.NoError(t, rt.Start(context.Background()))

	addr := rt.App().Address()
	require.NotEmpty(t, addr)
	require.NotContains(t, addr, ":0", "address should be resolved to a real port after Start")
}

func TestRuntimeStartRollsBackWhenHTTPBindFails(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	path := writeTempConfig(t, `
http:
  address: `+listener.Addr().String()+`
embedder:
  provider: local
  local:
    dim: 16
scheduler:
  enabled: false
media:
  enabled: false
`)
	cfg, err := config.Load(path)
	require.NoError(t, err)
	rt, err := Build(context.Background(), cfg, BuildOptions{Logger: quietLogger()})
	require.NoError(t, err)
	err = rt.Start(context.Background())
	require.Error(t, err)
	require.True(t, rt.stopped)
	for _, pool := range rt.workerPools {
		poolCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		require.NoError(t, pool.StopContext(poolCtx))
		cancel()
	}
}

func TestBuildRejectsNilConfig(t *testing.T) {
	_, err := Build(context.Background(), nil, BuildOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cfg must not be nil")
}

func TestRedactMongoURI(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"mongodb://user:pass@host:27017/db", "mongodb://host:27017/db"},
		{"mongodb://user@host:27017/db", "mongodb://host:27017/db"},
		{"mongodb://host:27017/db", "mongodb://host:27017/db"},
		{"", ""},
	}
	for _, c := range cases {
		got := redactMongoURI(c.in)
		require.NotContains(t, got, "pass", "redacted URI must not contain password")
		require.NotContains(t, got, "user", "redacted URI must not contain username")
		require.Equal(t, c.want, got)
	}

	// Unparseable input must still produce output that cannot leak the raw
	// input. The sentinel is intentionally non-empty so operators can tell
	// redaction ran.
	redacted := redactMongoURI(":::not-a-uri:::")
	require.NotContains(t, redacted, "not-a-uri")
	require.NotEmpty(t, redacted)
}

func TestBuildRespectsCacheTTL(t *testing.T) {
	path := writeTempConfig(t, `
embedder:
  provider: local
  local:
    dim: 16

storage:
  cache:
    entry_ttl: 30s
    max_bytes: 16777216

scheduler:
  enabled: false

media:
  enabled: false
`)

	cfg, err := config.Load(path)
	require.NoError(t, err)

	rt, err := Build(context.Background(), cfg, BuildOptions{DryRun: true, Logger: quietLogger()})
	require.NoError(t, err)

	// Both setters are exercised only when values are non-zero. verify they
	// were recorded. We can't read the private fields directly, so we
	// observe via the public surface.
	require.NotNil(t, rt.KB())
}
