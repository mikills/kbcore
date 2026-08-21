package configruntime

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/mikills/minnow/kb/config"
	"github.com/stretchr/testify/require"
)

func TestRuntime(t *testing.T) {
	t.Run("dry_run_has_no_filesystem_side_effects", func(t *testing.T) {
		dir := t.TempDir()
		blobRoot := filepath.Join(dir, "blobs-should-not-exist")
		cacheDir := filepath.Join(dir, "cache-should-not-exist")
		yamlPath := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(yamlPath, []byte(`
embedder:
  provider: local
  local:
    dim: 16

storage:
  blob:
    root: `+blobRoot+`
  cache:
    dir: `+cacheDir+`

scheduler:
  enabled: true        # would normally start a goroutine
  tick_interval: 1s

media:
  enabled: false
`), 0o644))

		cfg, err := config.Load(yamlPath)
		require.NoError(t, err)

		rt, err := Build(context.Background(), cfg, BuildOptions{DryRun: true, Logger: quietLogger()})
		require.NoError(t, err)

		require.NoDirExists(t, blobRoot, "DryRun must not mkdir blob root")
		require.NoDirExists(t, cacheDir, "DryRun must not mkdir cache dir")

		// Start in DryRun is also a no-op: still no mkdirs, still no listener.
		require.NoError(t, rt.Start(context.Background()))
		require.NoDirExists(t, blobRoot, "DryRun Start must remain side-effect free")
		require.NoDirExists(t, cacheDir, "DryRun Start must remain side-effect free")
		require.Empty(t, rt.App().Address(), "DryRun must not bind a port")
		require.NoError(t, rt.Wait())
		require.NoError(t, rt.Stop(context.Background()))
	})

	t.Run("dry_run_does_not_connect_to_mongo", func(t *testing.T) {
		yaml := `
embedder:
  provider: local
  local:
    dim: 16

scheduler:
  enabled: false

media:
  enabled: false

mongo:
  uri: mongodb://nonexistent.invalid:27017/?serverSelectionTimeoutMS=100
  database: minnow
  collections:
    manifests: m
    events: e
    inbox: i
    media: x
`
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

		cfg, err := config.Load(path)
		require.NoError(t, err)

		// If wireMongo were called, it would attempt a Ping against an
		// unresolvable hostname. DryRun must skip Mongo entirely.
		rt, err := Build(context.Background(), cfg, BuildOptions{DryRun: true, Logger: quietLogger()})
		require.NoError(t, err, "DryRun must not attempt Mongo connect")
		require.NotNil(t, rt.KB(), "KB must be wired even in DryRun")
	})

	t.Run("dry_run_does_not_start_scheduler_or_pools", func(t *testing.T) {
		yaml := `
embedder:
  provider: local
  local:
    dim: 16

scheduler:
  enabled: true
  tick_interval: 1s

media:
  enabled: false
`
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

		cfg, err := config.Load(path)
		require.NoError(t, err)

		rt, err := Build(context.Background(), cfg, BuildOptions{DryRun: true, Logger: quietLogger()})
		require.NoError(t, err)

		require.NotNil(t, rt.Scheduler(), "scheduler is constructed in DryRun")
		require.NotEmpty(t, rt.WorkerPools(), "worker pools are constructed (in-memory event store wiring)")

		// Without Start being called, no goroutines should be running.
		require.Empty(t, rt.App().Address(), "DryRun must not bind a listener even when Start is omitted")
	})

	t.Run("non_dry_run_creates_dirs_and_binds", func(t *testing.T) {
		dir := t.TempDir()
		blobRoot := filepath.Join(dir, "blobs")
		cacheDir := filepath.Join(dir, "cache")
		yamlPath := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(yamlPath, []byte(`
http:
  address: 127.0.0.1:0
  shutdown_timeout: 2s

embedder:
  provider: local
  local:
    dim: 16

storage:
  blob:
    root: `+blobRoot+`
  cache:
    dir: `+cacheDir+`

scheduler:
  enabled: false

media:
  enabled: false
`), 0o644))

		cfg, err := config.Load(yamlPath)
		require.NoError(t, err)

		rt, err := Build(context.Background(), cfg, BuildOptions{Logger: quietLogger()})
		require.NoError(t, err)
		t.Cleanup(func() { _ = rt.Stop(context.Background()) })

		require.NoError(t, rt.Start(context.Background()))
		require.DirExists(t, blobRoot, "non-DryRun Start must mkdir blob root")
		require.DirExists(t, cacheDir, "non-DryRun Start must mkdir cache dir")

		host, port, err := net.SplitHostPort(rt.App().Address())
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1", host)
		require.NotEqual(t, "0", port, "kernel must have picked a real port")
	})
}
