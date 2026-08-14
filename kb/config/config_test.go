package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func TestPathsOverlapCanonicalFilesystemIdentity(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)
	require.True(t, pathsOverlap("state", filepath.Join(cwd, "state")))
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "state"), 0o700))
	require.NoError(t, os.Symlink(filepath.Join(dir, "state"), filepath.Join(dir, "alias")))
	require.True(t, pathsOverlap(filepath.Join(dir, "state"), filepath.Join(dir, "alias")))
}

func TestLoad(t *testing.T) {
	t.Run("minimal file uses schema defaults", func(t *testing.T) {
		cfg, err := Load("testdata/minimal.yaml")
		require.NoError(t, err)

		// HTTP defaults: Address is filled by applyDefaults, timeouts are
		// nil pointers and resolved by helpers.
		require.Equal(t, "127.0.0.1:8080", cfg.HTTP.Address)
		require.Nil(t, cfg.HTTP.ReadHeaderTimeout, "absent key must stay nil for explicit/absent distinction")
		require.Equal(t, 5*time.Second, cfg.HTTPReadHeaderTimeout())
		require.Equal(t, 5*time.Second, cfg.HTTPShutdownTimeout())

		// Storage defaults (blob.root is an absolute path now).
		require.Equal(t, "local", cfg.Storage.Blob.Kind)
		require.True(t, filepath.IsAbs(cfg.Storage.Blob.Root), "relative paths must be resolved to absolute")
		require.Contains(t, cfg.Storage.Blob.Root, ".temp/fixtures")

		// Format defaults.
		require.Equal(t, "duckdb", cfg.Format.Kind)
		require.Equal(t, "128MB", cfg.Format.DuckDB.MemoryLimit)

		// Explicit embedder.
		require.Equal(t, "local", cfg.Embedder.Provider)
		require.NotNil(t, cfg.Embedder.Local)
		require.Equal(t, 64, cfg.Embedder.Local.Dim)

		// Mongo omitted => in-memory mode.
		require.Nil(t, cfg.Mongo)

		// Workers defaults filled in.
		require.Equal(t, 5, cfg.Workers.Defaults.MaxAttempts)
		require.Equal(t, 4, cfg.Workers.DocumentUpsert.Concurrency)
		require.Equal(t, 2, cfg.Workers.MediaUpload.Concurrency)
	})

	t.Run("full file round-trips every field", func(t *testing.T) {
		t.Setenv("MINNOW_TEST_MONGO_URI", "mongodb://example:27017")
		cfg, err := Load("testdata/full.yaml")
		require.NoError(t, err)

		require.Equal(t, 5*time.Minute, cfg.Workers.Defaults.VisibilityTimeout.AsDuration())
		require.NotNil(t, cfg.Workers.DocumentPublish.VisibilityTimeout)
		require.Equal(t, 10*time.Minute, cfg.Workers.DocumentPublish.VisibilityTimeout.AsDuration(),
			"per-pool override must beat workers.defaults")
		require.NotNil(t, cfg.Mongo)
		require.Equal(t, "mongodb://example:27017", cfg.Mongo.URI)
		require.ElementsMatch(t, []string{"application/pdf", "image/png", "image/jpeg"}, cfg.Media.ContentTypeAllowlist)

		// Sharding fields all explicit.
		require.NotNil(t, cfg.Sharding.ShardTriggerBytes)
		require.EqualValues(t, 67108864, *cfg.Sharding.ShardTriggerBytes)
		require.NotNil(t, cfg.Sharding.CompactionEnabled)
		require.True(t, *cfg.Sharding.CompactionEnabled)
	})

	t.Run("tiered storage applies safe defaults and resolves journal path", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		body := `
storage:
  blob:
    kind: tiered
    s3:
      bucket: minnow-test
    tiered:
      durability: local_journal
      journal:
        dir: state/journal
  cache:
    dir: state/cache
embedder:
  provider: local
  local:
    dim: 16
`
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		cfg, err := Load(path)
		require.NoError(t, err)
		require.Equal(t, "tiered", cfg.Storage.Blob.Kind)
		require.Equal(t, "us-east-1", cfg.Storage.Blob.S3.Region)
		require.Equal(t, "local_journal", cfg.Storage.Blob.Tiered.Durability)
		require.Equal(t, "local", cfg.Storage.Blob.Tiered.Journal.Kind)
		require.Equal(t, filepath.Join(dir, "state/journal"), cfg.Storage.Blob.Tiered.Journal.Dir)
		require.Equal(t, 10000, cfg.Storage.Blob.Tiered.Journal.MaxPendingEntries)
		require.EqualValues(t, 1073741824, cfg.Storage.Blob.Tiered.Journal.MaxPendingBytes)
		require.EqualValues(t, 268435456, cfg.Storage.Blob.Tiered.Journal.MinFreeBytes)
		require.Equal(t, 20, cfg.Storage.Blob.Tiered.Replication.MaxAttempts)
	})

	t.Run("cache watermarks default low threshold and validate hysteresis", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "minnow.yaml")
		body := `
storage:
  cache:
    high_watermark_percent: 80
    min_free_bytes: 1073741824
embedder:
  provider: local
  local:
    dim: 16
`
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		cfg, err := Load(path)
		require.NoError(t, err)
		require.Equal(t, 80, cfg.Storage.Cache.HighWatermarkPercent)
		require.Equal(t, 70, cfg.Storage.Cache.LowWatermarkPercent)

		invalid := strings.Replace(body, "high_watermark_percent: 80", "high_watermark_percent: 80\n    low_watermark_percent: 90", 1)
		require.NoError(t, os.WriteFile(path, []byte(invalid), 0o600))
		_, err = Load(path)
		require.ErrorContains(t, err, "low_watermark_percent must be less")
	})

	t.Run("tiered storage rejects cache-journal symlink aliases", func(t *testing.T) {
		dir := t.TempDir()
		realState := filepath.Join(dir, "state")
		require.NoError(t, os.MkdirAll(realState, 0o700))
		cacheAlias := filepath.Join(dir, "cache-alias")
		require.NoError(t, os.Symlink(realState, cacheAlias))
		path := filepath.Join(dir, "minnow.yaml")
		body := `
storage:
  blob:
    kind: tiered
    s3:
      bucket: minnow-test
    tiered:
      journal:
        dir: state
  cache:
    dir: cache-alias
embedder:
  provider: local
  local:
    dim: 16
`
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		_, err := Load(path)
		require.ErrorContains(t, err, "journal.dir must not equal")
	})

	t.Run("tiered storage rejects unsafe or incomplete configuration", func(t *testing.T) {
		base := `
storage:
  blob:
    kind: tiered
    s3:
      bucket: minnow-test
    tiered:
      durability: remote
      journal:
        dir: state
  cache:
    dir: state
embedder:
  provider: local
  local:
    dim: 16
`
		path := filepath.Join(t.TempDir(), "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(base), 0o600))
		_, err := Load(path)
		require.ErrorContains(t, err, "journal.dir must not equal")
	})

	t.Run("rejects misspelled nested lifecycle keys", func(t *testing.T) {
		validBase := "embedder:\n  provider: local\n  local:\n    dim: 16\n"
		for _, body := range []string{
			validBase + "scheduler:\n  enabeld: false\n",
			validBase + "mcp:\n  http_stateles: false\n",
		} {
			path := filepath.Join(t.TempDir(), "minnow.yaml")
			require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
			_, err := Load(path)
			require.Error(t, err)
		}
	})

	t.Run("rejects unknown keys with key name", func(t *testing.T) {
		_, err := Load("testdata/invalid_unknown_key.yaml")
		require.Error(t, err)
		require.Contains(t, err.Error(), "currency",
			"strict decoder must name the unknown field; got: %v", err)
	})

	t.Run("rejects malformed duration with line number", func(t *testing.T) {
		_, err := Load("testdata/invalid_duration.yaml")
		require.Error(t, err)
		require.Contains(t, err.Error(), "line", "error should reference a line number")
		require.Contains(t, err.Error(), "duration")
	})

	t.Run("rejects cross-field sharding violation", func(t *testing.T) {
		_, err := Load("testdata/invalid_sharding_fanout.yaml")
		require.Error(t, err)
		require.Contains(t, err.Error(), "adaptive_max")
		require.Contains(t, err.Error(), "query_shard_fanout")
	})

	t.Run("rejects unknown embedder provider", func(t *testing.T) {
		_, err := Load("testdata/invalid_embedder_provider.yaml")
		require.Error(t, err)
		require.Contains(t, err.Error(), "gpt4all")
	})

	t.Run("loads openai compatible embedder", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: openai_compatible
  openai_compatible:
    base_url: http://localhost:11434/v1
    model: nomic-embed-text
    token: ollama
    dimensions: 1024
`), 0o644))

		cfg, err := Load(path)
		require.NoError(t, err)
		require.Equal(t, "openai_compatible", cfg.Embedder.Provider)
		require.NotNil(t, cfg.Embedder.OpenAICompatible)
		require.Equal(t, "http://localhost:11434/v1", cfg.Embedder.OpenAICompatible.BaseURL)
		require.Equal(t, "nomic-embed-text", cfg.Embedder.OpenAICompatible.Model)
		require.Equal(t, "ollama", cfg.Embedder.OpenAICompatible.Token)
		require.Equal(t, 1024, cfg.Embedder.OpenAICompatible.Dimensions)
	})

	t.Run("rejects invalid openai compatible config", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: openai_compatible
  openai_compatible:
    base_url: ftp://example.com/v1
    model: model
`), 0o644))

		_, err := Load(path)
		require.Error(t, err)
		require.Contains(t, err.Error(), "base_url scheme")
	})

	t.Run("rejects mongo block without uri", func(t *testing.T) {
		_, err := Load("testdata/invalid_mongo_uri.yaml")
		require.Error(t, err)
		require.Contains(t, err.Error(), "mongo.uri")
	})

	t.Run("resolves unresolved env vars with all names listed", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
  local:
    dim: 64
mongo:
  uri: ${MINNOW_NOT_SET_A}
  database: ${MINNOW_NOT_SET_B}
  collections:
    manifests: m
    events: e
    inbox: i
    media: x
`), 0o644))

		_, err := Load(path)
		require.Error(t, err)
		require.Contains(t, err.Error(), "MINNOW_NOT_SET_A")
		require.Contains(t, err.Error(), "MINNOW_NOT_SET_B")
	})

	t.Run("env vars are interpolated into scalar values", func(t *testing.T) {
		t.Setenv("MINNOW_CFG_TEST_URI", "mongodb://injected:27017")
		t.Setenv("MINNOW_CFG_TEST_DB", "injected-db")
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
  local:
    dim: 64
mongo:
  uri: ${MINNOW_CFG_TEST_URI}
  database: ${MINNOW_CFG_TEST_DB}
  collections:
    manifests: m
    events: e
    inbox: i
    media: x
`), 0o644))

		cfg, err := Load(path)
		require.NoError(t, err)
		require.Equal(t, "mongodb://injected:27017", cfg.Mongo.URI)
		require.Equal(t, "injected-db", cfg.Mongo.Database)
	})

	t.Run("relative paths resolve against YAML file directory", func(t *testing.T) {
		outer := t.TempDir()
		nested := filepath.Join(outer, "deploy")
		require.NoError(t, os.MkdirAll(nested, 0o755))
		path := filepath.Join(nested, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
  local:
    dim: 64
storage:
  blob:
    root: ./blobs
  cache:
    dir: ./cache
format:
  duckdb:
    extension_dir: ./exts
`), 0o644))

		cfg, err := Load(path)
		require.NoError(t, err)
		require.Equal(t, filepath.Join(nested, "blobs"), cfg.Storage.Blob.Root,
			"blob.root must be resolved against the YAML's directory, not CWD")
		require.Equal(t, filepath.Join(nested, "cache"), cfg.Storage.Cache.Dir)
		require.Equal(t, filepath.Join(nested, "exts"), cfg.Format.DuckDB.ExtensionDir)
	})

	t.Run("rejects multi-document YAML", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
  local:
    dim: 64
---
embedder:
  provider: local
  local:
    dim: 99
`), 0o644))

		_, err := Load(path)
		require.Error(t, err)
		require.Contains(t, err.Error(), "additional YAML document")
	})
}

func TestDefaultPathResolution(t *testing.T) {
	t.Run("uses_local_config_first", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "minnow.yaml"), []byte(`
embedder:
  provider: local
  local:
    dim: 64
`), 0o644))

		path, err := ResolveDefaultPath()
		require.NoError(t, err)
		require.Equal(t, DefaultPath, path)
	})

	t.Run("falls_back_to_user_config", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		home := t.TempDir()
		t.Setenv("HOME", home)
		userPath, err := UserConfigPath()
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(filepath.Dir(userPath), 0o755))
		require.NoError(t, os.WriteFile(userPath, []byte(`
embedder:
  provider: local
  local:
    dim: 32
`), 0o644))

		path, err := ResolveDefaultPath()
		require.NoError(t, err)
		require.Equal(t, userPath, path)

		cfg, err := Load("")
		require.NoError(t, err)
		require.Equal(t, 32, cfg.Embedder.Local.Dim)
	})
}

func TestPoolConfigFor(t *testing.T) {
	t.Run("per-pool overrides beat defaults", func(t *testing.T) {
		t.Setenv("MINNOW_TEST_MONGO_URI", "mongodb://example:27017")
		cfg, err := Load("testdata/full.yaml")
		require.NoError(t, err)

		pub := cfg.PoolConfigFor(cfg.Workers.DocumentPublish)
		require.Equal(t, 2, pub.Concurrency)
		require.Equal(t, 10*time.Minute, pub.VisibilityTimeout, "override wins")
		require.Equal(t, 5, pub.MaxAttempts, "unoverridden fields come from defaults")

		ups := cfg.PoolConfigFor(cfg.Workers.DocumentUpsert)
		require.Equal(t, 5*time.Minute, ups.VisibilityTimeout, "no override => defaults")
	})
}

func TestShardingPolicyResolution(t *testing.T) {
	t.Run("absent block uses kb defaults", func(t *testing.T) {
		cfg, err := Load("testdata/minimal.yaml")
		require.NoError(t, err)

		policy := cfg.ShardingPolicy()
		require.Equal(t, kb.DefaultShardingPolicy().ShardTriggerBytes, policy.ShardTriggerBytes)
		require.Equal(t, kb.DefaultShardingPolicy().CompactionEnabled, policy.CompactionEnabled)
		require.False(t, policy.CompactionEnabledSet, "unset key must leave CompactionEnabledSet false")
	})

	t.Run("explicit compaction_enabled false preserves the sentinel", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
  local:
    dim: 64
sharding:
  compaction_enabled: false
`), 0o644))

		cfg, err := Load(path)
		require.NoError(t, err)

		policy := cfg.ShardingPolicy()
		require.False(t, policy.CompactionEnabled)
		require.True(t, policy.CompactionEnabledSet,
			"explicit YAML key must preserve the CompactionEnabledSet distinction")
	})

	t.Run("explicit compaction_enabled true also sets the sentinel", func(t *testing.T) {
		// The third state matters because kb.DefaultShardingPolicy() already
		// has CompactionEnabled=true. we still need CompactionEnabledSet to
		// flip so downstream code can tell user-confirmed-true from default.
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
  local:
    dim: 64
sharding:
  compaction_enabled: true
`), 0o644))

		cfg, err := Load(path)
		require.NoError(t, err)

		policy := cfg.ShardingPolicy()
		require.True(t, policy.CompactionEnabled)
		require.True(t, policy.CompactionEnabledSet,
			"explicit YAML key (even when value matches default) must mark the field as set")
	})
}

func TestConfigValidation(t *testing.T) {
	t.Run("rejects_bad_durations_and_counts", func(t *testing.T) {
		base := `
embedder:
  provider: local
  local:
    dim: 16
`
		cases := []struct {
			name    string
			extra   string
			wantSub string
		}{
			{
				name:    "negative read header timeout",
				extra:   "\nhttp:\n  read_header_timeout: -1s\n",
				wantSub: "http.read_header_timeout",
			},
			{
				name:    "zero shutdown timeout",
				extra:   "\nhttp:\n  shutdown_timeout: 0s\n",
				wantSub: "http.shutdown_timeout",
			},
			{
				name:    "zero scheduler tick",
				extra:   "\nscheduler:\n  tick_interval: 0s\n",
				wantSub: "scheduler.tick_interval",
			},
			{
				name:    "zero cache evict interval",
				extra:   "\nstorage:\n  cache:\n    evict_interval: 0s\n",
				wantSub: "storage.cache.evict_interval",
			},
			{
				name:    "negative cache entry TTL",
				extra:   "\nstorage:\n  cache:\n    entry_ttl: -5s\n",
				wantSub: "storage.cache.entry_ttl",
			},
			{
				name:    "graph enabled with zero parallelism",
				extra:   "\ngraph:\n  enabled: true\n  parallelism: 0\n",
				wantSub: "graph.parallelism",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, "minnow.yaml")
				require.NoError(t, os.WriteFile(path, []byte(base+tc.extra), 0o644))

				_, err := Load(path)
				require.Error(t, err, "validator must reject this config")
				require.Contains(t, err.Error(), tc.wantSub)
			})
		}
	})

	t.Run("interpolate_env_lists_all_missing", func(t *testing.T) {
		_, err := interpolateEnv([]byte(`
a: ${DOES_NOT_EXIST_A}
b: ${DOES_NOT_EXIST_B}
c: ${DOES_NOT_EXIST_A}  # duplicate should be collapsed
`))
		require.Error(t, err)
		msg := err.Error()
		require.Contains(t, msg, "DOES_NOT_EXIST_A")
		require.Contains(t, msg, "DOES_NOT_EXIST_B")
		// The names should be sorted and deduplicated.
		require.Equal(t, 1, strings.Count(msg, "DOES_NOT_EXIST_A"))
		// The error hints at the YAML-comment pitfall so operators don't stare
		// at a valid-looking config wondering which line is wrong.
		require.Contains(t, msg, "YAML comments")
	})

	t.Run("mongo_collections_must_be_distinct", func(t *testing.T) {
		t.Setenv("MINNOW_TEST_MONGO_URI", "mongodb://example:27017")
		cases := []struct {
			name    string
			body    string
			wantSub string
		}{
			{
				name: "two collections equal",
				body: `
embedder:
  provider: local
  local:
    dim: 16
mongo:
  uri: ${MINNOW_TEST_MONGO_URI}
  database: minnow
  collections:
    manifests: same
    events: same
    inbox: i
    media: m
`,
				wantSub: "distinct",
			},
			{
				name: "all four collections equal",
				body: `
embedder:
  provider: local
  local:
    dim: 16
mongo:
  uri: ${MINNOW_TEST_MONGO_URI}
  database: minnow
  collections:
    manifests: x
    events: x
    inbox: x
    media: x
`,
				wantSub: "distinct",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, "minnow.yaml")
				require.NoError(t, os.WriteFile(path, []byte(tc.body), 0o644))

				_, err := Load(path)
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantSub)
			})
		}
	})

	t.Run("scheduler_defaults_on_with_default_tick", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
  local:
    dim: 16
`), 0o644))

		cfg, err := Load(path)
		require.NoError(t, err)
		require.True(t, cfg.SchedulerEnabled())
		require.Equal(t, time.Minute, cfg.SchedulerTick())
	})

	t.Run("scheduler_can_be_disabled", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
  local:
    dim: 16
scheduler:
  enabled: false
`), 0o644))

		cfg, err := Load(path)
		require.NoError(t, err)
		require.False(t, cfg.SchedulerEnabled())
	})

	t.Run("media_tombstone_grace_bounded_by_pending_ttl", func(t *testing.T) {
		t.Run("tombstone_grace greater than pending_ttl is rejected", func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "minnow.yaml")
			require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
  local:
    dim: 16

media:
  enabled: true
  max_bytes: 1024
  pending_ttl: 1h
  tombstone_grace: 24h
  upload_completion_ttl: 15m
`), 0o644))

			_, err := Load(path)
			require.Error(t, err)
			require.Contains(t, err.Error(), "tombstone_grace")
			require.Contains(t, err.Error(), "pending_ttl")
		})

		t.Run("negative max_bytes is rejected when media enabled", func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "minnow.yaml")
			require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
  local:
    dim: 16

media:
  enabled: true
  max_bytes: -1
  pending_ttl: 24h
  tombstone_grace: 1h
  upload_completion_ttl: 15m
`), 0o644))

			_, err := Load(path)
			require.Error(t, err)
			require.Contains(t, err.Error(), "media.max_bytes")
		})
	})

	t.Run("embedder_provider_requires_its_block", func(t *testing.T) {
		t.Run("ollama provider without ollama block", func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "minnow.yaml")
			require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: ollama
`), 0o644))
			_, err := Load(path)
			require.Error(t, err)
			require.Contains(t, err.Error(), "embedder.ollama")
		})

		t.Run("local provider without local block", func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "minnow.yaml")
			require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
`), 0o644))
			_, err := Load(path)
			require.Error(t, err)
			require.Contains(t, err.Error(), "embedder.local")
		})
	})

	t.Run("path_traversal_rejected", func(t *testing.T) {
		outer := t.TempDir()
		nested := filepath.Join(outer, "deploy")
		require.NoError(t, os.MkdirAll(nested, 0o755))
		path := filepath.Join(nested, "minnow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
embedder:
  provider: local
  local:
    dim: 16
storage:
  blob:
    root: ../../../../../etc/evil
`), 0o644))

		_, err := Load(path)
		require.Error(t, err)
		require.Contains(t, err.Error(), "storage.blob.root")
		require.Contains(t, err.Error(), "outside")
	})
}

func TestGraphURLMustBeValid(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		wantSub string
	}{
		{"bare hostname without scheme", "localhost:11434", "scheme"},
		{"empty scheme but has path", "//host/path", "scheme"},
		{"ftp scheme", "ftp://host", "http or https"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "minnow.yaml")
			body := `
embedder:
  provider: local
  local:
    dim: 16

graph:
  enabled: true
  url: ` + tc.url + `
  model: llama3
`
			require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
			_, err := Load(path)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantSub)
		})
	}
}
