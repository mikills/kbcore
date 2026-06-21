package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestS3BlobConfig(t *testing.T) {
	load := func(t *testing.T, yaml string) (*Config, error) {
		t.Helper()
		dir := t.TempDir()
		path := filepath.Join(dir, "minnow.yaml")
		base := "embedder:\n  provider: local\n  local:\n    dim: 32\n"
		require.NoError(t, os.WriteFile(path, []byte(base+yaml), 0o644))
		return Load(path)
	}

	t.Run("s3_minimal", func(t *testing.T) {
		cfg, err := load(t, "storage:\n  blob:\n    kind: s3\n    s3:\n      bucket: my-bucket\n")
		require.NoError(t, err)
		require.Equal(t, "s3", cfg.Storage.Blob.Kind)
		require.Equal(t, "my-bucket", cfg.Storage.Blob.S3.Bucket)
		require.Equal(t, "us-east-1", cfg.Storage.Blob.S3.Region)
	})

	t.Run("s3_full", func(t *testing.T) {
		cfg, err := load(t, `storage:
  blob:
    kind: s3
    s3:
      bucket: my-bucket
      region: eu-west-1
      prefix: minnow/
      endpoint: http://localhost:9000
      access_key_id: minioadmin
      secret_access_key: minioadmin
`)
		require.NoError(t, err)
		s3 := cfg.Storage.Blob.S3
		require.Equal(t, "my-bucket", s3.Bucket)
		require.Equal(t, "eu-west-1", s3.Region)
		require.Equal(t, "minnow/", s3.Prefix)
		require.Equal(t, "http://localhost:9000", s3.Endpoint)
		require.Equal(t, "minioadmin", s3.AccessKeyID)
	})

	t.Run("s3_missing_bucket", func(t *testing.T) {
		_, err := load(t, "storage:\n  blob:\n    kind: s3\n    s3:\n      region: us-east-1\n")
		require.ErrorContains(t, err, "storage.blob.s3.bucket")
	})

	t.Run("s3_missing_s3_block", func(t *testing.T) {
		_, err := load(t, "storage:\n  blob:\n    kind: s3\n")
		require.ErrorContains(t, err, "storage.blob.s3 is required")
	})

	t.Run("unsupported_kind", func(t *testing.T) {
		_, err := load(t, "storage:\n  blob:\n    kind: gcs\n")
		require.ErrorContains(t, err, "not supported")
	})

	t.Run("local_still_works", func(t *testing.T) {
		cfg, err := load(t, "storage:\n  blob:\n    kind: local\n    root: /tmp/blobs\n")
		require.NoError(t, err)
		require.Equal(t, "local", cfg.Storage.Blob.Kind)
		require.Equal(t, "/tmp/blobs", cfg.Storage.Blob.Root)
	})
}
