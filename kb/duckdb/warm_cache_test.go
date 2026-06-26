package duckdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	kb "github.com/mikills/minnow/kb"
)

func TestSelectShardsForWarm(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	shards := []kb.SnapshotShardMetadata{
		{ShardID: "old", SealedAt: base},
		{ShardID: "new", SealedAt: base.Add(2 * time.Hour)},
		{ShardID: "mid", SealedAt: base.Add(1 * time.Hour)},
	}

	t.Run("picks most recently sealed first", func(t *testing.T) {
		got := selectShardsForWarm(shards, 2)
		assert.Equal(t, []string{"new", "mid"}, ids(got))
	})

	t.Run("returns all when n exceeds count", func(t *testing.T) {
		got := selectShardsForWarm(shards, 10)
		assert.Equal(t, []string{"new", "mid", "old"}, ids(got))
	})

	t.Run("does not mutate input order", func(t *testing.T) {
		_ = selectShardsForWarm(shards, 1)
		assert.Equal(t, []string{"old", "new", "mid"}, ids(shards))
	})
}

func TestKBIDsFromManifestObjects(t *testing.T) {
	objects := []kb.BlobObjectInfo{
		{Key: "alpha.duckdb.manifest.json"},
		{Key: "alpha.duckdb.manifest.json"}, // duplicate
		{Key: "beta.duckdb.manifest.json"},
		{Key: "beta.duckdb/part-00000"}, // shard data, not a manifest
		{Key: ".duckdb.manifest.json"},  // empty kbID
	}

	got := kbIDsFromManifestObjects(objects)
	assert.Equal(t, []string{"alpha", "beta"}, got)
}

func ids(shards []kb.SnapshotShardMetadata) []string {
	out := make([]string, len(shards))
	for i, s := range shards {
		out[i] = s.ShardID
	}
	return out
}
