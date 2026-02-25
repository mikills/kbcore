package kb

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBlobManifestStore(t *testing.T) {
	ctx := context.Background()
	kbID := "test-manifest-store"

	sampleManifest := SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         "duckdb_shard_files",
		KBID:           kbID,
		CreatedAt:      time.Now().UTC().Truncate(time.Millisecond),
		TotalSizeBytes: 1024,
		Shards: []SnapshotShardMetadata{
			{
				ShardID:    "shard-00000",
				Key:        kbID + ".duckdb.shards/abc/shard-00000.duckdb",
				SizeBytes:  1024,
				VectorRows: 10,
				CreatedAt:  time.Now().UTC().Truncate(time.Millisecond),
			},
		},
	}

	tests := []struct {
		name string
		run  func(t *testing.T, store ManifestStore)
	}{
		{
			name: "get_missing",
			run: func(t *testing.T, store ManifestStore) {
				_, err := store.Get(ctx, kbID)
				if !errors.Is(err, ErrManifestNotFound) {
					t.Fatalf("expected ErrManifestNotFound, got %v", err)
				}
			},
		},
		{
			name: "upsert_create_empty_version",
			run: func(t *testing.T, store ManifestStore) {
				version, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if version == "" {
					t.Fatal("expected non-empty version")
				}
			},
		},
		{
			name: "get_after_upsert",
			run: func(t *testing.T, store ManifestStore) {
				version, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				if err != nil {
					t.Fatalf("upsert: %v", err)
				}

				doc, err := store.Get(ctx, kbID)
				if err != nil {
					t.Fatalf("get: %v", err)
				}
				if doc.Version != version {
					t.Fatalf("version mismatch: got %q, want %q", doc.Version, version)
				}
				if doc.Manifest.KBID != kbID {
					t.Fatalf("kbID mismatch: got %q, want %q", doc.Manifest.KBID, kbID)
				}
				if len(doc.Manifest.Shards) != len(sampleManifest.Shards) {
					t.Fatalf("shard count mismatch: got %d, want %d", len(doc.Manifest.Shards), len(sampleManifest.Shards))
				}
			},
		},
		{
			name: "upsert_cas_conflict",
			run: func(t *testing.T, store ManifestStore) {
				_, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				if err != nil {
					t.Fatalf("first upsert: %v", err)
				}

				_, err = store.UpsertIfMatch(ctx, kbID, sampleManifest, "stale-version")
				if !errors.Is(err, ErrBlobVersionMismatch) {
					t.Fatalf("expected ErrBlobVersionMismatch, got %v", err)
				}
			},
		},
		{
			name: "upsert_cas_success",
			run: func(t *testing.T, store ManifestStore) {
				v1, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				if err != nil {
					t.Fatalf("first upsert: %v", err)
				}

				v2, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, v1)
				if err != nil {
					t.Fatalf("second upsert: %v", err)
				}
				if v2 == "" {
					t.Fatal("expected non-empty version after CAS update")
				}
			},
		},
		{
			name: "head_version_missing",
			run: func(t *testing.T, store ManifestStore) {
				version, err := store.HeadVersion(ctx, kbID)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if version != "" {
					t.Fatalf("expected empty version for missing manifest, got %q", version)
				}
			},
		},
		{
			name: "head_version_exists",
			run: func(t *testing.T, store ManifestStore) {
				upsertVersion, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				if err != nil {
					t.Fatalf("upsert: %v", err)
				}

				headVersion, err := store.HeadVersion(ctx, kbID)
				if err != nil {
					t.Fatalf("head: %v", err)
				}
				if headVersion != upsertVersion {
					t.Fatalf("version mismatch: head=%q, upsert=%q", headVersion, upsertVersion)
				}
			},
		},
		{
			name: "delete_existing",
			run: func(t *testing.T, store ManifestStore) {
				_, err := store.UpsertIfMatch(ctx, kbID, sampleManifest, "")
				if err != nil {
					t.Fatalf("upsert: %v", err)
				}

				if err := store.Delete(ctx, kbID); err != nil {
					t.Fatalf("delete: %v", err)
				}

				_, err = store.Get(ctx, kbID)
				if !errors.Is(err, ErrManifestNotFound) {
					t.Fatalf("expected ErrManifestNotFound after delete, got %v", err)
				}
			},
		},
		{
			name: "delete_missing",
			run: func(t *testing.T, store ManifestStore) {
				err := store.Delete(ctx, kbID)
				if err != nil {
					t.Fatalf("expected nil for deleting missing manifest, got %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			blobRoot := t.TempDir()
			blobStore := &LocalBlobStore{Root: blobRoot}
			store := &BlobManifestStore{Store: blobStore}
			tc.run(t, store)
		})
	}
}
