package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mikills/minnow/kb/duckdb/internal/connection"
)

// Where memlimit.Shape.MinDatabaseBytes comes from. Gated: it builds a real
// index several times, and the failing rung can abort the process.
func TestIndexBuildMemoryFloor(t *testing.T) {
	if os.Getenv("MINNOW_MEASURE_INDEX_FLOOR") == "" {
		t.Skip("set MINNOW_MEASURE_INDEX_FLOOR=1 to measure")
	}
	rows := envInt(t, "MINNOW_FLOOR_ROWS", 75000)
	dim := envInt(t, "MINNOW_FLOOR_DIM", 512)
	shape := os.Getenv("MINNOW_FLOOR_VECTORS")
	if shape == "" {
		shape = "random"
	}
	limits := []int{512, 384, 320, 288, 256, 240, 224, 208, 192, 176, 160, 144, 128, 112, 96, 88, 80, 72, 64, 56, 48, 40, 32}

	raw := int64(rows) * int64(dim) * 4
	t.Logf("%d rows x %d dim, %s vectors, %d MiB of raw vector data", rows, dim, shape, raw>>20)
	lowest := 0
	for _, mb := range limits {
		took, err := buildIndexAt(t, rows, dim, shape, fmt.Sprintf("%dMB", mb))
		if err != nil {
			t.Logf("%4dMB FAILED after %s", mb, took.Round(time.Millisecond))
			break
		}
		t.Logf("%4dMB ok in %s", mb, took.Round(time.Millisecond))
		lowest = mb
	}
	require.NotZero(t, lowest, "no limit in the list could build the index")
	t.Logf("RESULT rows=%d dim=%d shape=%s raw_mib=%d floor_mb=%d ratio=%.2f",
		rows, dim, shape, raw>>20, lowest, float64(lowest<<20)/float64(raw))
}

func buildIndexAt(t *testing.T, rows, dim int, shape, limit string) (time.Duration, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shard.duckdb")
	db, err := connection.Open(context.Background(), path, connection.Config{
		ExtensionDir: "../../extensions",
		MemoryLimit:  limit,
		TempDir:      t.TempDir(),
		Threads:      4,
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	require.NoError(t, seedVectors(ctx, db, rows, dim, shape))

	start := time.Now()
	_, err = db.ExecContext(ctx, `CREATE INDEX docs_vec_idx ON docs USING HNSW (embedding)`)
	return time.Since(start), err
}

func seedVectors(ctx context.Context, db *sql.DB, rows, dim int, shape string) error {
	create := fmt.Sprintf(`CREATE TABLE docs (id VARCHAR, embedding FLOAT[%d])`, dim)
	if _, err := db.ExecContext(ctx, create); err != nil {
		return err
	}
	// Real embeddings are spread through the space. A smooth function puts
	// every vector on a curve and builds an order of magnitude faster.
	element := "random() * 2 - 1"
	if shape == "smooth" {
		element = "sin(i * 0.001 + j)"
	}
	insert := fmt.Sprintf(`INSERT INTO docs SELECT
		'doc-' || i::VARCHAR,
		[%s FOR j IN range(%d)]::FLOAT[%d]
	FROM range(%d) t(i)`, element, dim, dim, rows)
	_, err := db.ExecContext(ctx, insert)
	return err
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	require.NoError(t, err, "%s must be a number", name)
	return value
}
