package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mikills/minnow/kb/duckdb/internal/connection"
)

// childDirEnv points a child process at the directory the parent will delete.
// A rung below the floor can abort rather than return an error, and an aborted
// process runs no cleanup, so t.TempDir() there strands gigabytes.
const childDirEnv = "MINNOW_FLOOR_CHILD_DIR"

const childLimitEnv = "MINNOW_FLOOR_CHILD_LIMIT"

// Where memlimit.MinDatabaseBytes comes from. Gated: it builds a real index
// once per rung.
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

	// One root the parent owns, so a rung that dies mid-build strands nothing.
	root, err := os.MkdirTemp("", "minnow-floor-")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(root) }()

	raw := int64(rows) * int64(dim) * 4
	t.Logf("%d rows x %d dim, %s vectors, %d MiB of raw vector data", rows, dim, shape, raw>>20)
	lowest := 0
	for _, mb := range limits {
		took, err := buildIndexInChild(t, root, mb)
		if err != nil {
			t.Logf("%4dMB FAILED after %s (%v)", mb, took.Round(time.Millisecond), err)
			break
		}
		t.Logf("%4dMB ok in %s", mb, took.Round(time.Millisecond))
		lowest = mb
	}
	require.NotZero(t, lowest, "no limit in the list could build the index")
	t.Logf("RESULT rows=%d dim=%d shape=%s raw_mib=%d floor_mb=%d ratio=%.2f",
		rows, dim, shape, raw>>20, lowest, float64(lowest<<20)/float64(raw))
}

var childTook = regexp.MustCompile(`BUILD_NS=(\d+)`)

// buildIndexInChild runs one rung in its own process. The parent owns the
// directory and removes it however the child ended, including the abort a rung
// under the floor can provoke.
func buildIndexInChild(t *testing.T, root string, limitMB int) (time.Duration, error) {
	t.Helper()
	dir := filepath.Join(root, fmt.Sprintf("rung-%d", limitMB))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
		_, err := os.Stat(dir)
		require.ErrorIs(t, err, os.ErrNotExist, "a dead child stranded its build directory")
	}()

	cmd := exec.Command(os.Args[0], "-test.run", "^TestIndexBuildFloorRung$", "-test.timeout", "60m")
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("%s=%s", childDirEnv, dir),
		fmt.Sprintf("%s=%d", childLimitEnv, limitMB),
	)
	start := time.Now()
	out, runErr := cmd.CombinedOutput()
	took := time.Since(start)
	if match := childTook.FindSubmatch(out); match != nil {
		if ns, convErr := strconv.ParseInt(string(match[1]), 10, 64); convErr == nil {
			took = time.Duration(ns)
		}
	}
	if runErr != nil {
		return took, fmt.Errorf("%w: %s", runErr, lastLine(out))
	}
	return took, nil
}

func lastLine(out []byte) string {
	const limit = 200
	text := string(out)
	if len(text) > limit {
		text = text[len(text)-limit:]
	}
	return text
}

// TestIndexBuildFloorRung is the child. It writes into the directory the parent
// gave it and never calls t.TempDir(), which the parent cannot clean up.
func TestIndexBuildFloorRung(t *testing.T) {
	dir := os.Getenv(childDirEnv)
	if dir == "" {
		t.Skip("spawned by TestIndexBuildMemoryFloor")
	}
	rows := envInt(t, "MINNOW_FLOOR_ROWS", 75000)
	dim := envInt(t, "MINNOW_FLOOR_DIM", 512)
	shape := os.Getenv("MINNOW_FLOOR_VECTORS")
	if shape == "" {
		shape = "random"
	}
	took, err := buildIndexAt(dir, rows, dim, shape, os.Getenv(childLimitEnv)+"MB")
	fmt.Printf("BUILD_NS=%d\n", took.Nanoseconds())
	require.NoError(t, err)
}

func buildIndexAt(dir string, rows, dim int, shape, limit string) (time.Duration, error) {
	tempDir := filepath.Join(dir, "spill")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return 0, err
	}
	db, err := connection.Open(context.Background(), filepath.Join(dir, "shard.duckdb"), connection.Config{
		ExtensionDir: "../../extensions",
		MemoryLimit:  limit,
		TempDir:      tempDir,
		Threads:      4,
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if err := seedVectors(ctx, db, rows, dim, shape); err != nil {
		return 0, err
	}
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
