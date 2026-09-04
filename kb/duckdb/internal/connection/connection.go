package connection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/duckdb/duckdb-go/v2"
)

const DefaultExtensionDir = "extensions"

func ResolveExtensionDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		candidate := filepath.Join(dir, DefaultExtensionDir)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

type Config struct {
	ExtensionDir string
	MemoryLimit  string
	TempDir      string
	OfflineExt   bool
	Threads      int
	// OnClose runs when sql.DB.Close closes the connector, which is how the
	// process budget learns a database is no longer holding its share.
	OnClose func()
}

func Open(ctx context.Context, dbPath string, cfg Config) (*sql.DB, error) {
	memLimit, err := NormalizeMemoryLimit(cfg.MemoryLimit)
	if err != nil {
		return nil, err
	}
	extensionDir, err := NormalizeExtensionDir(cfg.ExtensionDir, cfg.OfflineExt)
	if err != nil {
		return nil, err
	}
	tempDir, err := NormalizeTempDir(cfg.TempDir)
	if err != nil {
		return nil, err
	}
	connector, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(&closingConnector{Connector: connector, onClose: cfg.OnClose})
	// Callers serialize each shard DB through shardConn.mu. Keep database/sql
	// from opening additional native DuckDB connections behind that lock.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	cfg.ExtensionDir = extensionDir
	cfg.MemoryLimit = memLimit
	cfg.TempDir = tempDir
	if err := Configure(ctx, db, cfg); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return nil, fmt.Errorf("%w; close duckdb after configure failure: %v", err, closeErr)
		}
		return nil, err
	}
	return db, nil
}

// closingConnector reports a close to the budget. sql.DB closes the connector
// it was opened with, so this fires exactly once per database.
type closingConnector struct {
	*duckdb.Connector
	onClose func()
	once    sync.Once
}

func (c *closingConnector) Close() error {
	c.once.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
	})
	return c.Connector.Close()
}

func Configure(ctx context.Context, db *sql.DB, cfg Config) error {
	if cfg.ExtensionDir != "" {
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`SET extension_directory = '%s'`, QuoteSQLString(cfg.ExtensionDir))); err != nil {
			return fmt.Errorf("set extension_directory: %w", err)
		}
	}
	if err := DisableImplicitExtensionLoading(ctx, db); err != nil {
		return err
	}
	if err := LoadVSS(ctx, db, cfg.ExtensionDir, cfg.OfflineExt); err != nil {
		return err
	}
	if err := LoadFTS(ctx, db, cfg.ExtensionDir, cfg.OfflineExt); err != nil {
		return err
	}
	return ConfigureRuntime(ctx, db, cfg.MemoryLimit, cfg.TempDir, cfg.Threads)
}

func LoadFTS(ctx context.Context, db *sql.DB, extensionDir string, offlineExt bool) error {
	if _, err := db.ExecContext(ctx, `LOAD fts`); err == nil {
		return nil
	} else if offlineExt {
		return fmt.Errorf("failed to load fts extension in offline mode (check extension_directory %q): %w", extensionDir, err)
	}
	if _, installErr := db.ExecContext(ctx, `INSTALL fts`); installErr != nil {
		return fmt.Errorf("failed to install fts: %w", installErr)
	}
	if _, loadErr := db.ExecContext(ctx, `LOAD fts`); loadErr != nil {
		return fmt.Errorf("failed to load fts after install: %w", loadErr)
	}
	return nil
}

func DisableImplicitExtensionLoading(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `SET autoinstall_known_extensions = false`); err != nil {
		return fmt.Errorf("disable autoinstall: %w", err)
	}
	if _, err := db.ExecContext(ctx, `SET autoload_known_extensions = false`); err != nil {
		return fmt.Errorf("disable autoload: %w", err)
	}
	return nil
}

func LoadVSS(ctx context.Context, db *sql.DB, extensionDir string, offlineExt bool) error {
	if _, err := db.ExecContext(ctx, `LOAD vss`); err == nil {
		return nil
	} else if offlineExt {
		return fmt.Errorf("failed to load vss extension in offline mode (check extension_directory %q): %w", extensionDir, err)
	}
	if _, installErr := db.ExecContext(ctx, `INSTALL vss`); installErr != nil {
		return fmt.Errorf("failed to install vss: %w", installErr)
	}
	if _, loadErr := db.ExecContext(ctx, `LOAD vss`); loadErr != nil {
		return fmt.Errorf("failed to load vss after install: %w", loadErr)
	}
	return nil
}

func ConfigureRuntime(ctx context.Context, db *sql.DB, memLimit, tempDir string, threads int) error {
	if _, err := db.ExecContext(ctx, `SET hnsw_enable_experimental_persistence = true`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`SET memory_limit = '%s'`, QuoteSQLString(memLimit))); err != nil {
		return err
	}
	if tempDir != "" {
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`SET temp_directory = '%s'`, QuoteSQLString(tempDir))); err != nil {
			return fmt.Errorf("set temp_directory: %w", err)
		}
	}
	if threads <= 0 {
		threads = 1
	}
	_, err := db.ExecContext(ctx, fmt.Sprintf(`PRAGMA threads = %d`, threads))
	return err
}

func LoadVSSRawContext(ctx context.Context, db *sql.DB) error {
	extDir := ResolveExtensionDir()
	if extDir != "" {
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`SET extension_directory = '%s'`, extDir)); err != nil {
			return fmt.Errorf("set extension_directory: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, `SET autoinstall_known_extensions = false`); err != nil {
		return fmt.Errorf("disable autoinstall: %w", err)
	}
	if _, err := db.ExecContext(ctx, `SET autoload_known_extensions = false`); err != nil {
		return fmt.Errorf("disable autoload: %w", err)
	}
	if _, err := db.ExecContext(ctx, `LOAD vss`); err != nil {
		return fmt.Errorf("load vss: %w", err)
	}
	return nil
}

var memoryLimitPattern = regexp.MustCompile(`(?i)^\s*\d+\s*(b|kb|mb|gb|tb)\s*$`)

func NormalizeMemoryLimit(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		trimmed = "128MB"
	}
	if !memoryLimitPattern.MatchString(trimmed) {
		return "", fmt.Errorf("invalid memory limit %q", raw)
	}
	return trimmed, nil
}

func NormalizeExtensionDir(raw string, offline bool) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	if strings.ContainsRune(trimmed, '\x00') {
		return "", fmt.Errorf("invalid extension directory %q", raw)
	}
	cleaned := filepath.Clean(trimmed)
	info, err := os.Stat(cleaned)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if offline {
				return "", fmt.Errorf("extension directory %q does not exist in offline mode", cleaned)
			}
			return cleaned, nil
		}
		return "", fmt.Errorf("stat extension directory %q: %w", cleaned, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("extension directory %q is not a directory", cleaned)
	}
	return cleaned, nil
}

// NormalizeTempDir resolves the spill directory. Empty keeps DuckDB's default,
// which is a .tmp directory beside the database file.
func NormalizeTempDir(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	if strings.ContainsRune(trimmed, '\x00') {
		return "", fmt.Errorf("invalid temp directory %q", raw)
	}
	cleaned := filepath.Clean(trimmed)
	if err := os.MkdirAll(cleaned, 0o755); err != nil {
		return "", fmt.Errorf("temp directory %q: %w", cleaned, err)
	}
	return cleaned, nil
}

func QuoteSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
