package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"time"

	duckdbdriver "github.com/duckdb/duckdb-go/v2"
)

const (
	duckDBOutOfMemoryMarker = "Out of Memory Error"
	memoryProbeTimeout      = 5 * time.Second
)

// DuckDB reports the failed allocation but not what already holds the budget.
// The reading is taken after the failed statement unwinds, so it shows residual
// buffer state rather than the peak.
func logDuckDBMemoryOnError(ctx context.Context, db *sql.DB, operation string, err error) error {
	if err == nil || db == nil || !isDuckDBOutOfMemory(err) {
		return err
	}
	attrs := []any{"operation", operation, logKeyError, err}

	// The failing context may already be cancelled or past its deadline.
	probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), memoryProbeTimeout)
	defer cancel()

	rows, queryErr := db.QueryContext(
		probeCtx,
		`SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()`,
	)
	if queryErr != nil {
		attrs = append(attrs, "probe_error", queryErr)
		slog.Default().ErrorContext(ctx, "duckdb out of memory", attrs...)
		return err
	}
	defer rows.Close()

	var total int64
	for rows.Next() {
		var tag string
		var used, temporary int64
		if scanErr := rows.Scan(&tag, &used, &temporary); scanErr != nil {
			attrs = append(attrs, "probe_error", scanErr)
			break
		}
		total += used
		if used > 0 {
			attrs = append(attrs, "mem_"+strings.ToLower(tag)+"_bytes", used)
		}
		if temporary > 0 {
			attrs = append(attrs, "tmp_"+strings.ToLower(tag)+"_bytes", temporary)
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		attrs = append(attrs, "probe_error", rowsErr)
	}
	attrs = append(attrs, "mem_total_bytes", total)
	slog.Default().ErrorContext(ctx, "duckdb out of memory", attrs...)
	return err
}

// The driver exposes a typed error; the string check only covers errors that
// reached us as plain text.
func isDuckDBOutOfMemory(err error) bool {
	var duckErr *duckdbdriver.Error
	if errors.As(err, &duckErr) {
		return duckErr.Type == duckdbdriver.ErrorTypeOutOfMemory
	}
	return strings.Contains(err.Error(), duckDBOutOfMemoryMarker)
}
