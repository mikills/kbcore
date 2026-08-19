package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

type captureHandler struct {
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	h.records = append(h.records, record)
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

func captureLogs(t *testing.T) *captureHandler {
	t.Helper()
	handler := &captureHandler{}
	previous := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return handler
}

func (h *captureHandler) attrNames() []string {
	var names []string
	for _, record := range h.records {
		record.Attrs(func(a slog.Attr) bool {
			names = append(names, a.Key)
			return true
		})
	}
	return names
}

func (h *captureHandler) attr(name string) (slog.Value, bool) {
	for _, record := range h.records {
		var found slog.Value
		ok := false
		record.Attrs(func(a slog.Attr) bool {
			if a.Key == name {
				found, ok = a.Value, true
				return false
			}
			return true
		})
		if ok {
			return found, true
		}
	}
	return slog.Value{}, false
}

func TestLogDuckDBMemoryOnError(t *testing.T) {
	t.Run("passes through a nil error without logging", func(t *testing.T) {
		logs := captureLogs(t)
		if err := logDuckDBMemoryOnError(context.Background(), nil, "op", nil); err != nil {
			t.Fatalf("got %v, want nil", err)
		}
		if len(logs.records) != 0 {
			t.Fatalf("logged for a nil error: %v", logs.records)
		}
	})

	t.Run("ignores errors that are not out of memory", func(t *testing.T) {
		logs := captureLogs(t)
		want := errors.New("syntax error")
		if err := logDuckDBMemoryOnError(context.Background(), nil, "op", want); !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
		if len(logs.records) != 0 {
			t.Fatalf("logged for an unrelated error: %v", logs.records)
		}
	})

	t.Run("reports a probe error when the connection is already closed", func(t *testing.T) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Skipf("duckdb unavailable: %v", err)
		}
		if err := db.PingContext(context.Background()); err != nil {
			t.Skipf("duckdb unavailable: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		logs := captureLogs(t)
		want := errors.New("checkpoint db: Out of Memory Error: failed to allocate")
		if got := logDuckDBMemoryOnError(context.Background(), db, "checkpoint", want); !errors.Is(got, want) {
			t.Fatalf("probe changed the error: %v", got)
		}
		if _, ok := logs.attr("probe_error"); !ok {
			t.Fatalf("no probe_error attribute; got %v", logs.attrNames())
		}
	})

	t.Run("logs the memory breakdown on an out of memory error", func(t *testing.T) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Skipf("duckdb unavailable: %v", err)
		}
		defer db.Close()
		if err := db.PingContext(context.Background()); err != nil {
			t.Skipf("duckdb unavailable: %v", err)
		}
		// An empty database reports zero for every tag, which would let the
		// per-tag loop be deleted without failing this test.
		ctx := context.Background()
		if _, err := db.ExecContext(ctx, `CREATE TABLE t AS SELECT i, repeat('x', 500) AS pad FROM range(200000) s(i)`); err != nil {
			t.Skipf("cannot populate duckdb: %v", err)
		}
		var ignored int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&ignored); err != nil {
			t.Skipf("cannot read duckdb: %v", err)
		}

		logs := captureLogs(t)
		want := errors.New("Out of Memory Error: failed to allocate data of size 72.0 MiB")
		if got := logDuckDBMemoryOnError(ctx, db, "prepared_upsert", want); !errors.Is(got, want) {
			t.Fatalf("probe changed the error: %v", got)
		}
		if len(logs.records) != 1 {
			t.Fatalf("got %d records, want 1", len(logs.records))
		}
		total, ok := logs.attr("mem_total_bytes")
		if !ok {
			t.Fatal("no mem_total_bytes attribute; the breakdown was not captured")
		}
		if total.Int64() <= 0 {
			t.Fatalf("mem_total_bytes = %d, want a populated database to report memory", total.Int64())
		}
		perTag := 0
		for _, name := range logs.attrNames() {
			if strings.HasPrefix(name, "mem_") && name != "mem_total_bytes" {
				perTag++
			}
		}
		if perTag == 0 {
			t.Fatalf("no per-tag mem_* attributes; got %v", logs.attrNames())
		}
		operation, ok := logs.attr("operation")
		if !ok || operation.String() != "prepared_upsert" {
			t.Fatalf("operation attribute = %v, ok=%v", operation, ok)
		}
		if logs.records[0].Level != slog.LevelError {
			t.Fatalf("logged at %v, want Error", logs.records[0].Level)
		}
		if !strings.Contains(logs.records[0].Message, "out of memory") {
			t.Fatalf("message = %q", logs.records[0].Message)
		}
	})
}
