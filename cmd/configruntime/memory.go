package configruntime

import (
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"strings"

	"github.com/mikills/minnow/internal/budget"
	"github.com/mikills/minnow/internal/memlimit"
)

// AutoMemoryLimit asks minnow to size itself from the machine or cgroup it runs
// in rather than from a number written into the config.
const AutoMemoryLimit = "auto"

// FallbackMemoryLimit is what an unset memory_limit becomes where the host's
// ceiling cannot be read. It is deliberately small: a machine we know nothing
// about is one to be careful on.
const FallbackMemoryLimit = "128MB"

// detect is a seam so tests can drive a ceiling this machine does not have.
var detect = memlimit.Detect

// resolveMemoryLimit turns "auto" into a concrete DuckDB memory_limit and caps
// the Go heap with what is left, so the budgets add up to a share of the
// ceiling instead of each assuming the whole machine.
func resolveMemoryLimit(raw string, logger *slog.Logger, dryRun bool) (string, error) {
	trimmed := strings.TrimSpace(raw)
	requested := strings.EqualFold(trimmed, AutoMemoryLimit)
	sizing := trimmed == "" || requested

	plan, err := detect().Divide(budget.PlannedDatabases, presetGoHeap())
	if err == nil && !dryRun {
		// Installed whatever set memory_limit. An operator who pinned a size
		// still wants back-pressure and admission control; they just do not
		// want their number overwritten. Gating the governor on who chose the
		// limit left every shipped config with none of it.
		budget.SetProcess(budget.New(plan, sizing))
		if sizing && !plan.GoHeapPreset {
			setGoMemLimit(plan.GoHeap)
		}
	}
	if !sizing {
		return raw, nil
	}
	if err != nil {
		// Asking for auto and not getting it is an error. Having it chosen for
		// you and not getting it is a fallback, or an unset limit would stop
		// every deployment that cannot read a ceiling.
		if requested {
			return "", fmt.Errorf("format.duckdb.memory_limit %q: %w; set an explicit size instead", AutoMemoryLimit, err)
		}
		logger.Info("sizing memory from the host is unavailable, using the fixed default",
			"reason", err, "memory_limit", FallbackMemoryLimit)
		return FallbackMemoryLimit, nil
	}
	logger.Info("sized memory from the host",
		"source", plan.Source,
		"ceiling_mb", plan.Ceiling>>20,
		"budget_mb", plan.Budget>>20,
		"go_heap_mb", plan.GoHeap>>20,
		"go_heap_preset", plan.GoHeapPreset,
		"duckdb_total_mb", plan.DuckDBTotal>>20,
		"duckdb_per_db", plan.MemoryLimit(),
		"planned_databases", budget.PlannedDatabases,
	)
	return plan.MemoryLimit(), nil
}

// presetGoHeap is the effective GOMEMLIMIT, or 0 when nothing has set one. The
// runtime folds the environment variable into this value. A limit minnow set on
// an earlier Build needs no special case: feeding it back as the preset yields
// the same split it came from, so a second Build is stable.
func presetGoHeap() int64 {
	if current := debug.SetMemoryLimit(-1); current != math.MaxInt64 {
		return current
	}
	return 0
}

func setGoMemLimit(bytes int64) {
	if bytes > 0 {
		debug.SetMemoryLimit(bytes)
	}
}
