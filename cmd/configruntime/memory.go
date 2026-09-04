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

// AutoMemoryLimit sizes from the host rather than from the config.
const AutoMemoryLimit = "auto"

// FallbackMemoryLimit applies where no ceiling is readable. Deliberately small:
// nothing caps how many databases take an unsized limit.
const FallbackMemoryLimit = "128MB"

// detect is a seam so tests can drive a ceiling this machine does not have.
var detect = memlimit.Detect

// resolveMemoryLimit turns "auto" into a memory_limit and caps the Go heap with
// what is left, so the two add up to a share of the ceiling.
func resolveMemoryLimit(raw string, maxShardBytes int64, logger *slog.Logger, dryRun bool) (string, error) {
	trimmed := strings.TrimSpace(raw)
	requested := strings.EqualFold(trimmed, AutoMemoryLimit)
	sizing := trimmed == "" || requested

	limit := detect()
	plan, err := limit.Divide(maxShardBytes, budget.CachedReaders, presetGoHeap())
	sized := err == nil
	if !sized && limit.Usable() == nil {
		// Too small to divide, but a box that tight needs the governor most.
		plan = memlimit.Plan{Ceiling: limit.Ceiling, Dir: limit.Dir(), Source: limit.Source}
	}
	if plan.Ceiling > 0 && !dryRun {
		// Installed whatever set memory_limit: a pinned size still wants
		// back-pressure, it just does not want the number overwritten.
		budget.SetProcess(budget.New(plan, sizing && sized))
		if sizing && sized && !plan.GoHeapPreset {
			setGoMemLimit(plan.GoHeap)
		}
	}
	if !sizing {
		return raw, nil
	}
	if err != nil {
		// Asking for auto and missing is an error. Defaulting to it is not.
		if requested {
			return "", fmt.Errorf("format.duckdb.memory_limit %q: %w; set an explicit size instead", AutoMemoryLimit, err)
		}
		// Below what most shapes need to index, hence a warning.
		logger.Warn("sizing memory from the host is unavailable, using the fixed default",
			"reason", err, "memory_limit", FallbackMemoryLimit,
			"index_build_needs", memlimit.FormatMB(memlimit.MinDatabaseBytes(maxShardBytes)))
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
		"databases", plan.Databases,
		"min_per_db_mb", plan.MinPerDB>>20,
		"max_shard_mb", maxShardBytes>>20,
	)
	return plan.MemoryLimit(), nil
}

// presetGoHeap is the effective GOMEMLIMIT, or 0. Feeding back a limit minnow
// set earlier yields the same split, so a second Build is stable.
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
