package duckdb

import "github.com/mikills/minnow/internal/budget"

// budget returns the limits this format runs under. Every process-wide cap
// lives on one manager, so a caller cannot raise one and miss another.
func (f *DuckDBArtifactFormat) budget() *budget.Manager {
	if f.deps.Budget != nil {
		return f.deps.Budget
	}
	return budget.Process()
}

func (f *DuckDBArtifactFormat) buildThreads() int {
	return f.budget().BuildThreads(f.deps.BuildThreads)
}

func (f *DuckDBArtifactFormat) embedParallelism() int {
	return f.budget().EmbedParallelism(f.deps.EmbedParallelism)
}
