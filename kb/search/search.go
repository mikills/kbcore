package search

import "github.com/mikills/minnow/kb/media"

type ExpansionOptions struct {
	SeedK               int
	Hops                int
	MaxNeighborsPerNode int
	Alpha               float64
	Decay               float64
	EdgeTypes           []string
	UseDuckPGQ          bool
	MaxEntityResults    int
	OfflineExt          bool
}

type ExpandedResult struct {
	ID         string
	Content    string
	Distance   float64
	GraphScore float64
	Score      float64
	MediaRefs  []media.ChunkMediaRef
	Metadata   map[string]any
}

type Mode int

const (
	ModeVector Mode = iota
	ModeGraph
	ModeAdaptive
)

type Options struct {
	Mode           Mode
	TopK           int
	MaxDistance    *float64
	Expansion      *ExpansionOptions
	AdaptiveMinSim float64
}

type EdgeRow struct {
	Src    string
	Dst    string
	Weight float64
}

func NormalizeExpansionOptions(topK int, opts *ExpansionOptions) ExpansionOptions {
	defaults := ExpansionOptions{
		SeedK:               max(topK, 10),
		Hops:                2,
		MaxNeighborsPerNode: 25,
		Alpha:               0.7,
		Decay:               0.7,
		MaxEntityResults:    1000,
	}
	if opts == nil {
		return defaults
	}
	normalized := *opts
	if normalized.SeedK <= 0 {
		normalized.SeedK = defaults.SeedK
	}
	if normalized.Hops < 0 {
		normalized.Hops = defaults.Hops
	}
	if normalized.MaxNeighborsPerNode <= 0 {
		normalized.MaxNeighborsPerNode = defaults.MaxNeighborsPerNode
	}
	if normalized.Alpha < 0 || normalized.Alpha > 1 {
		normalized.Alpha = defaults.Alpha
	}
	if normalized.Decay <= 0 || normalized.Decay > 1 {
		normalized.Decay = defaults.Decay
	}
	if normalized.MaxEntityResults <= 0 {
		normalized.MaxEntityResults = defaults.MaxEntityResults
	}
	return normalized
}

func NormalizeOptions(opts *Options) Options {
	defaults := Options{Mode: ModeVector, AdaptiveMinSim: 0.35}
	if opts == nil {
		return defaults
	}
	normalized := *opts
	if normalized.Mode != ModeGraph && normalized.Mode != ModeAdaptive {
		normalized.Mode = defaults.Mode
	}
	if normalized.AdaptiveMinSim <= 0 || normalized.AdaptiveMinSim > 1 {
		normalized.AdaptiveMinSim = defaults.AdaptiveMinSim
	}
	return normalized
}
