package search

import (
	"fmt"
	"strings"

	"github.com/mikills/minnow/kb/media"
)

// FilterOp is a comparison operator for metadata predicate filters.
type FilterOp string

const (
	FilterOpEq  FilterOp = "="
	FilterOpNe  FilterOp = "!="
	FilterOpGt  FilterOp = ">"
	FilterOpGte FilterOp = ">="
	FilterOpLt  FilterOp = "<"
	FilterOpLte FilterOp = "<="
	FilterOpIn  FilterOp = "in"
)

// FilterExpr is a predicate on document metadata.
// Either (Field+Op+Value) for a leaf, or (And/Or) for a compound.
type FilterExpr struct {
	Field string   `json:"field,omitempty"`
	Op    FilterOp `json:"op,omitempty"`
	Value any      `json:"value,omitempty"`

	And []FilterExpr `json:"and,omitempty"`
	Or  []FilterExpr `json:"or,omitempty"`
}

// Validate returns an error if the expression is malformed.
func (f *FilterExpr) Validate() error {
	if f == nil {
		return nil
	}
	return validateFilterExpr(*f, 0)
}

const maxFilterDepth = 8

func validateFilterExpr(f FilterExpr, depth int) error {
	if depth > maxFilterDepth {
		return fmt.Errorf("filter expression exceeds maximum nesting depth of %d", maxFilterDepth)
	}
	isLeaf := f.Field != ""
	isAnd := len(f.And) > 0
	isOr := len(f.Or) > 0
	if isLeaf && (isAnd || isOr) {
		return fmt.Errorf("filter node cannot mix field predicate with and/or")
	}
	if isAnd && isOr {
		return fmt.Errorf("filter node cannot have both and and or")
	}
	if isLeaf {
		return validateLeafFilter(f)
	}
	children := f.And
	if isOr {
		children = f.Or
	}
	for _, child := range children {
		if err := validateFilterExpr(child, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func validateLeafFilter(f FilterExpr) error {
	if err := validateFilterField(f.Field); err != nil {
		return err
	}
	switch f.Op {
	case FilterOpEq, FilterOpNe, FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte, FilterOpIn:
	default:
		return fmt.Errorf("unsupported filter op %q", f.Op)
	}
	if f.Op == FilterOpIn {
		switch f.Value.(type) {
		case []any, []string, []float64:
		default:
			return fmt.Errorf("filter op \"in\" requires an array value")
		}
	}
	if f.Value == nil {
		return fmt.Errorf("filter value must not be nil")
	}
	return nil
}

func validateFilterField(field string) error {
	if field == "" {
		return fmt.Errorf("filter field must not be empty")
	}
	for _, r := range field {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return fmt.Errorf("filter field %q contains invalid character %q (only alphanumeric and _ allowed)", field, r)
		}
	}
	return nil
}

// Match evaluates the filter against a metadata map in Go (used for
// post-retrieval filtering of graph/adaptive search results).
// Returns true when f is nil or when the metadata satisfies the predicate.
func (f *FilterExpr) Match(metadata map[string]any) bool {
	if f == nil {
		return true
	}
	return matchExpr(*f, metadata)
}

func matchExpr(f FilterExpr, meta map[string]any) bool {
	if f.Field != "" {
		return matchLeaf(f, meta)
	}
	if len(f.And) > 0 {
		for _, child := range f.And {
			if !matchExpr(child, meta) {
				return false
			}
		}
		return true
	}
	if len(f.Or) > 0 {
		for _, child := range f.Or {
			if matchExpr(child, meta) {
				return true
			}
		}
		return false
	}
	return true // empty expr is a no-op
}

func matchLeaf(f FilterExpr, meta map[string]any) bool {
	val, ok := meta[f.Field]
	if !ok {
		return false
	}
	if f.Op == FilterOpIn {
		return matchIn(val, f.Value)
	}
	return matchCmp(val, f.Op, f.Value)
}

func matchCmp(got any, op FilterOp, want any) bool {
	switch op {
	case FilterOpEq:
		return matchEq(got, want)
	case FilterOpNe:
		return !matchEq(got, want)
	case FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte:
		gotN, gotOk := toFloat64(got)
		wantN, wantOk := toFloat64(want)
		if !gotOk || !wantOk {
			return false
		}
		switch op {
		case FilterOpGt:
			return gotN > wantN
		case FilterOpGte:
			return gotN >= wantN
		case FilterOpLt:
			return gotN < wantN
		case FilterOpLte:
			return gotN <= wantN
		}
	}
	return false
}

func matchEq(got, want any) bool {
	switch g := got.(type) {
	case string:
		w, ok := want.(string)
		return ok && g == w
	case float64:
		w, ok := toFloat64(want)
		return ok && g == w
	case bool:
		w, ok := want.(bool)
		return ok && g == w
	}
	return fmt.Sprintf("%v", got) == fmt.Sprintf("%v", want)
}

func matchIn(got any, want any) bool {
	switch v := want.(type) {
	case []any:
		for _, item := range v {
			if matchEq(got, item) {
				return true
			}
		}
	case []string:
		for _, item := range v {
			if matchEq(got, item) {
				return true
			}
		}
	case []float64:
		for _, item := range v {
			if matchEq(got, item) {
				return true
			}
		}
	}
	return false
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// BuildFilterSQL returns a safe SQL WHERE clause fragment for use in a DuckDB
// query. The metadata column must be named "metadata" and contain JSON.
// Returns ("", nil) when f is nil.
func BuildFilterSQL(f *FilterExpr) (string, error) {
	if f == nil {
		return "", nil
	}
	if err := f.Validate(); err != nil {
		return "", err
	}
	return buildFilterExprSQL(*f)
}

func buildFilterExprSQL(f FilterExpr) (string, error) {
	if f.Field != "" {
		return buildLeafSQL(f)
	}
	if len(f.And) > 0 {
		return buildCompoundSQL("AND", f.And)
	}
	if len(f.Or) > 0 {
		return buildCompoundSQL("OR", f.Or)
	}
	return "", nil // empty expr → no-op
}

func buildCompoundSQL(op string, exprs []FilterExpr) (string, error) {
	parts := make([]string, 0, len(exprs))
	for _, e := range exprs {
		sql, err := buildFilterExprSQL(e)
		if err != nil {
			return "", err
		}
		parts = append(parts, "("+sql+")")
	}
	return strings.Join(parts, " "+op+" "), nil
}

func buildLeafSQL(f FilterExpr) (string, error) {
	if f.Op == FilterOpIn {
		return buildInSQL(f.Field, f.Value)
	}
	valSQL, err := filterValueSQL(f.Value)
	if err != nil {
		return "", err
	}
	extract := jsonExtractSQL(f.Field, f.Value)
	switch f.Op {
	case FilterOpEq:
		return extract + " = " + valSQL, nil
	case FilterOpNe:
		return extract + " != " + valSQL, nil
	case FilterOpGt:
		return extract + " > " + valSQL, nil
	case FilterOpGte:
		return extract + " >= " + valSQL, nil
	case FilterOpLt:
		return extract + " < " + valSQL, nil
	case FilterOpLte:
		return extract + " <= " + valSQL, nil
	}
	return "", fmt.Errorf("unsupported op %q", f.Op)
}

// jsonExtractSQL returns the DuckDB JSON extraction expression for a field,
// cast to DOUBLE for numeric values and using json_extract_string for strings.
// For IN lists, pass one element (not the slice) to determine the type.
func jsonExtractSQL(field string, sampleValue any) string {
	switch sampleValue.(type) {
	case float64, int, int64:
		return fmt.Sprintf("CAST(json_extract(metadata, '$.%s') AS DOUBLE)", field)
	default:
		return fmt.Sprintf("json_extract_string(metadata, '$.%s')", field)
	}
}

func filterValueSQL(v any) (string, error) {
	switch val := v.(type) {
	case float64:
		return fmt.Sprintf("%g", val), nil
	case int:
		return fmt.Sprintf("%d", val), nil
	case int64:
		return fmt.Sprintf("%d", val), nil
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'", nil
	case bool:
		if val {
			return "'true'", nil
		}
		return "'false'", nil
	default:
		return "", fmt.Errorf("unsupported filter value type %T", v)
	}
}

func buildInSQL(field string, value any) (string, error) {
	var items []string
	var sample any
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			s, err := filterValueSQL(item)
			if err != nil {
				return "", err
			}
			items = append(items, s)
		}
		if len(v) > 0 {
			sample = v[0]
		}
	case []string:
		for _, item := range v {
			s, err := filterValueSQL(item)
			if err != nil {
				return "", err
			}
			items = append(items, s)
		}
		if len(v) > 0 {
			sample = v[0]
		}
	case []float64:
		for _, item := range v {
			s, err := filterValueSQL(item)
			if err != nil {
				return "", err
			}
			items = append(items, s)
		}
		if len(v) > 0 {
			sample = v[0]
		}
	default:
		return "", fmt.Errorf("filter op \"in\" requires an array value")
	}
	if len(items) == 0 {
		return "FALSE", nil
	}
	extract := jsonExtractSQL(field, sample)
	return extract + " IN (" + strings.Join(items, ", ") + ")", nil
}

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
	Filter         *FilterExpr
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
