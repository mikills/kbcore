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
	return "", fmt.Errorf("empty filter expression")
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
	extract := jsonExtractSQL(f.Field, f.Value)
	switch f.Op {
	case FilterOpEq:
		return extract + " = " + filterValueSQL(f.Value), nil
	case FilterOpNe:
		return extract + " != " + filterValueSQL(f.Value), nil
	case FilterOpGt:
		return extract + " > " + filterValueSQL(f.Value), nil
	case FilterOpGte:
		return extract + " >= " + filterValueSQL(f.Value), nil
	case FilterOpLt:
		return extract + " < " + filterValueSQL(f.Value), nil
	case FilterOpLte:
		return extract + " <= " + filterValueSQL(f.Value), nil
	case FilterOpIn:
		return buildInSQL(f.Field, f.Value)
	}
	return "", fmt.Errorf("unsupported op %q", f.Op)
}

// jsonExtractSQL returns the appropriate DuckDB JSON extraction expression,
// cast to the right type based on the value's Go type.
func jsonExtractSQL(field string, value any) string {
	switch value.(type) {
	case float64, int, int64, []float64:
		return fmt.Sprintf("CAST(json_extract(metadata, '$.%s') AS DOUBLE)", field)
	default:
		return fmt.Sprintf("json_extract_string(metadata, '$.%s')", field)
	}
}

func filterValueSQL(v any) string {
	switch val := v.(type) {
	case float64:
		return fmt.Sprintf("%g", val)
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case bool:
		if val {
			return "'true'"
		}
		return "'false'"
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

func buildInSQL(field string, value any) (string, error) {
	var items []string
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			items = append(items, filterValueSQL(item))
		}
	case []string:
		for _, item := range v {
			items = append(items, filterValueSQL(item))
		}
	case []float64:
		for _, item := range v {
			items = append(items, filterValueSQL(item))
		}
	default:
		return "", fmt.Errorf("filter op \"in\" requires an array value")
	}
	if len(items) == 0 {
		return "FALSE", nil
	}
	extract := jsonExtractSQL(field, value)
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
