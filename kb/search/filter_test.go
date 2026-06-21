package search

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterExprValidation(t *testing.T) {
	t.Run("nil is valid", func(t *testing.T) {
		var f *FilterExpr
		require.NoError(t, f.Validate())
	})
	t.Run("leaf eq string", func(t *testing.T) {
		require.NoError(t, (&FilterExpr{Field: "tenant", Op: FilterOpEq, Value: "acme"}).Validate())
	})
	t.Run("leaf gt number", func(t *testing.T) {
		require.NoError(t, (&FilterExpr{Field: "rank", Op: FilterOpGt, Value: 1.0}).Validate())
	})
	t.Run("leaf in array", func(t *testing.T) {
		require.NoError(t, (&FilterExpr{Field: "status", Op: FilterOpIn, Value: []any{"a", "b"}}).Validate())
	})
	t.Run("compound and", func(t *testing.T) {
		require.NoError(t, (&FilterExpr{And: []FilterExpr{
			{Field: "tenant", Op: FilterOpEq, Value: "x"},
			{Field: "rank", Op: FilterOpGt, Value: 0.0},
		}}).Validate())
	})
	t.Run("invalid field chars", func(t *testing.T) {
		err := (&FilterExpr{Field: "bad-field", Op: FilterOpEq, Value: "x"}).Validate()
		require.ErrorContains(t, err, "invalid character")
	})
	t.Run("unsupported op", func(t *testing.T) {
		err := (&FilterExpr{Field: "f", Op: "LIKE", Value: "x"}).Validate()
		require.ErrorContains(t, err, "unsupported filter op")
	})
	t.Run("nil value", func(t *testing.T) {
		err := (&FilterExpr{Field: "f", Op: FilterOpEq, Value: nil}).Validate()
		require.ErrorContains(t, err, "must not be nil")
	})
	t.Run("mixed leaf and compound", func(t *testing.T) {
		err := (&FilterExpr{Field: "f", Op: FilterOpEq, Value: "x", And: []FilterExpr{{}}}).Validate()
		require.ErrorContains(t, err, "cannot mix")
	})
	t.Run("in op with non-array", func(t *testing.T) {
		err := (&FilterExpr{Field: "f", Op: FilterOpIn, Value: "not-array"}).Validate()
		require.ErrorContains(t, err, "array value")
	})
	t.Run("empty struct is valid no-op", func(t *testing.T) {
		require.NoError(t, (&FilterExpr{}).Validate())
	})
}

func TestFilterExprMatch(t *testing.T) {
	meta := map[string]any{
		"tenant": "acme",
		"rank":   float64(5),
		"active": true,
	}

	t.Run("nil matches all", func(t *testing.T) {
		var f *FilterExpr
		require.True(t, f.Match(meta))
	})
	t.Run("eq string match", func(t *testing.T) {
		require.True(t, (&FilterExpr{Field: "tenant", Op: FilterOpEq, Value: "acme"}).Match(meta))
	})
	t.Run("eq string no match", func(t *testing.T) {
		require.False(t, (&FilterExpr{Field: "tenant", Op: FilterOpEq, Value: "globex"}).Match(meta))
	})
	t.Run("gt number match", func(t *testing.T) {
		require.True(t, (&FilterExpr{Field: "rank", Op: FilterOpGt, Value: float64(3)}).Match(meta))
	})
	t.Run("gt number no match", func(t *testing.T) {
		require.False(t, (&FilterExpr{Field: "rank", Op: FilterOpGt, Value: float64(10)}).Match(meta))
	})
	t.Run("boolean match", func(t *testing.T) {
		require.True(t, (&FilterExpr{Field: "active", Op: FilterOpEq, Value: true}).Match(meta))
		require.False(t, (&FilterExpr{Field: "active", Op: FilterOpEq, Value: false}).Match(meta))
	})
	t.Run("in match", func(t *testing.T) {
		require.True(t, (&FilterExpr{Field: "tenant", Op: FilterOpIn, Value: []any{"acme", "globex"}}).Match(meta))
		require.False(t, (&FilterExpr{Field: "tenant", Op: FilterOpIn, Value: []any{"globex"}}).Match(meta))
	})
	t.Run("and compound", func(t *testing.T) {
		f := &FilterExpr{And: []FilterExpr{
			{Field: "tenant", Op: FilterOpEq, Value: "acme"},
			{Field: "rank", Op: FilterOpGt, Value: float64(3)},
		}}
		require.True(t, f.Match(meta))
		f.And[1].Value = float64(10)
		require.False(t, f.Match(meta))
	})
	t.Run("missing field is false", func(t *testing.T) {
		require.False(t, (&FilterExpr{Field: "nonexistent", Op: FilterOpEq, Value: "x"}).Match(meta))
	})
	t.Run("empty expr is true", func(t *testing.T) {
		require.True(t, (&FilterExpr{}).Match(meta))
	})
}

func TestBuildFilterSQL(t *testing.T) {
	cases := []struct {
		name   string
		filter *FilterExpr
		want   string
	}{
		{
			name:   "nil returns empty",
			filter: nil,
			want:   "",
		},
		{
			name:   "eq string",
			filter: &FilterExpr{Field: "tenant", Op: FilterOpEq, Value: "acme"},
			want:   "json_extract_string(metadata, '$.tenant') = 'acme'",
		},
		{
			name:   "ne string",
			filter: &FilterExpr{Field: "tenant", Op: FilterOpNe, Value: "acme"},
			want:   "json_extract_string(metadata, '$.tenant') != 'acme'",
		},
		{
			name:   "gt number",
			filter: &FilterExpr{Field: "rank", Op: FilterOpGt, Value: 1.5},
			want:   "CAST(json_extract(metadata, '$.rank') AS DOUBLE) > 1.5",
		},
		{
			name:   "gte number",
			filter: &FilterExpr{Field: "rank", Op: FilterOpGte, Value: float64(0)},
			want:   "CAST(json_extract(metadata, '$.rank') AS DOUBLE) >= 0",
		},
		{
			name:   "lt number",
			filter: &FilterExpr{Field: "score", Op: FilterOpLt, Value: 10.0},
			want:   "CAST(json_extract(metadata, '$.score') AS DOUBLE) < 10",
		},
		{
			name:   "lte number",
			filter: &FilterExpr{Field: "score", Op: FilterOpLte, Value: 5.5},
			want:   "CAST(json_extract(metadata, '$.score') AS DOUBLE) <= 5.5",
		},
		{
			name:   "in strings",
			filter: &FilterExpr{Field: "status", Op: FilterOpIn, Value: []any{"active", "pending"}},
			want:   "json_extract_string(metadata, '$.status') IN ('active', 'pending')",
		},
		{
			name: "and compound",
			filter: &FilterExpr{And: []FilterExpr{
				{Field: "tenant", Op: FilterOpEq, Value: "acme"},
				{Field: "rank", Op: FilterOpGt, Value: 0.0},
			}},
			want: "(json_extract_string(metadata, '$.tenant') = 'acme') AND (CAST(json_extract(metadata, '$.rank') AS DOUBLE) > 0)",
		},
		{
			name: "or compound",
			filter: &FilterExpr{Or: []FilterExpr{
				{Field: "type", Op: FilterOpEq, Value: "a"},
				{Field: "type", Op: FilterOpEq, Value: "b"},
			}},
			want: "(json_extract_string(metadata, '$.type') = 'a') OR (json_extract_string(metadata, '$.type') = 'b')",
		},
		{
			name:   "string with single quote escaped",
			filter: &FilterExpr{Field: "name", Op: FilterOpEq, Value: "it's"},
			want:   "json_extract_string(metadata, '$.name') = 'it''s'",
		},
		{
			name:   "empty in returns FALSE",
			filter: &FilterExpr{Field: "x", Op: FilterOpIn, Value: []any{}},
			want:   "FALSE",
		},
		{
			name:   "numeric in via []any uses DOUBLE extract",
			filter: &FilterExpr{Field: "price", Op: FilterOpIn, Value: []any{float64(10), float64(20)}},
			want:   "CAST(json_extract(metadata, '$.price') AS DOUBLE) IN (10, 20)",
		},
		{
			name:   "empty expr is no-op",
			filter: &FilterExpr{},
			want:   "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BuildFilterSQL(tc.filter)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
