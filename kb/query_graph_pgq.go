package kb

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
)

func expandEntityScoresDuckPGQ(ctx context.Context, db *sql.DB, scores map[string]float64, options ExpansionOptions) (map[string]float64, error) {
	if err := ensureEntitiesTable(ctx, db); err != nil {
		return nil, err
	}
	if err := ensureDuckPGQLoaded(ctx, db); err != nil {
		return nil, err
	}

	seedIDs := mapKeys(scores)
	graphName, cleanup, err := ensurePropertyGraph(ctx, db, options.EdgeTypes)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	expansions, err := queryGraphScoresDuckPGQ(ctx, db, graphName, seedIDs, options.Hops, options.Decay, options.MaxNeighborsPerNode)
	if err != nil {
		return nil, err
	}
	for id, score := range expansions {
		scores[id] += score
	}
	return scores, nil
}

func ensureDuckPGQLoaded(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `LOAD duckpgq`); err == nil {
		return nil
	}
	if _, err := db.ExecContext(ctx, `INSTALL duckpgq FROM community`); err != nil {
		return fmt.Errorf("failed to install duckpgq: %w", err)
	}
	if _, err := db.ExecContext(ctx, `LOAD duckpgq`); err != nil {
		return fmt.Errorf("failed to load duckpgq: %w", err)
	}
	return nil
}

func ensurePropertyGraph(ctx context.Context, db *sql.DB, edgeTypes []string) (string, func(), error) {
	const baseGraphName = "kb_graph"
	const baseEdgeTable = "edges"
	const edgeLabel = "edge"

	if len(edgeTypes) == 0 {
		if err := createPropertyGraph(ctx, db, baseGraphName, baseEdgeTable, edgeLabel); err != nil {
			return "", nil, err
		}
		return baseGraphName, func() {}, nil
	}

	filteredTable := uniqueTempName("edges_filtered")
	graphName := uniqueTempName("kb_graph")

	if err := createFilteredEdgesTable(ctx, db, filteredTable, edgeTypes); err != nil {
		return "", nil, err
	}
	if err := createPropertyGraph(ctx, db, graphName, filteredTable, edgeLabel); err != nil {
		_ = dropTable(ctx, db, filteredTable)
		return "", nil, err
	}

	cleanup := func() {
		_ = dropPropertyGraph(ctx, db, graphName)
		_ = dropTable(ctx, db, filteredTable)
	}

	return graphName, cleanup, nil
}

func createFilteredEdgesTable(ctx context.Context, db *sql.DB, tableName string, edgeTypes []string) error {
	if len(edgeTypes) == 0 {
		return nil
	}
	placeholders := buildInClausePlaceholders(len(edgeTypes))
	query := fmt.Sprintf(`CREATE TEMP TABLE %s AS SELECT * FROM edges WHERE rel_type IN (%s)`, tableName, placeholders)
	args := make([]any, 0, len(edgeTypes))
	for _, edgeType := range edgeTypes {
		args = append(args, edgeType)
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to create filtered edges table: %w", err)
	}
	return nil
}

func createPropertyGraph(ctx context.Context, db *sql.DB, graphName, edgeTable, edgeLabel string) error {
	query := fmt.Sprintf(`
		CREATE PROPERTY GRAPH %s
		VERTEX TABLES (
			entities
		)
		EDGE TABLES (
			%s
				SOURCE KEY (src) REFERENCES entities (id)
				DESTINATION KEY (dst) REFERENCES entities (id)
				LABEL %s
		)
	`, graphName, edgeTable, edgeLabel)

	if _, err := db.ExecContext(ctx, query); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("failed to create property graph: %w", err)
	}
	return nil
}

func dropPropertyGraph(ctx context.Context, db *sql.DB, graphName string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(`DROP PROPERTY GRAPH %s`, graphName))
	if err != nil && !strings.Contains(err.Error(), "does not exist") {
		return err
	}
	return nil
}

func queryGraphScoresDuckPGQ(ctx context.Context, db *sql.DB, graphName string, seeds []string, hops int, decay float64, maxNeighborsPerNode int) (map[string]float64, error) {
	if len(seeds) == 0 || hops <= 0 {
		return map[string]float64{}, nil
	}

	placeholders := buildInClausePlaceholders(len(seeds))
	query := fmt.Sprintf(`
		SELECT src_id, dst_id, hop
		FROM (
			SELECT src_id, dst_id, hop,
				ROW_NUMBER() OVER (PARTITION BY src_id ORDER BY hop, dst_id) AS rn
			FROM GRAPH_TABLE (%s
				MATCH p = ANY SHORTEST (a:entities WHERE a.id IN (%s))-[e:edge]-{1,%d}(b:entities)
				COLUMNS (a.id AS src_id, b.id AS dst_id, path_length(p) AS hop)
			)
		)
		WHERE rn <= ?
	`, graphName, placeholders, hops)

	args := make([]any, 0, len(seeds)+1)
	for _, seed := range seeds {
		args = append(args, seed)
	}
	args = append(args, maxNeighborsPerNode)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("graph query failed: %w", err)
	}
	defer rows.Close()

	scores := make(map[string]float64)
	for rows.Next() {
		var srcID string
		var dstID string
		var hop int
		if err := rows.Scan(&srcID, &dstID, &hop); err != nil {
			return nil, fmt.Errorf("failed to scan graph row: %w", err)
		}
		if hop <= 0 {
			continue
		}
		score := math.Pow(decay, float64(hop))
		scores[dstID] += score
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("graph rows iteration error: %w", err)
	}

	return scores, nil
}
