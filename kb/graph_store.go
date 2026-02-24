// graph_store.go provides the DuckDB schema management, write, and query
// helpers for the entity relationship graph stored alongside document vectors.
//
// Graph model:
//
//   - entities     — named entities extracted from document chunks.
//   - edges        — directional relationships between entities, weighted by
//                    co-occurrence strength and annotated with chunk_id.
//   - doc_entities — doc-to-entity link table derived from edge chunk_ids and
//                    explicit EntityChunkMappings. Weights are accumulated per
//                    (doc_id, entity_id, chunk_id) triple.
//
// Large-input query strategy:
//
//   - Below tempTableThreshold (200 IDs), queries use SQL IN (?, ...) placeholders.
//   - At or above the threshold, IDs are bulk-inserted into a TEMP TABLE and
//     joined against, keeping the SQL parameter count bounded and allowing the
//     query planner to use index lookups.
//
// Graph pruning on delete:
//
//   - pruneGraphForDocsTx removes doc_entities rows for deleted docs, then
//     removes edges belonging to the same chunks, and finally sweeps any
//     entities no longer referenced by a remaining doc_entity or edge row.

package kb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
)

var tempTableCounter atomic.Int64

// uniqueTempName returns a process-unique name for a temporary table using the
// given prefix. The atomic counter is monotonically increasing so names never
// collide within a process even under concurrent callers.
func uniqueTempName(prefix string) string {
	n := tempTableCounter.Add(1)
	return fmt.Sprintf("%s_%d", prefix, n)
}

const tempTableThreshold = 200

// ensureEntityGraphTables probes for the edges and doc_entities tables and
// returns an error if either is absent. Used as a precondition check before
// graph queries on a shard that may not have been built with graph extraction.
func ensureEntityGraphTables(ctx context.Context, db *sql.DB) error {
	if err := ensureEdgesTable(ctx, db); err != nil {
		return err
	}
	if err := ensureDocEntitiesTable(ctx, db); err != nil {
		return err
	}
	return nil
}

// EnsureGraphTables creates the entities, edges, and doc_entities tables and
// their associated indexes if they do not already exist. Safe to call on both
// new and pre-existing databases (IF NOT EXISTS semantics throughout).
func EnsureGraphTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS entities (
			id TEXT PRIMARY KEY,
			name TEXT
		)
	`); err != nil {
		return fmt.Errorf("create entities table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS edges (
			src TEXT,
			dst TEXT,
			weight DOUBLE,
			rel_type TEXT,
			chunk_id TEXT
		)
	`); err != nil {
		return fmt.Errorf("create edges table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS doc_entities (
			doc_id TEXT,
			entity_id TEXT,
			weight DOUBLE,
			chunk_id TEXT
		)
	`); err != nil {
		return fmt.Errorf("create doc_entities table: %w", err)
	}

	indexes := []struct {
		name string
		ddl  string
	}{
		{"idx_edges_src", `CREATE INDEX IF NOT EXISTS idx_edges_src ON edges(src)`},
		{"idx_edges_dst", `CREATE INDEX IF NOT EXISTS idx_edges_dst ON edges(dst)`},
		{"idx_doc_entities_doc_id", `CREATE INDEX IF NOT EXISTS idx_doc_entities_doc_id ON doc_entities(doc_id)`},
		{"idx_doc_entities_entity_id", `CREATE INDEX IF NOT EXISTS idx_doc_entities_entity_id ON doc_entities(entity_id)`},
	}
	for _, idx := range indexes {
		if _, err := db.ExecContext(ctx, idx.ddl); err != nil {
			return fmt.Errorf("create index %s: %w", idx.name, err)
		}
	}

	return nil
}

// InsertGraphBuildResult writes the full GraphBuildResult into db within a
// single transaction. It is a convenience wrapper around InsertGraphBuildResultTx
// that manages the transaction lifecycle.
func InsertGraphBuildResult(ctx context.Context, db *sql.DB, result *GraphBuildResult) error {
	if result == nil {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin graph insert tx: %w", err)
	}
	if err := InsertGraphBuildResultTx(ctx, tx, result); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit graph insert tx: %w", err)
	}
	return nil
}

// InsertGraphBuildResultTx writes entities, edges, and doc-entity links into
// the given transaction.
//
// Insertion order:
//  1. Entities — INSERT OR IGNORE so duplicate IDs across chunks are
//     deduplicated without error.
//  2. Edges — all edges with weight defaulting to 1.0 when absent or <= 0.
//  3. doc_entities — derived by mapping each edge's chunk_id to its parent
//     doc_id via result.Chunks, then accumulating weights per
//     (doc_id, entity_id, chunk_id) triple. Standalone EntityChunkMappings
//     are added if not already covered by an edge entry.
func InsertGraphBuildResultTx(ctx context.Context, tx *sql.Tx, result *GraphBuildResult) error {
	if result == nil {
		return nil
	}

	if len(result.Entities) > 0 {
		var err error
		stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO entities (id, name) VALUES (?, ?)`)
		if err != nil {
			return fmt.Errorf("prepare entity insert: %w", err)
		}
		for _, ent := range result.Entities {
			if ent.ID == "" {
				continue
			}
			if _, err := stmt.ExecContext(ctx, ent.ID, ent.Name); err != nil {
				stmt.Close()
				return fmt.Errorf("insert entity %s: %w", ent.ID, err)
			}
		}
		if err := stmt.Close(); err != nil {
			return fmt.Errorf("close entity stmt: %w", err)
		}
	}

	if len(result.Edges) > 0 {
		var err error
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO edges (src, dst, weight, rel_type, chunk_id) VALUES (?, ?, ?, ?, ?)`)
		if err != nil {
			return fmt.Errorf("prepare edge insert: %w", err)
		}
		for _, edge := range result.Edges {
			if edge.SrcID == "" || edge.DstID == "" {
				continue
			}
			weight := edge.Weight
			if weight <= 0 {
				weight = 1.0
			}
			if _, err := stmt.ExecContext(ctx, edge.SrcID, edge.DstID, weight, edge.RelType, edge.ChunkID); err != nil {
				stmt.Close()
				return fmt.Errorf("insert edge %s->%s: %w", edge.SrcID, edge.DstID, err)
			}
		}
		if err := stmt.Close(); err != nil {
			return fmt.Errorf("close edge stmt: %w", err)
		}
	}

	chunkToDoc := make(map[string]string, len(result.Chunks))
	for _, chunk := range result.Chunks {
		if chunk.ChunkID == "" || chunk.DocID == "" {
			continue
		}
		chunkToDoc[chunk.ChunkID] = chunk.DocID
	}

	type docEntityKey struct {
		docID    string
		entityID string
		chunkID  string
	}
	docEntityWeights := make(map[docEntityKey]float64)

	for _, edge := range result.Edges {
		docID := chunkToDoc[edge.ChunkID]
		if docID == "" {
			continue
		}
		weight := edge.Weight
		if weight <= 0 {
			weight = 1.0
		}
		docEntityWeights[docEntityKey{docID: docID, entityID: edge.SrcID, chunkID: edge.ChunkID}] += weight
		docEntityWeights[docEntityKey{docID: docID, entityID: edge.DstID, chunkID: edge.ChunkID}] += weight
	}

	for _, m := range result.EntityChunkMappings {
		if m.EntityID == "" || m.ChunkID == "" {
			continue
		}
		docID := chunkToDoc[m.ChunkID]
		if docID == "" {
			continue
		}
		key := docEntityKey{docID: docID, entityID: m.EntityID, chunkID: m.ChunkID}
		if _, ok := docEntityWeights[key]; !ok {
			docEntityWeights[key] = 1.0
		}
	}

	if len(docEntityWeights) > 0 {
		var err error
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO doc_entities (doc_id, entity_id, weight, chunk_id) VALUES (?, ?, ?, ?)`)
		if err != nil {
			return fmt.Errorf("prepare doc_entity insert: %w", err)
		}
		for key, weight := range docEntityWeights {
			if _, err := stmt.ExecContext(ctx, key.docID, key.entityID, weight, key.chunkID); err != nil {
				stmt.Close()
				return fmt.Errorf("insert doc_entity %s->%s: %w", key.docID, key.entityID, err)
			}
		}
		if err := stmt.Close(); err != nil {
			return fmt.Errorf("close doc_entity stmt: %w", err)
		}
	}

	return nil
}

// ensureEdgesTable probes the edges table with a zero-row query and returns an
// error if the table is absent.
func ensureEdgesTable(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT 1 FROM edges LIMIT 0`)
	if err != nil {
		return fmt.Errorf("edges table missing: %w", err)
	}
	return rows.Close()
}

// ensureDocEntitiesTable probes the doc_entities table with a zero-row query
// and returns an error if the table is absent.
func ensureDocEntitiesTable(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT 1 FROM doc_entities LIMIT 0`)
	if err != nil {
		return fmt.Errorf("doc_entities table missing: %w", err)
	}
	return rows.Close()
}

// ensureEntitiesTable probes the entities table with a zero-row query and
// returns an error if the table is absent.
func ensureEntitiesTable(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT 1 FROM entities LIMIT 0`)
	if err != nil {
		return fmt.Errorf("entities table missing: %w", err)
	}
	return rows.Close()
}

// queryEdgesBySources returns all edges adjacent to any of the given source
// entity IDs, traversed bidirectionally (forward src→dst and reverse dst→src).
// Results are ranked per source node by descending weight and capped at
// maxNeighborsPerNode using a ROW_NUMBER window function.
//
// Routes to the temp table path when len(sources) >= tempTableThreshold.
func queryEdgesBySources(ctx context.Context, db *sql.DB, sources []string, edgeTypes []string, maxNeighborsPerNode int) ([]edgeRow, error) {
	if len(sources) == 0 {
		return nil, nil
	}

	if shouldUseTempTable(len(sources)) {
		return queryEdgesBySourcesTempTable(ctx, db, sources, edgeTypes, maxNeighborsPerNode)
	}

	var sb strings.Builder
	srcPlaceholders := buildInClausePlaceholders(len(sources))

	// We need source args twice (forward + reverse) plus optional edgeTypes twice
	args := make([]any, 0, 2*len(sources)+2*len(edgeTypes)+1)

	// Forward: src IN (sources) → (src, dst)
	// Reverse: dst IN (sources) → (dst as src, src as dst) so BFS caller sees consistent columns
	sb.WriteString(`
		WITH ranked AS (
			SELECT src, dst, COALESCE(weight, 1.0) AS weight
			FROM edges
			WHERE src IN (`)
	sb.WriteString(srcPlaceholders)
	sb.WriteString(")")
	for _, source := range sources {
		args = append(args, source)
	}
	if len(edgeTypes) > 0 {
		sb.WriteString(` AND rel_type IN (`)
		sb.WriteString(buildInClausePlaceholders(len(edgeTypes)))
		sb.WriteString(")")
		for _, et := range edgeTypes {
			args = append(args, et)
		}
	}

	sb.WriteString(`
			UNION ALL
			SELECT dst AS src, src AS dst, COALESCE(weight, 1.0) AS weight
			FROM edges
			WHERE dst IN (`)
	sb.WriteString(srcPlaceholders)
	sb.WriteString(")")
	for _, source := range sources {
		args = append(args, source)
	}
	if len(edgeTypes) > 0 {
		sb.WriteString(` AND rel_type IN (`)
		sb.WriteString(buildInClausePlaceholders(len(edgeTypes)))
		sb.WriteString(")")
		for _, et := range edgeTypes {
			args = append(args, et)
		}
	}

	sb.WriteString(`
		),
		limited AS (
			SELECT src, dst, weight,
				ROW_NUMBER() OVER (PARTITION BY src ORDER BY weight DESC) AS rn
			FROM ranked
		)
		SELECT src, dst, weight
		FROM limited
		WHERE rn <= ?
	`)
	args = append(args, maxNeighborsPerNode)

	rows, err := db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("edge query failed: %w", err)
	}
	defer rows.Close()

	results := make([]edgeRow, 0, len(sources))
	for rows.Next() {
		var row edgeRow
		if err := rows.Scan(&row.Src, &row.Dst, &row.Weight); err != nil {
			return nil, fmt.Errorf("failed to scan edge row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("edge rows iteration error: %w", err)
	}

	return results, nil
}

// queryEdgesBySourcesTempTable is the large-input variant of queryEdgesBySources.
// Sources are bulk-loaded into a TEMP TABLE and joined against for both forward
// and reverse edge lookups, keeping the SQL parameter count bounded to at most
// 2*len(edgeTypes)+1 regardless of source set size.
func queryEdgesBySourcesTempTable(ctx context.Context, db *sql.DB, sources []string, edgeTypes []string, maxNeighborsPerNode int) ([]edgeRow, error) {
	tempName := uniqueTempName("tmp_edge_sources")
	if err := createTempIDTable(ctx, db, tempName); err != nil {
		return nil, err
	}
	defer func() { _ = dropTable(ctx, db, tempName) }()

	if err := insertIDs(ctx, db, tempName, sources); err != nil {
		return nil, err
	}

	var sb strings.Builder
	// edgeTypes are typically small; still use IN (...) placeholders for them.
	args := make([]any, 0, 2*len(edgeTypes)+1)

	sb.WriteString(fmt.Sprintf(`
		WITH ranked AS (
			SELECT src, dst, COALESCE(weight, 1.0) AS weight
			FROM edges
			WHERE src IN (SELECT id FROM %s)`, tempName))
	if len(edgeTypes) > 0 {
		sb.WriteString(` AND rel_type IN (`)
		sb.WriteString(buildInClausePlaceholders(len(edgeTypes)))
		sb.WriteString(`)`)
		for _, et := range edgeTypes {
			args = append(args, et)
		}
	}
	sb.WriteString(fmt.Sprintf(`
			UNION ALL
			SELECT dst AS src, src AS dst, COALESCE(weight, 1.0) AS weight
			FROM edges
			WHERE dst IN (SELECT id FROM %s)`, tempName))
	if len(edgeTypes) > 0 {
		sb.WriteString(` AND rel_type IN (`)
		sb.WriteString(buildInClausePlaceholders(len(edgeTypes)))
		sb.WriteString(`)`)
		for _, et := range edgeTypes {
			args = append(args, et)
		}
	}
	sb.WriteString(`
		),
		limited AS (
			SELECT src, dst, weight,
				ROW_NUMBER() OVER (PARTITION BY src ORDER BY weight DESC) AS rn
			FROM ranked
		)
		SELECT src, dst, weight
		FROM limited
		WHERE rn <= ?
	`)
	args = append(args, maxNeighborsPerNode)

	rows, err := db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("edge query failed: %w", err)
	}
	defer rows.Close()

	results := make([]edgeRow, 0, len(sources))
	for rows.Next() {
		var row edgeRow
		if err := rows.Scan(&row.Src, &row.Dst, &row.Weight); err != nil {
			return nil, fmt.Errorf("failed to scan edge row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("edge rows iteration error: %w", err)
	}

	return results, nil
}

// queryEntitiesForDocs returns a map of entity ID to accumulated weight across
// all doc_entities rows matching the given doc IDs. Weights are summed per
// entity; zero or negative row weights are replaced with 1.0. Empty entity IDs
// are skipped.
//
// Routes to the temp table path when len(docIDs) >= tempTableThreshold.
func queryEntitiesForDocs(ctx context.Context, db *sql.DB, docIDs []string) (map[string]float64, error) {
	if len(docIDs) == 0 {
		return map[string]float64{}, nil
	}

	if shouldUseTempTable(len(docIDs)) {
		return queryEntitiesForDocsTempTable(ctx, db, docIDs)
	}

	placeholders := buildInClausePlaceholders(len(docIDs))
	query := fmt.Sprintf(`
		SELECT entity_id, COALESCE(weight, 1.0) AS weight
		FROM doc_entities
		WHERE doc_id IN (%s)
	`, placeholders)

	args := make([]any, 0, len(docIDs))
	for _, id := range docIDs {
		args = append(args, id)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("entity query failed: %w", err)
	}
	defer rows.Close()

	entities := make(map[string]float64)
	for rows.Next() {
		var id string
		var weight float64
		if err := rows.Scan(&id, &weight); err != nil {
			return nil, fmt.Errorf("failed to scan entity: %w", err)
		}
		if id == "" {
			continue
		}
		if weight <= 0 {
			weight = 1.0
		}
		entities[id] += weight
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("entity rows iteration error: %w", err)
	}

	return entities, nil
}

// queryDocsForEntities scores documents by graph proximity to the given entity
// set. For each (doc, entity) pair in doc_entities, the doc score is
// incremented by entityScore * rowWeight, where entityScore is from the input
// map and rowWeight is the stored doc_entity weight (defaulting to 1.0).
//
// Routes to the temp table path when len(entityScores) >= tempTableThreshold.
func queryDocsForEntities(ctx context.Context, db *sql.DB, entityScores map[string]float64) (map[string]float64, error) {
	if len(entityScores) == 0 {
		return map[string]float64{}, nil
	}

	if shouldUseTempTable(len(entityScores)) {
		return queryDocsForEntitiesTempTable(ctx, db, entityScores)
	}

	entityIDs := mapKeys(entityScores)
	placeholders := buildInClausePlaceholders(len(entityIDs))
	query := fmt.Sprintf(`
		SELECT doc_id, entity_id, COALESCE(weight, 1.0) AS weight
		FROM doc_entities
		WHERE entity_id IN (%s)
	`, placeholders)

	args := make([]any, 0, len(entityIDs))
	for _, id := range entityIDs {
		args = append(args, id)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("doc_entities query failed: %w", err)
	}
	defer rows.Close()

	docScores := make(map[string]float64)
	for rows.Next() {
		var docID string
		var entityID string
		var weight float64
		if err := rows.Scan(&docID, &entityID, &weight); err != nil {
			return nil, fmt.Errorf("failed to scan doc_entities row: %w", err)
		}
		entityScore := entityScores[entityID]
		if entityScore == 0 {
			continue
		}
		if weight <= 0 {
			weight = 1.0
		}
		docScores[docID] += entityScore * weight
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("doc_entities rows iteration error: %w", err)
	}

	return docScores, nil
}

// queryEntitiesForDocsTempTable is the large-input variant of queryEntitiesForDocs.
// Doc IDs are bulk-inserted into a TEMP TABLE and joined against doc_entities,
// avoiding an unbounded IN clause.
func queryEntitiesForDocsTempTable(ctx context.Context, db *sql.DB, docIDs []string) (map[string]float64, error) {
	tempName := uniqueTempName("tmp_doc_ids")
	if err := createTempIDTable(ctx, db, tempName); err != nil {
		return nil, err
	}
	defer func() {
		_ = dropTable(ctx, db, tempName)
	}()

	if err := insertIDs(ctx, db, tempName, docIDs); err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT de.entity_id, COALESCE(de.weight, 1.0) AS weight
		FROM doc_entities de
		INNER JOIN %s ids ON de.doc_id = ids.id
	`, tempName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("entity query failed: %w", err)
	}
	defer rows.Close()

	entities := make(map[string]float64)
	for rows.Next() {
		var id string
		var weight float64
		if err := rows.Scan(&id, &weight); err != nil {
			return nil, fmt.Errorf("failed to scan entity: %w", err)
		}
		if id == "" {
			continue
		}
		if weight <= 0 {
			weight = 1.0
		}
		entities[id] += weight
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("entity rows iteration error: %w", err)
	}

	return entities, nil
}

// queryDocsForEntitiesTempTable is the large-input variant of queryDocsForEntities.
// Entity IDs are bulk-inserted into a TEMP TABLE and joined against doc_entities.
func queryDocsForEntitiesTempTable(ctx context.Context, db *sql.DB, entityScores map[string]float64) (map[string]float64, error) {
	entityIDs := mapKeys(entityScores)
	tempName := uniqueTempName("tmp_entity_ids")
	if err := createTempIDTable(ctx, db, tempName); err != nil {
		return nil, err
	}
	defer func() {
		_ = dropTable(ctx, db, tempName)
	}()

	if err := insertIDs(ctx, db, tempName, entityIDs); err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT de.doc_id, de.entity_id, COALESCE(de.weight, 1.0) AS weight
		FROM doc_entities de
		INNER JOIN %s ids ON de.entity_id = ids.id
	`, tempName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("doc_entities query failed: %w", err)
	}
	defer rows.Close()

	docScores := make(map[string]float64)
	for rows.Next() {
		var docID string
		var entityID string
		var weight float64
		if err := rows.Scan(&docID, &entityID, &weight); err != nil {
			return nil, fmt.Errorf("failed to scan doc_entities row: %w", err)
		}
		entityScore := entityScores[entityID]
		if entityScore == 0 {
			continue
		}
		if weight <= 0 {
			weight = 1.0
		}
		docScores[docID] += entityScore * weight
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("doc_entities rows iteration error: %w", err)
	}

	return docScores, nil
}

// shouldUseTempTable reports whether count meets or exceeds the threshold above
// which IN clause placeholders are replaced by a TEMP TABLE join.
func shouldUseTempTable(count int) bool {
	return count >= tempTableThreshold
}

// createTempIDTable creates a session-scoped TEMP TABLE with a single TEXT id
// column. The caller is responsible for dropping it when no longer needed,
// typically via a deferred dropTable call.
func createTempIDTable(ctx context.Context, db *sql.DB, name string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TEMP TABLE %s (id TEXT)`, name)); err != nil {
		return fmt.Errorf("create temp id table: %w", err)
	}
	return nil
}

// insertIDs bulk-inserts ids into the named table within a single transaction.
// The table is assumed to have a single TEXT column named id.
func insertIDs(ctx context.Context, db *sql.DB, tableName string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin id insert tx: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`INSERT INTO %s (id) VALUES (?)`, tableName))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare id insert: %w", err)
	}
	for _, id := range ids {
		if _, err := stmt.ExecContext(ctx, id); err != nil {
			stmt.Close()
			tx.Rollback()
			return fmt.Errorf("insert id: %w", err)
		}
	}
	if err := stmt.Close(); err != nil {
		tx.Rollback()
		return fmt.Errorf("close id stmt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit id insert tx: %w", err)
	}
	return nil
}

// dropTable drops the named table, ignoring the case where it does not exist.
func dropTable(ctx context.Context, db *sql.DB, tableName string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName))
	return err
}

// pruneGraphForDocsTx removes graph rows associated with the given doc IDs
// from within an existing transaction.
//
// Deletion order:
//  1. Collect distinct chunk_ids referenced by the doc_entities rows being
//     removed (only when cleanupEdgesAndEntities is true).
//  2. Delete doc_entities rows for the target doc IDs.
//  3. If cleanupEdgesAndEntities: delete edges whose chunk_id matches any of
//     the collected chunk IDs.
//  4. If cleanupEdgesAndEntities: sweep entities that are no longer referenced
//     by any remaining doc_entity or edge row (orphan sweep).
//
// When cleanupEdgesAndEntities is false only doc_entities rows are removed.
// This is used during tombstone-only updates where edge/entity cleanup is
// deferred to compaction.
func pruneGraphForDocsTx(ctx context.Context, tx *sql.Tx, docIDs []string, cleanupEdgesAndEntities bool) error {
	if len(docIDs) == 0 {
		return nil
	}

	hasDocEntities, err := tableExists(ctx, tx, "doc_entities")
	if err != nil {
		return err
	}
	if !hasDocEntities {
		return nil
	}

	placeholders := buildInClausePlaceholders(len(docIDs))
	args := make([]any, 0, len(docIDs))
	for _, id := range docIDs {
		args = append(args, id)
	}

	chunkIDs := make([]string, 0)
	if cleanupEdgesAndEntities {
		rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT DISTINCT chunk_id FROM doc_entities WHERE doc_id IN (%s)`, placeholders), args...)
		if err != nil {
			return fmt.Errorf("query doc chunk ids: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var chunkID string
			if err := rows.Scan(&chunkID); err != nil {
				return fmt.Errorf("scan chunk id: %w", err)
			}
			if strings.TrimSpace(chunkID) != "" {
				chunkIDs = append(chunkIDs, chunkID)
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate chunk id rows: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM doc_entities WHERE doc_id IN (%s)`, placeholders), args...); err != nil {
		return fmt.Errorf("prune doc_entities: %w", err)
	}

	if !cleanupEdgesAndEntities {
		return nil
	}

	hasEdges, err := tableExists(ctx, tx, "edges")
	if err != nil {
		return err
	}
	if hasEdges && len(chunkIDs) > 0 {
		edgePlaceholders := buildInClausePlaceholders(len(chunkIDs))
		edgeArgs := make([]any, 0, len(chunkIDs))
		for _, id := range chunkIDs {
			edgeArgs = append(edgeArgs, id)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM edges WHERE chunk_id IN (%s)`, edgePlaceholders), edgeArgs...); err != nil {
			return fmt.Errorf("prune edges by chunk: %w", err)
		}
	}

	hasEntities, err := tableExists(ctx, tx, "entities")
	if err != nil {
		return err
	}
	if hasEntities {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM entities e
			WHERE NOT EXISTS (
				SELECT 1 FROM doc_entities de WHERE de.entity_id = e.id
			)
			AND NOT EXISTS (
				SELECT 1 FROM edges ed WHERE ed.src = e.id OR ed.dst = e.id
			)
		`); err != nil {
			return fmt.Errorf("prune orphan entities: %w", err)
		}
	}

	return nil
}
