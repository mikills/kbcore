package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	duckdbdriver "github.com/duckdb/duckdb-go/v2"
	kb "github.com/mikills/minnow/kb"
	graph "github.com/mikills/minnow/kb/duckdb/internal/graph"
)

func applyPreparedDocsAppender(ctx context.Context, db *sql.DB, docs []preparedUpsertDoc) error {
	return applyPreparedDocsAndGraphAppender(ctx, db, docs, nil, false)
}

func applyPreparedDocsAndGraphAppender(
	ctx context.Context,
	db *sql.DB,
	docs []preparedUpsertDoc,
	graphResult *kb.GraphBuildResult,
	ensureGraphTables bool,
) error {
	if len(docs) == 0 {
		return nil
	}
	table := fmt.Sprintf("_minnow_docs_upsert_%d", time.Now().UnixNano())
	conn, err := createPreparedDocsStagingTable(ctx, db, table, len(docs[0].Embedding))
	if err != nil {
		return err
	}
	defer conn.Close()
	defer conn.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	if err := appendPreparedDocs(ctx, conn, table, docs); err != nil {
		return err
	}
	return mergePreparedDocsAndGraphStaging(
		ctx,
		preparedDocsStagingMerge{
			conn:              conn,
			table:             table,
			docs:              docs,
			graphResult:       graphResult,
			ensureGraphTables: ensureGraphTables,
		},
	)
}

func createPreparedDocsStagingTable(ctx context.Context, db *sql.DB, table string, dim int) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open appender conn: %w", err)
	}
	createSQL := fmt.Sprintf(
		`CREATE TABLE %s (id TEXT, content TEXT, embedding FLOAT[%d], media_refs TEXT, metadata TEXT)`,
		table,
		dim,
	)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		conn.Close()
		return nil, fmt.Errorf("create appender staging table: %w", err)
	}
	return conn, nil
}

func appendPreparedDocs(ctx context.Context, conn *sql.Conn, table string, docs []preparedUpsertDoc) error {
	return conn.Raw(func(raw any) error {
		driverConn, ok := raw.(driver.Conn)
		if !ok {
			return fmt.Errorf("duckdb raw connection has type %T", raw)
		}
		appender, err := duckdbdriver.NewAppenderFromConn(driverConn, "", table)
		if err != nil {
			return fmt.Errorf("create docs appender: %w", err)
		}
		defer appender.Close()
		for _, prepared := range docs {
			if err := appendPreparedDocRow(appender, prepared); err != nil {
				return err
			}
		}
		if err := appender.Close(); err != nil {
			return fmt.Errorf("close docs appender: %w", err)
		}
		return nil
	})
}

func appendPreparedDocRow(appender *duckdbdriver.Appender, prepared preparedUpsertDoc) error {
	doc := prepared.Doc
	mediaRefsJSON, err := encodeMediaRefs(doc.MediaIDs, doc.MediaRefs)
	if err != nil {
		return fmt.Errorf("encode media refs for doc %q: %w", doc.ID, err)
	}
	var mediaRefsValue any
	if mediaRefsJSON.Valid {
		mediaRefsValue = mediaRefsJSON.String
	}
	metadataJSON, err := encodeMetadata(doc.Metadata)
	if err != nil {
		return fmt.Errorf("encode metadata for doc %q: %w", doc.ID, err)
	}
	var metadataValue any
	if metadataJSON.Valid {
		metadataValue = metadataJSON.String
	}
	if err := appender.AppendRow(doc.ID, doc.Text, prepared.Embedding, mediaRefsValue, metadataValue); err != nil {
		return kb.WrapEmbeddingDimensionMismatch(
			fmt.Errorf("append doc %q: %w", doc.ID, err),
			"upsert embedding dimension is incompatible with stored vectors",
		)
	}
	return nil
}

type preparedDocsStagingMerge struct {
	conn              *sql.Conn
	table             string
	docs              []preparedUpsertDoc
	graphResult       *kb.GraphBuildResult
	ensureGraphTables bool
}

func mergePreparedDocsAndGraphStaging(ctx context.Context, input preparedDocsStagingMerge) error {
	tx, err := input.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin appender upsert tx: %w", err)
	}
	if err := deletePreparedDocRows(ctx, tx, input.docs); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO docs (id, content, embedding, media_refs, metadata) SELECT id, content, embedding, media_refs, metadata FROM %s`, input.table)); err != nil {
		tx.Rollback()
		return kb.WrapEmbeddingDimensionMismatch(
			fmt.Errorf("bulk insert prepared docs: %w", err),
			"upsert embedding dimension is incompatible with stored vectors",
		)
	}
	if err := applyPreparedGraphTx(ctx, tx, input.docs, input.graphResult, input.ensureGraphTables); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit appender upsert tx: %w", err)
	}
	return nil
}

func deletePreparedDocRows(ctx context.Context, tx *sql.Tx, docs []preparedUpsertDoc) error {
	stmtDelete, err := tx.PrepareContext(ctx, `DELETE FROM docs WHERE id = ?`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare docs delete: %w", err)
	}
	defer stmtDelete.Close()
	stmtUndelete, err := tx.PrepareContext(ctx, `DELETE FROM doc_tombstones WHERE doc_id = ?`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare doc_tombstones delete: %w", err)
	}
	defer stmtUndelete.Close()
	for _, prepared := range docs {
		if err := deletePreparedDocRow(ctx, tx, stmtDelete, stmtUndelete, prepared.Doc.ID); err != nil {
			return err
		}
	}
	return nil
}

func deletePreparedDocRow(
	ctx context.Context,
	tx *sql.Tx,
	stmtDelete *sql.Stmt,
	stmtUndelete *sql.Stmt,
	docID string,
) error {
	if _, err := stmtDelete.ExecContext(ctx, docID); err != nil {
		tx.Rollback()
		return fmt.Errorf("delete existing doc %q before upsert: %w", docID, err)
	}
	if _, err := stmtUndelete.ExecContext(ctx, docID); err != nil {
		tx.Rollback()
		return fmt.Errorf("clear tombstone for doc %q: %w", docID, err)
	}
	return nil
}

func applyPreparedDocsTx(
	ctx context.Context,
	tx *sql.Tx,
	docs []preparedUpsertDoc,
	graphResult *kb.GraphBuildResult,
	ensureGraphTables bool,
) error {
	if len(docs) == 0 {
		return commitEmptyPreparedDocsTx(tx)
	}
	stmts, err := preparePreparedDocsTxStatements(ctx, tx, len(docs[0].Embedding))
	if err != nil {
		return err
	}
	defer stmts.close()
	if err := upsertPreparedDocsTx(ctx, tx, stmts, docs); err != nil {
		return err
	}
	if err := applyPreparedGraphTx(ctx, tx, docs, graphResult, ensureGraphTables); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit upsert tx: %w", err)
	}
	return nil
}

type preparedDocsTxStatements struct {
	delete   *sql.Stmt
	insert   *sql.Stmt
	undelete *sql.Stmt
}

func (s preparedDocsTxStatements) close() {
	s.delete.Close()
	s.insert.Close()
	s.undelete.Close()
}

func commitEmptyPreparedDocsTx(tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit empty upsert tx: %w", err)
	}
	return nil
}

func preparePreparedDocsTxStatements(ctx context.Context, tx *sql.Tx, dim int) (preparedDocsTxStatements, error) {
	stmtDelete, err := tx.PrepareContext(ctx, `DELETE FROM docs WHERE id = ?`)
	if err != nil {
		tx.Rollback()
		return preparedDocsTxStatements{}, fmt.Errorf("prepare docs delete: %w", err)
	}
	insertSQL := fmt.Sprintf(
		`INSERT INTO docs (id, content, embedding, media_refs, metadata) VALUES (?, ?, CAST(? AS FLOAT[%d]), ?, ?)`,
		dim,
	)
	stmtInsert, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		stmtDelete.Close()
		tx.Rollback()
		return preparedDocsTxStatements{}, fmt.Errorf("prepare docs insert: %w", err)
	}
	stmtUndelete, err := tx.PrepareContext(ctx, `DELETE FROM doc_tombstones WHERE doc_id = ?`)
	if err != nil {
		stmtDelete.Close()
		stmtInsert.Close()
		tx.Rollback()
		return preparedDocsTxStatements{}, fmt.Errorf("prepare doc_tombstones delete: %w", err)
	}
	return preparedDocsTxStatements{delete: stmtDelete, insert: stmtInsert, undelete: stmtUndelete}, nil
}

func upsertPreparedDocsTx(
	ctx context.Context,
	tx *sql.Tx,
	stmts preparedDocsTxStatements,
	docs []preparedUpsertDoc,
) error {
	for _, prepared := range docs {
		if err := upsertPreparedDocTx(ctx, tx, stmts, prepared); err != nil {
			return err
		}
	}
	return nil
}

func upsertPreparedDocTx(
	ctx context.Context,
	tx *sql.Tx,
	stmts preparedDocsTxStatements,
	prepared preparedUpsertDoc,
) error {
	doc := prepared.Doc
	if _, err := stmts.delete.ExecContext(ctx, doc.ID); err != nil {
		tx.Rollback()
		return fmt.Errorf("delete existing doc %q before upsert: %w", doc.ID, err)
	}
	mediaRefsJSON, err := encodeMediaRefs(doc.MediaIDs, doc.MediaRefs)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("encode media refs for doc %q: %w", doc.ID, err)
	}
	metadataJSON, err := encodeMetadata(doc.Metadata)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("encode metadata for doc %q: %w", doc.ID, err)
	}
	if _, err := stmts.insert.ExecContext(ctx, doc.ID, doc.Text, FormatVectorForSQL(prepared.Embedding), mediaRefsJSON, metadataJSON); err != nil {
		tx.Rollback()
		return kb.WrapEmbeddingDimensionMismatch(
			fmt.Errorf("upsert doc %q: %w", doc.ID, err),
			"upsert embedding dimension is incompatible with stored vectors",
		)
	}
	if _, err := stmts.undelete.ExecContext(ctx, doc.ID); err != nil {
		tx.Rollback()
		return fmt.Errorf("clear tombstone for doc %q: %w", doc.ID, err)
	}
	return nil
}

func applyPreparedGraphTx(
	ctx context.Context,
	tx *sql.Tx,
	docs []preparedUpsertDoc,
	graphResult *kb.GraphBuildResult,
	ensureGraphTables bool,
) error {
	if graphResult == nil {
		return nil
	}
	if ensureGraphTables {
		if err := ensureGraphTablesForTx(ctx, tx); err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := graph.PruneForDocsTx(ctx, tx, preparedDocIDs(docs), true); err != nil {
		tx.Rollback()
		return err
	}
	if err := InsertGraphBuildResultTx(ctx, tx, graphResult); err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func preparedDocIDs(docs []preparedUpsertDoc) []string {
	docIDs := make([]string, 0, len(docs))
	for _, prepared := range docs {
		docIDs = append(docIDs, prepared.Doc.ID)
	}
	return docIDs
}
