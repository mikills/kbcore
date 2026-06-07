package duckdb

import (
	"database/sql"
	"encoding/json"
	"fmt"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/mediarefs"
)

func encodeMediaRefs(mediaIDs []string, explicit []kb.ChunkMediaRef) (sql.NullString, error) {
	return mediarefs.Encode(mediaIDs, explicit)
}

func decodeMediaRefs(raw sql.NullString) ([]kb.ChunkMediaRef, error) {
	return mediarefs.Decode(raw)
}

func encodeMetadata(metadata map[string]any) (sql.NullString, error) {
	if metadata == nil {
		return sql.NullString{}, nil
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return sql.NullString{}, fmt.Errorf("marshal metadata: %w", err)
	}
	if string(data) == "null" {
		return sql.NullString{}, nil
	}
	return sql.NullString{String: string(data), Valid: true}, nil
}

func decodeMetadata(raw sql.NullString) (map[string]any, error) {
	if !raw.Valid || raw.String == "" {
		return nil, nil
	}
	var metadata map[string]any
	if err := json.Unmarshal([]byte(raw.String), &metadata); err != nil {
		return nil, err
	}
	if len(metadata) == 0 {
		return map[string]any{}, nil
	}
	return metadata, nil
}
