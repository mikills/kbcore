package kb

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDocumentUpsertWorkerPreservesPreChunkedIDs(t *testing.T) {
	k := NewKB(nil, "")
	payload, err := json.Marshal(DocumentUpsertPayload{
		KBID: "code-main", PreChunked: true,
		Documents: []Document{{ID: "stable-code-id", Text: "package main"}},
	})
	require.NoError(t, err)
	event := &KBEvent{EventID: "source", KBID: "code-main", Payload: payload}

	result, err := (&DocumentUpsertWorker{KB: k, ID: "upsert"}).Handle(context.Background(), event)
	require.NoError(t, err)
	require.Len(t, result.FollowUps, 1)
	var chunked DocumentChunkedPayload
	require.NoError(t, json.Unmarshal(result.FollowUps[0].Payload, &chunked))
	require.Len(t, chunked.Documents, 1)
	require.Equal(t, "stable-code-id", chunked.Documents[0].ID)
}
