package cmd

import (
	"encoding/json"
	"testing"

	kb "github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func TestWorkerFailureReason(t *testing.T) {
	payload, err := json.Marshal(kb.WorkerFailedPayload{
		Stage: "document.embedded",
		Error: "cache budget exceeded: current_bytes=562311233 max_bytes=536870912",
	})
	require.NoError(t, err)
	event := &kb.KBEvent{EventID: "evt", KBID: "kb", Kind: kb.EventWorkerFailed, Payload: payload}

	out := eventStatusPayload(event)

	require.Equal(t, "cache budget exceeded: current_bytes=562311233 max_bytes=536870912", out["last_error"])
	require.Equal(t, out["last_error"], out["error"])
}

func TestEventErrorPrecedence(t *testing.T) {
	payload, err := json.Marshal(kb.WorkerFailedPayload{Stage: "document.embedded", Error: "from payload"})
	require.NoError(t, err)
	event := &kb.KBEvent{
		EventID: "evt", KBID: "kb", Kind: kb.EventWorkerFailed,
		Payload: payload, LastError: "from the event",
	}

	require.Equal(t, "from the event", eventStatusPayload(event)["last_error"])
}
