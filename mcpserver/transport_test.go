package mcpserver

import (
	"context"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestHTTPHandlerBoundsStatefulSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server := mcp.NewServer(&mcp.Implementation{Name: "test-server", Version: "v0"}, nil)
	httpServer := httptest.NewServer(NewHTTPHandler(server, Config{
		HTTPStateful:       true,
		HTTPSessionTimeout: time.Minute,
		HTTPMaxSessions:    1,
	}))
	defer httpServer.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0"}, nil)
	first, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
	require.NoError(t, err)

	_, err = client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
	require.Error(t, err)
	require.Len(t, slices.Collect(server.Sessions()), 1)
	require.NoError(t, first.Close())
	require.Eventually(t, func() bool {
		return len(slices.Collect(server.Sessions())) == 0
	}, time.Second, 10*time.Millisecond)

	second, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
	require.NoError(t, err)
	require.NoError(t, second.Close())
}

func TestHTTPHandlerExpiresAbandonedSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := mcp.NewServer(&mcp.Implementation{Name: "test-server", Version: "v0"}, nil)
	httpServer := httptest.NewServer(NewHTTPHandler(server, Config{
		HTTPStateful:       true,
		HTTPSessionTimeout: 50 * time.Millisecond,
	}))
	t.Cleanup(httpServer.Close)

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	require.Len(t, slices.Collect(server.Sessions()), 1)

	require.Eventually(t, func() bool {
		return len(slices.Collect(server.Sessions())) == 0
	}, 5*time.Second, 10*time.Millisecond)
}
