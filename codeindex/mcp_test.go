package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestMCPIsReadOnly(t *testing.T) {
	_, err := parseMCPCLIOptions([]string{"--yes"})
	require.Error(t, err)
	opts, err := parseMCPCLIOptions([]string{"--kb", "shared", "--index-key", "api"})
	require.NoError(t, err)
	require.Equal(t, "shared", opts.kbID)
	require.Equal(t, "api", opts.indexKey)

	server := newMCPServer(&mcpService{})
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	require.NoError(t, err)
	defer serverSession.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	require.NoError(t, err)
	defer clientSession.Close()

	tools, err := clientSession.ListTools(context.Background(), nil)
	require.NoError(t, err)
	names := make([]string, 0, len(tools.Tools))
	for _, tool := range tools.Tools {
		names = append(names, tool.Name)
		require.NotNil(t, tool.Annotations)
		require.True(t, tool.Annotations.ReadOnlyHint)
	}
	sort.Strings(names)
	require.Equal(t, []string{"codeindex_search", "codeindex_status"}, names)
}

func TestMCPSearchResolvesCurrentBranch(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	runTestGit(t, root, "config", "user.email", "codeindex@example.com")
	runTestGit(t, root, "config", "user.name", "Code Index")
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o644))
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "-m", "initial")
	target, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	state := emptyIndexState(target)
	state.UpdatedAt = time.Now().UTC()
	state.Files["main.go"] = stateFile{Hash: "hash", ChunkIDs: []string{"chunk"}}
	_, err = saveIndexState(target, state)
	require.NoError(t, err)

	var request map[string]any
	hosted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/code/search", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kb_id":   state.KBID,
			"results": []map[string]any{{"id": "chunk", "path": "main.go", "content": "package main"}},
		})
	}))
	defer hosted.Close()
	cfg := defaultConfig()
	cfg.Minnow.URL = hosted.URL
	service := &mcpService{cfg: cfg, root: root}

	_, response, err := service.search(context.Background(), nil, mcpSearchInput{Query: "entry point"})
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Equal(t, state.KBID, request["kb_id"])
	require.Equal(t, state.ScopeID, request["scope_id"])

	runTestGit(t, root, "switch", "-c", "feature")
	_, _, err = service.search(context.Background(), nil, mcpSearchInput{Query: "entry point"})
	require.ErrorContains(t, err, "current branch has not been indexed")
}

func TestMCPStatusKeepsPendingStateOffline(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	target, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	state := emptyIndexState(target)
	state.Files["main.go"] = stateFile{ChunkIDs: []string{"chunk"}}
	old := state
	old.UpdatedAt = time.Now().UTC()
	old.Files = map[string]stateFile{"old.go": {ChunkIDs: []string{"old"}}}
	_, err = saveIndexState(target, old)
	require.NoError(t, err)
	require.NoError(t, saveRunCheckpoint(target, runCheckpoint{
		Phase: runPhaseFinalizing, State: state,
	}))
	cfg := defaultConfig()
	cfg.Minnow.URL = "://invalid"
	service := &mcpService{cfg: cfg, root: root}

	_, status, err := service.status(context.Background(), nil, mcpStatusInput{})
	require.NoError(t, err)
	require.False(t, status.Indexed)
	require.True(t, status.Recoverable)
	require.Equal(t, string(runPhaseFinalizing), status.Phase)
	require.Equal(t, 1, status.ChunkCount)
}

func TestMCPPendingSession(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "-b", "main")
	target, err := resolveTarget(indexCLIOptions{root: root})
	require.NoError(t, err)
	state := emptyIndexState(target)
	state.Files["main.go"] = stateFile{ChunkIDs: []string{"chunk"}}
	require.NoError(t, saveRunCheckpoint(target, runCheckpoint{
		Phase: runPhaseFinalizing, State: state,
	}))
	saveSession(target, "instance:token")

	pending := true
	requests := make(map[string]int)
	hosted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests[r.URL.Path]++
		switch r.URL.Path {
		case "/v1/code/status":
			var request map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			require.Equal(t, "instance:token", request["session_id"])
			_ = json.NewEncoder(w).Encode(map[string]any{"pending": pending})
		case "/healthz":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"capabilities": []string{"ingest_sessions", "document_scopes", "session_commit_scope"},
			})
		case "/v1/scopes/" + target.ScopeID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"scope_id": target.ScopeID, "document_ids": []string{"chunk"}, "revision": "rev",
			})
		case "/v1/code/search":
			_ = json.NewEncoder(w).Encode(map[string]any{"kb_id": state.KBID, "results": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer hosted.Close()
	cfg := defaultConfig()
	cfg.Minnow.URL = hosted.URL
	service := &mcpService{cfg: cfg, root: root}

	_, status, err := service.status(context.Background(), nil, mcpStatusInput{})
	require.NoError(t, err)
	require.False(t, status.Indexed)
	_, _, err = service.search(context.Background(), nil, mcpSearchInput{Query: "entry point"})
	require.ErrorContains(t, err, "finalization is still pending")
	require.Equal(t, 2, requests["/v1/code/status"])

	pending = false
	_, status, err = service.status(context.Background(), nil, mcpStatusInput{})
	require.NoError(t, err)
	require.True(t, status.Indexed)
	_, _, err = service.search(context.Background(), nil, mcpSearchInput{Query: "entry point"})
	require.NoError(t, err)

	require.NoError(t, saveRunCheckpoint(target, runCheckpoint{
		Phase: runPhaseFinalized, State: state,
	}))
	_, status, err = service.status(context.Background(), nil, mcpStatusInput{})
	require.NoError(t, err)
	require.True(t, status.Indexed)
	_, _, err = service.search(context.Background(), nil, mcpSearchInput{Query: "entry point"})
	require.NoError(t, err)
	require.Equal(t, 2, requests["/v1/code/search"])
}

func TestMCPIdentityOverrides(t *testing.T) {
	for _, tc := range []struct {
		name, kbID, indexKey string
	}{
		{name: "kb", kbID: "shared"},
		{name: "index key", indexKey: "api"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			runTestGit(t, root, "init", "-b", "main")
			target, err := resolveTarget(indexCLIOptions{
				root: root, kbID: tc.kbID, indexKey: tc.indexKey,
			})
			require.NoError(t, err)
			state := emptyIndexState(target)
			state.UpdatedAt = time.Now().UTC()
			_, err = saveIndexState(target, state)
			require.NoError(t, err)

			service := &mcpService{root: root, kbID: tc.kbID, indexKey: tc.indexKey}
			selected, _, pending, err := service.selection()
			require.NoError(t, err)
			require.Nil(t, pending)
			require.Equal(t, target.IndexKey, selected.IndexKey)
			require.Equal(t, target.KBID, selected.KBID)
		})
	}
}

// Clients hide a server's stderr, so a config failure has to survive the
// handshake and come back from a tool call.
func TestMCPStartupFailure(t *testing.T) {
	t.Run("serves_despite_missing_env", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte("minnow:\n  token: ${CODEINDEX_ABSENT_VAR}\n"), 0o644))
		service := newMCPService(mcpCLIOptions{configPath: path, root: "."})

		_, _, err := service.status(context.Background(), nil, mcpStatusInput{})
		require.ErrorContains(t, err, "codeindex mcp did not start")
		require.ErrorContains(t, err, "env_vars")
	})

	t.Run("reports_from_search_too", func(t *testing.T) {
		service := newMCPService(mcpCLIOptions{configPath: filepath.Join(t.TempDir(), "absent.yaml")})
		_, _, err := service.search(context.Background(), nil, mcpSearchInput{Query: "anything"})
		require.ErrorContains(t, err, "codeindex mcp did not start")
	})

	// Telling someone to forward an environment variable when their YAML is
	// malformed sends them the wrong way.
	t.Run("omits_env_advice_for_other_failures", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte("minnow:\n url: x\n bad: [\n"), 0o644))
		service := newMCPService(mcpCLIOptions{configPath: path, root: "."})

		_, _, err := service.status(context.Background(), nil, mcpStatusInput{})
		require.ErrorContains(t, err, "codeindex mcp did not start")
		require.NotContains(t, err.Error(), "env_vars")
	})

	// A name forwarded but never exported arrives set and empty.
	t.Run("empty_env_counts_as_missing", func(t *testing.T) {
		t.Setenv("CODEINDEX_EMPTY_VAR", "")
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte("minnow:\n  token: ${CODEINDEX_EMPTY_VAR}\n"), 0o644))

		require.ErrorIs(t, newMCPService(mcpCLIOptions{configPath: path}).startErr, errMissingConfigEnv)
	})

	t.Run("ready_when_config_loads", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte("minnow:\n  url: https://example.com\n"), 0o644))
		require.NoError(t, newMCPService(mcpCLIOptions{configPath: path, root: "."}).ready())
	})
}

func TestMCPUsageOutput(t *testing.T) {
	t.Setenv("CODEINDEX_TOKEN", "super-secret-value")
	t.Setenv("CODEINDEX_MINNOW_URL", "https://user:super-secret-value@minnow.example.com")
	t.Setenv("CODEINDEX_REPO_ROOT", "")
	t.Setenv("MINNOW_REPO_ROOT", "")

	usage := captureStderr(t, func() { require.Equal(t, 2, writeMCPUsage()) })

	require.Contains(t, usage, "Usage: codeindex mcp")
	require.Contains(t, usage, `(default ".")`)
	require.Contains(t, usage, "https://minnow.example.com")
	require.NotContains(t, usage, "super-secret-value")
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	read, write, err := os.Pipe()
	require.NoError(t, err)
	original := os.Stderr
	os.Stderr = write
	defer func() { os.Stderr = original }()

	fn()
	require.NoError(t, write.Close())
	out, err := io.ReadAll(read)
	require.NoError(t, err)
	return string(out)
}

// A wrong command line is visible to whoever typed it, so it must not start a
// server that blocks on stdio forever. runMCP returns before any transport is
// built, so calling it here touches neither stdin nor stdout.
func TestMCPUsageExits(t *testing.T) {
	for name, args := range map[string][]string{
		"help":     {"--help"},
		"bad_flag": {"--bogus"},
		"bad_root": {"--root", ""},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, 2, runMCP(context.Background(), args))
		})
	}
}
