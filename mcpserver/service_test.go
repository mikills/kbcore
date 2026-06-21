package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mikills/minnow/kb"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestService(t *testing.T) {
	t.Run("indexing gate blocks async ingest", func(t *testing.T) {
		svc := newTestService(func(s *Service) { s.Config = Config{Enabled: true} })
		_, _, err := svc.ingestAsync(context.Background(), nil, ingestInput{Documents: oneDoc()})
		require.ErrorContains(t, err, "indexing tools are disabled")
	})

	t.Run("destructive gate blocks delete kb", func(t *testing.T) {
		svc := newTestService(func(s *Service) { s.Config = Config{Enabled: true} })
		_, _, err := svc.deleteKnowledgeBase(context.Background(), nil, kbIDInput{KBID: "kb-1"})
		require.ErrorContains(t, err, "destructive tools are disabled")

		svc.Config.AllowDestructive = true
		_, out, err := svc.deleteKnowledgeBase(context.Background(), nil, kbIDInput{KBID: "kb-1"})
		require.NoError(t, err)
		require.True(t, out.Deleted)
	})

	t.Run("operation status propagates not found", func(t *testing.T) {
		svc := newTestService(func(s *Service) {
			s.GetEvent = func(context.Context, string) (*kb.KBEvent, error) { return nil, kb.ErrEventNotFound }
		})
		_, err := svc.operationStatusOutput(context.Background(), "missing")
		require.True(t, errors.Is(err, kb.ErrEventNotFound))
	})

	t.Run("force compaction needs kb id", func(t *testing.T) {
		svc := newTestService(func(s *Service) {
			s.Config = Config{Enabled: true, AllowAdmin: true}
			s.ForceCompaction = func(_ context.Context, kbID string) (*kb.CompactionPublishResult, error) {
				require.NotEmpty(t, kbID, "compaction must never run with empty kb_id")
				return &kb.CompactionPublishResult{}, nil
			}
		})
		for _, missing := range []string{"", "   "} {
			_, _, err := svc.forceCompaction(context.Background(), nil, kbIDInput{KBID: missing})
			require.ErrorContains(t, err, "kb_id required", "input %q must be rejected", missing)
		}
		_, _, err := svc.forceCompaction(context.Background(), nil, kbIDInput{KBID: "kb-1"})
		require.NoError(t, err)
	})

	t.Run("query rejects huge k", func(t *testing.T) {
		svc := newTestService(func(s *Service) {
			s.Search = func(context.Context, string, []float32, *kb.SearchOptions) ([]kb.ExpandedResult, error) {
				require.Fail(t, "Search should not be called when K is out of bounds")
				return nil, nil
			}
		})
		_, _, err := svc.query(context.Background(), nil, queryInput{Query: "hi", K: queryToolMaxK + 1})
		require.ErrorContains(t, err, "k must be <=")
	})

	t.Run("async ingest submits operation", func(t *testing.T) {
		svc := newTestService(func(s *Service) {
			s.AppendDocumentUpsert = func(_ context.Context, p kb.DocumentUpsertPayload, idem, corr string) (string, string, error) {
				require.Equal(t, "kb-1", p.KBID)
				require.Len(t, p.Documents, 1)
				require.NotNil(t, p.Options.GraphEnabled)
				require.False(t, *p.Options.GraphEnabled)
				require.Equal(t, "idem-1", idem)
				require.Equal(t, "corr-1", corr)
				return "evt-1", idem, nil
			}
		})
		_, out, err := svc.ingestAsync(context.Background(), nil, ingestInput{
			KBID: "kb-1", Documents: oneDoc(),
			IdempotencyKey: "idem-1", CorrelationID: "corr-1",
		})
		require.NoError(t, err)
		require.Equal(t, "evt-1", out.EventID)
		require.Equal(t, "accepted", out.Status)
		require.Equal(t, []string{"doc-1"}, out.DocIDs)
	})

	t.Run("code indexing submits options", func(t *testing.T) {
		svc := newTestService(func(s *Service) {
			s.Config.CodeIndex = CodeIndexDefaults{
				Include:      []string{"**/*.go"},
				Exclude:      []string{"vendor/**"},
				MaxFileBytes: 1234,
				ChunkSize:    100,
				ChunkOverlap: 10,
				ResourcePolicy: kb.CodeIndexResourcePolicy{
					EmbedBatchSize: 7,
					MaxBatchBytes:  2048,
					MaxHeapBytes:   4096,
					MaxRSSBytes:    8192,
					LargeRepoFiles: 11,
				},
				RequireConfirm: true,
			}
			s.IndexCodebase = func(_ context.Context, opts kb.CodeIndexOptions) (kb.CodeIndexResult, error) {
				require.Equal(t, "kb-code", opts.KBID)
				require.Equal(t, "/repo", opts.Root)
				require.Equal(t, []string{"**/*.go"}, opts.Include)
				require.Equal(t, []string{"vendor/**"}, opts.Exclude)
				require.EqualValues(t, 1234, opts.MaxFileBytes)
				require.True(t, opts.IncludeUntracked)
				require.Equal(t, 7, opts.EmbedBatchSize)
				require.Equal(t, 2048, opts.MaxBatchBytes)
				require.Equal(t, uint64(4096), opts.MaxHeapBytes)
				require.Equal(t, uint64(8192), opts.MaxRSSBytes)
				require.Equal(t, 11, opts.LargeRepoFiles)
				require.True(t, opts.RequireConfirm)
				return kb.CodeIndexResult{KBID: opts.KBID, Root: opts.Root, IndexedFiles: 1}, nil
			}
		})
		_, out, err := svc.indexCodebase(
			context.Background(),
			nil,
			codeIndexInput{KBID: "kb-code", Root: "/repo", IncludeUntracked: true},
		)
		require.NoError(t, err)
		require.Equal(t, 1, out.IndexedFiles)
	})

	t.Run("code search returns enriched results", func(t *testing.T) {
		svc := newTestService(func(s *Service) {
			s.SearchCode = func(_ context.Context, kbID, query string, opts kb.CodeSearchOptions) ([]kb.CodeSearchResult, error) {
				require.Equal(t, "kb-code", kbID)
				require.Equal(t, "handler", query)
				require.Equal(t, "go", opts.Language)
				return []kb.CodeSearchResult{{ID: "c1", Path: "main.go", Language: "go", StartLine: 1, EndLine: 5}}, nil
			}
		})
		_, out, err := svc.codeSearch(
			context.Background(),
			nil,
			codeSearchInput{KBID: "kb-code", Query: "handler", K: 5, Language: "go"},
		)
		require.NoError(t, err)
		require.Len(t, out.Results, 1)
		require.Equal(t, "main.go", out.Results[0].Path)
	})

	t.Run("sync ingest completes", func(t *testing.T) {
		svc := newTestService(func(s *Service) { withSyncTimeout(s, time.Second) })
		_, out, err := svc.ingestSync(context.Background(), nil, ingestSyncInput{Documents: oneDoc()})
		require.NoError(t, err)
		require.False(t, out.TimedOut)
		require.Equal(t, "completed", out.Status)
		require.Equal(t, "evt-terminal", out.Terminal["event_id"])
	})

	t.Run("sync ingest times out", func(t *testing.T) {
		svc := newTestService(func(s *Service) {
			withSyncTimeout(s, time.Millisecond)
			// Override the default "Done" event with one that never reaches a
			// terminal state so the loop must time out.
			s.GetEvent = func(context.Context, string) (*kb.KBEvent, error) {
				return &kb.KBEvent{
					EventID: "evt-pending",
					KBID:    "kb-1",
					Kind:    kb.EventDocumentUpsert,
					Status:  kb.EventStatusPending,
				}, nil
			}
			s.FindOperationTerminal = func(context.Context, string) (*kb.KBEvent, error) { return nil, kb.ErrEventNotFound }
		})
		_, out, err := svc.ingestSync(context.Background(), nil, ingestSyncInput{Documents: oneDoc()})
		require.NoError(t, err)
		require.True(t, out.TimedOut)
		require.Equal(t, "timeout", out.Status)
	})

	t.Run("sync ingest should bail on perm error", func(t *testing.T) {
		// GetEvent returns a non-NotFound error every iteration. The loop must
		// surface it immediately rather than spin until MaxSyncTimeout.
		svc := newTestService(func(s *Service) {
			withSyncTimeout(s, time.Hour)
			s.GetEvent = func(context.Context, string) (*kb.KBEvent, error) {
				return nil, errors.New("event store unavailable")
			}
		})
		start := time.Now()
		_, _, err := svc.ingestSync(context.Background(), nil, ingestSyncInput{Documents: oneDoc()})
		require.ErrorContains(t, err, "event store unavailable")
		require.Less(t, time.Since(start), time.Second, "should bail before any meaningful poll")
	})

	t.Run("sync ingest polls past not found", func(t *testing.T) {
		// Event takes a moment to materialize in the inbox. loop must keep
		// polling on ErrEventNotFound and complete normally when it appears.
		calls := 0
		svc := newTestService(func(s *Service) {
			withSyncTimeout(s, time.Second)
			s.GetEvent = func(context.Context, string) (*kb.KBEvent, error) {
				calls++
				if calls < 3 {
					return nil, kb.ErrEventNotFound
				}
				return &kb.KBEvent{
					EventID: "evt-eventually",
					KBID:    "kb-1",
					Kind:    kb.EventDocumentUpsert,
					Status:  kb.EventStatusDone,
				}, nil
			}
		})
		_, out, err := svc.ingestSync(context.Background(), nil, ingestSyncInput{Documents: oneDoc()})
		require.NoError(t, err)
		require.Equal(t, "completed", out.Status)
		require.GreaterOrEqual(t, calls, 3, "loop must continue past initial NotFound responses")
	})
}

func TestMCPClient(t *testing.T) {
	t.Run("in memory client uses tools and resources", func(t *testing.T) {
		ctx := context.Background()
		svc := newTestService(func(s *Service) {
			s.Search = func(_ context.Context, kbID string, _ []float32, opts *kb.SearchOptions) ([]kb.ExpandedResult, error) {
				require.Equal(t, "kb-1", kbID)
				require.Equal(t, 1, opts.TopK)
				return []kb.ExpandedResult{{ID: "doc-1", Content: "hello", Distance: 0.25}}, nil
			}
		})
		cs := connectInMemoryMCP(t, ctx, New(*svc))
		defer cs.Close()

		tools, err := cs.ListTools(ctx, nil)
		require.NoError(t, err)
		names := toolNames(tools.Tools)
		require.Contains(t, names, "minnow_query")
		require.Contains(t, names, "minnow_ingest_documents_async")
		require.Contains(t, names, "minnow_index_codebase")
		require.Contains(t, names, "minnow_code_search")

		queryRes, err := cs.CallTool(
			ctx,
			&mcp.CallToolParams{
				Name:      "minnow_query",
				Arguments: json.RawMessage(`{"kb_id":"kb-1","query":"hello","k":1}`),
			},
		)
		require.NoError(t, err)
		require.False(t, queryRes.IsError)
		var queryOut queryOutput
		decodeStructured(t, queryRes.StructuredContent, &queryOut)
		require.Len(t, queryOut.Results, 1)
		require.Equal(t, "doc-1", queryOut.Results[0].ID)

		ingestRes, err := cs.CallTool(
			ctx,
			&mcp.CallToolParams{
				Name: "minnow_ingest_documents_async",
				Arguments: json.RawMessage(
					`{"kb_id":"kb-1","documents":[{"id":"doc-2","text":"new"}],"graph_enabled":false}`,
				),
			},
		)
		require.NoError(t, err)
		require.False(t, ingestRes.IsError)

		resource, err := cs.ReadResource(ctx, &mcp.ReadResourceParams{URI: "minnow://operations/evt-1"})
		require.NoError(t, err)
		require.Len(t, resource.Contents, 1)
		require.Contains(t, resource.Contents[0].Text, "evt-terminal")
	})

	t.Run("in memory client calls code search", func(t *testing.T) {
		ctx := context.Background()
		svc := newTestService(func(s *Service) {
			s.SearchCode = func(context.Context, string, string, kb.CodeSearchOptions) ([]kb.CodeSearchResult, error) {
				return []kb.CodeSearchResult{
					{
						ID:        "code-1",
						Content:   "func main() {}",
						Path:      "main.go",
						Language:  "go",
						StartLine: 1,
						EndLine:   1,
					},
				}, nil
			}
		})
		cs := connectInMemoryMCP(t, ctx, New(*svc))
		defer cs.Close()

		res, err := cs.CallTool(
			ctx,
			&mcp.CallToolParams{
				Name:      "minnow_code_search",
				Arguments: json.RawMessage(`{"kb_id":"kb-1","query":"main","k":1,"language":"go"}`),
			},
		)
		require.NoError(t, err)
		require.False(t, res.IsError)
		var out codeSearchOutput
		decodeStructured(t, res.StructuredContent, &out)
		require.Equal(t, "code-1", out.Results[0].ID)
		require.Equal(t, "main.go", out.Results[0].Path)
	})

	t.Run("http transport calls tool", func(t *testing.T) {
		ctx := context.Background()
		svc := newTestService(func(s *Service) {
			s.Config = Config{Enabled: true, HTTPJSONResponse: true, HTTPStateless: true}
			s.Search = func(context.Context, string, []float32, *kb.SearchOptions) ([]kb.ExpandedResult, error) {
				return []kb.ExpandedResult{{ID: "doc-http", Content: "from http", Distance: 0.1}}, nil
			}
		})
		server := New(*svc)
		httpServer := httptest.NewServer(NewHTTPHandler(server, svc.Config))
		defer httpServer.Close()

		client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0.0.1"}, nil)
		cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
		require.NoError(t, err)
		defer cs.Close()

		res, err := cs.CallTool(
			ctx,
			&mcp.CallToolParams{Name: "minnow_query", Arguments: json.RawMessage(`{"query":"hello","k":1}`)},
		)
		require.NoError(t, err)
		require.False(t, res.IsError)
		var out queryOutput
		decodeStructured(t, res.StructuredContent, &out)
		require.Equal(t, "doc-http", out.Results[0].ID)
	})
}

func TestRegisterTools(t *testing.T) {
	cases := []struct {
		name     string
		cfg      Config
		expected []string
		hidden   []string
	}{
		{
			name: "read only",
			cfg:  Config{Enabled: true, ReadOnly: true},
			expected: []string{
				"minnow_query", "minnow_operation_status", "minnow_list_media", "minnow_get_media",
				"minnow_query_vectors",
			},
			hidden: []string{
				"minnow_ingest_documents_async", "minnow_ingest_documents_sync", "minnow_index_codebase",
				"minnow_upsert_vectors",
				"minnow_delete_media", "minnow_delete_knowledge_base",
				"minnow_sweep_cache", "minnow_clear_cache", "minnow_force_compaction", "minnow_install_code_hooks",
			},
		},
		{
			name: "indexing only",
			cfg:  Config{Enabled: true, AllowIndexing: true},
			expected: []string{
				"minnow_query", "minnow_operation_status", "minnow_list_media", "minnow_get_media",
				"minnow_ingest_documents_async", "minnow_upsert_vectors", "minnow_query_vectors",
			},
			hidden: []string{
				"minnow_ingest_documents_sync",
				"minnow_delete_media", "minnow_delete_knowledge_base",
				"minnow_sweep_cache", "minnow_clear_cache", "minnow_force_compaction",
			},
		},
		{
			name: "indexing and sync",
			cfg:  Config{Enabled: true, AllowIndexing: true, AllowSyncIndexing: true},
			expected: []string{
				"minnow_ingest_documents_async", "minnow_ingest_documents_sync",
			},
		},
		{
			name: "code tools require implementations",
			cfg:  Config{Enabled: true, AllowIndexing: true},
			hidden: []string{
				"minnow_index_codebase", "minnow_code_index_status", "minnow_code_search",
			},
		},

		{
			name: "destructive",
			cfg:  Config{Enabled: true, AllowDestructive: true},
			expected: []string{
				"minnow_delete_media", "minnow_delete_knowledge_base",
			},
			hidden: []string{"minnow_clear_cache"},
		},
		{
			name: "admin alone",
			cfg:  Config{Enabled: true, AllowAdmin: true},
			expected: []string{
				"minnow_sweep_cache", "minnow_force_compaction", "minnow_code_hooks_status", "minnow_install_code_hooks", "minnow_uninstall_code_hooks",
			},
			hidden: []string{"minnow_clear_cache"},
		},
		{
			name: "admin and destructive unlocks clear cache",
			cfg:  Config{Enabled: true, AllowAdmin: true, AllowDestructive: true},
			expected: []string{
				"minnow_sweep_cache", "minnow_force_compaction", "minnow_clear_cache",
				"minnow_delete_media", "minnow_delete_knowledge_base", "minnow_code_hooks_status", "minnow_install_code_hooks", "minnow_uninstall_code_hooks",
			},
		},
		{
			name: "all tools register without panic",
			cfg: Config{
				Enabled:           true,
				AllowIndexing:     true,
				AllowSyncIndexing: true,
				AllowAdmin:        true,
				AllowDestructive:  true,
			},
			expected: []string{
				"minnow_query", "minnow_ingest_documents_async", "minnow_ingest_documents_sync",
				"minnow_upsert_vectors", "minnow_query_vectors",
				"minnow_delete_media", "minnow_delete_knowledge_base",
				"minnow_sweep_cache", "minnow_force_compaction", "minnow_clear_cache", "minnow_code_hooks_status",
			},
		},
	}
	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assertRegisteredTools(t, ctx, tc.name, tc.cfg, tc.expected, tc.hidden) })
	}
}

func assertRegisteredTools(t *testing.T, ctx context.Context, name string, cfg Config, expected, hidden []string) {
	t.Helper()
	cs := connectInMemoryMCP(t, ctx, New(Service{Config: cfg}))
	defer cs.Close()
	tools, err := cs.ListTools(ctx, nil)
	require.NoError(t, err)
	names := toolNames(tools.Tools)
	for _, want := range expected {
		require.Contains(t, names, want, "expected %q to be registered for cfg %s", want, name)
	}
	for _, hiddenTool := range hidden {
		require.NotContains(t, names, hiddenTool, "expected %q to be hidden for cfg %s", hiddenTool, name)
	}
}

func TestQueryInputSchema(t *testing.T) {
	t.Run("advertises k bounds", func(t *testing.T) {
		schema, err := queryInputSchema()
		require.NoError(t, err)
		require.NotNil(t, schema.Properties["k"], "k property must exist on schema")
		require.NotNil(t, schema.Properties["k"].Minimum, "k must advertise a minimum")
		require.NotNil(t, schema.Properties["k"].Maximum, "k must advertise a maximum")
		require.Equal(t, float64(1), *schema.Properties["k"].Minimum)
		require.Equal(t, float64(queryToolMaxK), *schema.Properties["k"].Maximum)
	})
}

// newTestService returns a Service with happy-path closures. callers override
// what differs through the modify callback.
func newTestService(modify func(*Service)) *Service {
	svc := &Service{
		Config: Config{Enabled: true, AllowIndexing: true},
		Embed:  func(context.Context, string) ([]float32, error) { return []float32{0.1, 0.2}, nil },
		Search: func(context.Context, string, []float32, *kb.SearchOptions) ([]kb.ExpandedResult, error) {
			return []kb.ExpandedResult{{ID: "doc-1", Content: "hello", Distance: 0.25}}, nil
		},
		AppendDocumentUpsert: func(context.Context, kb.DocumentUpsertPayload, string, string) (string, string, error) {
			return "evt-1", "idem-1", nil
		},
		GetEvent: func(context.Context, string) (*kb.KBEvent, error) {
			return &kb.KBEvent{
				EventID: "evt-1",
				KBID:    "kb-1",
				Kind:    kb.EventDocumentUpsert,
				Status:  kb.EventStatusDone,
			}, nil
		},
		FindOperationTerminal: func(context.Context, string) (*kb.KBEvent, error) {
			return &kb.KBEvent{
				EventID: "evt-terminal",
				KBID:    "kb-1",
				Kind:    kb.EventKBPublished,
				Status:  kb.EventStatusDone,
			}, nil
		},
		DeleteKnowledgeBase: func(context.Context, string) error { return nil },
		IndexCodebase: func(_ context.Context, opts kb.CodeIndexOptions) (kb.CodeIndexResult, error) {
			return kb.CodeIndexResult{KBID: opts.KBID, Root: opts.Root}, nil
		},
		CodeIndexStatus: func(_ context.Context, kbID string) (kb.CodeIndexStatus, error) {
			return kb.CodeIndexStatus{KBID: kbID, Indexed: true}, nil
		},
		SearchCode: func(context.Context, string, string, kb.CodeSearchOptions) ([]kb.CodeSearchResult, error) {
			return []kb.CodeSearchResult{
				{ID: "code-1", Content: "func main() {}", Path: "main.go", Language: "go"},
			}, nil
		},
	}
	if modify != nil {
		modify(svc)
	}
	return svc
}

func withSyncTimeout(s *Service, d time.Duration) {
	s.Config.AllowSyncIndexing = true
	s.Config.DefaultSyncTimeout = d
	s.Config.MaxSyncTimeout = d
}

func oneDoc() []ingestDocInput {
	return []ingestDocInput{{ID: "doc-1", Text: "hello"}}
}

func connectInMemoryMCP(t *testing.T, ctx context.Context, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { serverSession.Close() })
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0.0.1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	return clientSession
}

func toolNames(tools []*mcp.Tool) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
	}
	return names
}

func decodeStructured(t *testing.T, structured any, out any) {
	t.Helper()
	data, err := json.Marshal(structured)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, out))
}
