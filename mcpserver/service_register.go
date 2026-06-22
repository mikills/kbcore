package mcpserver

import (
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// queryInputSchema returns the queryInput JSONSchema with explicit bounds
// on K. Agents reading `tools/list` see the constraint up front instead of
// only learning of it via a runtime error. Handler still validates K > 0 as
// defense-in-depth.
func queryInputSchema() (*jsonschema.Schema, error) {
	schema, err := jsonschema.For[queryInput](nil)
	if err != nil {
		return nil, fmt.Errorf("build queryInput schema: %w", err)
	}
	if k, ok := schema.Properties["k"]; ok {
		min := float64(1)
		max := float64(queryToolMaxK)
		k.Minimum = &min
		k.Maximum = &max
	}
	return schema, nil
}

func queryToolSchemaOrDefault() *jsonschema.Schema {
	schema, err := queryInputSchema()
	if err == nil {
		return schema
	}
	return &jsonschema.Schema{}
}

func New(s Service) *mcp.Server {
	cfg := s.Config.normalized()
	s.Config = cfg
	server := mcp.NewServer(&mcp.Implementation{Name: "minnow", Version: "v0.1.0"}, &mcp.ServerOptions{
		Instructions: "Minnow MCP exposes retrieval, indexing, operation status, and explicitly gated maintenance tools for coding agents.",
		Logger:       s.Logger,
	})
	registerTools(server, &s)
	registerResources(server, &s)
	return server
}

// queryToolMaxK caps the K value the schema advertises so agents do not
// request unboundedly large result sets. Handler still enforces > 0.
const queryToolMaxK = 200

// registerTools registers only the tools permitted by s.Config so that
// `tools/list` reflects what the agent can actually call. Per-handler gates
// remain as defense-in-depth, so a misconfigured Service still rejects
// disallowed calls instead of silently executing them.
func registerTools(server *mcp.Server, s *Service) {
	cfg := s.Config

	queryTool := &mcp.Tool{
		Name:        "minnow_query",
		Description: "Query a Minnow knowledge base using vector, graph, or adaptive search.",
		InputSchema: queryToolSchemaOrDefault(),
	}
	mcp.AddTool(server, queryTool, s.query)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "minnow_query_vectors",
		Description: "Query a Minnow knowledge base with a raw vector, returning IDs, distances, and metadata.",
	}, s.queryVectors)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "minnow_fetch_vectors",
		Description: "Fetch metadata for specific vector IDs without performing a similarity search.",
	}, s.fetchVectors)
	registerVectorIndexingTools(server, s, cfg)
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "minnow_operation_status",
			Description: "Read the status, stages, and terminal event for an async Minnow operation.",
		},
		s.operationStatus,
	)
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "minnow_list_media",
			Description: "List media metadata for a knowledge base when media is configured.",
		},
		s.listMedia,
	)
	mcp.AddTool(
		server,
		&mcp.Tool{Name: "minnow_get_media", Description: "Get media metadata by media ID when media is configured."},
		s.getMedia,
	)
	registerCodeTools(server, s)
	registerIndexingTools(server, s, cfg)
	registerDestructiveTools(server, s, cfg)
	registerAdminTools(server, s, cfg)
}

func registerCodeTools(server *mcp.Server, s *Service) {
	if s.CodeIndexStatus != nil {
		mcp.AddTool(
			server,
			&mcp.Tool{
				Name:        "minnow_code_index_status",
				Description: "Read codebase indexing status for a Minnow knowledge base.",
			},
			s.codeIndexStatus,
		)
	}
	if s.SearchCode != nil {
		mcp.AddTool(
			server,
			&mcp.Tool{
				Name:        "minnow_code_search",
				Description: "Search indexed code chunks with optional path and language filters.",
			},
			s.codeSearch,
		)
	}
}

func registerVectorIndexingTools(server *mcp.Server, s *Service, cfg Config) {
	if cfg.ReadOnly || !cfg.AllowIndexing {
		return
	}
	mcp.AddTool(server, &mcp.Tool{
		Name:        "minnow_upsert_vectors",
		Description: "Upsert pre-computed vectors directly into a Minnow knowledge base, bypassing the embedding pipeline.",
	}, s.upsertVectors)
}

func registerIndexingTools(server *mcp.Server, s *Service, cfg Config) {
	if cfg.ReadOnly || !cfg.AllowIndexing {
		return
	}
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "minnow_ingest_documents_async",
			Description: "Submit text documents for asynchronous indexing. Returns an operation ID.",
		},
		s.ingestAsync,
	)
	if s.IndexCodebase != nil {
		mcp.AddTool(
			server,
			&mcp.Tool{
				Name:        "minnow_index_codebase",
				Description: "Index or refresh a local codebase incrementally for a Minnow knowledge base.",
			},
			s.indexCodebase,
		)
	}
	if cfg.AllowSyncIndexing {
		mcp.AddTool(
			server,
			&mcp.Tool{
				Name:        "minnow_ingest_documents_sync",
				Description: "Submit text documents for indexing and wait for publish up to a bounded timeout.",
			},
			s.ingestSync,
		)
	}
}

func registerDestructiveTools(server *mcp.Server, s *Service, cfg Config) {
	if cfg.ReadOnly || !cfg.AllowDestructive {
		return
	}
	mcp.AddTool(
		server,
		&mcp.Tool{Name: "minnow_delete_media", Description: "Destructive tool: tombstone media by media ID."},
		s.deleteMedia,
	)
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "minnow_delete_knowledge_base",
			Description: "Destructive tool: delete a knowledge base manifest, shards, cache, and media metadata.",
		},
		s.deleteKnowledgeBase,
	)
}

func registerAdminTools(server *mcp.Server, s *Service, cfg Config) {
	if !cfg.AllowAdmin {
		return
	}
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "minnow_code_hooks_status",
			Description: "Admin tool: inspect Minnow git hook installation status for a repository.",
		},
		s.codeHooksStatus,
	)
	if !cfg.ReadOnly {
		mcp.AddTool(
			server,
			&mcp.Tool{
				Name:        "minnow_install_code_hooks",
				Description: "Admin tool: install optional Minnow git hooks for incremental codebase indexing.",
			},
			s.installCodeHooks,
		)
		mcp.AddTool(
			server,
			&mcp.Tool{
				Name:        "minnow_uninstall_code_hooks",
				Description: "Admin tool: uninstall Minnow git hooks from a repository.",
			},
			s.uninstallCodeHooks,
		)
	}
	mcp.AddTool(
		server,
		&mcp.Tool{Name: "minnow_sweep_cache", Description: "Admin tool: run policy-based cache eviction."},
		s.sweepCache,
	)
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "minnow_force_compaction",
			Description: "Admin tool: trigger compaction for a knowledge base when compaction debt exists.",
		},
		s.forceCompaction,
	)
	if !cfg.ReadOnly && cfg.AllowDestructive {
		mcp.AddTool(
			server,
			&mcp.Tool{
				Name:        "minnow_clear_cache",
				Description: "Admin destructive tool: clear all local cache entries.",
			},
			s.clearCache,
		)
	}
}
