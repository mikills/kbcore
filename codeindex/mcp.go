package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type mcpService struct {
	cfg      Config
	root     string
	kbID     string
	indexKey string
}

type mcpCLIOptions struct {
	configPath string
	minnowURL  string
	token      string
	root       string
	kbID       string
	indexKey   string
}

type mcpSearchInput struct {
	Query    string `json:"query" jsonschema:"Natural language code query."`
	K        int    `json:"k,omitempty" jsonschema:"Number of results. Defaults to 10, maximum 200."`
	Path     string `json:"path,omitempty" jsonschema:"Optional case-sensitive path substring."`
	Language string `json:"language,omitempty" jsonschema:"Optional code language."`
}

type mcpStatusInput struct{}

func runMCP(ctx context.Context, args []string) int {
	opts, err := parseMCPCLIOptions(args)
	if err != nil {
		return writeCommandError(err, 2)
	}
	cfg, err := loadConfig(opts.configPath)
	if err != nil {
		return writeCommandError(err, 1)
	}
	if strings.TrimSpace(opts.minnowURL) != "" {
		cfg.Minnow.URL = strings.TrimSpace(opts.minnowURL)
	}
	if opts.token != "" {
		cfg.Minnow.Token = opts.token
	}
	service := &mcpService{
		cfg: cfg, root: opts.root, kbID: opts.kbID, indexKey: opts.indexKey,
	}
	server := newMCPServer(service)
	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil {
		return writeCommandError(fmt.Errorf("serve codeindex MCP: %w", err), 1)
	}
	return 0
}

func parseMCPCLIOptions(args []string) (mcpCLIOptions, error) {
	opts := mcpCLIOptions{
		configPath: os.Getenv("CODEINDEX_CONFIG"),
		minnowURL:  os.Getenv("CODEINDEX_MINNOW_URL"),
		token:      os.Getenv("CODEINDEX_TOKEN"),
		root:       firstNonEmpty(os.Getenv("CODEINDEX_REPO_ROOT"), os.Getenv("MINNOW_REPO_ROOT"), "."),
	}
	fs := flag.NewFlagSet("codeindex mcp", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.configPath, "config", opts.configPath, "codeindex config path")
	fs.StringVar(&opts.minnowURL, "minnow-url", opts.minnowURL, "Minnow HTTP base URL")
	fs.StringVar(&opts.token, "token", opts.token, "Minnow bearer token")
	fs.StringVar(&opts.root, "root", opts.root, "repository or directory root")
	fs.StringVar(&opts.kbID, "kb", "", "knowledge base identity override")
	fs.StringVar(&opts.indexKey, "index-key", "", "index identity override")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if fs.NArg() != 0 {
		return opts, fmt.Errorf("unexpected argument: %s", fs.Arg(0))
	}
	if strings.TrimSpace(opts.root) == "" {
		return opts, fmt.Errorf("--root requires a value")
	}
	return opts, nil
}

func newMCPServer(service *mcpService) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "codeindex", Version: versionString()}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name: "codeindex_search", Description: "Search the indexed code for the current repository and Git branch.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, service.search)
	mcp.AddTool(server, &mcp.Tool{
		Name: "codeindex_status", Description: "Read local indexing status for the current repository and Git branch.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, service.status)
	return server
}

func (s *mcpService) search(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in mcpSearchInput,
) (*mcp.CallToolResult, codeSearchResponse, error) {
	in.Query = strings.TrimSpace(in.Query)
	if in.Query == "" {
		return nil, codeSearchResponse{}, fmt.Errorf("query is required")
	}
	if in.K <= 0 {
		in.K = 10
	}
	if in.K > 200 {
		return nil, codeSearchResponse{}, fmt.Errorf("k must be <= 200")
	}
	target, state, checkpoint, err := s.selection()
	if err != nil {
		return nil, codeSearchResponse{}, err
	}
	client, err := newMinnowClient(s.cfg)
	if err != nil {
		return nil, codeSearchResponse{}, err
	}
	if checkpoint != nil && checkpoint.Phase != runPhaseFinalized {
		if sessionID := loadSession(target); sessionID != "" {
			pending, pendingErr := client.sessionPending(ctx, state.KBID, sessionID)
			if pendingErr != nil {
				return nil, codeSearchResponse{}, pendingErr
			}
			if pending {
				return nil, codeSearchResponse{}, fmt.Errorf("code index finalization is still pending")
			}
		}
		if err := client.check(ctx); err != nil {
			return nil, codeSearchResponse{}, err
		}
		if err := client.refreshScope(ctx, state.KBID, state.ScopeID); err != nil {
			return nil, codeSearchResponse{}, err
		}
		if !client.scopeMatches(stateChunkIDs(state)) {
			return nil, codeSearchResponse{}, fmt.Errorf("code index finalization is still pending")
		}
	}
	response, err := client.searchCode(ctx, codeSearchRequest{
		KBID: state.KBID, ScopeID: state.ScopeID, Query: in.Query, K: in.K,
		Path: in.Path, Language: in.Language,
	})
	if err != nil {
		return nil, codeSearchResponse{}, fmt.Errorf("search %s on %s: %w", target.IndexKey, target.Ref, err)
	}
	return nil, response, nil
}

func (s *mcpService) status(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in mcpStatusInput,
) (*mcp.CallToolResult, indexStatus, error) {
	target, state, checkpoint, err := s.selection()
	if err != nil {
		return nil, indexStatus{}, err
	}
	status := statusFromState(target, s.cfg.Minnow.URL, indexStatePath(target), state)
	if checkpoint != nil {
		status.Phase = string(checkpoint.Phase)
		status.Recoverable = true
		status.Indexed = checkpoint.Phase == runPhaseFinalized
		if status.Indexed {
			return nil, status, nil
		}
		if sessionID := loadSession(target); sessionID != "" {
			client, clientErr := newMinnowClient(s.cfg)
			if clientErr != nil {
				return nil, status, nil
			}
			pending, pendingErr := client.sessionPending(ctx, state.KBID, sessionID)
			if pendingErr != nil || pending {
				return nil, status, nil
			}
		}
		client, clientErr := newMinnowClient(s.cfg)
		if clientErr != nil {
			return nil, status, nil
		}
		if clientErr = client.check(ctx); clientErr != nil {
			return nil, status, nil
		}
		if clientErr = client.refreshScope(ctx, state.KBID, state.ScopeID); clientErr != nil {
			return nil, status, nil
		}
		if client.scopeMatches(stateChunkIDs(state)) {
			status.Indexed = true
			status.Phase = string(runPhaseFinalized)
		}
	}
	return nil, status, nil
}

func (s *mcpService) selection() (indexTarget, indexState, *runCheckpoint, error) {
	target, err := resolveTarget(indexCLIOptions{root: s.root, kbID: s.kbID, indexKey: s.indexKey})
	if err != nil {
		return indexTarget{}, indexState{}, nil, fmt.Errorf("resolve repository: %w", err)
	}
	state, _, exists, err := loadIndexState(target)
	if err != nil {
		return indexTarget{}, indexState{}, nil, err
	}
	checkpoint, pending, err := loadRunCheckpoint(target)
	if err != nil {
		return indexTarget{}, indexState{}, nil, err
	}
	if pending {
		target.KBID = checkpoint.State.KBID
		target.ScopeID = checkpoint.State.ScopeID
		return target, checkpoint.State, &checkpoint, nil
	}
	if exists {
		target.KBID = state.KBID
		target.ScopeID = state.ScopeID
		return target, state, nil, nil
	}
	return indexTarget{}, indexState{}, nil, fmt.Errorf("current branch has not been indexed")
}
