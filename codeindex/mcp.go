package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type mcpService struct {
	cfg      Config
	root     string
	kbID     string
	indexKey string
	startErr error
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
	// A usage or flag error means the command line is wrong, which the person
	// running it can see. Only a config failure needs to reach the client.
	opts, err := parseMCPCLIOptions(args)
	if errors.Is(err, flag.ErrHelp) {
		return writeMCPUsage()
	}
	if err != nil {
		return writeCommandError(err, 2)
	}
	service := newMCPService(opts)
	if err := newMCPServer(service).Run(ctx, &mcp.StdioTransport{}); err != nil {
		return writeCommandError(fmt.Errorf("serve codeindex MCP: %w", err), 1)
	}
	if service.startErr != nil {
		return 1
	}
	return 0
}

// newMCPService keeps a config failure instead of exiting on it. Clients do not
// show a server's stderr, so exiting here surfaces only "connection closed"
// during the handshake, with nothing naming the cause.
func newMCPService(opts mcpCLIOptions) *mcpService {
	cfg, err := loadConfig(opts.configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return &mcpService{startErr: err}
	}
	if strings.TrimSpace(opts.minnowURL) != "" {
		cfg.Minnow.URL = strings.TrimSpace(opts.minnowURL)
	}
	if opts.token != "" {
		cfg.Minnow.Token = opts.token
	}
	return &mcpService{cfg: cfg, root: opts.root, kbID: opts.kbID, indexKey: opts.indexKey}
}

func (s *mcpService) ready() error {
	if s.startErr == nil {
		return nil
	}
	if !errors.Is(s.startErr, errMissingConfigEnv) {
		return fmt.Errorf("codeindex mcp did not start: %w", s.startErr)
	}
	return fmt.Errorf(
		"codeindex mcp did not start: %w\n"+
			"An MCP server does not inherit your shell environment. Forward the "+
			"variable in the client's server entry: Codex names it in env_vars in "+
			"~/.codex/config.toml, Claude Code takes -e NAME=value, OpenCode uses "+
			"an environment map.",
		s.startErr,
	)
}

func writeMCPUsage() int {
	opts := defaultMCPCLIOptions()
	// PrintDefaults would otherwise put a credential into terminal scrollback,
	// CI logs, and pasted bug reports.
	opts.token = ""
	opts.minnowURL = withoutURLCredentials(opts.minnowURL)
	fs := newMCPFlagSet(&opts)
	fs.SetOutput(os.Stderr)
	fmt.Fprintln(os.Stderr, "Usage: codeindex mcp [flags]")
	fs.PrintDefaults()
	return 2
}

// withoutURLCredentials strips userinfo, which net/http would otherwise send as
// a Basic-auth header, making it as much a secret as the bearer token.
func withoutURLCredentials(raw string) string {
	trimmed := strings.TrimSpace(raw)
	parsed, err := url.Parse(trimmed)
	if err != nil {
		// Nothing can be located inside a URL that will not parse, so a
		// userinfo separator has to be assumed to hide a credential.
		if strings.Contains(trimmed, "@") {
			return "(redacted)"
		}
		return raw
	}
	if parsed.User == nil {
		return raw
	}
	parsed.User = nil
	return parsed.String()
}

func defaultMCPCLIOptions() mcpCLIOptions {
	return mcpCLIOptions{
		configPath: os.Getenv("CODEINDEX_CONFIG"),
		minnowURL:  os.Getenv("CODEINDEX_MINNOW_URL"),
		token:      os.Getenv("CODEINDEX_TOKEN"),
		root:       firstNonEmpty(os.Getenv("CODEINDEX_REPO_ROOT"), os.Getenv("MINNOW_REPO_ROOT"), "."),
	}
}

func parseMCPCLIOptions(args []string) (mcpCLIOptions, error) {
	opts := defaultMCPCLIOptions()
	fs := newMCPFlagSet(&opts)
	fs.SetOutput(io.Discard)
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

func newMCPFlagSet(opts *mcpCLIOptions) *flag.FlagSet {
	fs := flag.NewFlagSet("codeindex mcp", flag.ContinueOnError)
	fs.StringVar(&opts.configPath, "config", opts.configPath, "codeindex config path")
	fs.StringVar(&opts.minnowURL, "minnow-url", opts.minnowURL, "Minnow HTTP base URL")
	fs.StringVar(&opts.token, "token", opts.token, "Minnow bearer token")
	fs.StringVar(&opts.root, "root", opts.root, "repository or directory root")
	fs.StringVar(&opts.kbID, "kb", "", "knowledge base identity override")
	fs.StringVar(&opts.indexKey, "index-key", "", "index identity override")
	return fs
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
	if err := s.ready(); err != nil {
		return nil, codeSearchResponse{}, err
	}
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
	if err := s.ready(); err != nil {
		return nil, indexStatus{}, err
	}
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
