package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	minnowcode "github.com/mikills/minnow/codeindex/indexer"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "index" {
		args = args[1:]
	}
	os.Exit(run(context.Background(), args))
}

func run(ctx context.Context, args []string) int {
	if len(args) == 0 {
		printUsage()
		return 2
	}
	switch args[0] {
	case "setup":
		return runSetup(args[1:])
	case "codebase", "refresh":
		return runRefresh(ctx, args[1:])
	case "status":
		return runStatus(args[1:])
	case "hooks":
		return runHooks(ctx, args[1:])
	case "-h", "--help":
		printUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown codeindex command: %s\n", args[0])
		return 2
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: codeindex <setup|codebase|refresh|status|hooks>")
}

type indexCLIOptions struct {
	configPath       string
	minnowURL        string
	token            string
	kbID             string
	indexKey         string
	description      string
	root             string
	binary           string
	includeUntracked bool
	quiet            bool
	force            bool
	yes              bool
	lowResource      bool
	requestBatchSize int
	maxBatchBytes    int
	maxHeapBytes     uint64
	maxRSSBytes      uint64
	largeRepoFiles   int
	throttle         time.Duration
}

func parseIndexCLIOptions(args []string) (indexCLIOptions, error) {
	root := firstNonEmpty(os.Getenv("CODEINDEX_REPO_ROOT"), os.Getenv("MINNOW_REPO_ROOT"), ".")
	opts := indexCLIOptions{
		configPath: os.Getenv("CODEINDEX_CONFIG"),
		minnowURL:  os.Getenv("CODEINDEX_MINNOW_URL"),
		token:      os.Getenv("CODEINDEX_TOKEN"),
		root:       root,
	}
	fs := flag.NewFlagSet("codeindex", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.configPath, "config", opts.configPath, "codeindex config path")
	fs.StringVar(&opts.minnowURL, "minnow-url", opts.minnowURL, "Minnow HTTP base URL")
	fs.StringVar(&opts.token, "token", opts.token, "Minnow bearer token")
	fs.StringVar(&opts.kbID, "kb", opts.kbID, "knowledge base id override")
	fs.StringVar(&opts.indexKey, "index-key", opts.indexKey, "index key override")
	fs.StringVar(&opts.description, "description", opts.description, "index description")
	fs.StringVar(&opts.root, "root", opts.root, "repository or directory root")
	fs.BoolVar(&opts.includeUntracked, "include-untracked", false, "include untracked Git files")
	fs.StringVar(&opts.binary, "binary", opts.binary, "codeindex binary path for hooks")
	fs.BoolVar(&opts.quiet, "quiet", false, "suppress JSON output")
	fs.BoolVar(&opts.force, "force", false, "force hook installation or confirm a large index")
	fs.BoolVar(&opts.yes, "yes", false, "confirm a large index")
	fs.BoolVar(&opts.yes, "y", false, "confirm a large index")
	fs.BoolVar(&opts.lowResource, "low-resource", false, "use conservative request batching")
	fs.IntVar(&opts.requestBatchSize, "batch-size", 0, "chunks per Minnow request")
	fs.IntVar(&opts.maxBatchBytes, "max-batch-bytes", 0, "text bytes per Minnow request")
	fs.Uint64Var(&opts.maxHeapBytes, "max-heap-bytes", 0, "maximum Go memory footprint in bytes")
	fs.Uint64Var(&opts.maxRSSBytes, "max-rss-bytes", 0, "maximum resident set bytes")
	fs.IntVar(&opts.largeRepoFiles, "large-repo-files", 0, "large repository confirmation threshold")
	fs.DurationVar(&opts.throttle, "throttle", 0, "delay between Minnow requests")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if fs.NArg() > 0 {
		return opts, fmt.Errorf("unexpected argument: %s", fs.Arg(0))
	}
	if strings.TrimSpace(opts.root) == "" {
		return opts, fmt.Errorf("--root requires a value")
	}
	if opts.requestBatchSize < 0 || opts.maxBatchBytes < 0 || opts.largeRepoFiles < 0 {
		return opts, fmt.Errorf("numeric index flags must be non-negative")
	}
	if opts.throttle < 0 {
		return opts, fmt.Errorf("--throttle must be a non-negative duration")
	}
	return opts, nil
}

func runRefresh(ctx context.Context, args []string) int {
	opts, err := parseIndexCLIOptions(args)
	if err != nil {
		return writeCommandError(err, 2)
	}
	cfg, err := loadConfig(opts.configPath)
	if err != nil {
		return writeCommandError(err, 1)
	}
	applyConnectionOverrides(&cfg, opts)
	result, err := refreshIndex(ctx, cfg, opts)
	if err != nil {
		return writeCommandError(fmt.Errorf("index codebase: %w", err), 1)
	}
	if !opts.quiet {
		if err := writeJSON(result); err != nil {
			return writeCommandError(fmt.Errorf("write json: %w", err), 1)
		}
	}
	return 0
}

func runStatus(args []string) int {
	opts, err := parseIndexCLIOptions(args)
	if err != nil {
		return writeCommandError(err, 2)
	}
	target, err := resolveTarget(opts)
	if err != nil {
		return writeCommandError(fmt.Errorf("resolve index: %w", err), 1)
	}
	state, path, stateExists, err := loadIndexState(target)
	if err != nil {
		return writeCommandError(fmt.Errorf("load index state: %w", err), 1)
	}
	if stateExists {
		target.KBID = state.KBID
	} else {
		target.KBID = ""
	}
	status := statusFromState(target, strings.TrimSpace(opts.minnowURL), path, state)
	if err := writeJSON(status); err != nil {
		return writeCommandError(fmt.Errorf("write json: %w", err), 1)
	}
	return 0
}

func runHooks(ctx context.Context, args []string) int {
	if len(args) == 0 {
		return writeCommandError(fmt.Errorf("usage: codeindex hooks <install|uninstall|status>"), 2)
	}
	action := args[0]
	opts, err := parseIndexCLIOptions(args[1:])
	if err != nil {
		return writeCommandError(err, 2)
	}
	var status any
	switch action {
	case "install":
		configPath, configErr := resolvedConfigPath(opts.configPath)
		if configErr != nil {
			return writeCommandError(configErr, 1)
		}
		status, err = minnowcode.InstallCodeIndexHooks(ctx, minnowcode.CodeHookOptions{
			Root: opts.root, KBID: opts.kbID, IndexKey: opts.indexKey, Binary: opts.binary,
			Config: configPath, Force: opts.force,
		})
	case "uninstall":
		status, err = minnowcode.UninstallCodeIndexHooks(ctx, opts.root)
	case "status":
		status, err = minnowcode.CodeIndexHookStatus(ctx, opts.root)
	default:
		return writeCommandError(fmt.Errorf("unknown hooks command: %s", action), 2)
	}
	if err != nil {
		return writeCommandError(fmt.Errorf("codeindex hooks %s: %w", action, err), 1)
	}
	if err := writeJSON(status); err != nil {
		return writeCommandError(fmt.Errorf("write json: %w", err), 1)
	}
	return 0
}

func applyConnectionOverrides(cfg *Config, opts indexCLIOptions) {
	if strings.TrimSpace(opts.minnowURL) != "" {
		cfg.Minnow.URL = strings.TrimSpace(opts.minnowURL)
	}
	if opts.token != "" {
		cfg.Minnow.Token = opts.token
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func writeCommandError(err error, code int) int {
	fmt.Fprintln(os.Stderr, err)
	return code
}

func writeJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
