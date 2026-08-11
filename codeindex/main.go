package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/mikills/minnow/cmd/configruntime"
	"github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/config"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "index" {
		args = args[1:]
	}
	code := runIndexSubcommand(context.Background(), args, logger)
	os.Exit(code)
}

func runIndexSubcommand(ctx context.Context, args []string, logger *slog.Logger) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: codeindex <codebase|refresh|status|hooks>")
		return 2
	}
	switch args[0] {
	case "codebase", "refresh":
		return runIndexRefresh(ctx, args[1:], logger)
	case "status":
		return runIndexStatus(ctx, args[1:], logger)
	case "hooks":
		return runIndexHooks(ctx, args[1:])
	case "-h", "--help":
		fmt.Fprintln(os.Stderr, "usage: codeindex <codebase|refresh|status|hooks>")
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown codeindex subcommand: %s\n", args[0])
		return 2
	}
}

type indexCLIOptions struct {
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
	embedBatchSize   int
	maxBatchBytes    int
	maxHeapBytes     uint64
	maxRSSBytes      uint64
	largeRepoFiles   int
	throttle         time.Duration
}

func parseIndexCLIOptions(args []string) (indexCLIOptions, error) {
	root := os.Getenv("CODEINDEX_REPO_ROOT")
	if root == "" {
		root = os.Getenv("MINNOW_REPO_ROOT")
	}
	opts := indexCLIOptions{indexKey: "default", root: root}
	if opts.root == "" {
		opts.root = "."
	}

	fs := flag.NewFlagSet("codeindex", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.kbID, "kb", opts.kbID, "knowledge base id")
	fs.StringVar(&opts.indexKey, "index-key", opts.indexKey, "code index registry key")
	fs.StringVar(&opts.description, "description", opts.description, "code index description")
	fs.StringVar(&opts.root, "root", opts.root, "repository root")
	fs.BoolVar(&opts.includeUntracked, "include-untracked", opts.includeUntracked, "include untracked git files")
	fs.StringVar(&opts.binary, "binary", opts.binary, "codeindex binary path for hooks")
	fs.BoolVar(&opts.quiet, "quiet", opts.quiet, "suppress JSON output")
	fs.BoolVar(&opts.force, "force", opts.force, "force operation or confirm large indexes")
	fs.BoolVar(&opts.yes, "yes", opts.yes, "confirm prompts")
	fs.BoolVar(&opts.yes, "y", opts.yes, "confirm prompts")
	fs.BoolVar(&opts.lowResource, "low-resource", opts.lowResource, "use conservative indexing resource defaults")
	fs.IntVar(&opts.embedBatchSize, "batch-size", opts.embedBatchSize, "embedding batch size")
	fs.IntVar(&opts.maxBatchBytes, "max-batch-bytes", opts.maxBatchBytes, "maximum text bytes per embedding batch")
	fs.Uint64Var(&opts.maxHeapBytes, "max-heap-bytes", opts.maxHeapBytes, "maximum Go heap/system bytes")
	fs.Uint64Var(&opts.maxRSSBytes, "max-rss-bytes", opts.maxRSSBytes, "maximum resident set bytes")
	fs.IntVar(&opts.largeRepoFiles, "large-repo-files", opts.largeRepoFiles, "large repository confirmation threshold")
	fs.DurationVar(&opts.throttle, "throttle", opts.throttle, "delay between embedding batches")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if fs.NArg() > 0 {
		return opts, fmt.Errorf("unexpected argument: %s", fs.Arg(0))
	}
	return opts, validateIndexCLIOptions(opts)
}

func validateIndexCLIOptions(opts indexCLIOptions) error {
	return firstCLIValidationErr(
		validateOptionalCLIValue("--kb", opts.kbID),
		validateRequiredCLIValue("--index-key", opts.indexKey),
		validateRequiredCLIValue("--root", opts.root),
		validateOptionalCLIValue("--binary", opts.binary),
		validateNonNegativeIndexNumbers(opts),
		validateNonNegativeDuration("--throttle", opts.throttle),
	)
}

func firstCLIValidationErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func validateOptionalCLIValue(name string, value string) error {
	if value != "" && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s requires a value", name)
	}
	return nil
}

func validateRequiredCLIValue(name string, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s requires a value", name)
	}
	return nil
}

func validateNonNegativeIndexNumbers(opts indexCLIOptions) error {
	if opts.embedBatchSize < 0 || opts.maxBatchBytes < 0 || opts.largeRepoFiles < 0 {
		return fmt.Errorf("numeric index flags must be non-negative")
	}
	return nil
}

func validateNonNegativeDuration(name string, value time.Duration) error {
	if value < 0 {
		return fmt.Errorf("%s must be a non-negative duration", name)
	}
	return nil
}

func buildRuntimeForCLI(ctx context.Context, logger *slog.Logger) (*config.Config, *configruntime.Runtime, error) {
	cfg, err := config.Load(os.Getenv("MINNOW_CONFIG"))
	if err != nil {
		return nil, nil, fmt.Errorf("load config: %w", err)
	}
	rt, err := configruntime.Build(ctx, cfg, configruntime.BuildOptions{Logger: logger})
	if err != nil {
		return nil, nil, fmt.Errorf("build runtime: %w", err)
	}
	if err := rt.StartBackground(ctx); err != nil {
		return nil, nil, fmt.Errorf("start runtime: %w", err)
	}
	return cfg, rt, nil
}

func runIndexRefresh(ctx context.Context, args []string, logger *slog.Logger) int {
	opts, err := parseIndexCLIOptions(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}
	cfg, rt, err := buildRuntimeForCLI(ctx, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	defer stopRuntimeForCLI(ctx, logger, rt)

	result, err := rt.KB().IndexCodebase(ctx, codeIndexOptionsForCLI(cfg, opts))
	if err != nil {
		fmt.Fprintf(os.Stderr, "index codebase: %v\n", err)
		return 1
	}
	if !opts.quiet {
		if err := writeJSON(result); err != nil {
			fmt.Fprintf(os.Stderr, "write json: %v\n", err)
			return 1
		}
	}
	return 0
}

func stopRuntimeForCLI(ctx context.Context, logger *slog.Logger, rt *configruntime.Runtime) {
	if err := rt.Stop(context.WithoutCancel(ctx)); err != nil {
		logger.Warn("runtime stop failed", "error", err)
	}
}

func codeIndexOptionsForCLI(cfg *config.Config, opts indexCLIOptions) kb.CodeIndexOptions {
	indexOpts := configruntime.CodeIndexOptionsFromConfig(cfg, opts.kbID, opts.root)
	indexOpts.IndexKey = opts.indexKey
	indexOpts.Description = opts.description
	indexOpts.ConfirmedLarge = opts.yes || opts.force
	if opts.includeUntracked {
		indexOpts.IncludeUntracked = true
	}
	applyLowResourceCLIOptions(&opts)
	applyCLIResourceOverrides(&indexOpts, opts)
	return indexOpts
}

func applyLowResourceCLIOptions(opts *indexCLIOptions) {
	if !opts.lowResource {
		return
	}
	if opts.embedBatchSize == 0 {
		opts.embedBatchSize = 16
	}
	if opts.maxBatchBytes == 0 {
		opts.maxBatchBytes = 128 * 1024
	}
	if opts.throttle == 0 {
		opts.throttle = 250 * time.Millisecond
	}
}

func applyCLIResourceOverrides(indexOpts *kb.CodeIndexOptions, opts indexCLIOptions) {
	if opts.embedBatchSize > 0 {
		indexOpts.EmbedBatchSize = opts.embedBatchSize
	}
	if opts.maxBatchBytes > 0 {
		indexOpts.MaxBatchBytes = opts.maxBatchBytes
	}
	if opts.maxHeapBytes > 0 {
		indexOpts.MaxHeapBytes = opts.maxHeapBytes
	}
	if opts.maxRSSBytes > 0 {
		indexOpts.MaxRSSBytes = opts.maxRSSBytes
	}
	if opts.largeRepoFiles > 0 {
		indexOpts.LargeRepoFiles = opts.largeRepoFiles
	}
	if opts.throttle > 0 {
		indexOpts.Throttle = opts.throttle
	}
}

func runIndexStatus(ctx context.Context, args []string, logger *slog.Logger) int {
	opts, err := parseIndexCLIOptions(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}
	_, rt, err := buildRuntimeForCLI(ctx, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	defer stopRuntimeForCLI(ctx, logger, rt)
	selection, err := kb.ResolveCodeIndexSelection(opts.root, opts.indexKey, opts.kbID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve code index: %v\n", err)
		return 1
	}
	status, err := rt.KB().CodeIndexStatus(ctx, selection.KBID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "code index status: %v\n", err)
		return 1
	}
	status.IndexKey = selection.IndexKey
	status.Description = selection.Description
	if err := writeJSON(status); err != nil {
		fmt.Fprintf(os.Stderr, "write json: %v\n", err)
		return 1
	}
	return 0
}

func runIndexHooks(ctx context.Context, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(
			os.Stderr,
			"usage: codeindex hooks <install|uninstall|status> [--kb id] [--index-key key] [--root path] [--binary codeindex] [--force]",
		)
		return 2
	}
	action := args[0]
	opts, err := parseIndexCLIOptions(args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}
	var status any
	switch action {
	case "install":
		selection, selErr := kb.ResolveCodeIndexSelection(opts.root, opts.indexKey, opts.kbID)
		if selErr != nil {
			fmt.Fprintf(os.Stderr, "resolve code index: %v\n", selErr)
			return 1
		}
		status, err = kb.InstallCodeIndexHooks(
			ctx,
			kb.CodeHookOptions{
				Root:     opts.root,
				KBID:     selection.KBID,
				IndexKey: selection.IndexKey,
				Binary:   opts.binary,
				Force:    opts.force,
			},
		)
	case "uninstall":
		status, err = kb.UninstallCodeIndexHooks(ctx, opts.root)
	case "status":
		status, err = kb.CodeIndexHookStatus(ctx, opts.root)
	default:
		fmt.Fprintf(os.Stderr, "unknown hooks subcommand: %s\n", action)
		return 2
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "codeindex hooks %s: %v\n", action, err)
		return 1
	}
	if err := writeJSON(status); err != nil {
		fmt.Fprintf(os.Stderr, "write json: %v\n", err)
		return 1
	}
	return 0
}

func writeJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
