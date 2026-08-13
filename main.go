package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	appcmd "github.com/mikills/minnow/cmd"
	"github.com/mikills/minnow/cmd/configruntime"
	"github.com/mikills/minnow/kb/config"
	"github.com/mikills/minnow/mcpserver"
)

var backgroundContext = context.Background()

const version = "v0.2.2"

func main() {
	logger := newLogger(os.Getenv("MINNOW_LOG_FORMAT"))
	ctx := backgroundContext
	if code, handled := runTopLevelCommand(ctx, os.Args[1:], logger); handled {
		os.Exit(code)
	}
	if err := runServer(ctx, logger); err != nil {
		logger.Error("minnow exited with error", "error", err)
		os.Exit(1)
	}
}

func runTopLevelCommand(ctx context.Context, args []string, logger *slog.Logger) (int, bool) {
	if len(args) == 0 {
		return 0, false
	}
	switch args[0] {
	case "--version", "version":
		fmt.Println("minnow " + version)
		return 0, true
	case "-h", "--help":
		printUsage()
		return 0, true
	case "mcp":
		return runMCPSubcommand(ctx, args[1:]), true
	case "index":
		return runLegacyCodeIndexCommand(ctx, args[1:]), true
	case "config":
		return runConfigSubcommand(ctx, args[1:], logger), true
	case "setup":
		return runSetupSubcommand(args[1:]), true
	default:
		return 0, false
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: minnow [mcp|config|setup|version]")
	fmt.Fprintln(os.Stderr, "       minnow --version")
	fmt.Fprintln(os.Stderr, "       minnow mcp stdio")
	fmt.Fprintln(os.Stderr, "       minnow index ... (compatibility alias for codeindex)")
	fmt.Fprintln(os.Stderr, "       minnow config <validate|init>")
	fmt.Fprintln(os.Stderr, "       minnow setup")
}

func runLegacyCodeIndexCommand(ctx context.Context, args []string) int {
	binary, err := findCodeIndexBinary()
	if err != nil {
		fmt.Fprintln(os.Stderr, "minnow index has moved to codeindex; install github.com/mikills/minnow/codeindex@latest or use --binary with codeindex")
		return 1
	}
	cmd := exec.CommandContext(ctx, binary, legacyCodeIndexArgs(binary, args)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}
		fmt.Fprintf(os.Stderr, "run codeindex: %v\n", err)
		return 1
	}
	return 0
}

func legacyCodeIndexArgs(binary string, args []string) []string {
	forwarded := append([]string{"index"}, args...)
	if len(args) >= 2 && args[0] == "hooks" && args[1] == "install" && !hasBinaryFlag(args[2:]) {
		forwarded = append(forwarded, "--binary", binary)
	}
	return forwarded
}

func hasBinaryFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--binary" || strings.HasPrefix(arg, "--binary=") {
			return true
		}
	}
	return false
}

func findCodeIndexBinary() (string, error) {
	if binary, err := exec.LookPath("codeindex"); err == nil {
		return binary, nil
	}
	executable, err := os.Executable()
	if err != nil {
		return "", err
	}
	sibling := filepath.Join(filepath.Dir(executable), "codeindex")
	return exec.LookPath(sibling)
}

func runMCPSubcommand(baseCtx context.Context, args []string) int {
	if len(args) == 0 || args[0] != "stdio" {
		fmt.Fprintln(os.Stderr, "usage: minnow mcp stdio")
		return 2
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := config.Load(os.Getenv("MINNOW_CONFIG"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(baseCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	rt, err := configruntime.Build(ctx, cfg, configruntime.BuildOptions{Logger: logger})
	if err != nil {
		fmt.Fprintf(os.Stderr, "build runtime: %v\n", err)
		return 1
	}
	mcpCfg := rt.MCPConfig()
	if !mcpCfg.Enabled || !mcpCfg.StdioEnabled {
		fmt.Fprintln(os.Stderr, "mcp stdio transport is not enabled")
		return 1
	}
	if err := rt.StartBackground(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "start runtime: %v\n", err)
		return 1
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.HTTPShutdownTimeout())
		defer cancel()
		if err := rt.Stop(shutdownCtx); err != nil {
			logger.Warn("runtime stop failed", "error", err)
		}
	}()
	server := appcmd.NewMCPServerFromKB(rt.KB(), mcpCfg, logger)
	if err := mcpserver.RunStdio(ctx, server); err != nil {
		fmt.Fprintf(os.Stderr, "mcp stdio failed: %v\n", err)
		return 1
	}
	return 0
}

// runServer loads the YAML config, builds the runtime, and serves HTTP until
// SIGINT/SIGTERM. This is the only entry point that binds ports and connects
// to external services.
func runServer(baseCtx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load(os.Getenv("MINNOW_CONFIG"))
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(baseCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	rt, err := configruntime.Build(ctx, cfg, configruntime.BuildOptions{Logger: logger})
	if err != nil {
		return fmt.Errorf("build runtime: %w", err)
	}

	if err := rt.Start(ctx); err != nil {
		return fmt.Errorf("start runtime: %w", err)
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- rt.Wait() }()

	var serveErr error
	select {
	case serveErr = <-waitCh:
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.HTTPShutdownTimeout())
	defer cancel()
	stopErr := rt.Stop(shutdownCtx)
	if serveErr == nil {
		select {
		case serveErr = <-waitCh:
		case <-shutdownCtx.Done():
			serveErr = shutdownCtx.Err()
		}
	}
	return errors.Join(serveErr, stopErr)
}

// runConfigSubcommand implements the `minnow config ...` CLI. Today the only
// leaf is `validate`, which runs Load + Build(DryRun=true) and exits 0/1.
func runConfigSubcommand(ctx context.Context, args []string, logger *slog.Logger) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: minnow config <subcommand>")
		fmt.Fprintln(os.Stderr, "subcommands: validate [path], init dev-openai [path] [--force]")
		return 2
	}

	switch args[0] {
	case "validate":
		return runConfigValidate(ctx, args[1:], logger)
	case "init":
		return runConfigInit(args[1:])
	case "-h", "--help":
		fmt.Fprintln(os.Stderr, "usage: minnow config <subcommand>")
		fmt.Fprintln(os.Stderr, "subcommands: validate [path], init dev-openai [path] [--force]")
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown config subcommand: %s\n", args[0])
		return 2
	}
}

func runConfigValidate(ctx context.Context, args []string, logger *slog.Logger) int {
	path := os.Getenv("MINNOW_CONFIG")
	if len(args) >= 1 {
		path = args[0]
	}

	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config invalid: %v\n", err)
		return 1
	}

	if _, err := configruntime.Build(ctx, cfg, configruntime.BuildOptions{DryRun: true, Logger: logger}); err != nil {
		fmt.Fprintf(os.Stderr, "config build failed: %v\n", err)
		return 1
	}

	fmt.Println("config OK")
	return 0
}

func runConfigInit(args []string) int {
	path, force, code, ok := parseConfigInitArgs(args)
	if !ok {
		return code
	}
	if path == "" {
		resolved, err := config.UserConfigPath()
		if err != nil {
			fmt.Fprintf(os.Stderr, "resolve user config path: %v\n", err)
			return 1
		}
		path = resolved
	}
	if err := writeConfigTemplate(path, devOpenAIConfigTemplate(), force); err != nil {
		fmt.Fprintf(os.Stderr, "write config: %v\n", err)
		return 1
	}
	fmt.Printf("wrote %s\n", path)
	return 0
}

func parseConfigInitArgs(args []string) (string, bool, int, bool) {
	if len(args) == 0 || args[0] != "dev-openai" {
		fmt.Fprintln(os.Stderr, "usage: minnow config init dev-openai [path] [--force]")
		return "", false, 2, false
	}
	path := ""
	force := false
	for _, arg := range args[1:] {
		parsedPath, parsedForce, code, ok := parseConfigInitArg(arg, path, force)
		if !ok {
			return "", false, code, false
		}
		path, force = parsedPath, parsedForce
	}
	return path, force, 0, true
}

func parseConfigInitArg(arg string, path string, force bool) (string, bool, int, bool) {
	switch {
	case arg == "--force":
		return path, true, 0, true
	case strings.HasPrefix(arg, "-"):
		fmt.Fprintf(os.Stderr, "unknown flag: %s\n", arg)
		return "", false, 2, false
	case path == "":
		return arg, force, 0, true
	default:
		fmt.Fprintln(os.Stderr, "usage: minnow config init dev-openai [path] [--force]")
		return "", false, 2, false
	}
}

func writeConfigTemplate(path string, data []byte, force bool) error {
	if !force {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%s already exists (use --force to overwrite)", path)
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if !force {
		flag = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	}
	f, err := os.OpenFile(path, flag, filePermOwnerReadWrite)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}

const filePermOwnerReadWrite fs.FileMode = 0o600

func devOpenAIConfigTemplate() []byte {
	return []byte(`# Minnow OpenAI-backed developer config.
# Set OPENAI_API_KEY in the environment used by your terminal or MCP client.

storage:
  blob:
    root: ./blobs
  cache:
    dir: ./cache

format:
  duckdb:
    extension_dir: ./extensions
    offline: false

embedder:
  provider: openai_compatible
  openai_compatible:
    base_url: https://api.openai.com/v1
    model: text-embedding-3-small
    token: ${OPENAI_API_KEY}
    dimensions: 0

code_index:
  include: ["**/*"]
  max_file_bytes: 1048576
  chunk_size: 1200
  chunk_overlap: 120
  include_untracked: false

mcp:
  enabled: true
  transports: [http, stdio]
  http_path: /mcp
  allow_indexing: true
  allow_sync_indexing: true
`)
}

func newLogger(format string) *slog.Logger {
	if format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}
