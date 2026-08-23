package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	minnowcode "github.com/mikills/minnow/kb/codeindex"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Minnow    MinnowConfig    `yaml:"minnow"`
	CodeIndex CodeIndexConfig `yaml:"code_index"`
}

type MinnowConfig struct {
	URL   string `yaml:"url"`
	Token string `yaml:"token,omitempty"`
}

type CodeIndexConfig struct {
	Include          []string `yaml:"include,omitempty"`
	Exclude          []string `yaml:"exclude,omitempty"`
	MaxFileBytes     int64    `yaml:"max_file_bytes,omitempty"`
	ChunkSize        int      `yaml:"chunk_size,omitempty"`
	ChunkOverlap     int      `yaml:"chunk_overlap,omitempty"`
	IncludeUntracked bool     `yaml:"include_untracked,omitempty"`
	RequestBatchSize int      `yaml:"request_batch_size,omitempty"`
	MaxBatchBytes    int      `yaml:"max_batch_bytes,omitempty"`
	Throttle         string   `yaml:"throttle,omitempty"`
	MaxHeapBytes     uint64   `yaml:"max_heap_bytes,omitempty"`
	MaxRSSBytes      uint64   `yaml:"max_rss_bytes,omitempty"`
	LargeRepoFiles   int      `yaml:"large_repo_files,omitempty"`
	RequireConfirm   *bool    `yaml:"require_confirm,omitempty"`
	PollInterval     string   `yaml:"poll_interval,omitempty"`
	OperationTimeout string   `yaml:"operation_timeout,omitempty"`
}

type setupCLIOptions struct {
	configPath string
	minnowURL  string
	tokenEnv   string
	force      bool
}

func runSetup(args []string) int {
	opts, err := parseSetupCLIOptions(args)
	if err != nil {
		return writeCommandError(err, 2)
	}
	path, err := resolvedConfigPath(opts.configPath)
	if err != nil {
		return writeCommandError(err, 1)
	}
	if err := writeConfig(path, setupConfig(opts), opts.force); err != nil {
		return writeCommandError(fmt.Errorf("write config: %w", err), 1)
	}
	fmt.Printf("wrote %s\n", path)
	return 0
}

func parseSetupCLIOptions(args []string) (setupCLIOptions, error) {
	opts := setupCLIOptions{
		configPath: os.Getenv("CODEINDEX_CONFIG"),
		minnowURL:  firstNonEmpty(os.Getenv("CODEINDEX_MINNOW_URL"), "http://127.0.0.1:8080"),
	}
	fs := flag.NewFlagSet("codeindex setup", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.configPath, "config", opts.configPath, "config path")
	fs.StringVar(&opts.minnowURL, "minnow-url", opts.minnowURL, "Minnow HTTP base URL")
	fs.StringVar(&opts.tokenEnv, "token-env", opts.tokenEnv, "environment variable containing the bearer token")
	fs.BoolVar(&opts.force, "force", false, "overwrite an existing config")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if fs.NArg() != 0 {
		return opts, fmt.Errorf("unexpected argument: %s", fs.Arg(0))
	}
	if err := validateMinnowURL(opts.minnowURL); err != nil {
		return opts, err
	}
	return opts, nil
}

func setupConfig(opts setupCLIOptions) Config {
	cfg := defaultConfig()
	cfg.Minnow.URL = strings.TrimRight(strings.TrimSpace(opts.minnowURL), "/")
	if strings.TrimSpace(opts.tokenEnv) != "" {
		cfg.Minnow.Token = "${" + strings.TrimSpace(opts.tokenEnv) + "}"
	}
	return cfg
}

func defaultConfig() Config {
	requireConfirm := true
	return Config{
		Minnow: MinnowConfig{URL: "http://127.0.0.1:8080"},
		CodeIndex: CodeIndexConfig{
			Include:          append([]string(nil), minnowcode.DefaultIncludePatterns...),
			Exclude:          append([]string(nil), minnowcode.DefaultExcludePatterns...),
			MaxFileBytes:     minnowcode.DefaultMaxFileBytes,
			ChunkSize:        minnowcode.DefaultChunkSize,
			ChunkOverlap:     minnowcode.DefaultChunkOverlap,
			RequestBatchSize: minnowcode.DefaultEmbedBatchSize,
			MaxBatchBytes:    minnowcode.DefaultMaxBatchBytes,
			Throttle:         minnowcode.DefaultThrottle.String(),
			MaxHeapBytes:     minnowcode.DefaultMaxHeapBytes,
			MaxRSSBytes:      minnowcode.DefaultMaxRSSBytes,
			LargeRepoFiles:   minnowcode.DefaultLargeRepoFiles,
			RequireConfirm:   &requireConfirm,
			PollInterval:     "500ms",
			OperationTimeout: "10m",
		},
	}
}

func loadConfig(path string) (Config, error) {
	resolved, err := resolvedConfigPath(path)
	if err != nil {
		return Config{}, err
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Config{}, fmt.Errorf("codeindex config not found at %s; run `codeindex setup --minnow-url http://127.0.0.1:8080`", resolved)
		}
		return Config{}, fmt.Errorf("read config %q: %w", resolved, err)
	}
	expanded, err := expandConfigEnv(string(data))
	if err != nil {
		return Config{}, fmt.Errorf("%s: %w", resolved, err)
	}
	cfg := defaultConfig()
	dec := yaml.NewDecoder(bytes.NewBufferString(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("%s: %w", resolved, err)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("%s: %w", resolved, err)
	}
	return cfg, nil
}

func (cfg Config) validate() error {
	if err := validateMinnowURL(cfg.Minnow.URL); err != nil {
		return err
	}
	if cfg.CodeIndex.RequestBatchSize <= 0 || cfg.CodeIndex.MaxBatchBytes <= 0 {
		return fmt.Errorf("code_index request batch limits must be greater than zero")
	}
	if cfg.CodeIndex.MaxFileBytes <= 0 || cfg.CodeIndex.ChunkSize <= 0 {
		return fmt.Errorf("code_index file and chunk sizes must be greater than zero")
	}
	if cfg.CodeIndex.ChunkOverlap < 0 || cfg.CodeIndex.ChunkOverlap >= cfg.CodeIndex.ChunkSize {
		return fmt.Errorf("code_index.chunk_overlap must be non-negative and less than chunk_size")
	}
	if _, err := cfg.pollInterval(); err != nil {
		return err
	}
	if _, err := cfg.operationTimeout(); err != nil {
		return err
	}
	return nil
}

func validateMinnowURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("minnow.url must be an absolute http or https URL")
	}
	return nil
}

func (cfg Config) pollInterval() (time.Duration, error) {
	return parsePositiveDuration("code_index.poll_interval", cfg.CodeIndex.PollInterval)
}

func (cfg Config) operationTimeout() (time.Duration, error) {
	return parsePositiveDuration("code_index.operation_timeout", cfg.CodeIndex.OperationTimeout)
}

func parsePositiveDuration(name, raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return d, nil
}

func resolvedConfigPath(path string) (string, error) {
	if strings.TrimSpace(path) != "" {
		return filepath.Abs(path)
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config directory: %w", err)
	}
	return filepath.Join(dir, "codeindex", "config.yaml"), nil
}

func writeConfig(path string, cfg Config, force bool) error {
	if !force {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%s already exists; use --force to overwrite", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if !force {
		flag = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	}
	file, err := os.OpenFile(path, flag, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(data)
	return err
}

func expandConfigEnv(raw string) (string, error) {
	var missing []string
	expanded := os.Expand(raw, func(name string) string {
		value, ok := os.LookupEnv(name)
		if !ok {
			missing = append(missing, name)
		}
		return value
	})
	if len(missing) != 0 {
		return "", fmt.Errorf("missing environment variables: %s", strings.Join(missing, ", "))
	}
	return expanded, nil
}
