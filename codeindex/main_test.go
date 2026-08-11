package main

import (
	"testing"
	"time"
)

func TestIndexCLIOptions(t *testing.T) {
	t.Run("flagset parser accepts aliases and typed values", func(t *testing.T) {
		opts, err := parseIndexCLIOptions([]string{
			"--kb", "code", "--index-key", "api", "--root", ".",
			"--batch-size", "4", "--max-rss-bytes", "1024", "--throttle", "5ms", "-y",
		})
		if err != nil {
			t.Fatal(err)
		}
		if opts.kbID != "code" || opts.indexKey != "api" || opts.embedBatchSize != 4 || opts.maxRSSBytes != 1024 || !opts.yes {
			t.Fatalf("unexpected options: %+v", opts)
		}
		if opts.throttle != 5*time.Millisecond {
			t.Fatalf("unexpected throttle: %s", opts.throttle)
		}
	})

	t.Run("rejects positional args", func(t *testing.T) {
		_, err := parseIndexCLIOptions([]string{"unexpected"})
		if err == nil {
			t.Fatal("expected positional argument error")
		}
	})
}
