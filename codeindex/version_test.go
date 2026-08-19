package main

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	previous := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	fn()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	os.Stdout = previous
	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestVersionCommand(t *testing.T) {
	for _, arg := range []string{"--version", "version"} {
		t.Run(arg, func(t *testing.T) {
			var code int
			out := captureStdout(t, func() { code = run(context.Background(), []string{arg}) })
			if code != 0 {
				t.Fatalf("exit code %d, want 0", code)
			}
			if !strings.HasPrefix(out, "codeindex ") {
				t.Fatalf("output %q, want it to start with %q", out, "codeindex ")
			}
			if strings.TrimSpace(strings.TrimPrefix(out, "codeindex ")) == "" {
				t.Fatalf("no version reported: %q", out)
			}
		})
	}
}
