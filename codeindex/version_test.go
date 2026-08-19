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
	defer func() { os.Stdout = previous }()
	defer reader.Close()
	fn()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
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
			if want := "codeindex " + versionString() + "\n"; out != want {
				t.Fatalf("output %q, want %q", out, want)
			}
		})
	}
}

func TestVersionStringIsReported(t *testing.T) {
	got := versionString()
	if got == "" {
		t.Fatal("versionString returned an empty string")
	}
	// Under `go test` the build info reports "(devel)", so this exercises the
	// fallback; installed binaries report a tag or pseudo-version instead.
	if got != "unknown" && !strings.HasPrefix(got, "v") {
		t.Fatalf("versionString = %q, want a v-prefixed version or \"unknown\"", got)
	}
}
