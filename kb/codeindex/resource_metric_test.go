package codeindex

import (
	"context"
	"runtime"
	"testing"
)

func TestFootprintSurvivesFreeAndGC(t *testing.T) {
	base, ok := currentFootprintBytes()
	if !ok {
		t.Skip("footprint metric unavailable")
	}
	const blockBytes = 128 << 20
	block := make([]byte, blockBytes)
	for i := 0; i < len(block); i += 4096 {
		block[i] = 1
	}
	runtime.KeepAlive(block)
	block = nil
	runtime.GC()

	after, ok := currentFootprintBytes()
	if !ok {
		t.Fatal("footprint metric became unavailable")
	}
	if after < base+blockBytes/4 {
		t.Fatalf("footprint collapsed after GC: base=%d after=%d; the guard cannot see memory the process still holds", base, after)
	}
}

func TestHeapGuardTripsOnRetainedMemory(t *testing.T) {
	const blockBytes = 64 << 20
	block := make([]byte, blockBytes)
	for i := 0; i < len(block); i += 4096 {
		block[i] = 1
	}
	defer runtime.KeepAlive(block)

	footprint, ok := currentFootprintBytes()
	if !ok {
		t.Skip("footprint metric unavailable")
	}
	if err := (ResourcePolicy{MaxHeapBytes: footprint / 2}).Check(context.Background()); err == nil {
		t.Fatal("guard did not trip below the current footprint")
	}
	if err := (ResourcePolicy{MaxHeapBytes: footprint * 4}).Check(context.Background()); err != nil {
		t.Fatalf("guard tripped well above the current footprint: %v", err)
	}
}
