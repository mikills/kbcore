//go:build !darwin && !linux

package memlimit

func detectCeiling() Limit { return Limit{} }
