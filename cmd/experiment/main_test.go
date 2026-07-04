package main

import (
	"strings"
	"testing"
)

func TestRunBoundaryRejectsUnsupportedConcurrency(t *testing.T) {
	err := runBoundaryCommand([]string{"--concurrency", "31"})
	if err == nil || !strings.Contains(err.Error(), "29 or 30") {
		t.Fatalf("error = %v, want unsupported-concurrency error", err)
	}
}
