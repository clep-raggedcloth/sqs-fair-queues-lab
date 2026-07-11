package main

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"
)

func TestParseLowOptionsUsesCapacity20LoadDefaults(t *testing.T) {
	opts, err := parseLowOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if opts.probeMessages != 1200 || opts.probeInterval != 500*time.Millisecond ||
		opts.baselineDuration != 100*time.Second || opts.baselineInterval != 500*time.Millisecond || baselineMessageCount(opts) != 200 {
		t.Fatalf("unexpected run-low defaults: %+v, baseline messages=%d", opts, baselineMessageCount(opts))
	}
}

func TestLowHelpShowsEffectiveDefaults(t *testing.T) {
	fs, _ := lowRunFlags()
	var output bytes.Buffer
	fs.SetOutput(&output)
	err := fs.Parse([]string{"--help"})
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("Parse(--help) error=%v, want flag.ErrHelp", err)
	}
	help := output.String()
	for _, expected := range []string{
		"(default 1200)",
		"(default 500ms)",
		"(default 1m40s)",
	} {
		if !strings.Contains(help, expected) {
			t.Fatalf("help does not contain %q:\n%s", expected, help)
		}
	}
}

func TestRunLowHelpIsSuccessful(t *testing.T) {
	err := runLowCommand([]string{"--help"})
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("runLowCommand(--help) error=%v, want flag.ErrHelp", err)
	}
	if !commandSucceeded(err) {
		t.Fatal("commandSucceeded(flag.ErrHelp) = false, want true")
	}
}

func TestParseLowOptionsPreservesExplicitProbeLoad(t *testing.T) {
	opts, err := parseLowOptions([]string{"--probes", "40", "--probe-interval", "250ms"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.probeMessages != 40 || opts.probeInterval != 250*time.Millisecond {
		t.Fatalf("probes=%d interval=%s, want probes=40 interval=250ms", opts.probeMessages, opts.probeInterval)
	}
	if opts.baselineDuration != 100*time.Second || opts.baselineInterval != 500*time.Millisecond || baselineMessageCount(opts) != 200 {
		t.Fatalf("explicit probe flags unexpectedly changed baseline defaults: %+v", opts)
	}
}
