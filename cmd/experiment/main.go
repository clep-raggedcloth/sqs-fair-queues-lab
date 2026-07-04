package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/experiment"
	"github.com/aoiito/sqs-fair-queue-verification/internal/message"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type runOptions struct {
	configPath    string
	resultsDir    string
	workMS        int
	burstMessages int
	probeMessages int
	probeInterval time.Duration
	warmup        int
	sendWorkers   int
	mode          string
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run-reaction":
		err = runCommand(os.Args[2:], 100, "reaction")
	case "run-low":
		err = runLowCommand(os.Args[2:])
	case "collect":
		err = collectCommand(os.Args[2:])
	case "purge":
		err = purgeCommand(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage:
  experiment run-reaction [flags]
  experiment run-low --mode short|long [flags]
  experiment collect --manifest results/<id>/manifest.json [flags]
  experiment purge [flags]`)
}

func baseRunFlags(name string, args []string) (*flag.FlagSet, *runOptions) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	opts := &runOptions{}
	fs.StringVar(&opts.configPath, "config", "build/experiment-config.json", "Terraform-generated experiment config")
	fs.StringVar(&opts.resultsDir, "results-dir", "results", "Local results directory")
	fs.IntVar(&opts.workMS, "work-ms", 2000, "Consumer processing time per message")
	fs.IntVar(&opts.burstMessages, "burst", 5000, "Tenant A burst messages per scenario")
	fs.IntVar(&opts.probeMessages, "probes", 300, "Quiet-tenant probe messages per scenario")
	fs.DurationVar(&opts.probeInterval, "probe-interval", 100*time.Millisecond, "Interval between B/C probes")
	fs.IntVar(&opts.warmup, "warmup", 200, "Balanced warm-up messages per scenario")
	fs.IntVar(&opts.sendWorkers, "send-workers", 16, "Concurrent SendMessageBatch workers")
	_ = args
	return fs, opts
}

func runCommand(args []string, concurrency int, kind string) error {
	fs, opts := baseRunFlags("run-reaction", args)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return executeRun(context.Background(), *opts, concurrency, kind)
}

func runLowCommand(args []string) error {
	fs, opts := baseRunFlags("run-low", args)
	fs.StringVar(&opts.mode, "mode", "short", "short tests the count path; long observes possible processing-time detection")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch opts.mode {
	case "short":
		if !flagWasSet(fs, "probes") {
			opts.probeMessages = 150
		}
	case "long":
		if !flagWasSet(fs, "probes") {
			opts.probeMessages = 1200
		}
	default:
		return fmt.Errorf("mode must be short or long")
	}
	return executeRun(context.Background(), *opts, 29, "low-concurrency")
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func executeRun(parent context.Context, opts runOptions, concurrency int, kind string) error {
	config, err := experiment.LoadConfig(opts.configPath)
	if err != nil {
		return err
	}
	scenarios, err := config.Pair(concurrency)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Minute)
	defer cancel()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(config.Region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	sender := experiment.NewSender(sqs.NewFromConfig(awsCfg))
	if err := sender.EnsureEmpty(ctx, scenarios); err != nil {
		return err
	}

	warmupID := "warmup-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	fmt.Printf("warming up %d scenarios with %d balanced messages each\n", len(scenarios), opts.warmup)
	if err := sendWarmup(ctx, sender, scenarios, warmupID, opts); err != nil {
		return err
	}
	if err := sender.WaitForDrain(ctx, scenarios); err != nil {
		return fmt.Errorf("wait for warm-up drain: %w", err)
	}

	experimentID := fmt.Sprintf("%s-%s", kind, time.Now().UTC().Format("20060102T150405.000000000Z"))
	startedAt := time.Now().UTC()
	fmt.Printf("starting %s\n", experimentID)
	if err := sendMeasurement(ctx, sender, scenarios, experimentID, opts); err != nil {
		return err
	}
	fmt.Println("all messages accepted; waiting for both queues to drain")
	if err := sender.WaitForDrain(ctx, scenarios); err != nil {
		return fmt.Errorf("wait for experiment drain: %w", err)
	}

	names := make([]string, 0, len(scenarios))
	for name := range scenarios {
		names = append(names, name)
	}
	sort.Strings(names)
	manifest := experiment.Manifest{
		ExperimentID: experimentID, Kind: kind, Mode: opts.mode, Scenarios: names,
		StartedAt: startedAt.Format(time.RFC3339Nano), CompletedAt: time.Now().UTC().Format(time.RFC3339Nano),
		WorkMS: opts.workMS, BurstMessages: opts.burstMessages, ProbeMessages: opts.probeMessages,
		ProbeIntervalMS:     int(opts.probeInterval.Milliseconds()),
		ObservationWindowMS: opts.probeMessages * int(opts.probeInterval.Milliseconds()),
		WarmupMessages:      opts.warmup,
		MaximumConcurrency:  concurrency,
	}
	path, err := experiment.SaveManifest(opts.resultsDir, manifest)
	if err != nil {
		return err
	}
	fmt.Println("manifest:", path)
	fmt.Printf("collect with: build/experiment collect --config %s --manifest %s\n", opts.configPath, path)
	return nil
}

func sendWarmup(ctx context.Context, sender *experiment.Sender, scenarios map[string]experiment.Scenario, experimentID string, opts runOptions) error {
	return forEachScenario(scenarios, func(name string, scenario experiment.Scenario) error {
		works := make([]message.Work, 0, opts.warmup)
		for i := 0; i < opts.warmup; i++ {
			tenant := fmt.Sprintf("warmup-%02d", i%20)
			works = append(works, message.New(experimentID, name, tenant, "warmup", i, min(opts.workMS, 250), time.Now()))
		}
		return sender.SendMany(ctx, name, scenario, works, opts.sendWorkers)
	})
}

func sendMeasurement(ctx context.Context, sender *experiment.Sender, scenarios map[string]experiment.Scenario, experimentID string, opts runOptions) error {
	return forEachScenario(scenarios, func(name string, scenario experiment.Scenario) error {
		var wg sync.WaitGroup
		errCh := make(chan error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			works := make([]message.Work, 0, opts.burstMessages)
			for i := 0; i < opts.burstMessages; i++ {
				works = append(works, message.New(experimentID, name, "A", "burst", i, opts.workMS, time.Now()))
			}
			errCh <- sender.SendMany(ctx, name, scenario, works, opts.sendWorkers)
		}()
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(opts.probeInterval)
			defer ticker.Stop()
			for i := 0; i < opts.probeMessages; i++ {
				if i > 0 {
					select {
					case <-ctx.Done():
						errCh <- ctx.Err()
						return
					case <-ticker.C:
					}
				}
				tenant := "B"
				if i%2 == 1 {
					tenant = "C"
				}
				work := message.New(experimentID, name, tenant, "probe", i, opts.workMS, time.Now())
				if err := sender.SendOne(ctx, name, scenario, work); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func forEachScenario(scenarios map[string]experiment.Scenario, fn func(string, experiment.Scenario) error) error {
	errCh := make(chan error, len(scenarios))
	var wg sync.WaitGroup
	for name, scenario := range scenarios {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- fn(name, scenario)
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func collectCommand(args []string) error {
	fs := flag.NewFlagSet("collect", flag.ContinueOnError)
	configPath := fs.String("config", "build/experiment-config.json", "Terraform-generated experiment config")
	manifestPath := fs.String("manifest", "", "Experiment manifest path")
	resultsDir := fs.String("results-dir", "results", "Local results directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" {
		return fmt.Errorf("--manifest is required")
	}
	config, err := experiment.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	manifest, err := experiment.LoadManifest(*manifestPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(config.Region))
	if err != nil {
		return err
	}
	collector := experiment.NewCollector(cloudwatchlogs.NewFromConfig(awsCfg))
	rows, err := collector.Collect(ctx, config, manifest)
	if err != nil {
		return err
	}
	csvPath, err := experiment.WriteCSV(*resultsDir, manifest, rows)
	if err != nil {
		return err
	}
	summaryPath, err := experiment.WriteSummary(*resultsDir, manifest, rows)
	if err != nil {
		return err
	}
	observationSummaryPath, err := experiment.WriteObservationSummary(*resultsDir, manifest, rows)
	if err != nil {
		return err
	}
	recoveryPath, err := experiment.WriteRecoveryEstimates(*resultsDir, manifest, rows)
	if err != nil {
		return err
	}
	fmt.Printf("collected %d message starts\nevents: %s\nfull summary: %s\nobservation summary: %s\nrecovery estimate: %s\n", len(rows), csvPath, summaryPath, observationSummaryPath, recoveryPath)
	return nil
}

func purgeCommand(args []string) error {
	fs := flag.NewFlagSet("purge", flag.ContinueOnError)
	configPath := fs.String("config", "build/experiment-config.json", "Terraform-generated experiment config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	config, err := experiment.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(config.Region))
	if err != nil {
		return err
	}
	if err := experiment.NewSender(sqs.NewFromConfig(awsCfg)).Purge(ctx, config.Scenarios); err != nil {
		return err
	}
	fmt.Println("purge requested for all experiment queues")
	return nil
}
