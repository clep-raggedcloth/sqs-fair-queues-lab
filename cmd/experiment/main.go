package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/experiment"
	"github.com/aoiito/sqs-fair-queue-verification/internal/message"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type runOptions struct {
	configPath          string
	resultsDir          string
	workMS              int
	burstMessages       int
	probeMessages       int
	probeInterval       time.Duration
	queueSampleInterval time.Duration
	baselineDuration    time.Duration
	warmup              int
	sendWorkers         int
	mode                string
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
	case "run-boundary":
		err = runBoundaryCommand(os.Args[2:])
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
  experiment run-boundary --concurrency 29|30 [flags]
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
	fs.DurationVar(&opts.queueSampleInterval, "queue-sample-interval", time.Second, "Interval between direct SQS queue-depth observations")
	fs.DurationVar(&opts.baselineDuration, "baseline-duration", 20*time.Second, "Duration of the pre-burst B/C baseline phase")
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
	fs.StringVar(&opts.mode, "mode", "short", "short observes early behavior below 30; long observes possible processing-time detection")
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

func runBoundaryCommand(args []string) error {
	fs, opts := baseRunFlags("run-boundary", args)
	concurrency := fs.Int("concurrency", 30, "Lambda maximum concurrency; must be 29 or 30")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *concurrency != 29 && *concurrency != 30 {
		return fmt.Errorf("concurrency must be 29 or 30")
	}
	if !flagWasSet(fs, "probes") {
		opts.probeMessages = 150
	}
	opts.mode = "boundary"
	return executeRun(context.Background(), *opts, *concurrency, "count-boundary")
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
	if opts.burstMessages < 1 || opts.probeMessages < 1 {
		return fmt.Errorf("burst and probes must be at least 1")
	}
	if opts.probeInterval <= 0 || opts.queueSampleInterval <= 0 || opts.baselineDuration < 0 {
		return fmt.Errorf("probe-interval and queue-sample-interval must be positive and baseline-duration must not be negative")
	}
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
	fmt.Printf("starting %s with %s of B/C baseline traffic\n", experimentID, opts.baselineDuration)
	if err := sendBaseline(ctx, sender, scenarios, experimentID, opts); err != nil {
		return fmt.Errorf("send baseline: %w", err)
	}
	if err := sender.WaitForDrain(ctx, scenarios); err != nil {
		return fmt.Errorf("wait for baseline drain: %w", err)
	}
	type sampleResult struct {
		samples []experiment.QueueDepthSample
		err     error
	}
	type measurementResult struct {
		timings map[string]experiment.ScenarioTiming
		err     error
	}
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	sampleResultCh := make(chan sampleResult, 1)
	go func() {
		samples, err := sender.SampleQueueDepths(runCtx, scenarios, opts.queueSampleInterval)
		sampleResultCh <- sampleResult{samples: samples, err: err}
	}()
	measurementResultCh := make(chan measurementResult, 1)
	go func() {
		timings, err := sendMeasurement(runCtx, sender, scenarios, experimentID, opts)
		measurementResultCh <- measurementResult{timings: timings, err: err}
	}()

	var sampled sampleResult
	var timings map[string]experiment.ScenarioTiming
	var runErr error
	select {
	case sampled = <-sampleResultCh:
		if sampled.err != nil {
			runErr = fmt.Errorf("sample SQS queue depths: %w", sampled.err)
		} else {
			runErr = fmt.Errorf("SQS queue-depth sampler stopped unexpectedly")
		}
		stopRun()
		measured := <-measurementResultCh
		timings = measured.timings
	case measured := <-measurementResultCh:
		timings = measured.timings
		if measured.err != nil {
			runErr = measured.err
			stopRun()
			sampled = <-sampleResultCh
			break
		}

		fmt.Println("all messages accepted; waiting for both queues to drain")
		drainResultCh := make(chan error, 1)
		go func() { drainResultCh <- sender.WaitForDrain(runCtx, scenarios) }()
		select {
		case sampled = <-sampleResultCh:
			if sampled.err != nil {
				runErr = fmt.Errorf("sample SQS queue depths: %w", sampled.err)
			} else {
				runErr = fmt.Errorf("SQS queue-depth sampler stopped unexpectedly")
			}
			stopRun()
			<-drainResultCh
		case err := <-drainResultCh:
			if err != nil {
				runErr = fmt.Errorf("wait for experiment drain: %w", err)
			}
			stopRun()
			sampled = <-sampleResultCh
			if sampled.err != nil && runErr == nil {
				runErr = fmt.Errorf("sample SQS queue depths: %w", sampled.err)
			}
		}
	}
	queueSampleErrorCount := 0
	for _, sample := range sampled.samples {
		if sample.Status == experiment.QueueSampleStatusError {
			queueSampleErrorCount++
		}
	}

	names := make([]string, 0, len(scenarios))
	for name := range scenarios {
		names = append(names, name)
	}
	sort.Strings(names)
	manifest := experiment.Manifest{
		ExperimentID: experimentID, Kind: kind, Mode: opts.mode, Scenarios: names,
		StartedAt: startedAt.Format(time.RFC3339Nano), CompletedAt: time.Now().UTC().Format(time.RFC3339Nano),
		RunStatus: "complete",
		WorkMS:    opts.workMS, BurstMessages: opts.burstMessages, ProbeMessages: opts.probeMessages,
		ProbeIntervalMS:       int(opts.probeInterval.Milliseconds()),
		QueueSampleIntervalMS: int(opts.queueSampleInterval.Milliseconds()),
		QueueSampleCount:      len(sampled.samples),
		QueueSampleErrorCount: queueSampleErrorCount,
		ObservationWindowMS:   opts.probeMessages * int(opts.probeInterval.Milliseconds()),
		WarmupMessages:        opts.warmup,
		BaselineDurationMS:    int(opts.baselineDuration.Milliseconds()),
		MaximumConcurrency:    concurrency,
		ScenarioTimings:       timings,
	}
	if runErr != nil {
		manifest.RunStatus = "failed"
		manifest.RunError = runErr.Error()
	}
	path, err := experiment.SaveManifest(opts.resultsDir, manifest)
	if err != nil {
		return err
	}
	queueSamplesPath, err := experiment.WriteQueueDepthSamples(opts.resultsDir, manifest, sampled.samples)
	if err != nil {
		return err
	}
	fmt.Println("manifest:", path)
	fmt.Println("queue-depth samples:", queueSamplesPath)
	fmt.Printf("collect with: build/experiment collect --config %s --manifest %s\n", opts.configPath, path)
	if runErr != nil {
		return fmt.Errorf("experiment failed (partial results saved): %w", runErr)
	}
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

func sendBaseline(ctx context.Context, sender *experiment.Sender, scenarios map[string]experiment.Scenario, experimentID string, opts runOptions) error {
	if opts.baselineDuration == 0 {
		return nil
	}
	messageCount := max(1, int(opts.baselineDuration/opts.probeInterval))
	return forEachScenario(scenarios, func(name string, scenario experiment.Scenario) error {
		ticker := time.NewTicker(opts.probeInterval)
		defer ticker.Stop()
		for i := 0; i < messageCount; i++ {
			if i > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
			}
			tenant := "B"
			if i%2 == 1 {
				tenant = "C"
			}
			work := message.New(experimentID, name, tenant, "baseline", i, opts.workMS, time.Now())
			if err := sender.SendOne(ctx, name, scenario, work); err != nil {
				return err
			}
		}
		return nil
	})
}

func sendMeasurement(ctx context.Context, sender *experiment.Sender, scenarios map[string]experiment.Scenario, experimentID string, opts runOptions) (map[string]experiment.ScenarioTiming, error) {
	timings := make(map[string]experiment.ScenarioTiming, len(scenarios))
	var timingsMu sync.Mutex
	err := forEachScenario(scenarios, func(name string, scenario experiment.Scenario) error {
		scenarioCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		errCh := make(chan error, 2)
		burstStarted := make(chan time.Time, 1)
		wg.Add(2)
		go func() {
			defer wg.Done()
			works := make([]message.Work, 0, opts.burstMessages)
			for i := 0; i < opts.burstMessages; i++ {
				works = append(works, message.New(experimentID, name, "A", "burst", i, opts.workMS, time.Now()))
			}
			err := sender.SendManyWithFirstAccepted(scenarioCtx, name, scenario, works, opts.sendWorkers, func(at time.Time) {
				burstStarted <- at
			})
			if err != nil {
				cancel()
			}
			errCh <- err
		}()
		go func() {
			defer wg.Done()
			var startedAt time.Time
			select {
			case <-scenarioCtx.Done():
				errCh <- scenarioCtx.Err()
				return
			case startedAt = <-burstStarted:
			}
			timingsMu.Lock()
			timings[name] = experiment.ScenarioTiming{BurstStartedAt: startedAt.Format(time.RFC3339Nano)}
			timingsMu.Unlock()
			ticker := time.NewTicker(opts.probeInterval)
			defer ticker.Stop()
			for i := 0; i < opts.probeMessages; i++ {
				if i > 0 {
					select {
					case <-scenarioCtx.Done():
						errCh <- scenarioCtx.Err()
						return
					case <-ticker.C:
					}
				}
				tenant := "B"
				if i%2 == 1 {
					tenant = "C"
				}
				work := message.New(experimentID, name, tenant, "probe", i, opts.workMS, time.Now())
				if err := sender.SendOne(scenarioCtx, name, scenario, work); err != nil {
					cancel()
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
	if err != nil {
		return timings, err
	}
	return timings, nil
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
	handlerActivePath, handlerActiveSummaryPath, err := experiment.WriteHandlerActiveEstimates(*resultsDir, manifest, rows)
	if err != nil {
		return err
	}
	queueSamplesPath := filepath.Join(*resultsDir, manifest.ExperimentID, "queue-depth-samples.csv")
	queueAlignedPath := "not generated (raw queue samples are unavailable)"
	shareProxyPath := "not generated (raw queue samples are unavailable)"
	queueSamples, err := experiment.ReadQueueDepthSamples(queueSamplesPath)
	if err == nil {
		queueAlignedPath, shareProxyPath, err = experiment.WriteAlignedQueueEvidence(*resultsDir, manifest, rows, queueSamples)
		if err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read queue-depth samples: %w", err)
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
	metricSeries, err := experiment.CollectMetrics(ctx, cloudwatch.NewFromConfig(awsCfg), config, manifest)
	if err != nil {
		return fmt.Errorf("collect CloudWatch metrics: %w", err)
	}
	metricsPath, err := experiment.WriteMetrics(*resultsDir, manifest, metricSeries)
	if err != nil {
		return err
	}
	fmt.Printf("collected %d message starts\nevents: %s\nhandler-active estimate: %s\nhandler-active summary: %s\naligned queue-depth samples: %s\nconcurrency-share proxy: %s\nfull summary: %s\nobservation summary: %s\nrecovery estimate: %s\nmetrics: %s\n", len(rows), csvPath, handlerActivePath, handlerActiveSummaryPath, queueAlignedPath, shareProxyPath, summaryPath, observationSummaryPath, recoveryPath, metricsPath)
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
