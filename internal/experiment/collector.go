package experiment

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/consumer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

type LogsAPI interface {
	StartQuery(context.Context, *cloudwatchlogs.StartQueryInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error)
	GetQueryResults(context.Context, *cloudwatchlogs.GetQueryResultsInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error)
}

const (
	cloudWatchLogsQueryLimit = 10_000
	maximumQueryWindow       = 30 * time.Second
	minimumQueryWindow       = time.Second
)

type Collector struct {
	client       LogsAPI
	pollInterval time.Duration
}

type logQueryResult struct {
	messages    []string
	resultCount int
}

func NewCollector(client LogsAPI) *Collector {
	return &Collector{client: client, pollInterval: 2 * time.Second}
}

func (c *Collector) Collect(ctx context.Context, config Config, manifest Manifest) ([]consumer.EventLog, error) {
	start, err := manifest.StartTime()
	if err != nil {
		return nil, err
	}
	end, err := manifest.CompletionTime()
	if err != nil {
		return nil, err
	}
	entries := map[string]consumer.EventLog{}
	for _, name := range manifest.Scenarios {
		scenario, ok := config.Scenarios[name]
		if !ok {
			return nil, fmt.Errorf("scenario %q is missing from config", name)
		}
		rows, err := c.queryWindow(ctx, scenario.LogGroup, manifest.ExperimentID, start.Add(-time.Minute), end.Add(time.Minute))
		if err != nil {
			return nil, fmt.Errorf("query %s: %w", name, err)
		}
		for _, row := range rows {
			entry, ok := decodeEventLog(row)
			if ok && entry.ExperimentID == manifest.ExperimentID {
				entries[eventKey(entry)] = entry
			}
		}
	}
	result := make([]consumer.EventLog, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].HandlerStartMS < result[j].HandlerStartMS })
	return result, nil
}

// queryWindow avoids the CloudWatch Logs Insights 10,000-result limit by using
// short fixed windows. Saturated windows are split further as a safeguard.
// Query boundaries can overlap at whole seconds, so callers must deduplicate
// the decoded events after collecting.
func (c *Collector) queryWindow(ctx context.Context, logGroup, experimentID string, start, end time.Time) ([]string, error) {
	if end.Sub(start) > maximumQueryWindow {
		next := start.Add(maximumQueryWindow)
		left, err := c.queryWindow(ctx, logGroup, experimentID, start, next)
		if err != nil {
			return nil, err
		}
		right, err := c.queryWindow(ctx, logGroup, experimentID, next, end)
		if err != nil {
			return nil, err
		}
		return append(left, right...), nil
	}

	result, err := c.query(ctx, logGroup, experimentID, start, end)
	if err != nil {
		return nil, err
	}
	if result.resultCount < cloudWatchLogsQueryLimit {
		return result.messages, nil
	}
	if end.Sub(start) <= minimumQueryWindow {
		return nil, fmt.Errorf("CloudWatch Logs Insights returned %d results for the minimum query window %s to %s", result.resultCount, start.Format(time.RFC3339), end.Format(time.RFC3339))
	}

	middle := start.Add(end.Sub(start) / 2).Truncate(time.Second)
	if !middle.After(start) || !middle.Before(end) {
		return nil, fmt.Errorf("cannot split saturated query window %s to %s", start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
	left, err := c.queryWindow(ctx, logGroup, experimentID, start, middle)
	if err != nil {
		return nil, err
	}
	right, err := c.queryWindow(ctx, logGroup, experimentID, middle, end)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}

func (c *Collector) query(ctx context.Context, logGroup, experimentID string, start, end time.Time) (logQueryResult, error) {
	out, err := c.client.StartQuery(ctx, &cloudwatchlogs.StartQueryInput{
		LogGroupName: aws.String(logGroup),
		StartTime:    aws.Int64(start.Unix()),
		EndTime:      aws.Int64(end.Unix()),
		Limit:        aws.Int32(cloudWatchLogsQueryLimit),
		QueryString:  aws.String(logQuery(experimentID)),
	})
	if err != nil {
		return logQueryResult{}, err
	}
	for {
		select {
		case <-ctx.Done():
			return logQueryResult{}, ctx.Err()
		default:
		}
		query, err := c.client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: out.QueryId})
		if err != nil {
			return logQueryResult{}, err
		}
		switch query.Status {
		case types.QueryStatusComplete:
			var messages []string
			for _, fields := range query.Results {
				for _, field := range fields {
					if aws.ToString(field.Field) == "@message" {
						messages = append(messages, aws.ToString(field.Value))
					}
				}
			}
			return logQueryResult{messages: messages, resultCount: len(query.Results)}, nil
		case types.QueryStatusFailed, types.QueryStatusCancelled, types.QueryStatusTimeout:
			return logQueryResult{}, fmt.Errorf("CloudWatch Logs Insights query ended with status %s", query.Status)
		}
		pollInterval := c.pollInterval
		if pollInterval <= 0 {
			pollInterval = 2 * time.Second
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return logQueryResult{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func logQuery(experimentID string) string {
	return fmt.Sprintf("fields @message | filter @message like /message_started/ | filter @message like /%s/ | sort @timestamp asc", logsInsightsRegexLiteral(experimentID))
}

func logsInsightsRegexLiteral(value string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "/", "\\/")
	return replacer.Replace(value)
}

func eventKey(entry consumer.EventLog) string {
	if entry.MessageID != "" {
		return entry.Scenario + ":" + entry.MessageID
	}
	return strings.Join([]string{
		entry.ExperimentID,
		entry.Scenario,
		entry.Tenant,
		entry.Phase,
		strconv.Itoa(entry.Sequence),
		strconv.FormatInt(entry.HandlerStartMS, 10),
	}, ":")
}

func decodeEventLog(line string) (consumer.EventLog, bool) {
	var direct consumer.EventLog
	if json.Unmarshal([]byte(line), &direct) == nil && direct.EventType == "message_started" {
		return direct, true
	}
	var envelope struct {
		Record string `json:"record"`
	}
	if json.Unmarshal([]byte(line), &envelope) == nil && envelope.Record != "" {
		trimmed := strings.TrimSpace(envelope.Record)
		if json.Unmarshal([]byte(trimmed), &direct) == nil && direct.EventType == "message_started" {
			return direct, true
		}
	}
	start, end := strings.Index(line, "{"), strings.LastIndex(line, "}")
	if start >= 0 && end > start && json.Unmarshal([]byte(line[start:end+1]), &direct) == nil && direct.EventType == "message_started" {
		return direct, true
	}
	return consumer.EventLog{}, false
}

func WriteCSV(resultsDir string, manifest Manifest, rows []consumer.EventLog) (string, error) {
	path := filepath.Join(resultsDir, manifest.ExperimentID, "events.csv")
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	defer w.Flush()
	_ = w.Write([]string{"experiment_id", "scenario", "tenant", "phase", "sequence", "message_id", "sqs_sent_ms", "handler_started_ms", "dwell_ms", "work_ms"})
	for _, row := range rows {
		_ = w.Write([]string{
			row.ExperimentID, row.Scenario, row.Tenant, row.Phase, strconv.Itoa(row.Sequence), row.MessageID,
			strconv.FormatInt(row.SQSSentMS, 10), strconv.FormatInt(row.HandlerStartMS, 10), strconv.FormatInt(row.DwellMS, 10), strconv.Itoa(row.WorkMS),
		})
	}
	return path, w.Error()
}

type Summary struct {
	Scenario string `json:"scenario"`
	Tenant   string `json:"tenant"`
	Phase    string `json:"phase"`
	Count    int    `json:"count"`
	P50MS    int64  `json:"p50_dwell_ms"`
	P95MS    int64  `json:"p95_dwell_ms"`
	MaxMS    int64  `json:"max_dwell_ms"`
}

type RecoveryEstimate struct {
	Scenario                  string           `json:"scenario"`
	BurstStartedMS            int64            `json:"burst_started_ms"`
	BurstStartSource          string           `json:"burst_start_source"`
	ThresholdSource           string           `json:"threshold_source"`
	BaselineCountByTenant     map[string]int   `json:"baseline_count_by_tenant"`
	BaselineP95MSByTenant     map[string]int64 `json:"baseline_p95_ms_by_tenant"`
	ThresholdMSByTenant       map[string]int64 `json:"low_dwell_threshold_ms_by_tenant"`
	WindowSizePerTenant       int              `json:"window_size_per_tenant"`
	RequiredLowDwellPerTenant int              `json:"required_low_dwell_messages_per_tenant"`
	DisturbanceObserved       bool             `json:"disturbance_observed"`
	RecoveryObserved          bool             `json:"recovery_observed"`
	TenantRecoveryLatencyMS   map[string]int64 `json:"tenant_recovery_latency_ms,omitempty"`
	RecoveryLatencyMS         *int64           `json:"recovery_latency_ms,omitempty"`
}

func WriteSummary(resultsDir string, manifest Manifest, rows []consumer.EventLog) (string, error) {
	return writeSummary(filepath.Join(resultsDir, manifest.ExperimentID, "summary.json"), rows)
}

func WriteObservationSummary(resultsDir string, manifest Manifest, rows []consumer.EventLog) (string, error) {
	scenarioSet := make(map[string]struct{}, len(manifest.Scenarios))
	for _, scenario := range manifest.Scenarios {
		scenarioSet[scenario] = struct{}{}
	}
	for _, row := range rows {
		scenarioSet[row.Scenario] = struct{}{}
	}
	burstStarts := make(map[string]int64, len(scenarioSet))
	for scenario := range scenarioSet {
		burstStart, _, err := burstStartForScenario(manifest, scenario, rows)
		if err != nil {
			return "", err
		}
		burstStarts[scenario] = burstStart.UnixMilli()
	}
	filtered := make([]consumer.EventLog, 0, len(rows))
	for _, row := range rows {
		if row.Phase == "baseline" {
			continue
		}
		burstStartMS, ok := burstStarts[row.Scenario]
		if !ok {
			continue
		}
		observationEndMS := burstStartMS + int64(manifest.ObservationWindowMS)
		// The observation window describes when probes were submitted. Keep a
		// probe even when queueing delay pushes its handler start past the end of
		// the window; excluding those rows would preferentially discard the
		// slowest messages and bias dwell-time summaries downward.
		if row.SQSSentMS <= observationEndMS {
			filtered = append(filtered, row)
		}
	}
	return writeSummary(filepath.Join(resultsDir, manifest.ExperimentID, "observation-summary.json"), filtered)
}

func writeSummary(path string, rows []consumer.EventLog) (string, error) {
	grouped := map[string][]int64{}
	for _, row := range rows {
		key := row.Scenario + "\x00" + row.Tenant + "\x00" + row.Phase
		grouped[key] = append(grouped[key], row.DwellMS)
	}
	var summaries []Summary
	for key, values := range grouped {
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		parts := strings.Split(key, "\x00")
		summaries = append(summaries, Summary{
			Scenario: parts[0], Tenant: parts[1], Phase: parts[2], Count: len(values),
			P50MS: percentile(values, 0.50), P95MS: percentile(values, 0.95), MaxMS: values[len(values)-1],
		})
	}
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].Scenario == summaries[j].Scenario {
			if summaries[i].Tenant == summaries[j].Tenant {
				return summaries[i].Phase < summaries[j].Phase
			}
			return summaries[i].Tenant < summaries[j].Tenant
		}
		return summaries[i].Scenario < summaries[j].Scenario
	})
	data, err := json.MarshalIndent(summaries, "", "  ")
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, append(data, '\n'), 0o644)
}

func WriteRecoveryEstimates(resultsDir string, manifest Manifest, rows []consumer.EventLog) (string, error) {
	const windowSize = 10
	const requiredLow = 8

	estimates := make([]RecoveryEstimate, 0, len(manifest.Scenarios))
	for _, scenario := range manifest.Scenarios {
		burstStart, burstStartSource, err := burstStartForScenario(manifest, scenario, rows)
		if err != nil {
			return "", err
		}
		observationEndMS := burstStart.UnixMilli() + int64(manifest.ObservationWindowMS)
		estimate := RecoveryEstimate{
			Scenario: scenario, BurstStartedMS: burstStart.UnixMilli(), BurstStartSource: burstStartSource,
			ThresholdSource: "baseline_p95", BaselineCountByTenant: map[string]int{},
			BaselineP95MSByTenant: map[string]int64{}, ThresholdMSByTenant: map[string]int64{},
			WindowSizePerTenant: windowSize, RequiredLowDwellPerTenant: requiredLow,
			TenantRecoveryLatencyMS: map[string]int64{},
		}

		baselineByTenant := map[string][]int64{"B": {}, "C": {}}
		probesByTenant := map[string][]consumer.EventLog{"B": {}, "C": {}}
		for _, row := range rows {
			if row.Scenario != scenario || (row.Tenant != "B" && row.Tenant != "C") {
				continue
			}
			switch row.Phase {
			case "baseline":
				baselineByTenant[row.Tenant] = append(baselineByTenant[row.Tenant], row.DwellMS)
			case "probe":
				if row.SQSSentMS <= observationEndMS {
					probesByTenant[row.Tenant] = append(probesByTenant[row.Tenant], row)
				}
			}
		}

		for _, tenant := range []string{"B", "C"} {
			values := baselineByTenant[tenant]
			sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
			estimate.BaselineCountByTenant[tenant] = len(values)
			if len(values) == 0 {
				estimate.ThresholdSource = "fallback_2x_work_ms"
				estimate.ThresholdMSByTenant[tenant] = int64(manifest.WorkMS * 2)
			} else {
				p95 := percentile(values, 0.95)
				estimate.BaselineP95MSByTenant[tenant] = p95
				estimate.ThresholdMSByTenant[tenant] = max(p95*2, p95+250)
			}
			sort.Slice(probesByTenant[tenant], func(i, j int) bool {
				return probesByTenant[tenant][i].HandlerStartMS < probesByTenant[tenant][j].HandlerStartMS
			})
		}

		disturbedAtMS := int64(0)
		for _, tenant := range []string{"B", "C"} {
			threshold := estimate.ThresholdMSByTenant[tenant]
			for _, probe := range probesByTenant[tenant] {
				if probe.DwellMS > threshold && (disturbedAtMS == 0 || probe.HandlerStartMS < disturbedAtMS) {
					disturbedAtMS = probe.HandlerStartMS
				}
			}
		}
		estimate.DisturbanceObserved = disturbedAtMS != 0
		if estimate.DisturbanceObserved {
			latestRecoveryAtMS := int64(0)
			for _, tenant := range []string{"B", "C"} {
				recoveryAtMS, ok := recoveryTime(probesByTenant[tenant], estimate.ThresholdMSByTenant[tenant], disturbedAtMS, windowSize, requiredLow)
				if !ok {
					latestRecoveryAtMS = 0
					break
				}
				latency := recoveryAtMS - burstStart.UnixMilli()
				estimate.TenantRecoveryLatencyMS[tenant] = latency
				latestRecoveryAtMS = max(latestRecoveryAtMS, recoveryAtMS)
			}
			if latestRecoveryAtMS > 0 && len(estimate.TenantRecoveryLatencyMS) == 2 {
				latency := latestRecoveryAtMS - burstStart.UnixMilli()
				estimate.RecoveryObserved = true
				estimate.RecoveryLatencyMS = &latency
			}
		}
		estimates = append(estimates, estimate)
	}

	path := filepath.Join(resultsDir, manifest.ExperimentID, "recovery-estimate.json")
	data, err := json.MarshalIndent(estimates, "", "  ")
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, append(data, '\n'), 0o644)
}

func recoveryTime(probes []consumer.EventLog, threshold, disturbedAtMS int64, windowSize, requiredLow int) (int64, bool) {
	for windowStart := 0; windowStart+windowSize <= len(probes); windowStart++ {
		if probes[windowStart].HandlerStartMS <= disturbedAtMS {
			continue
		}
		lowCount := 0
		for _, probe := range probes[windowStart : windowStart+windowSize] {
			if probe.DwellMS <= threshold {
				lowCount++
			}
		}
		if lowCount >= requiredLow {
			// Recovery is only established once the complete qualifying window has
			// been observed, so report the final message in that window.
			return probes[windowStart+windowSize-1].HandlerStartMS, true
		}
	}
	return 0, false
}

func burstStartForScenario(manifest Manifest, scenario string, rows []consumer.EventLog) (time.Time, string, error) {
	var firstSentMS int64
	for _, row := range rows {
		if row.Scenario == scenario && row.Tenant == "A" && row.Phase == "burst" && row.SQSSentMS > 0 && (firstSentMS == 0 || row.SQSSentMS < firstSentMS) {
			firstSentMS = row.SQSSentMS
		}
	}
	if firstSentMS > 0 {
		return time.UnixMilli(firstSentMS).UTC(), "first_a_sqs_sent_timestamp", nil
	}
	start, err := manifest.BurstStartTime(scenario)
	if err != nil {
		return time.Time{}, "", err
	}
	source := "manifest_started_at_fallback"
	if timing, ok := manifest.ScenarioTimings[scenario]; ok && timing.BurstStartedAt != "" {
		source = "first_a_batch_accepted"
	}
	return start, source, nil
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}
