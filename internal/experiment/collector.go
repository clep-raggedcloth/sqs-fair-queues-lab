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

type Collector struct{ client LogsAPI }

func NewCollector(client LogsAPI) *Collector { return &Collector{client: client} }

func (c *Collector) Collect(ctx context.Context, config Config, manifest Manifest) ([]consumer.EventLog, error) {
	start, err := manifest.StartTime()
	if err != nil {
		return nil, err
	}
	end, err := manifest.CompletionTime()
	if err != nil {
		return nil, err
	}
	var result []consumer.EventLog
	for _, name := range manifest.Scenarios {
		scenario, ok := config.Scenarios[name]
		if !ok {
			return nil, fmt.Errorf("scenario %q is missing from config", name)
		}
		rows, err := c.query(ctx, scenario.LogGroup, start.Add(-time.Minute), end.Add(time.Minute))
		if err != nil {
			return nil, fmt.Errorf("query %s: %w", name, err)
		}
		for _, row := range rows {
			entry, ok := decodeEventLog(row)
			if ok && entry.ExperimentID == manifest.ExperimentID {
				result = append(result, entry)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].HandlerStartMS < result[j].HandlerStartMS })
	return result, nil
}

func (c *Collector) query(ctx context.Context, logGroup string, start, end time.Time) ([]string, error) {
	out, err := c.client.StartQuery(ctx, &cloudwatchlogs.StartQueryInput{
		LogGroupName: aws.String(logGroup),
		StartTime:    aws.Int64(start.Unix()),
		EndTime:      aws.Int64(end.Unix()),
		Limit:        aws.Int32(10000),
		QueryString:  aws.String("fields @message | filter @message like /message_started/ | sort @timestamp asc"),
	})
	if err != nil {
		return nil, err
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
		query, err := c.client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: out.QueryId})
		if err != nil {
			return nil, err
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
			return messages, nil
		case types.QueryStatusFailed, types.QueryStatusCancelled, types.QueryStatusTimeout:
			return nil, fmt.Errorf("CloudWatch Logs Insights query ended with status %s", query.Status)
		}
	}
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
	Count    int    `json:"count"`
	P50MS    int64  `json:"p50_dwell_ms"`
	P95MS    int64  `json:"p95_dwell_ms"`
	MaxMS    int64  `json:"max_dwell_ms"`
}

type RecoveryEstimate struct {
	Scenario                 string `json:"scenario"`
	ThresholdMS              int64  `json:"low_dwell_threshold_ms"`
	WindowSize               int    `json:"window_size"`
	RequiredLowDwellMessages int    `json:"required_low_dwell_messages"`
	DisturbanceObserved      bool   `json:"disturbance_observed"`
	RecoveryObserved         bool   `json:"recovery_observed"`
	RecoveryLatencyMS        *int64 `json:"recovery_latency_ms,omitempty"`
}

func WriteSummary(resultsDir string, manifest Manifest, rows []consumer.EventLog) (string, error) {
	return writeSummary(filepath.Join(resultsDir, manifest.ExperimentID, "summary.json"), rows)
}

func WriteObservationSummary(resultsDir string, manifest Manifest, rows []consumer.EventLog) (string, error) {
	start, err := manifest.StartTime()
	if err != nil {
		return "", err
	}
	observationEndMS := start.UnixMilli() + int64(manifest.ObservationWindowMS)
	filtered := make([]consumer.EventLog, 0, len(rows))
	for _, row := range rows {
		if row.HandlerStartMS <= observationEndMS {
			filtered = append(filtered, row)
		}
	}
	return writeSummary(filepath.Join(resultsDir, manifest.ExperimentID, "observation-summary.json"), filtered)
}

func writeSummary(path string, rows []consumer.EventLog) (string, error) {
	grouped := map[string][]int64{}
	for _, row := range rows {
		key := row.Scenario + "\x00" + row.Tenant
		grouped[key] = append(grouped[key], row.DwellMS)
	}
	var summaries []Summary
	for key, values := range grouped {
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		parts := strings.Split(key, "\x00")
		summaries = append(summaries, Summary{
			Scenario: parts[0], Tenant: parts[1], Count: len(values),
			P50MS: percentile(values, 0.50), P95MS: percentile(values, 0.95), MaxMS: values[len(values)-1],
		})
	}
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].Scenario == summaries[j].Scenario {
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
	start, err := manifest.StartTime()
	if err != nil {
		return "", err
	}
	threshold := int64(manifest.WorkMS * 2)
	observationEndMS := start.UnixMilli() + int64(manifest.ObservationWindowMS)
	const windowSize = 20
	const requiredLow = 16
	grouped := map[string][]consumer.EventLog{}
	for _, row := range rows {
		if row.HandlerStartMS <= observationEndMS && row.Phase == "probe" && (row.Tenant == "B" || row.Tenant == "C") {
			grouped[row.Scenario] = append(grouped[row.Scenario], row)
		}
	}

	estimates := make([]RecoveryEstimate, 0, len(manifest.Scenarios))
	for _, scenario := range manifest.Scenarios {
		probes := grouped[scenario]
		sort.Slice(probes, func(i, j int) bool { return probes[i].HandlerStartMS < probes[j].HandlerStartMS })
		estimate := RecoveryEstimate{
			Scenario: scenario, ThresholdMS: threshold, WindowSize: windowSize,
			RequiredLowDwellMessages: requiredLow,
		}
		disturbedAt := -1
		for index, probe := range probes {
			if probe.DwellMS > threshold {
				disturbedAt = index
				estimate.DisturbanceObserved = true
				break
			}
		}
		if disturbedAt >= 0 {
			for windowStart := disturbedAt + 1; windowStart+windowSize <= len(probes); windowStart++ {
				lowCount := 0
				for _, probe := range probes[windowStart : windowStart+windowSize] {
					if probe.DwellMS <= threshold {
						lowCount++
					}
				}
				if lowCount >= requiredLow {
					latency := probes[windowStart].HandlerStartMS - start.UnixMilli()
					estimate.RecoveryObserved = true
					estimate.RecoveryLatencyMS = &latency
					break
				}
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

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}
