package experiment

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/aoiito/sqs-fair-queue-verification/internal/consumer"
)

const allTenants = "_all"

type HandlerActivePoint struct {
	Scenario                string
	Tenant                  string
	TimestampMS             int64
	ElapsedMS               int64
	EstimatedActiveMessages int
	HandlerActiveShare      float64
}

type HandlerActiveSummary struct {
	Scenario                                    string  `json:"scenario"`
	Tenant                                      string  `json:"tenant"`
	PeakEstimatedActive                         int     `json:"peak_estimated_active_messages"`
	PeakHandlerActiveShare                      float64 `json:"peak_handler_active_share"`
	DurationAtOrAbove30MS                       int64   `json:"duration_at_or_above_30_ms"`
	DurationMeetingHandlerActiveCountAndShareMS int64   `json:"duration_meeting_handler_active_count_and_share_ms"`
}

type HandlerActiveReport struct {
	Semantics string                 `json:"semantics"`
	Threshold int                    `json:"threshold"`
	Series    []HandlerActiveSummary `json:"series"`
}

type activeBoundary struct {
	TimestampMS int64
	Tenant      string
	Delta       int
}

// BuildHandlerActiveEstimates reconstructs a guaranteed-active portion of each
// message's SQS in-flight interval. Because BatchSize is one and the handler
// sleeps for WorkMS after logging message_started, every message is active for
// [HandlerStartMS, HandlerStartMS+WorkMS). Time before the handler starts and
// deletion latency after it returns are deliberately excluded.
func BuildHandlerActiveEstimates(manifest Manifest, rows []consumer.EventLog) ([]HandlerActivePoint, []HandlerActiveSummary, error) {
	rowsByScenario := make(map[string][]consumer.EventLog)
	for _, row := range rows {
		rowsByScenario[row.Scenario] = append(rowsByScenario[row.Scenario], row)
	}

	scenarioSet := make(map[string]struct{}, len(manifest.Scenarios)+len(rowsByScenario))
	for _, scenario := range manifest.Scenarios {
		scenarioSet[scenario] = struct{}{}
	}
	for scenario := range rowsByScenario {
		scenarioSet[scenario] = struct{}{}
	}
	scenarios := make([]string, 0, len(scenarioSet))
	for scenario := range scenarioSet {
		scenarios = append(scenarios, scenario)
	}
	sort.Strings(scenarios)

	var points []HandlerActivePoint
	var summaries []HandlerActiveSummary
	for _, scenario := range scenarios {
		scenarioRows := rowsByScenario[scenario]
		if len(scenarioRows) == 0 {
			continue
		}
		burstStart, _, err := burstStartForScenario(manifest, scenario, rows)
		if err != nil {
			return nil, nil, err
		}

		tenantSet := map[string]struct{}{}
		boundaries := make([]activeBoundary, 0, len(scenarioRows)*2)
		for _, row := range scenarioRows {
			tenantSet[row.Tenant] = struct{}{}
			if row.WorkMS <= 0 {
				continue
			}
			boundaries = append(boundaries,
				activeBoundary{TimestampMS: row.HandlerStartMS, Tenant: row.Tenant, Delta: 1},
				activeBoundary{TimestampMS: row.HandlerStartMS + int64(row.WorkMS), Tenant: row.Tenant, Delta: -1},
			)
		}
		if len(boundaries) == 0 {
			continue
		}
		sort.Slice(boundaries, func(i, j int) bool {
			if boundaries[i].TimestampMS == boundaries[j].TimestampMS {
				return boundaries[i].Tenant < boundaries[j].Tenant
			}
			return boundaries[i].TimestampMS < boundaries[j].TimestampMS
		})
		tenants := make([]string, 0, len(tenantSet))
		for tenant := range tenantSet {
			tenants = append(tenants, tenant)
		}
		sort.Strings(tenants)

		counts := make(map[string]int, len(tenants))
		peaks := make(map[string]int, len(tenants)+1)
		peakShares := make(map[string]float64, len(tenants)+1)
		durations := make(map[string]int64, len(tenants)+1)
		criterionDurations := make(map[string]int64, len(tenants)+1)
		for index := 0; index < len(boundaries); {
			timestamp := boundaries[index].TimestampMS
			for index < len(boundaries) && boundaries[index].TimestampMS == timestamp {
				counts[boundaries[index].Tenant] += boundaries[index].Delta
				index++
			}
			total := 0
			for _, tenant := range tenants {
				total += counts[tenant]
			}
			for _, tenant := range tenants {
				share := 0.0
				if total > 0 {
					share = float64(counts[tenant]) / float64(total)
				}
				points = append(points, HandlerActivePoint{
					Scenario: scenario, Tenant: tenant, TimestampMS: timestamp,
					ElapsedMS: timestamp - burstStart.UnixMilli(), EstimatedActiveMessages: counts[tenant], HandlerActiveShare: share,
				})
				peaks[tenant] = max(peaks[tenant], counts[tenant])
				peakShares[tenant] = max(peakShares[tenant], share)
			}
			totalShare := 0.0
			if total > 0 {
				totalShare = 1
			}
			points = append(points, HandlerActivePoint{
				Scenario: scenario, Tenant: allTenants, TimestampMS: timestamp,
				ElapsedMS: timestamp - burstStart.UnixMilli(), EstimatedActiveMessages: total, HandlerActiveShare: totalShare,
			})
			peaks[allTenants] = max(peaks[allTenants], total)
			peakShares[allTenants] = max(peakShares[allTenants], totalShare)

			if index < len(boundaries) {
				duration := boundaries[index].TimestampMS - timestamp
				for _, tenant := range tenants {
					if counts[tenant] >= 30 {
						durations[tenant] += duration
					}
					if total > 0 && counts[tenant] >= 30 && float64(counts[tenant])/float64(total) > 0.10 {
						criterionDurations[tenant] += duration
					}
				}
				if total >= 30 {
					durations[allTenants] += duration
					criterionDurations[allTenants] += duration
				}
			}
		}

		for _, tenant := range append(tenants, allTenants) {
			summaries = append(summaries, HandlerActiveSummary{
				Scenario: scenario, Tenant: tenant, PeakEstimatedActive: peaks[tenant],
				PeakHandlerActiveShare: peakShares[tenant], DurationAtOrAbove30MS: durations[tenant],
				DurationMeetingHandlerActiveCountAndShareMS: criterionDurations[tenant],
			})
		}
	}
	return points, summaries, nil
}

func WriteHandlerActiveEstimates(resultsDir string, manifest Manifest, rows []consumer.EventLog) (string, string, error) {
	points, summaries, err := BuildHandlerActiveEstimates(manifest, rows)
	if err != nil {
		return "", "", err
	}
	dir := filepath.Join(resultsDir, manifest.ExperimentID)
	csvPath := filepath.Join(dir, "handler-active-estimate.csv")
	file, err := os.Create(csvPath)
	if err != nil {
		return "", "", err
	}
	w := csv.NewWriter(file)
	_ = w.Write([]string{"experiment_id", "scenario", "tenant", "timestamp_ms", "elapsed_ms", "estimated_active_messages", "handler_active_share"})
	for _, point := range points {
		_ = w.Write([]string{
			manifest.ExperimentID, point.Scenario, point.Tenant,
			strconv.FormatInt(point.TimestampMS, 10), strconv.FormatInt(point.ElapsedMS, 10),
			strconv.Itoa(point.EstimatedActiveMessages), strconv.FormatFloat(point.HandlerActiveShare, 'f', 6, 64),
		})
	}
	w.Flush()
	writeErr := w.Error()
	closeErr := file.Close()
	if writeErr != nil {
		return "", "", writeErr
	}
	if closeErr != nil {
		return "", "", closeErr
	}

	report := HandlerActiveReport{
		Semantics: "active-message counts are lower-bound estimates from half-open handler work intervals [handler_started_ms, handler_started_ms + work_ms); handler_active_share is only the composition of these intervals and is not a lower bound for SQS in-flight share",
		Threshold: 30,
		Series:    summaries,
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("encode handler-active summary: %w", err)
	}
	summaryPath := filepath.Join(dir, "handler-active-summary.json")
	if err := os.WriteFile(summaryPath, append(data, '\n'), 0o644); err != nil {
		return "", "", err
	}
	return csvPath, summaryPath, nil
}
