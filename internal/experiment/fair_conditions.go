package experiment

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/consumer"
)

const (
	fairQueueCountThreshold           = 30
	fairQueueShareThreshold           = 0.10
	processingTimeProxyWindow         = 60 * time.Second
	processingTimeProxySampleInterval = time.Second
)

type ConcurrencyShareConditionEvidence struct {
	CountThreshold                  int     `json:"count_threshold"`
	ShareThreshold                  float64 `json:"share_threshold"`
	ValidSampleCount                int     `json:"valid_sample_count"`
	PeakAHandlerActive              int     `json:"peak_a_handler_active"`
	PeakApproximateNotVisible       int     `json:"peak_approximate_not_visible"`
	PeakCountShareProxy             float64 `json:"peak_count_share_proxy"`
	CountConditionObserved          bool    `json:"count_condition_observed"`
	ShareConditionObserved          bool    `json:"share_condition_observed"`
	BothConditionsObserved          bool    `json:"both_conditions_observed"`
	FirstBothConditionsElapsedMS    *int64  `json:"first_both_conditions_elapsed_ms,omitempty"`
	QueueTotalReachedCountThreshold bool    `json:"queue_total_reached_count_threshold"`
	Status                          string  `json:"status"`
}

type ProcessingTimeConditionEvidence struct {
	ShareThreshold           float64 `json:"share_threshold"`
	ProxyWindowMS            int64   `json:"proxy_window_ms"`
	SampleIntervalMS         int64   `json:"sample_interval_ms"`
	ValidSampleCount         int     `json:"valid_sample_count"`
	PeakProcessingShare      float64 `json:"peak_processing_time_share_proxy"`
	ConditionObserved        bool    `json:"condition_observed"`
	FirstObservedElapsedMS   *int64  `json:"first_observed_elapsed_ms,omitempty"`
	DurationAboveThresholdMS int64   `json:"duration_above_threshold_ms"`
	Status                   string  `json:"status"`
}

type FairQueueConditionEvidence struct {
	Scenario            string                            `json:"scenario"`
	Applicable          bool                              `json:"applicable"`
	ApplicabilityReason string                            `json:"applicability_reason"`
	ConcurrencyShare    ConcurrencyShareConditionEvidence `json:"concurrency_share"`
	ProcessingTimeShare ProcessingTimeConditionEvidence   `json:"processing_time_share"`
}

type FairQueueConditionReport struct {
	Semantics string                       `json:"semantics"`
	Scenarios []FairQueueConditionEvidence `json:"scenarios"`
}

func WriteFairQueueConditionEvidence(resultsDir string, config Config, manifest Manifest, rows []consumer.EventLog, queueSamples []QueueDepthSample) (string, error) {
	points, _, err := BuildHandlerActiveEstimates(manifest, rows)
	if err != nil {
		return "", err
	}
	aPointsByScenario := make(map[string][]HandlerActivePoint)
	for _, point := range points {
		if point.Tenant == "A" {
			aPointsByScenario[point.Scenario] = append(aPointsByScenario[point.Scenario], point)
		}
	}

	report := FairQueueConditionReport{
		Semantics: "Concurrency share uses A handler-active as a lower-bound numerator and SQS ApproximateNumberOfMessagesNotVisible as an approximate denominator. Processing-time share is a 60-second rolling proxy reconstructed from handler work intervals; the AWS internal processing-time window is not public.",
	}
	for _, scenario := range manifest.Scenarios {
		scenarioConfig, ok := config.Scenarios[scenario]
		if !ok {
			return "", fmt.Errorf("scenario %q is missing from config", scenario)
		}
		burstStart, _, err := burstStartForScenario(manifest, scenario, rows)
		if err != nil {
			return "", err
		}
		observationEnd := burstStart.Add(time.Duration(manifest.ObservationWindowMS) * time.Millisecond)
		evidence := FairQueueConditionEvidence{
			Scenario: scenario, Applicable: scenarioConfig.UseMessageGroupID,
			ApplicabilityReason: "MessageGroupId groups messages by tenant",
			ConcurrencyShare: ConcurrencyShareConditionEvidence{
				CountThreshold: fairQueueCountThreshold, ShareThreshold: fairQueueShareThreshold, Status: "no_valid_samples",
			},
			ProcessingTimeShare: ProcessingTimeConditionEvidence{
				ShareThreshold:   fairQueueShareThreshold,
				ProxyWindowMS:    processingTimeProxyWindow.Milliseconds(),
				SampleIntervalMS: processingTimeProxySampleInterval.Milliseconds(),
				Status:           "no_processing_observed",
			},
		}
		if !evidence.Applicable {
			evidence.ApplicabilityReason = "not applicable: messages without MessageGroupId are not grouped as tenant A by SQS"
			evidence.ConcurrencyShare.Status = "not_applicable_without_message_group_id"
			evidence.ProcessingTimeShare.Status = "not_applicable_without_message_group_id"
			report.Scenarios = append(report.Scenarios, evidence)
			continue
		}

		for _, sample := range queueSamples {
			if sample.Scenario != scenario || sample.Status != QueueSampleStatusOK || sample.SampledAt.Before(burstStart) || sample.SampledAt.After(observationEnd) {
				continue
			}
			condition := &evidence.ConcurrencyShare
			condition.ValidSampleCount++
			aActive := activeCountAt(aPointsByScenario[scenario], sample.SampledAt.UnixMilli())
			condition.PeakAHandlerActive = max(condition.PeakAHandlerActive, aActive)
			condition.PeakApproximateNotVisible = max(condition.PeakApproximateNotVisible, sample.ApproximateNotVisible)
			if aActive >= fairQueueCountThreshold {
				condition.CountConditionObserved = true
			}
			if sample.ApproximateNotVisible >= fairQueueCountThreshold {
				condition.QueueTotalReachedCountThreshold = true
			}
			if sample.ApproximateNotVisible <= 0 {
				continue
			}
			share := float64(aActive) / float64(sample.ApproximateNotVisible)
			condition.PeakCountShareProxy = max(condition.PeakCountShareProxy, share)
			if share > fairQueueShareThreshold {
				condition.ShareConditionObserved = true
			}
			if aActive >= fairQueueCountThreshold && share > fairQueueShareThreshold {
				condition.BothConditionsObserved = true
				elapsed := sample.SampledAt.UnixMilli() - burstStart.UnixMilli()
				if condition.FirstBothConditionsElapsedMS == nil || elapsed < *condition.FirstBothConditionsElapsedMS {
					condition.FirstBothConditionsElapsedMS = &elapsed
				}
			}
		}
		switch {
		case evidence.ConcurrencyShare.BothConditionsObserved:
			evidence.ConcurrencyShare.Status = "both_conditions_observed"
		case evidence.ConcurrencyShare.ValidSampleCount > 0 && evidence.ConcurrencyShare.QueueTotalReachedCountThreshold:
			evidence.ConcurrencyShare.Status = "not_observed_but_route_cannot_be_excluded"
		case evidence.ConcurrencyShare.ValidSampleCount > 0:
			evidence.ConcurrencyShare.Status = "not_observed_in_samples"
		}

		processing := &evidence.ProcessingTimeShare
		for sampledAt := burstStart.Add(processingTimeProxySampleInterval); !sampledAt.After(observationEnd); sampledAt = sampledAt.Add(processingTimeProxySampleInterval) {
			aProcessingMS, totalProcessingMS := processingTimeInWindow(rows, scenario, sampledAt.Add(-processingTimeProxyWindow), sampledAt)
			if totalProcessingMS <= 0 {
				continue
			}
			processing.ValidSampleCount++
			share := float64(aProcessingMS) / float64(totalProcessingMS)
			processing.PeakProcessingShare = max(processing.PeakProcessingShare, share)
			if share > fairQueueShareThreshold {
				processing.ConditionObserved = true
				processing.DurationAboveThresholdMS += processingTimeProxySampleInterval.Milliseconds()
				if processing.FirstObservedElapsedMS == nil {
					elapsed := sampledAt.UnixMilli() - burstStart.UnixMilli()
					processing.FirstObservedElapsedMS = &elapsed
				}
			}
		}
		if processing.ConditionObserved {
			processing.Status = "condition_observed_by_proxy"
		} else if processing.ValidSampleCount > 0 {
			processing.Status = "not_observed_by_proxy"
		}
		report.Scenarios = append(report.Scenarios, evidence)
	}

	path := filepath.Join(resultsDir, manifest.ExperimentID, "fair-queue-condition-evidence.json")
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode fair queue condition evidence: %w", err)
	}
	return path, os.WriteFile(path, append(data, '\n'), 0o644)
}

func processingTimeInWindow(rows []consumer.EventLog, scenario string, windowStart, windowEnd time.Time) (int64, int64) {
	var aProcessingMS int64
	var totalProcessingMS int64
	windowStartMS := windowStart.UnixMilli()
	windowEndMS := windowEnd.UnixMilli()
	for _, row := range rows {
		if row.Scenario != scenario || row.WorkMS <= 0 {
			continue
		}
		startMS := max(row.HandlerStartMS, windowStartMS)
		endMS := min(row.HandlerStartMS+int64(row.WorkMS), windowEndMS)
		if endMS <= startMS {
			continue
		}
		durationMS := endMS - startMS
		totalProcessingMS += durationMS
		if row.Tenant == "A" {
			aProcessingMS += durationMS
		}
	}
	return aProcessingMS, totalProcessingMS
}
