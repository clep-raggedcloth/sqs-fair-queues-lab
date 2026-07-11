package experiment

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/consumer"
)

func TestWriteFairQueueConditionEvidenceReportsCountAndProcessingTimeConditions(t *testing.T) {
	dir := t.TempDir()
	start := time.Unix(5000, 0).UTC()
	manifest := Manifest{
		ExperimentID: "exp", Scenarios: []string{"fair-c100"}, ObservationWindowMS: 120_000,
		StartedAt: start.Format(time.RFC3339Nano),
		ScenarioTimings: map[string]ScenarioTiming{
			"fair-c100": {BurstStartedAt: start.Format(time.RFC3339Nano)},
		},
	}
	if _, err := SaveManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	rows := make([]consumer.EventLog, 0, 100)
	for index := 0; index < 30; index++ {
		rows = append(rows, consumer.EventLog{
			Scenario: "fair-c100", Tenant: "A", Phase: "burst", Sequence: index,
			SQSSentMS: start.UnixMilli(), HandlerStartMS: start.UnixMilli(), WorkMS: 60_000,
		})
	}
	for index := 0; index < 70; index++ {
		rows = append(rows, consumer.EventLog{
			Scenario: "fair-c100", Tenant: "B", Phase: "probe", Sequence: index,
			SQSSentMS: start.UnixMilli(), HandlerStartMS: start.UnixMilli(), WorkMS: 60_000,
		})
	}
	queueSamples := []QueueDepthSample{{
		Scenario: "fair-c100", SampledAt: start.Add(time.Second), ApproximateNotVisible: 100, Status: QueueSampleStatusOK,
	}}
	config := Config{Scenarios: map[string]Scenario{"fair-c100": {UseMessageGroupID: true}}}
	path, err := WriteFairQueueConditionEvidence(dir, config, manifest, rows, queueSamples)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var report FairQueueConditionReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Scenarios) != 1 {
		t.Fatalf("scenario count=%d, want 1", len(report.Scenarios))
	}
	evidence := report.Scenarios[0]
	if !evidence.ConcurrencyShare.BothConditionsObserved || evidence.ConcurrencyShare.FirstBothConditionsElapsedMS == nil ||
		*evidence.ConcurrencyShare.FirstBothConditionsElapsedMS != 1000 {
		t.Fatalf("unexpected concurrency evidence: %+v", evidence.ConcurrencyShare)
	}
	if !evidence.ProcessingTimeShare.ConditionObserved || evidence.ProcessingTimeShare.FirstObservedElapsedMS == nil ||
		*evidence.ProcessingTimeShare.FirstObservedElapsedMS != 1000 || evidence.ProcessingTimeShare.PeakProcessingShare != 0.30 {
		t.Fatalf("unexpected processing-time evidence: %+v", evidence.ProcessingTimeShare)
	}
}

func TestFairQueueConditionEvidenceMarksBaselineNotApplicable(t *testing.T) {
	dir := t.TempDir()
	start := time.Unix(5500, 0).UTC()
	manifest := Manifest{
		ExperimentID: "exp", Scenarios: []string{"baseline-c20"}, ObservationWindowMS: 1000,
		StartedAt: start.Format(time.RFC3339Nano),
	}
	if _, err := SaveManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	config := Config{Scenarios: map[string]Scenario{"baseline-c20": {UseMessageGroupID: false}}}
	path, err := WriteFairQueueConditionEvidence(dir, config, manifest, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var report FairQueueConditionReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Scenarios) != 1 || report.Scenarios[0].Applicable ||
		report.Scenarios[0].ConcurrencyShare.Status != "not_applicable_without_message_group_id" {
		t.Fatalf("unexpected baseline evidence: %+v", report.Scenarios)
	}
}

func TestProcessingTimeInWindowClipsIntervals(t *testing.T) {
	start := time.Unix(6000, 0).UTC()
	rows := []consumer.EventLog{
		{Scenario: "fair-c20", Tenant: "A", HandlerStartMS: start.Add(-time.Second).UnixMilli(), WorkMS: 2000},
		{Scenario: "fair-c20", Tenant: "B", HandlerStartMS: start.UnixMilli(), WorkMS: 2000},
	}
	aMS, totalMS := processingTimeInWindow(rows, "fair-c20", start, start.Add(time.Second))
	if aMS != 1000 || totalMS != 2000 {
		t.Fatalf("processing time A=%d total=%d, want 1000/2000", aMS, totalMS)
	}
}
