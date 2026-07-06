package experiment

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/consumer"
)

func TestDecodeEventLogWithLambdaPrefix(t *testing.T) {
	line := "2026-01-01T00:00:00Z\trequest-id\t{\"event_type\":\"message_started\",\"experiment_id\":\"exp\",\"scenario\":\"fair-c100\"}"
	entry, ok := decodeEventLog(line)
	if !ok {
		t.Fatal("decodeEventLog returned false")
	}
	if entry.ExperimentID != "exp" || entry.Scenario != "fair-c100" {
		t.Fatalf("unexpected entry: %+v", entry)
	}
}

func TestWriteObservationSummaryUsesSendTimeAndKeepsLateStarts(t *testing.T) {
	dir := t.TempDir()
	start := time.Unix(1000, 0).UTC()
	manifest := Manifest{
		ExperimentID:        "exp",
		StartedAt:           start.Format(time.RFC3339Nano),
		ObservationWindowMS: 10_000,
	}
	if _, err := SaveManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	path, err := WriteObservationSummary(dir, manifest, []consumer.EventLog{
		{Scenario: "fair-c29", Tenant: "B", SQSSentMS: start.Add(4 * time.Second).UnixMilli(), HandlerStartMS: start.Add(5 * time.Second).UnixMilli(), DwellMS: 100},
		{Scenario: "fair-c29", Tenant: "B", SQSSentMS: start.Add(8 * time.Second).UnixMilli(), HandlerStartMS: start.Add(20 * time.Second).UnixMilli(), DwellMS: 5000},
		{Scenario: "fair-c29", Tenant: "B", SQSSentMS: start.Add(12 * time.Second).UnixMilli(), HandlerStartMS: start.Add(20 * time.Second).UnixMilli(), DwellMS: 9000},
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var summaries []Summary
	if err := json.Unmarshal(data, &summaries); err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].Count != 2 || summaries[0].MaxMS != 5000 {
		t.Fatalf("unexpected summaries: %+v", summaries)
	}
}

func TestWriteRecoveryEstimatesUsesBaselineAndFirstASentTimestamp(t *testing.T) {
	dir := t.TempDir()
	start := time.Unix(1000, 0).UTC()
	manifest := Manifest{
		ExperimentID: "exp", Scenarios: []string{"fair-c29"}, WorkMS: 2000,
		StartedAt:           start.Add(-20 * time.Second).Format(time.RFC3339Nano),
		ObservationWindowMS: 20_000,
	}
	if _, err := SaveManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	rows := []consumer.EventLog{{
		Scenario: "fair-c29", Tenant: "A", Phase: "burst",
		SQSSentMS: start.UnixMilli(), HandlerStartMS: start.Add(time.Second).UnixMilli(),
	}}
	for _, tenant := range []string{"B", "C"} {
		for i := 0; i < 20; i++ {
			rows = append(rows, consumer.EventLog{
				Scenario: "fair-c29", Tenant: tenant, Phase: "baseline", DwellMS: 100,
				SQSSentMS:      start.Add(-time.Duration(20-i) * time.Second).UnixMilli(),
				HandlerStartMS: start.Add(-time.Duration(20-i)*time.Second).UnixMilli() + 100,
			})
		}
		rows = append(rows, consumer.EventLog{
			Scenario: "fair-c29", Tenant: tenant, Phase: "probe", DwellMS: 1000,
			SQSSentMS: start.Add(time.Second).UnixMilli(), HandlerStartMS: start.Add(2 * time.Second).UnixMilli(),
		})
		for i := 0; i < 10; i++ {
			rows = append(rows, consumer.EventLog{
				Scenario: "fair-c29", Tenant: tenant, Phase: "probe", DwellMS: 100,
				SQSSentMS:      start.Add(time.Duration(2+i) * time.Second).UnixMilli(),
				HandlerStartMS: start.Add(time.Duration(2+i)*time.Second + 100*time.Millisecond).UnixMilli(),
			})
		}
	}

	path, err := WriteRecoveryEstimates(dir, manifest, rows)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var estimates []RecoveryEstimate
	if err := json.Unmarshal(data, &estimates); err != nil {
		t.Fatal(err)
	}
	if len(estimates) != 1 {
		t.Fatalf("estimate count = %d, want 1", len(estimates))
	}
	got := estimates[0]
	if got.BurstStartedMS != start.UnixMilli() || got.BurstStartSource != "first_a_sqs_sent_timestamp" {
		t.Fatalf("unexpected burst start: %+v", got)
	}
	if got.ThresholdMSByTenant["B"] != 350 || got.ThresholdMSByTenant["C"] != 350 {
		t.Fatalf("unexpected thresholds: %+v", got.ThresholdMSByTenant)
	}
	if !got.DisturbanceObserved || !got.RecoveryObserved || len(got.TenantRecoveryLatencyMS) != 2 {
		t.Fatalf("unexpected recovery estimate: %+v", got)
	}
}

func TestBuildHandlerActiveEstimatesReconstructsTenantConcurrency(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	manifest := Manifest{
		ExperimentID: "exp", Scenarios: []string{"fair-c100"},
		StartedAt: start.Format(time.RFC3339Nano),
		ScenarioTimings: map[string]ScenarioTiming{
			"fair-c100": {BurstStartedAt: start.Format(time.RFC3339Nano)},
		},
	}
	rows := []consumer.EventLog{
		{Scenario: "fair-c100", Tenant: "A", HandlerStartMS: start.UnixMilli(), WorkMS: 2000},
		{Scenario: "fair-c100", Tenant: "A", HandlerStartMS: start.Add(time.Second).UnixMilli(), WorkMS: 2000},
		{Scenario: "fair-c100", Tenant: "B", HandlerStartMS: start.Add(time.Second).UnixMilli(), WorkMS: 1000},
	}
	points, summaries, err := BuildHandlerActiveEstimates(manifest, rows)
	if err != nil {
		t.Fatal(err)
	}
	findPoint := func(elapsed int64, tenant string) (HandlerActivePoint, bool) {
		for _, point := range points {
			if point.ElapsedMS == elapsed && point.Tenant == tenant {
				return point, true
			}
		}
		return HandlerActivePoint{}, false
	}
	if point, ok := findPoint(1000, "A"); !ok || point.EstimatedActiveMessages != 2 {
		t.Fatalf("A at 1000ms = %+v, found=%v", point, ok)
	}
	if point, ok := findPoint(1000, allTenants); !ok || point.EstimatedActiveMessages != 3 {
		t.Fatalf("total at 1000ms = %+v, found=%v", point, ok)
	}
	if point, ok := findPoint(2000, "A"); !ok || point.EstimatedActiveMessages != 1 {
		t.Fatalf("A at half-open boundary = %+v, found=%v", point, ok)
	}
	for _, summary := range summaries {
		if summary.Scenario == "fair-c100" && summary.Tenant == "A" {
			if summary.PeakEstimatedActive != 2 || summary.DurationAtOrAbove30MS != 0 {
				t.Fatalf("unexpected A summary: %+v", summary)
			}
			return
		}
	}
	t.Fatal("A summary was not generated")
}

func TestBuildHandlerActiveEstimatesSummarizesCountAndShareCriterion(t *testing.T) {
	start := time.Unix(2000, 0).UTC()
	manifest := Manifest{
		ExperimentID: "exp", Scenarios: []string{"fair-c100"}, StartedAt: start.Format(time.RFC3339Nano),
	}
	rows := make([]consumer.EventLog, 0, 31)
	for i := 0; i < 30; i++ {
		rows = append(rows, consumer.EventLog{
			Scenario: "fair-c100", Tenant: "A", Phase: "burst", SQSSentMS: start.UnixMilli(),
			HandlerStartMS: start.UnixMilli(), WorkMS: 1000,
		})
	}
	rows = append(rows, consumer.EventLog{
		Scenario: "fair-c100", Tenant: "B", Phase: "probe", HandlerStartMS: start.UnixMilli(), WorkMS: 1000,
	})
	_, summaries, err := BuildHandlerActiveEstimates(manifest, rows)
	if err != nil {
		t.Fatal(err)
	}
	for _, summary := range summaries {
		if summary.Tenant != "A" {
			continue
		}
		if summary.PeakEstimatedActive != 30 || summary.PeakHandlerActiveShare <= 0.10 {
			t.Fatalf("unexpected peak summary: %+v", summary)
		}
		if summary.DurationAtOrAbove30MS != 1000 || summary.DurationMeetingHandlerActiveCountAndShareMS != 1000 {
			t.Fatalf("unexpected criterion duration: %+v", summary)
		}
		return
	}
	t.Fatal("A summary was not generated")
}
