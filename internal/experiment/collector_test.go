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

func TestWriteObservationSummaryExcludesLateStarts(t *testing.T) {
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
		{Scenario: "fair-c29", Tenant: "B", HandlerStartMS: start.Add(5 * time.Second).UnixMilli(), DwellMS: 100},
		{Scenario: "fair-c29", Tenant: "B", HandlerStartMS: start.Add(20 * time.Second).UnixMilli(), DwellMS: 5000},
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
	if len(summaries) != 1 || summaries[0].Count != 1 || summaries[0].MaxMS != 100 {
		t.Fatalf("unexpected summaries: %+v", summaries)
	}
}
