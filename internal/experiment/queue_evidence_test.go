package experiment

import (
	"encoding/csv"
	"os"
	"testing"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/consumer"
)

func TestQueueSamplesRoundTripAndAlignToFirstASentTimestamp(t *testing.T) {
	dir := t.TempDir()
	start := time.Unix(3000, 0).UTC()
	manifest := Manifest{
		ExperimentID: "exp", Scenarios: []string{"fair-c100"}, StartedAt: start.Format(time.RFC3339Nano),
		ScenarioTimings: map[string]ScenarioTiming{
			"fair-c100": {BurstStartedAt: start.Format(time.RFC3339Nano)},
		},
	}
	if _, err := SaveManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	samples := []QueueDepthSample{
		{Scenario: "fair-c100", SampledAt: start.Add(200 * time.Millisecond), ApproximateVisible: 1000, ApproximateNotVisible: 100, Status: QueueSampleStatusOK},
		{Scenario: "fair-c100", SampledAt: start.Add(300 * time.Millisecond), Status: QueueSampleStatusError, Error: "temporary failure"},
	}
	rawPath, err := WriteQueueDepthSamples(dir, manifest, samples)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := ReadQueueDepthSamples(rawPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded[0].ApproximateNotVisible != 100 || loaded[1].Status != QueueSampleStatusError {
		t.Fatalf("unexpected round-trip samples: %+v", loaded)
	}

	rows := make([]consumer.EventLog, 0, 30)
	for i := 0; i < 30; i++ {
		rows = append(rows, consumer.EventLog{
			Scenario: "fair-c100", Tenant: "A", Phase: "burst", SQSSentMS: start.Add(100 * time.Millisecond).UnixMilli(),
			HandlerStartMS: start.Add(100 * time.Millisecond).UnixMilli(), WorkMS: 1000,
		})
	}
	alignedPath, proxyPath, err := WriteAlignedQueueEvidence(dir, manifest, rows, loaded)
	if err != nil {
		t.Fatal(err)
	}
	aligned := readCSVForTest(t, alignedPath)
	if aligned[1][4] != "100" || aligned[1][5] != "first_a_sqs_sent_timestamp" {
		t.Fatalf("unexpected aligned row: %v", aligned[1])
	}
	proxy := readCSVForTest(t, proxyPath)
	if proxy[1][5] != "30" || proxy[1][7] != "0.300000" || proxy[1][10] != "true" || proxy[1][11] != "ok" {
		t.Fatalf("unexpected proxy row: %v", proxy[1])
	}
	if proxy[2][11] != "sample_error" || proxy[2][6] != "" || proxy[2][7] != "" {
		t.Fatalf("unexpected error proxy row: %v", proxy[2])
	}
}

func readCSVForTest(t *testing.T, path string) [][]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}
