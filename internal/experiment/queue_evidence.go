package experiment

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/consumer"
)

func WriteAlignedQueueEvidence(resultsDir string, manifest Manifest, rows []consumer.EventLog, samples []QueueDepthSample) (string, string, error) {
	points, _, err := BuildHandlerActiveEstimates(manifest, rows)
	if err != nil {
		return "", "", err
	}
	aPointsByScenario := make(map[string][]HandlerActivePoint)
	for _, point := range points {
		if point.Tenant == "A" {
			aPointsByScenario[point.Scenario] = append(aPointsByScenario[point.Scenario], point)
		}
	}
	for scenario := range aPointsByScenario {
		sort.Slice(aPointsByScenario[scenario], func(i, j int) bool {
			return aPointsByScenario[scenario][i].TimestampMS < aPointsByScenario[scenario][j].TimestampMS
		})
	}

	type burstReference struct {
		timestampMS int64
		source      string
	}
	burstByScenario := make(map[string]burstReference)
	for _, sample := range samples {
		if _, ok := burstByScenario[sample.Scenario]; ok {
			continue
		}
		burstStart, source, err := burstStartForScenario(manifest, sample.Scenario, rows)
		if err != nil {
			return "", "", err
		}
		burstByScenario[sample.Scenario] = burstReference{timestampMS: burstStart.UnixMilli(), source: source}
	}

	dir := filepath.Join(resultsDir, manifest.ExperimentID)
	alignedPath := filepath.Join(dir, "queue-depth-aligned.csv")
	alignedFile, err := os.Create(alignedPath)
	if err != nil {
		return "", "", err
	}
	aligned := csv.NewWriter(alignedFile)
	_ = aligned.Write([]string{
		"experiment_id", "scenario", "sampled_at", "sampled_at_ms", "elapsed_ms", "elapsed_source",
		"approximate_visible", "approximate_not_visible", "sample_status", "sample_error",
	})

	proxyPath := filepath.Join(dir, "concurrency-share-proxy.csv")
	proxyFile, err := os.Create(proxyPath)
	if err != nil {
		_ = alignedFile.Close()
		return "", "", err
	}
	proxy := csv.NewWriter(proxyFile)
	_ = proxy.Write([]string{
		"experiment_id", "scenario", "sampled_at_ms", "elapsed_ms", "elapsed_source",
		"a_handler_active", "approximate_not_visible", "count_share_proxy",
		"a_count_at_least_30", "share_proxy_above_10_percent", "criteria_proxy_met", "proxy_status",
	})

	for _, sample := range samples {
		burst := burstByScenario[sample.Scenario]
		elapsedMS := sample.SampledAt.UnixMilli() - burst.timestampMS
		visible := ""
		notVisible := ""
		if sample.Status == QueueSampleStatusOK {
			visible = strconv.Itoa(sample.ApproximateVisible)
			notVisible = strconv.Itoa(sample.ApproximateNotVisible)
		}
		_ = aligned.Write([]string{
			manifest.ExperimentID, sample.Scenario, sample.SampledAt.Format(time.RFC3339Nano),
			strconv.FormatInt(sample.SampledAt.UnixMilli(), 10), strconv.FormatInt(elapsedMS, 10), burst.source,
			visible, notVisible, sample.Status, sample.Error,
		})

		aActive := activeCountAt(aPointsByScenario[sample.Scenario], sample.SampledAt.UnixMilli())
		proxyValue := ""
		countAtLeast30 := aActive >= 30
		shareAbove10 := false
		criteriaMet := false
		proxyStatus := "ok"
		if sample.Status != QueueSampleStatusOK {
			proxyStatus = "sample_error"
		} else if sample.ApproximateNotVisible == 0 {
			if aActive > 0 {
				proxyStatus = "inconsistent_zero_denominator"
			} else {
				proxyStatus = "no_in_flight"
			}
		} else {
			share := float64(aActive) / float64(sample.ApproximateNotVisible)
			proxyValue = strconv.FormatFloat(share, 'f', 6, 64)
			shareAbove10 = share > 0.10
			criteriaMet = countAtLeast30 && shareAbove10
		}
		_ = proxy.Write([]string{
			manifest.ExperimentID, sample.Scenario, strconv.FormatInt(sample.SampledAt.UnixMilli(), 10),
			strconv.FormatInt(elapsedMS, 10), burst.source, strconv.Itoa(aActive), notVisible, proxyValue,
			strconv.FormatBool(countAtLeast30), strconv.FormatBool(shareAbove10), strconv.FormatBool(criteriaMet), proxyStatus,
		})
	}

	aligned.Flush()
	proxy.Flush()
	alignedWriteErr := aligned.Error()
	proxyWriteErr := proxy.Error()
	alignedCloseErr := alignedFile.Close()
	proxyCloseErr := proxyFile.Close()
	for _, err := range []error{alignedWriteErr, proxyWriteErr, alignedCloseErr, proxyCloseErr} {
		if err != nil {
			return "", "", fmt.Errorf("write aligned queue evidence: %w", err)
		}
	}
	return alignedPath, proxyPath, nil
}

func activeCountAt(points []HandlerActivePoint, timestampMS int64) int {
	index := sort.Search(len(points), func(i int) bool { return points[i].TimestampMS > timestampMS })
	if index == 0 {
		return 0
	}
	return points[index-1].EstimatedActiveMessages
}
