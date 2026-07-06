package experiment

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

const (
	QueueSampleStatusOK                  = "ok"
	QueueSampleStatusError               = "error"
	maxConsecutiveQueueSampleErrorRounds = 5
)

type QueueDepthSample struct {
	Scenario              string
	SampledAt             time.Time
	ApproximateVisible    int
	ApproximateNotVisible int
	Status                string
	Error                 string
}

// SampleQueueDepths records direct SQS queue-attribute observations throughout
// an experiment. The values remain approximate according to the SQS API; the
// higher sampling frequency is intended to expose short-lived observed peaks,
// not to turn the attributes into an exact counter.
func (s *Sender) SampleQueueDepths(ctx context.Context, scenarios map[string]Scenario, interval time.Duration) ([]QueueDepthSample, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("queue sample interval must be positive")
	}
	names := make([]string, 0, len(scenarios))
	for name := range scenarios {
		names = append(names, name)
	}
	sort.Strings(names)

	var samples []QueueDepthSample
	sampleOnce := func() (bool, error) {
		hadError := false
		for _, name := range names {
			visible, notVisible, err := s.queueDepth(ctx, scenarios[name])
			if err != nil {
				if ctx.Err() != nil {
					return hadError, ctx.Err()
				}
				hadError = true
				samples = append(samples, QueueDepthSample{
					Scenario: name, SampledAt: time.Now().UTC(), Status: QueueSampleStatusError, Error: err.Error(),
				})
				continue
			}
			samples = append(samples, QueueDepthSample{
				Scenario: name, SampledAt: time.Now().UTC(),
				ApproximateVisible: visible, ApproximateNotVisible: notVisible, Status: QueueSampleStatusOK,
			})
		}
		return hadError, nil
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	consecutiveErrorRounds := 0
	for {
		hadError, err := sampleOnce()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return samples, nil
			}
			return samples, err
		}
		if hadError {
			consecutiveErrorRounds++
			if consecutiveErrorRounds >= maxConsecutiveQueueSampleErrorRounds {
				return samples, fmt.Errorf("SQS queue-depth sampling failed for %d consecutive rounds", consecutiveErrorRounds)
			}
		} else {
			consecutiveErrorRounds = 0
		}
		select {
		case <-ctx.Done():
			return samples, nil
		case <-ticker.C:
		}
	}
}

func WriteQueueDepthSamples(resultsDir string, manifest Manifest, samples []QueueDepthSample) (string, error) {
	path := filepath.Join(resultsDir, manifest.ExperimentID, "queue-depth-samples.csv")
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	w := csv.NewWriter(file)
	_ = w.Write([]string{
		"experiment_id", "scenario", "sampled_at", "sampled_at_ms",
		"approximate_visible", "approximate_not_visible", "sample_status", "sample_error",
	})
	for _, sample := range samples {
		visible := ""
		notVisible := ""
		if sample.Status == QueueSampleStatusOK {
			visible = strconv.Itoa(sample.ApproximateVisible)
			notVisible = strconv.Itoa(sample.ApproximateNotVisible)
		}
		_ = w.Write([]string{
			manifest.ExperimentID, sample.Scenario, sample.SampledAt.Format(time.RFC3339Nano),
			strconv.FormatInt(sample.SampledAt.UnixMilli(), 10),
			visible, notVisible, sample.Status, sample.Error,
		})
	}
	w.Flush()
	writeErr := w.Error()
	closeErr := file.Close()
	if writeErr != nil {
		return "", writeErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	return path, nil
}

func ReadQueueDepthSamples(path string) ([]QueueDepthSample, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}
	columns := make(map[string]int, len(header))
	for index, name := range header {
		columns[name] = index
	}
	required := []string{"scenario", "sampled_at_ms", "approximate_visible", "approximate_not_visible"}
	for _, name := range required {
		if _, ok := columns[name]; !ok {
			return nil, fmt.Errorf("queue samples column %q is missing", name)
		}
	}
	var samples []QueueDepthSample
	for rowNumber := 2; ; rowNumber++ {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read queue samples row %d: %w", rowNumber, err)
		}
		value := func(name string) string {
			index, ok := columns[name]
			if !ok || index >= len(row) {
				return ""
			}
			return row[index]
		}
		timestampMS, err := strconv.ParseInt(value("sampled_at_ms"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("queue samples row %d has invalid sampled_at_ms: %w", rowNumber, err)
		}
		status := value("sample_status")
		if status == "" {
			status = QueueSampleStatusOK
		}
		if status != QueueSampleStatusOK && status != QueueSampleStatusError {
			return nil, fmt.Errorf("queue samples row %d has invalid sample_status %q", rowNumber, status)
		}
		sample := QueueDepthSample{
			Scenario: value("scenario"), SampledAt: time.UnixMilli(timestampMS).UTC(),
			Status: status, Error: value("sample_error"),
		}
		if status == QueueSampleStatusOK {
			sample.ApproximateVisible, err = strconv.Atoi(value("approximate_visible"))
			if err != nil {
				return nil, fmt.Errorf("queue samples row %d has invalid approximate_visible: %w", rowNumber, err)
			}
			sample.ApproximateNotVisible, err = strconv.Atoi(value("approximate_not_visible"))
			if err != nil {
				return nil, fmt.Errorf("queue samples row %d has invalid approximate_not_visible: %w", rowNumber, err)
			}
		}
		samples = append(samples, sample)
	}
	return samples, nil
}
