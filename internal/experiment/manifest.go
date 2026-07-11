package experiment

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Manifest struct {
	ExperimentID          string                    `json:"experiment_id"`
	Kind                  string                    `json:"kind"`
	Mode                  string                    `json:"mode,omitempty"`
	RunStatus             string                    `json:"run_status,omitempty"`
	RunError              string                    `json:"run_error,omitempty"`
	Scenarios             []string                  `json:"scenarios"`
	StartedAt             string                    `json:"started_at"`
	CompletedAt           string                    `json:"completed_at"`
	WorkMS                int                       `json:"work_ms"`
	BurstMessages         int                       `json:"burst_messages_per_scenario"`
	ProbeMessages         int                       `json:"probe_messages_per_scenario"`
	ProbeIntervalMS       int                       `json:"probe_interval_ms"`
	QueueSampleIntervalMS int                       `json:"queue_sample_interval_ms"`
	QueueSampleCount      int                       `json:"queue_sample_count"`
	QueueSampleErrorCount int                       `json:"queue_sample_error_count"`
	ObservationWindowMS   int                       `json:"observation_window_ms"`
	WarmupMessages        int                       `json:"warmup_messages_per_scenario"`
	BaselineDurationMS    int                       `json:"baseline_duration_ms"`
	BaselineIntervalMS    int                       `json:"baseline_interval_ms"`
	BaselineMessages      int                       `json:"baseline_messages_per_scenario"`
	MaximumConcurrency    int                       `json:"maximum_concurrency"`
	ScenarioTimings       map[string]ScenarioTiming `json:"scenario_timings,omitempty"`
}

type ScenarioTiming struct {
	BurstStartedAt string `json:"burst_started_at"`
}

func (m Manifest) StartTime() (time.Time, error) {
	return time.Parse(time.RFC3339Nano, m.StartedAt)
}

func (m Manifest) CompletionTime() (time.Time, error) {
	return time.Parse(time.RFC3339Nano, m.CompletedAt)
}

func (m Manifest) BurstStartTime(scenario string) (time.Time, error) {
	if timing, ok := m.ScenarioTimings[scenario]; ok && timing.BurstStartedAt != "" {
		return time.Parse(time.RFC3339Nano, timing.BurstStartedAt)
	}
	return m.StartTime()
}

func SaveManifest(resultsDir string, manifest Manifest) (string, error) {
	dir := filepath.Join(resultsDir, manifest.ExperimentID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create results directory: %w", err)
	}
	path := filepath.Join(dir, "manifest.json")
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode manifest: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return "", fmt.Errorf("write manifest: %w", err)
	}
	return path, nil
}

func LoadManifest(path string) (Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	return manifest, nil
}
