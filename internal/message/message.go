package message

import "time"

// Work is the payload shared by the experiment runner and the consumer.
// SentAt is informative; dwell time is calculated from SQS SentTimestamp.
type Work struct {
	ExperimentID string `json:"experiment_id"`
	Scenario     string `json:"scenario"`
	Tenant       string `json:"tenant"`
	Phase        string `json:"phase"`
	Sequence     int    `json:"sequence"`
	WorkMS       int    `json:"work_ms"`
	SentAt       string `json:"producer_sent_at"`
}

func New(experimentID, scenario, tenant, phase string, sequence, workMS int, now time.Time) Work {
	return Work{
		ExperimentID: experimentID,
		Scenario:     scenario,
		Tenant:       tenant,
		Phase:        phase,
		Sequence:     sequence,
		WorkMS:       workMS,
		SentAt:       now.UTC().Format(time.RFC3339Nano),
	}
}
