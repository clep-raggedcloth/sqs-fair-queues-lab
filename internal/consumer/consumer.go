package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/message"
	"github.com/aws/aws-lambda-go/events"
)

type EventLog struct {
	EventType       string `json:"event_type"`
	ExperimentID    string `json:"experiment_id"`
	Scenario        string `json:"scenario"`
	Tenant          string `json:"tenant"`
	Phase           string `json:"phase"`
	Sequence        int    `json:"sequence"`
	MessageID       string `json:"message_id"`
	SQSMessageGroup string `json:"sqs_message_group_id,omitempty"`
	SQSSentMS       int64  `json:"sqs_sent_ms"`
	HandlerStartMS  int64  `json:"handler_started_ms"`
	DwellMS         int64  `json:"dwell_ms"`
	WorkMS          int    `json:"work_ms"`
}

type Handler struct {
	now   func() time.Time
	sleep func(time.Duration)
	log   func(string)
}

func NewHandler(logFn func(string)) *Handler {
	return &Handler{now: time.Now, sleep: time.Sleep, log: logFn}
}

func (h *Handler) Handle(_ context.Context, event events.SQSEvent) error {
	for _, record := range event.Records {
		var work message.Work
		if err := json.Unmarshal([]byte(record.Body), &work); err != nil {
			return fmt.Errorf("decode message %s: %w", record.MessageId, err)
		}
		if work.WorkMS < 0 {
			return fmt.Errorf("message %s has negative work_ms", record.MessageId)
		}

		start := h.now()
		sentMS, err := strconv.ParseInt(record.Attributes["SentTimestamp"], 10, 64)
		if err != nil {
			return fmt.Errorf("parse SentTimestamp for %s: %w", record.MessageId, err)
		}

		entry := EventLog{
			EventType:       "message_started",
			ExperimentID:    work.ExperimentID,
			Scenario:        work.Scenario,
			Tenant:          work.Tenant,
			Phase:           work.Phase,
			Sequence:        work.Sequence,
			MessageID:       record.MessageId,
			SQSMessageGroup: record.Attributes["MessageGroupId"],
			SQSSentMS:       sentMS,
			HandlerStartMS:  start.UnixMilli(),
			DwellMS:         start.UnixMilli() - sentMS,
			WorkMS:          work.WorkMS,
		}
		encoded, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("encode event log: %w", err)
		}
		h.log(string(encoded))
		h.sleep(time.Duration(work.WorkMS) * time.Millisecond)
	}
	return nil
}
