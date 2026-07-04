package consumer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/message"
	"github.com/aws/aws-lambda-go/events"
)

func TestHandleLogsDwellTime(t *testing.T) {
	work := message.New("exp-1", "fair-c29", "A", "burst", 7, 250, time.Unix(0, 0))
	body, err := json.Marshal(work)
	if err != nil {
		t.Fatal(err)
	}

	var got string
	h := &Handler{
		now:   func() time.Time { return time.UnixMilli(1500) },
		sleep: func(time.Duration) {},
		log:   func(line string) { got = line },
	}
	err = h.Handle(context.Background(), events.SQSEvent{Records: []events.SQSMessage{{
		MessageId: "message-1",
		Body:      string(body),
		Attributes: map[string]string{
			"SentTimestamp":  "1000",
			"MessageGroupId": "A",
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}

	var entry EventLog
	if err := json.Unmarshal([]byte(got), &entry); err != nil {
		t.Fatal(err)
	}
	if entry.DwellMS != 500 {
		t.Fatalf("DwellMS = %d, want 500", entry.DwellMS)
	}
	if entry.Scenario != "fair-c29" || entry.Tenant != "A" {
		t.Fatalf("unexpected entry: %+v", entry)
	}
}
