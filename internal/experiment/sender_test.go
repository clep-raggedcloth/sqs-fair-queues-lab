package experiment

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/message"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type fakeSQSClient struct {
	batchCalls atomic.Int32
}

type missingAttributeSQSClient struct{ fakeSQSClient }

func (f *missingAttributeSQSClient) GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{}}, nil
}

func (f *fakeSQSClient) SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeSQSClient) SendMessageBatch(context.Context, *sqs.SendMessageBatchInput, ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	f.batchCalls.Add(1)
	return &sqs.SendMessageBatchOutput{}, nil
}

func (f *fakeSQSClient) GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{
		"ApproximateNumberOfMessages":           "42",
		"ApproximateNumberOfMessagesNotVisible": "29",
	}}, nil
}

func TestSampleQueueDepthsRecordsApproximateAttributes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	samples, err := NewSender(&fakeSQSClient{}).SampleQueueDepths(ctx, map[string]Scenario{
		"fair-c20": {QueueURL: "queue"},
	}, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) < 2 {
		t.Fatalf("sample count = %d, want at least 2", len(samples))
	}
	for _, sample := range samples {
		if sample.Scenario != "fair-c20" || sample.ApproximateVisible != 42 || sample.ApproximateNotVisible != 29 || sample.Status != QueueSampleStatusOK {
			t.Fatalf("unexpected sample: %+v", sample)
		}
	}
}

func TestSampleQueueDepthsRecordsErrorsBeforeConsecutiveFailure(t *testing.T) {
	samples, err := NewSender(&missingAttributeSQSClient{}).SampleQueueDepths(context.Background(), map[string]Scenario{
		"fair-c20": {QueueURL: "queue"},
	}, time.Millisecond)
	if err == nil {
		t.Fatal("SampleQueueDepths() error = nil, want consecutive-failure error")
	}
	if len(samples) != maxConsecutiveQueueSampleErrorRounds {
		t.Fatalf("sample count = %d, want %d", len(samples), maxConsecutiveQueueSampleErrorRounds)
	}
	for _, sample := range samples {
		if sample.Status != QueueSampleStatusError || sample.Error == "" {
			t.Fatalf("unexpected error sample: %+v", sample)
		}
	}
}

func TestParseQueueAttributeRejectsMissingMalformedAndNegativeValues(t *testing.T) {
	name := types.QueueAttributeNameApproximateNumberOfMessagesNotVisible
	tests := []struct {
		name       string
		attributes map[string]string
		want       int
		wantError  bool
	}{
		{name: "valid", attributes: map[string]string{string(name): "29"}, want: 29},
		{name: "missing", attributes: map[string]string{}, wantError: true},
		{name: "malformed", attributes: map[string]string{string(name): "unknown"}, wantError: true},
		{name: "negative", attributes: map[string]string{string(name): "-1"}, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseQueueAttribute(test.attributes, name)
			if (err != nil) != test.wantError {
				t.Fatalf("parseQueueAttribute() error = %v, wantError=%v", err, test.wantError)
			}
			if got != test.want {
				t.Fatalf("parseQueueAttribute() = %d, want %d", got, test.want)
			}
		})
	}
}

func (f *fakeSQSClient) PurgeQueue(context.Context, *sqs.PurgeQueueInput, ...func(*sqs.Options)) (*sqs.PurgeQueueOutput, error) {
	return &sqs.PurgeQueueOutput{}, nil
}

func TestSendManyWithFirstAcceptedCallsBarrierOnce(t *testing.T) {
	client := &fakeSQSClient{}
	works := make([]message.Work, 25)
	for i := range works {
		works[i] = message.New("exp", "fair-c20", "A", "burst", i, 1, time.Now())
	}
	var callbackCalls atomic.Int32
	err := NewSender(client).SendManyWithFirstAccepted(context.Background(), "fair-c20", Scenario{QueueURL: "queue", UseMessageGroupID: true}, works, 4, func(time.Time) {
		callbackCalls.Add(1)
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.batchCalls.Load() != 3 {
		t.Fatalf("batch calls = %d, want 3", client.batchCalls.Load())
	}
	if callbackCalls.Load() != 1 {
		t.Fatalf("callback calls = %d, want 1", callbackCalls.Load())
	}
}

var _ SQSAPI = (*fakeSQSClient)(nil)
