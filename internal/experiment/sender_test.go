package experiment

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/message"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type fakeSQSClient struct {
	batchCalls atomic.Int32
}

func (f *fakeSQSClient) SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeSQSClient) SendMessageBatch(context.Context, *sqs.SendMessageBatchInput, ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	f.batchCalls.Add(1)
	return &sqs.SendMessageBatchOutput{}, nil
}

func (f *fakeSQSClient) GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	return &sqs.GetQueueAttributesOutput{}, nil
}

func (f *fakeSQSClient) PurgeQueue(context.Context, *sqs.PurgeQueueInput, ...func(*sqs.Options)) (*sqs.PurgeQueueOutput, error) {
	return &sqs.PurgeQueueOutput{}, nil
}

func TestSendManyWithFirstAcceptedCallsBarrierOnce(t *testing.T) {
	client := &fakeSQSClient{}
	works := make([]message.Work, 25)
	for i := range works {
		works[i] = message.New("exp", "fair-c29", "A", "burst", i, 1, time.Now())
	}
	var callbackCalls atomic.Int32
	err := NewSender(client).SendManyWithFirstAccepted(context.Background(), "fair-c29", Scenario{QueueURL: "queue", UseMessageGroupID: true}, works, 4, func(time.Time) {
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
