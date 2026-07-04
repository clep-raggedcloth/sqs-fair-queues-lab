package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aoiito/sqs-fair-queue-verification/internal/message"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSAPI interface {
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	SendMessageBatch(context.Context, *sqs.SendMessageBatchInput, ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	PurgeQueue(context.Context, *sqs.PurgeQueueInput, ...func(*sqs.Options)) (*sqs.PurgeQueueOutput, error)
}

type Sender struct {
	client SQSAPI
}

func NewSender(client SQSAPI) *Sender { return &Sender{client: client} }

func (s *Sender) SendOne(ctx context.Context, scenarioName string, scenario Scenario, work message.Work) error {
	body, err := json.Marshal(work)
	if err != nil {
		return err
	}
	input := &sqs.SendMessageInput{QueueUrl: aws.String(scenario.QueueURL), MessageBody: aws.String(string(body))}
	if scenario.UseMessageGroupID {
		input.MessageGroupId = aws.String(work.Tenant)
	}
	_, err = s.client.SendMessage(ctx, input)
	if err != nil {
		return fmt.Errorf("send to %s: %w", scenarioName, err)
	}
	return nil
}

func (s *Sender) SendMany(ctx context.Context, scenarioName string, scenario Scenario, works []message.Work, workers int) error {
	if workers < 1 {
		workers = 1
	}
	type job struct {
		entries []types.SendMessageBatchRequestEntry
	}
	batches := make([]job, 0, (len(works)+9)/10)
	for start := 0; start < len(works); start += 10 {
		end := min(start+10, len(works))
		entries := make([]types.SendMessageBatchRequestEntry, 0, end-start)
		for i, work := range works[start:end] {
			body, err := json.Marshal(work)
			if err != nil {
				return err
			}
			entry := types.SendMessageBatchRequestEntry{
				Id:          aws.String(strconv.Itoa(i)),
				MessageBody: aws.String(string(body)),
			}
			if scenario.UseMessageGroupID {
				entry.MessageGroupId = aws.String(work.Tenant)
			}
			entries = append(entries, entry)
		}
		batches = append(batches, job{entries: entries})
	}

	jobs := make(chan job, len(batches))
	for _, batch := range batches {
		jobs <- batch
	}
	close(jobs)

	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				out, err := s.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
					QueueUrl: aws.String(scenario.QueueURL),
					Entries:  batch.entries,
				})
				if err != nil {
					errOnce.Do(func() { firstErr = fmt.Errorf("send batch to %s: %w", scenarioName, err) })
					return
				}
				if len(out.Failed) > 0 {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("send batch to %s: %d entries failed; first=%s", scenarioName, len(out.Failed), aws.ToString(out.Failed[0].Message))
					})
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func (s *Sender) EnsureEmpty(ctx context.Context, scenarios map[string]Scenario) error {
	for name, scenario := range scenarios {
		visible, inFlight, err := s.queueDepth(ctx, scenario)
		if err != nil {
			return fmt.Errorf("inspect %s: %w", name, err)
		}
		if visible > 0 || inFlight > 0 {
			return fmt.Errorf("%s is not empty (visible=%d, in-flight=%d); wait or run purge", name, visible, inFlight)
		}
	}
	return nil
}

func (s *Sender) WaitForDrain(ctx context.Context, scenarios map[string]Scenario) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	consecutiveEmpty := 0
	for {
		allEmpty := true
		for _, scenario := range scenarios {
			visible, inFlight, err := s.queueDepth(ctx, scenario)
			if err != nil {
				return err
			}
			if visible > 0 || inFlight > 0 {
				allEmpty = false
			}
		}
		if allEmpty {
			consecutiveEmpty++
			if consecutiveEmpty >= 2 {
				return nil
			}
		} else {
			consecutiveEmpty = 0
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Sender) Purge(ctx context.Context, scenarios map[string]Scenario) error {
	for name, scenario := range scenarios {
		if _, err := s.client.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: aws.String(scenario.QueueURL)}); err != nil {
			return fmt.Errorf("purge %s: %w", name, err)
		}
	}
	return nil
}

func (s *Sender) queueDepth(ctx context.Context, scenario Scenario) (int, int, error) {
	out, err := s.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(scenario.QueueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		return 0, 0, err
	}
	visible, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	inFlight, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	return visible, inFlight, nil
}
