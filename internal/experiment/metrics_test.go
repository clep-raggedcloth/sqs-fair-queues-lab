package experiment

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

type fakeMetricsClient struct {
	input *cloudwatch.GetMetricDataInput
}

func (f *fakeMetricsClient) GetMetricData(_ context.Context, input *cloudwatch.GetMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	f.input = input
	return &cloudwatch.GetMetricDataOutput{MetricDataResults: []types.MetricDataResult{
		{
			Id: aws.String("m0"), StatusCode: types.StatusCodeComplete,
			Timestamps: []time.Time{time.Unix(120, 0)}, Values: []float64{1},
		},
		{
			Id: aws.String("m3"), StatusCode: types.StatusCodeComplete,
			Timestamps: []time.Time{time.Unix(120, 0)}, Values: []float64{29},
		},
	}}, nil
}

func TestCollectMetricsBuildsSQSAndLambdaQueries(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	client := &fakeMetricsClient{}
	series, err := CollectMetrics(context.Background(), client, Config{Scenarios: map[string]Scenario{
		"fair-c20": {
			QueueURL:     "https://sqs.ap-northeast-1.amazonaws.com/123456789012/queue-from-url",
			LogGroup:     "/aws/lambda/function-from-log-group",
			QueueName:    "fair-queue",
			FunctionName: "fair-function",
		},
	}}, Manifest{
		Scenarios: []string{"fair-c20"}, StartedAt: start.Format(time.RFC3339Nano), CompletedAt: start.Add(3 * time.Minute).Format(time.RFC3339Nano),
		ObservationWindowMS: 120_000,
		ScenarioTimings: map[string]ScenarioTiming{
			"fair-c20": {BurstStartedAt: start.Add(20 * time.Second).Format(time.RFC3339Nano)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(client.input.MetricDataQueries) != 4 {
		t.Fatalf("query count = %d, want 4", len(client.input.MetricDataQueries))
	}
	if len(series) != 4 {
		t.Fatalf("series count = %d, want 4", len(series))
	}
	if series[0].MetricName != "ApproximateNumberOfNoisyGroups" || len(series[0].Points) != 1 || series[0].Points[0].Value != 1 {
		t.Fatalf("unexpected SQS series: %+v", series[0])
	}
	if point := series[0].Points[0]; point.Phase != "measurement" || point.ElapsedMS != 0 {
		t.Fatalf("unexpected metric phase alignment: %+v", point)
	}
	if series[3].MetricName != "ConcurrentExecutions" || series[3].Dimensions["FunctionName"] != "fair-function" || series[3].Points[0].Value != 29 {
		t.Fatalf("unexpected Lambda series: %+v", series[3])
	}
}

func TestMetricPointPhaseSeparatesBaselineMeasurementAndDrain(t *testing.T) {
	burstStart := time.Unix(120, 0).UTC()
	observationEnd := burstStart.Add(2 * time.Minute)
	tests := []struct {
		name      string
		timestamp time.Time
		want      string
	}{
		{name: "baseline", timestamp: burstStart.Add(-2 * time.Minute), want: "baseline"},
		{name: "burst boundary overlap", timestamp: burstStart.Add(-30 * time.Second), want: "boundary_overlap"},
		{name: "measurement", timestamp: burstStart, want: "measurement"},
		{name: "observation boundary overlap", timestamp: observationEnd.Add(-30 * time.Second), want: "boundary_overlap"},
		{name: "drain", timestamp: observationEnd, want: "drain"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := metricPointPhase(test.timestamp, burstStart, observationEnd); got != test.want {
				t.Fatalf("metricPointPhase()=%q, want %q", got, test.want)
			}
		})
	}
}

func TestScenarioMetricNamesCanBeDerivedFromExistingConfig(t *testing.T) {
	scenario := Scenario{
		QueueURL: "https://sqs.ap-northeast-1.amazonaws.com/123456789012/fair-c20",
		LogGroup: "/aws/lambda/fair-c20-consumer",
	}
	queueName, err := scenarioQueueName(scenario)
	if err != nil || queueName != "fair-c20" {
		t.Fatalf("queue name = %q, err = %v", queueName, err)
	}
	functionName, err := scenarioFunctionName(scenario)
	if err != nil || functionName != "fair-c20-consumer" {
		t.Fatalf("function name = %q, err = %v", functionName, err)
	}
}
