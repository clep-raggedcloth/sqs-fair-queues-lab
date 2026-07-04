package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

const metricPeriodSeconds int32 = 60

type MetricsAPI interface {
	GetMetricData(context.Context, *cloudwatch.GetMetricDataInput, ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error)
}

type MetricPoint struct {
	Timestamp string  `json:"timestamp"`
	Value     float64 `json:"value"`
}

type MetricSeries struct {
	Scenario      string            `json:"scenario"`
	Namespace     string            `json:"namespace"`
	MetricName    string            `json:"metric_name"`
	Statistic     string            `json:"statistic"`
	PeriodSeconds int32             `json:"period_seconds"`
	Dimensions    map[string]string `json:"dimensions"`
	Status        string            `json:"status"`
	Points        []MetricPoint     `json:"points"`
}

type metricDefinition struct {
	scenario   string
	namespace  string
	metricName string
	dimensions map[string]string
}

func CollectMetrics(ctx context.Context, client MetricsAPI, config Config, manifest Manifest) ([]MetricSeries, error) {
	start, err := manifest.StartTime()
	if err != nil {
		return nil, err
	}
	end, err := manifest.CompletionTime()
	if err != nil {
		return nil, err
	}

	definitions := make([]metricDefinition, 0, len(manifest.Scenarios)*4)
	for _, scenarioName := range manifest.Scenarios {
		scenario, ok := config.Scenarios[scenarioName]
		if !ok {
			return nil, fmt.Errorf("scenario %q is missing from config", scenarioName)
		}
		queueName, err := scenarioQueueName(scenario)
		if err != nil {
			return nil, fmt.Errorf("scenario %s: %w", scenarioName, err)
		}
		functionName, err := scenarioFunctionName(scenario)
		if err != nil {
			return nil, fmt.Errorf("scenario %s: %w", scenarioName, err)
		}
		for _, metricName := range []string{
			"ApproximateNumberOfNoisyGroups",
			"ApproximateNumberOfMessagesNotVisible",
			"ApproximateNumberOfMessagesNotVisibleInQuietGroups",
		} {
			definitions = append(definitions, metricDefinition{
				scenario: scenarioName, namespace: "AWS/SQS", metricName: metricName,
				dimensions: map[string]string{"QueueName": queueName},
			})
		}
		definitions = append(definitions, metricDefinition{
			scenario: scenarioName, namespace: "AWS/Lambda", metricName: "ConcurrentExecutions",
			dimensions: map[string]string{"FunctionName": functionName},
		})
	}

	queries := make([]types.MetricDataQuery, 0, len(definitions))
	seriesByID := make(map[string]*MetricSeries, len(definitions))
	orderedIDs := make([]string, 0, len(definitions))
	for index, definition := range definitions {
		id := fmt.Sprintf("m%d", index)
		dimensions := make([]types.Dimension, 0, len(definition.dimensions))
		for name, value := range definition.dimensions {
			dimensions = append(dimensions, types.Dimension{Name: aws.String(name), Value: aws.String(value)})
		}
		queries = append(queries, types.MetricDataQuery{
			Id: aws.String(id),
			MetricStat: &types.MetricStat{
				Metric: &types.Metric{
					Namespace: aws.String(definition.namespace), MetricName: aws.String(definition.metricName), Dimensions: dimensions,
				},
				Period: aws.Int32(metricPeriodSeconds), Stat: aws.String("Maximum"),
			},
			ReturnData: aws.Bool(true),
		})
		seriesByID[id] = &MetricSeries{
			Scenario: definition.scenario, Namespace: definition.namespace, MetricName: definition.metricName,
			Statistic: "Maximum", PeriodSeconds: metricPeriodSeconds, Dimensions: definition.dimensions,
			Points: []MetricPoint{},
		}
		orderedIDs = append(orderedIDs, id)
	}

	input := &cloudwatch.GetMetricDataInput{
		StartTime: aws.Time(start.Add(-time.Minute)), EndTime: aws.Time(end.Add(time.Minute)),
		MetricDataQueries: queries, ScanBy: types.ScanByTimestampAscending,
	}
	for {
		output, err := client.GetMetricData(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, result := range output.MetricDataResults {
			series := seriesByID[aws.ToString(result.Id)]
			if series == nil {
				continue
			}
			series.Status = string(result.StatusCode)
			for index := range min(len(result.Timestamps), len(result.Values)) {
				series.Points = append(series.Points, MetricPoint{
					Timestamp: result.Timestamps[index].UTC().Format(time.RFC3339), Value: result.Values[index],
				})
			}
		}
		if output.NextToken == nil || aws.ToString(output.NextToken) == "" {
			break
		}
		input.NextToken = output.NextToken
	}

	result := make([]MetricSeries, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		series := seriesByID[id]
		sort.Slice(series.Points, func(i, j int) bool { return series.Points[i].Timestamp < series.Points[j].Timestamp })
		result = append(result, *series)
	}
	return result, nil
}

func WriteMetrics(resultsDir string, manifest Manifest, series []MetricSeries) (string, error) {
	outputPath := filepath.Join(resultsDir, manifest.ExperimentID, "metrics.json")
	data, err := json.MarshalIndent(series, "", "  ")
	if err != nil {
		return "", err
	}
	return outputPath, os.WriteFile(outputPath, append(data, '\n'), 0o644)
}

func scenarioQueueName(scenario Scenario) (string, error) {
	if scenario.QueueName != "" {
		return scenario.QueueName, nil
	}
	parsed, err := url.Parse(scenario.QueueURL)
	if err != nil {
		return "", fmt.Errorf("parse queue_url: %w", err)
	}
	name := path.Base(strings.TrimSuffix(parsed.Path, "/"))
	if name == "." || name == "/" || name == "" {
		return "", fmt.Errorf("queue_name is missing and cannot be derived from queue_url")
	}
	return name, nil
}

func scenarioFunctionName(scenario Scenario) (string, error) {
	if scenario.FunctionName != "" {
		return scenario.FunctionName, nil
	}
	name := strings.TrimPrefix(scenario.LogGroup, "/aws/lambda/")
	if name == "" || name == scenario.LogGroup {
		return "", fmt.Errorf("function_name is missing and cannot be derived from log_group")
	}
	return name, nil
}
