package experiment

import (
	"encoding/json"
	"fmt"
	"os"
)

type Scenario struct {
	QueueURL           string `json:"queue_url"`
	QueueName          string `json:"queue_name"`
	LogGroup           string `json:"log_group"`
	FunctionName       string `json:"function_name"`
	UseMessageGroupID  bool   `json:"use_message_group_id"`
	MaximumConcurrency int    `json:"maximum_concurrency"`
}

type Config struct {
	Region    string              `json:"region"`
	Scenarios map[string]Scenario `json:"scenarios"`
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if config.Region == "" || len(config.Scenarios) == 0 {
		return Config{}, fmt.Errorf("config must contain region and scenarios")
	}
	return config, nil
}

func (c Config) Pair(maximumConcurrency int) (map[string]Scenario, error) {
	requirements := []struct {
		name              string
		useMessageGroupID bool
	}{
		{name: fmt.Sprintf("fair-c%d", maximumConcurrency), useMessageGroupID: true},
		{name: fmt.Sprintf("baseline-c%d", maximumConcurrency), useMessageGroupID: false},
	}
	result := make(map[string]Scenario, len(requirements))
	for _, requirement := range requirements {
		scenario, ok := c.Scenarios[requirement.name]
		if !ok {
			return nil, fmt.Errorf("scenario %q is missing from config", requirement.name)
		}
		if scenario.MaximumConcurrency != maximumConcurrency {
			return nil, fmt.Errorf("scenario %q has maximum_concurrency %d, want %d", requirement.name, scenario.MaximumConcurrency, maximumConcurrency)
		}
		if scenario.UseMessageGroupID != requirement.useMessageGroupID {
			return nil, fmt.Errorf("scenario %q has use_message_group_id %t, want %t", requirement.name, scenario.UseMessageGroupID, requirement.useMessageGroupID)
		}
		result[requirement.name] = scenario
	}
	return result, nil
}
