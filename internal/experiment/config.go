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
	names := []string{
		fmt.Sprintf("fair-c%d", maximumConcurrency),
		fmt.Sprintf("baseline-c%d", maximumConcurrency),
	}
	result := make(map[string]Scenario, len(names))
	for _, name := range names {
		scenario, ok := c.Scenarios[name]
		if !ok {
			return nil, fmt.Errorf("scenario %q is missing from config", name)
		}
		result[name] = scenario
	}
	return result, nil
}
