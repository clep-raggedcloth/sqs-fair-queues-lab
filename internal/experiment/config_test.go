package experiment

import "testing"

func TestPairSupportsC20FairAndBaselineScenarios(t *testing.T) {
	config := Config{Scenarios: map[string]Scenario{
		"fair-c20":     {MaximumConcurrency: 20, UseMessageGroupID: true},
		"baseline-c20": {MaximumConcurrency: 20},
	}}
	pair, err := config.Pair(20)
	if err != nil {
		t.Fatal(err)
	}
	if len(pair) != 2 || !pair["fair-c20"].UseMessageGroupID || pair["baseline-c20"].MaximumConcurrency != 20 {
		t.Fatalf("unexpected pair: %+v", pair)
	}
}

func TestPairRejectsInvalidFairBaselineConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		scenarios map[string]Scenario
	}{
		{
			name: "wrong maximum concurrency",
			scenarios: map[string]Scenario{
				"fair-c20":     {MaximumConcurrency: 29, UseMessageGroupID: true},
				"baseline-c20": {MaximumConcurrency: 20},
			},
		},
		{
			name: "fair scenario without message group ID",
			scenarios: map[string]Scenario{
				"fair-c20":     {MaximumConcurrency: 20},
				"baseline-c20": {MaximumConcurrency: 20},
			},
		},
		{
			name: "baseline scenario with message group ID",
			scenarios: map[string]Scenario{
				"fair-c20":     {MaximumConcurrency: 20, UseMessageGroupID: true},
				"baseline-c20": {MaximumConcurrency: 20, UseMessageGroupID: true},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := (Config{Scenarios: test.scenarios}).Pair(20); err == nil {
				t.Fatal("Pair() succeeded with invalid fair/baseline configuration")
			}
		})
	}
}
