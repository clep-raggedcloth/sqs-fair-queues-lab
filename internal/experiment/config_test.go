package experiment

import "testing"

func TestPairSupportsC30BoundaryScenarios(t *testing.T) {
	config := Config{Scenarios: map[string]Scenario{
		"fair-c30":     {MaximumConcurrency: 30, UseMessageGroupID: true},
		"baseline-c30": {MaximumConcurrency: 30},
	}}
	pair, err := config.Pair(30)
	if err != nil {
		t.Fatal(err)
	}
	if len(pair) != 2 || !pair["fair-c30"].UseMessageGroupID || pair["baseline-c30"].MaximumConcurrency != 30 {
		t.Fatalf("unexpected pair: %+v", pair)
	}
}
