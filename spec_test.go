package flume_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/zoobzio/flume"
	"github.com/zoobzio/pipz"
)

// testData implements pipz.Cloner[testData] for spec tests.
type testData struct {
	Value string
}

func (t testData) Clone() testData {
	return testData{Value: t.Value}
}

func TestSpec_Empty(t *testing.T) {
	factory := flume.New[testData]()
	spec := factory.Spec()

	if len(spec.Processors) != 0 {
		t.Errorf("expected 0 processors, got %d", len(spec.Processors))
	}
	if len(spec.Predicates) != 0 {
		t.Errorf("expected 0 predicates, got %d", len(spec.Predicates))
	}
	if len(spec.Conditions) != 0 {
		t.Errorf("expected 0 conditions, got %d", len(spec.Conditions))
	}
	if len(spec.Channels) != 0 {
		t.Errorf("expected 0 channels, got %d", len(spec.Channels))
	}
	if len(spec.Connectors) == 0 {
		t.Error("expected connectors to be populated")
	}
}

func TestSpec_Processors(t *testing.T) {
	factory := flume.New[testData]()

	// Add without metadata
	factory.Add(
		pipz.Apply[testData]("processor-a", func(_ context.Context, d testData) (testData, error) {
			return d, nil
		}),
	)

	// Add with metadata
	factory.AddWithMeta(flume.ProcessorMeta[testData]{
		Processor: pipz.Apply[testData]("processor-b", func(_ context.Context, d testData) (testData, error) {
			return d, nil
		}),
		Description: "Processes B data",
		Tags:        []string{"processing", "test"},
	})

	spec := factory.Spec()

	if len(spec.Processors) != 2 {
		t.Fatalf("expected 2 processors, got %d", len(spec.Processors))
	}

	// Should be sorted alphabetically
	if spec.Processors[0].Name != "processor-a" {
		t.Errorf("expected first processor to be 'processor-a', got %s", spec.Processors[0].Name)
	}
	if spec.Processors[0].Description != "" {
		t.Errorf("expected empty description for processor-a, got %s", spec.Processors[0].Description)
	}

	if spec.Processors[1].Name != "processor-b" {
		t.Errorf("expected second processor to be 'processor-b', got %s", spec.Processors[1].Name)
	}
	if spec.Processors[1].Description != "Processes B data" {
		t.Errorf("expected description 'Processes B data', got %s", spec.Processors[1].Description)
	}
	if len(spec.Processors[1].Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(spec.Processors[1].Tags))
	}
}

func TestSpec_Predicates(t *testing.T) {
	factory := flume.New[testData]()

	factory.AddPredicate(
		flume.Predicate[testData]{
			Name:        "is-valid",
			Description: "Checks if data is valid",
			Predicate:   func(_ context.Context, _ testData) bool { return true },
		},
		flume.Predicate[testData]{
			Name:        "is-active",
			Description: "Checks if data is active",
			Predicate:   func(_ context.Context, _ testData) bool { return true },
		},
	)

	spec := factory.Spec()

	if len(spec.Predicates) != 2 {
		t.Fatalf("expected 2 predicates, got %d", len(spec.Predicates))
	}

	// Should be sorted alphabetically
	if spec.Predicates[0].Name != "is-active" {
		t.Errorf("expected first predicate to be 'is-active', got %s", spec.Predicates[0].Name)
	}
	if spec.Predicates[0].Description != "Checks if data is active" {
		t.Errorf("unexpected description: %s", spec.Predicates[0].Description)
	}

	if spec.Predicates[1].Name != "is-valid" {
		t.Errorf("expected second predicate to be 'is-valid', got %s", spec.Predicates[1].Name)
	}
}

func TestSpec_Conditions(t *testing.T) {
	factory := flume.New[testData]()

	factory.AddCondition(
		flume.Condition[testData]{
			Name:        "route-type",
			Description: "Determines routing based on type",
			Values:      []string{"typeA", "typeB", "typeC"},
			Condition:   func(_ context.Context, _ testData) string { return "typeA" },
		},
	)

	spec := factory.Spec()

	if len(spec.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(spec.Conditions))
	}

	cond := spec.Conditions[0]
	if cond.Name != "route-type" {
		t.Errorf("expected name 'route-type', got %s", cond.Name)
	}
	if cond.Description != "Determines routing based on type" {
		t.Errorf("unexpected description: %s", cond.Description)
	}
	if len(cond.Values) != 3 {
		t.Errorf("expected 3 values, got %d", len(cond.Values))
	}
	if cond.Values[0] != "typeA" || cond.Values[1] != "typeB" || cond.Values[2] != "typeC" {
		t.Errorf("unexpected values: %v", cond.Values)
	}
}

func TestSpec_Channels(t *testing.T) {
	factory := flume.New[testData]()

	ch1 := make(chan testData, 1)
	ch2 := make(chan testData, 1)
	defer close(ch1)
	defer close(ch2)

	factory.AddChannel("output-b", ch1)
	factory.AddChannel("output-a", ch2)

	spec := factory.Spec()

	if len(spec.Channels) != 2 {
		t.Fatalf("expected 2 channels, got %d", len(spec.Channels))
	}

	// Should be sorted alphabetically
	if spec.Channels[0].Name != "output-a" {
		t.Errorf("expected first channel to be 'output-a', got %s", spec.Channels[0].Name)
	}
	if spec.Channels[1].Name != "output-b" {
		t.Errorf("expected second channel to be 'output-b', got %s", spec.Channels[1].Name)
	}
}

func TestSpec_Connectors(t *testing.T) {
	factory := flume.New[testData]()
	spec := factory.Spec()

	// Should have all connector types
	expectedTypes := []string{
		"ref",
		"sequence",
		"concurrent",
		"race",
		"fallback",
		"retry",
		"timeout",
		"filter",
		"switch",
		"circuit-breaker",
		"rate-limit",
		"stream",
	}

	if len(spec.Connectors) != len(expectedTypes) {
		t.Errorf("expected %d connectors, got %d", len(expectedTypes), len(spec.Connectors))
	}

	connectorMap := make(map[string]flume.ConnectorSpec)
	for _, c := range spec.Connectors {
		connectorMap[c.Type] = c
	}

	for _, expectedType := range expectedTypes {
		if _, ok := connectorMap[expectedType]; !ok {
			t.Errorf("missing connector type: %s", expectedType)
		}
	}

	// Verify filter has expected fields
	filter := connectorMap["filter"]
	if filter.Description == "" {
		t.Error("filter should have a description")
	}

	hasPredicateField := false
	hasThenField := false
	for _, f := range filter.RequiredFields {
		if f.Name == "predicate" {
			hasPredicateField = true
		}
		if f.Name == "then" {
			hasThenField = true
		}
	}
	if !hasPredicateField {
		t.Error("filter should have 'predicate' required field")
	}
	if !hasThenField {
		t.Error("filter should have 'then' required field")
	}

	hasElseField := false
	for _, f := range filter.OptionalFields {
		if f.Name == "else" {
			hasElseField = true
		}
	}
	if !hasElseField {
		t.Error("filter should have 'else' optional field")
	}
}

func TestSpec_Deterministic(t *testing.T) {
	factory := flume.New[testData]()

	// Add items in random order
	factory.Add(
		pipz.Apply[testData]("z-processor", func(_ context.Context, d testData) (testData, error) { return d, nil }),
		pipz.Apply[testData]("a-processor", func(_ context.Context, d testData) (testData, error) { return d, nil }),
		pipz.Apply[testData]("m-processor", func(_ context.Context, d testData) (testData, error) { return d, nil }),
	)

	// Call Spec multiple times and verify order is consistent
	for i := 0; i < 10; i++ {
		spec := factory.Spec()

		if spec.Processors[0].Name != "a-processor" {
			t.Errorf("iteration %d: expected first processor 'a-processor', got %s", i, spec.Processors[0].Name)
		}
		if spec.Processors[1].Name != "m-processor" {
			t.Errorf("iteration %d: expected second processor 'm-processor', got %s", i, spec.Processors[1].Name)
		}
		if spec.Processors[2].Name != "z-processor" {
			t.Errorf("iteration %d: expected third processor 'z-processor', got %s", i, spec.Processors[2].Name)
		}
	}
}

func TestSpecJSON(t *testing.T) {
	factory := flume.New[testData]()

	// Add some components
	factory.AddWithMeta(flume.ProcessorMeta[testData]{
		Processor: pipz.Apply[testData]("json-processor", func(_ context.Context, d testData) (testData, error) {
			return d, nil
		}),
		Description: "Processor for JSON test",
		Tags:        []string{"json", "test"},
	})

	factory.AddPredicate(flume.Predicate[testData]{
		Name:        "json-predicate",
		Description: "Predicate for JSON test",
		Predicate:   func(_ context.Context, _ testData) bool { return true },
	})

	jsonStr, err := factory.SpecJSON()
	if err != nil {
		t.Fatalf("SpecJSON failed: %v", err)
	}

	// Verify it's valid JSON by unmarshaling
	var spec flume.FactorySpec
	if err := json.Unmarshal([]byte(jsonStr), &spec); err != nil {
		t.Fatalf("SpecJSON produced invalid JSON: %v", err)
	}

	// Verify content
	if len(spec.Processors) != 1 {
		t.Errorf("expected 1 processor, got %d", len(spec.Processors))
	}
	if spec.Processors[0].Name != "json-processor" {
		t.Errorf("expected processor name 'json-processor', got %s", spec.Processors[0].Name)
	}
	if spec.Processors[0].Description != "Processor for JSON test" {
		t.Errorf("unexpected description: %s", spec.Processors[0].Description)
	}

	if len(spec.Predicates) != 1 {
		t.Errorf("expected 1 predicate, got %d", len(spec.Predicates))
	}

	// Verify connectors are included
	if len(spec.Connectors) == 0 {
		t.Error("expected connectors to be populated")
	}
}

func TestAddWithMeta(t *testing.T) {
	factory := flume.New[testData]()

	processor := pipz.Apply[testData]("test-processor", func(_ context.Context, d testData) (testData, error) {
		d.Value = "processed"
		return d, nil
	})

	factory.AddWithMeta(flume.ProcessorMeta[testData]{
		Processor:   processor,
		Description: "Test processor that sets value",
		Tags:        []string{"test", "example"},
	})

	// Verify it's registered
	if !factory.HasProcessor("test-processor") {
		t.Error("processor should be registered")
	}

	// Verify it works in a schema
	schema := flume.Schema{
		Node: flume.Node{
			Ref: "test-processor",
		},
	}

	pipeline, err := factory.Build(schema)
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	result, err := pipeline.Process(context.Background(), testData{Value: "original"})
	if err != nil {
		t.Fatalf("failed to process: %v", err)
	}

	if result.Value != "processed" {
		t.Errorf("expected value 'processed', got %s", result.Value)
	}
}
