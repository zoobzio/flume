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

	// Define identities upfront
	processorAID := factory.Identity("processor-a", "Basic processor A")
	processorBID := factory.Identity("processor-b", "Processes B data")

	// Add without metadata
	factory.Add(
		pipz.Apply(processorAID, func(_ context.Context, d testData) (testData, error) {
			return d, nil
		}),
	)

	// Add with metadata
	factory.AddWithMeta(flume.ProcessorMeta[testData]{
		Processor: pipz.Apply(processorBID, func(_ context.Context, d testData) (testData, error) {
			return d, nil
		}),
		Tags: []string{"processing", "test"},
	})

	spec := factory.Spec()

	if len(spec.Processors) != 2 {
		t.Fatalf("expected 2 processors, got %d", len(spec.Processors))
	}

	// Should be sorted alphabetically
	if spec.Processors[0].Name != "processor-a" {
		t.Errorf("expected first processor to be 'processor-a', got %s", spec.Processors[0].Name)
	}
	if spec.Processors[0].Description != "Basic processor A" {
		t.Errorf("expected description 'Basic processor A' for processor-a, got %s", spec.Processors[0].Description)
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

	// Define identities upfront
	isValidID := factory.Identity("is-valid", "Checks if data is valid")
	isActiveID := factory.Identity("is-active", "Checks if data is active")

	factory.AddPredicate(
		flume.Predicate[testData]{
			Identity:  isValidID,
			Predicate: func(_ context.Context, _ testData) bool { return true },
		},
		flume.Predicate[testData]{
			Identity:  isActiveID,
			Predicate: func(_ context.Context, _ testData) bool { return true },
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

	// Define identities upfront
	routeTypeID := factory.Identity("route-type", "Determines routing based on type")

	factory.AddCondition(
		flume.Condition[testData]{
			Identity:  routeTypeID,
			Values:    []string{"typeA", "typeB", "typeC"},
			Condition: func(_ context.Context, _ testData) string { return "typeA" },
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

func TestSpec_ConditionsSorting(t *testing.T) {
	factory := flume.New[testData]()

	// Define identities in non-alphabetical order
	zConditionID := factory.Identity("z-condition", "Z condition for sorting test")
	aConditionID := factory.Identity("a-condition", "A condition for sorting test")
	mConditionID := factory.Identity("m-condition", "M condition for sorting test")

	// Add conditions in random order
	factory.AddCondition(
		flume.Condition[testData]{
			Identity:  zConditionID,
			Values:    []string{"z1", "z2"},
			Condition: func(_ context.Context, _ testData) string { return "z1" },
		},
		flume.Condition[testData]{
			Identity:  aConditionID,
			Values:    []string{"a1", "a2"},
			Condition: func(_ context.Context, _ testData) string { return "a1" },
		},
		flume.Condition[testData]{
			Identity:  mConditionID,
			Values:    []string{"m1", "m2"},
			Condition: func(_ context.Context, _ testData) string { return "m1" },
		},
	)

	// Call Spec multiple times to verify deterministic ordering
	for i := 0; i < 10; i++ {
		spec := factory.Spec()

		if len(spec.Conditions) != 3 {
			t.Fatalf("iteration %d: expected 3 conditions, got %d", i, len(spec.Conditions))
		}

		// Should be sorted alphabetically
		if spec.Conditions[0].Name != "a-condition" {
			t.Errorf("iteration %d: expected first condition 'a-condition', got %s", i, spec.Conditions[0].Name)
		}
		if spec.Conditions[1].Name != "m-condition" {
			t.Errorf("iteration %d: expected second condition 'm-condition', got %s", i, spec.Conditions[1].Name)
		}
		if spec.Conditions[2].Name != "z-condition" {
			t.Errorf("iteration %d: expected third condition 'z-condition', got %s", i, spec.Conditions[2].Name)
		}
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

	// Define identities upfront
	zProcessorID := factory.Identity("z-processor", "Processor Z for ordering test")
	aProcessorID := factory.Identity("a-processor", "Processor A for ordering test")
	mProcessorID := factory.Identity("m-processor", "Processor M for ordering test")

	// Add items in random order
	factory.Add(
		pipz.Apply(zProcessorID, func(_ context.Context, d testData) (testData, error) { return d, nil }),
		pipz.Apply(aProcessorID, func(_ context.Context, d testData) (testData, error) { return d, nil }),
		pipz.Apply(mProcessorID, func(_ context.Context, d testData) (testData, error) { return d, nil }),
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

	// Define identities upfront
	jsonProcessorID := factory.Identity("json-processor", "Processor for JSON test")
	jsonPredicateID := factory.Identity("json-predicate", "Predicate for JSON test")

	// Add some components
	factory.AddWithMeta(flume.ProcessorMeta[testData]{
		Processor: pipz.Apply(jsonProcessorID, func(_ context.Context, d testData) (testData, error) {
			return d, nil
		}),
		Tags: []string{"json", "test"},
	})

	factory.AddPredicate(flume.Predicate[testData]{
		Identity:  jsonPredicateID,
		Predicate: func(_ context.Context, _ testData) bool { return true },
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

	// Define identities upfront
	testProcessorID := factory.Identity("test-processor", "Test processor that sets value")

	processor := pipz.Apply(testProcessorID, func(_ context.Context, d testData) (testData, error) {
		d.Value = "processed"
		return d, nil
	})

	factory.AddWithMeta(flume.ProcessorMeta[testData]{
		Processor: processor,
		Tags:      []string{"test", "example"},
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
