package flume_test

import (
	"context"
	"errors"
	"testing"

	"github.com/zoobzio/flume"
	"github.com/zoobzio/pipz"
)

// TestData implements pipz.Cloner[TestData].
type TestData struct {
	Value   string
	Counter int
}

func (t TestData) Clone() TestData {
	return TestData{
		Value:   t.Value,
		Counter: t.Counter,
	}
}

func TestFactoryBasic(t *testing.T) {
	factory := flume.New[TestData]()

	// Define identities upfront with meaningful descriptions
	uppercaseID := factory.Identity("uppercase", "Appends _UPPER suffix to value")
	incrementID := factory.Identity("increment", "Increments counter by one")
	logID := factory.Identity("log", "Logs the data for debugging")

	// Register processors using managed identities
	factory.Add(
		pipz.Transform(uppercaseID, func(_ context.Context, d TestData) TestData {
			d.Value += "_UPPER"
			return d
		}),
		pipz.Apply(incrementID, func(_ context.Context, d TestData) (TestData, error) {
			d.Counter++
			return d, nil
		}),
		pipz.Effect(logID, func(_ context.Context, _ TestData) error {
			// In real use, this would log
			return nil
		}),
	)

	// Build a simple sequence
	schema := flume.Schema{
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "uppercase"},
				{Ref: "increment"},
				{Ref: "log"},
			},
		},
	}

	pipeline, err := factory.Build(schema)
	if err != nil {
		t.Fatalf("Failed to build pipeline: %v", err)
	}

	// Test the pipeline
	ctx := context.Background()
	input := TestData{Value: "test", Counter: 0}
	result, pErr := pipeline.Process(ctx, input)

	if pErr != nil {
		t.Fatalf("Pipeline error: %v", pErr)
	}

	if result.Value != "test_UPPER" {
		t.Errorf("Expected value 'test_UPPER', got '%s'", result.Value)
	}

	if result.Counter != 1 {
		t.Errorf("Expected counter 1, got %d", result.Counter)
	}
}

func TestFactoryWithPredicates(t *testing.T) {
	factory := flume.New[TestData]()

	// Define identities
	doubleID := factory.Identity("double", "Doubles the counter value")
	addTenID := factory.Identity("add-ten", "Adds ten to the counter")
	isHighID := factory.Identity("is-high", "Checks if counter exceeds threshold")

	// Register processors
	factory.Add(pipz.Transform(doubleID, func(_ context.Context, d TestData) TestData {
		d.Counter *= 2
		return d
	}))

	factory.Add(pipz.Transform(addTenID, func(_ context.Context, d TestData) TestData {
		d.Counter += 10
		return d
	}))

	// Add predicate
	factory.AddPredicate(flume.Predicate[TestData]{
		Identity: isHighID,
		Predicate: func(_ context.Context, d TestData) bool {
			return d.Counter > 5
		},
	})

	// Build filter pipeline
	schema := flume.Schema{
		Node: flume.Node{
			Type:      "filter",
			Predicate: "is-high",
			Then: &flume.Node{
				Ref: "double",
			},
			Else: &flume.Node{
				Ref: "add-ten",
			},
		},
	}

	pipeline, err := factory.Build(schema)
	if err != nil {
		t.Fatalf("Failed to build filter pipeline: %v", err)
	}

	// Test with high value
	ctx := context.Background()
	highData := TestData{Counter: 10}
	result, pErr := pipeline.Process(ctx, highData)

	if pErr != nil {
		t.Fatalf("Pipeline error: %v", pErr)
	}

	if result.Counter != 20 {
		t.Errorf("Expected counter 20 for high value, got %d", result.Counter)
	}

	// Test with low value
	lowData := TestData{Counter: 2}
	result, pErr = pipeline.Process(ctx, lowData)

	if pErr != nil {
		t.Fatalf("Pipeline error: %v", pErr)
	}

	if result.Counter != 12 {
		t.Errorf("Expected counter 12 for low value, got %d", result.Counter)
	}
}

func TestFactoryWithConditions(t *testing.T) {
	factory := flume.New[TestData]()

	// Define identities
	routeAID := factory.Identity("route-a", "Handles route A processing")
	routeBID := factory.Identity("route-b", "Handles route B processing")
	routeDefaultID := factory.Identity("route-default", "Handles default route")
	valueSwitchID := factory.Identity("value-switch", "Routes based on input value")

	// Register processors
	factory.Add(pipz.Transform(routeAID, func(_ context.Context, d TestData) TestData {
		d.Value = "route-a"
		return d
	}))

	factory.Add(pipz.Transform(routeBID, func(_ context.Context, d TestData) TestData {
		d.Value = "route-b"
		return d
	}))

	factory.Add(pipz.Transform(routeDefaultID, func(_ context.Context, d TestData) TestData {
		d.Value = "route-default"
		return d
	}))

	// Add condition
	factory.AddCondition(flume.Condition[TestData]{
		Identity: valueSwitchID,
		Condition: func(_ context.Context, d TestData) string {
			switch d.Value {
			case "a":
				return "route-a"
			case "b":
				return "route-b"
			default:
				return "default"
			}
		},
	})

	// Build switch pipeline
	schema := flume.Schema{
		Node: flume.Node{
			Type:      "switch",
			Condition: "value-switch",
			Routes: map[string]flume.Node{
				"route-a": {Ref: "route-a"},
				"route-b": {Ref: "route-b"},
				"default": {Ref: "route-default"},
			},
		},
	}

	pipeline, err := factory.Build(schema)
	if err != nil {
		t.Fatalf("Failed to build switch pipeline: %v", err)
	}

	// Test different routes
	ctx := context.Background()
	tests := []struct {
		input    string
		expected string
	}{
		{"a", "route-a"},
		{"b", "route-b"},
		{"c", "route-default"},
	}

	for _, test := range tests {
		result, pErr := pipeline.Process(ctx, TestData{Value: test.input})
		if pErr != nil {
			t.Fatalf("Pipeline error for input '%s': %v", test.input, pErr)
		}
		if result.Value != test.expected {
			t.Errorf("For input '%s', expected '%s', got '%s'", test.input, test.expected, result.Value)
		}
	}
}

func TestFactoryErrors(t *testing.T) {
	factory := flume.New[TestData]()

	// Test missing processor
	schema := flume.Schema{
		Node: flume.Node{
			Ref: "nonexistent",
		},
	}

	_, err := factory.Build(schema)
	if err == nil {
		t.Error("Expected error for missing processor")
	}

	// Test invalid node type
	schema = flume.Schema{
		Node: flume.Node{
			Type: "invalid",
		},
	}

	_, err = factory.Build(schema)
	if err == nil {
		t.Error("Expected error for invalid node type")
	}

	// Test filter without predicate
	schema = flume.Schema{
		Node: flume.Node{
			Type: "filter",
			Then: &flume.Node{Ref: "test"},
		},
	}

	_, err = factory.Build(schema)
	if err == nil {
		t.Error("Expected error for filter without predicate")
	}
}

func TestFactoryComplexPipeline(t *testing.T) {
	t.Run("retry succeeds", func(t *testing.T) {
		factory := flume.New[TestData]()

		// Define identities
		mayFailID := factory.Identity("may-fail", "Processor that may fail temporarily")
		fallbackID := factory.Identity("fallback", "Fallback processor for failures")

		// Register processor that fails first 2 times
		errorCount := 0
		factory.Add(pipz.Apply(mayFailID, func(_ context.Context, d TestData) (TestData, error) {
			errorCount++
			if errorCount <= 2 {
				return d, errors.New("temporary error")
			}
			d.Value = "success"
			return d, nil
		}))

		factory.Add(pipz.Transform(fallbackID, func(_ context.Context, d TestData) TestData {
			d.Value = "fallback"
			return d
		}))

		// Build a retry inside a fallback
		schema := flume.Schema{
			Node: flume.Node{
				Type: "fallback",
				Children: []flume.Node{
					{
						Type:     "retry",
						Attempts: 3,
						Child:    &flume.Node{Ref: "may-fail"},
					},
					{Ref: "fallback"},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("Failed to build complex pipeline: %v", err)
		}

		// Test - should succeed on third attempt
		ctx := context.Background()
		result, pErr := pipeline.Process(ctx, TestData{})

		if pErr != nil {
			t.Fatalf("Fallback should not error: %v", pErr)
		}

		if result.Value != "success" {
			t.Errorf("Expected 'success' after retries, got '%s'", result.Value)
		}
	})

	t.Run("fallback activates", func(t *testing.T) {
		factory := flume.New[TestData]()

		// Define identities
		mayFailID := factory.Identity("may-fail", "Processor that always fails")
		fallbackID := factory.Identity("fallback", "Fallback processor for failures")

		// Register processor that always fails
		factory.Add(pipz.Apply(mayFailID, func(_ context.Context, d TestData) (TestData, error) {
			return d, errors.New("permanent error")
		}))

		factory.Add(pipz.Transform(fallbackID, func(_ context.Context, d TestData) TestData {
			d.Value = "fallback"
			return d
		}))

		// Build a retry inside a fallback
		schema := flume.Schema{
			Node: flume.Node{
				Type: "fallback",
				Children: []flume.Node{
					{
						Type:     "retry",
						Attempts: 3,
						Child:    &flume.Node{Ref: "may-fail"},
					},
					{Ref: "fallback"},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("Failed to build complex pipeline: %v", err)
		}

		ctx := context.Background()
		result, pErr := pipeline.Process(ctx, TestData{})

		if pErr != nil {
			t.Fatalf("Fallback should not error: %v", pErr)
		}

		if result.Value != "fallback" {
			t.Errorf("Expected 'fallback', got '%s'", result.Value)
		}
	})
}

func TestFactoryBackoff(t *testing.T) {
	factory := flume.New[TestData]()

	testID := factory.Identity("test", "Test processor for backoff")
	factory.Add(pipz.Transform(testID, func(_ context.Context, d TestData) TestData {
		d.Value = "backoff-test"
		return d
	}))

	// Test backoff schema
	schema := flume.Schema{
		Node: flume.Node{
			Type:     "retry",
			Attempts: 2,
			Backoff:  "100ms",
			Child:    &flume.Node{Ref: "test"},
		},
	}

	pipeline, err := factory.Build(schema)
	if err != nil {
		t.Fatalf("Failed to build backoff pipeline: %v", err)
	}

	ctx := context.Background()
	result, pErr := pipeline.Process(ctx, TestData{})

	if pErr != nil {
		t.Fatalf("Backoff pipeline error: %v", pErr)
	}

	if result.Value != "backoff-test" {
		t.Errorf("Expected 'backoff-test', got '%s'", result.Value)
	}
}

func TestFactoryDynamicSchema(t *testing.T) {
	factory := flume.New[TestData]()

	// Define identities
	nonExistentID := factory.Identity("non-existent", "Non-existent binding for testing")
	step1ID := factory.Identity("step1", "First step in sequence")
	step2ID := factory.Identity("step2", "Second step in sequence")
	dynamicID := factory.Identity("dynamic", "Dynamic pipeline binding")

	// Test getting non-existent binding
	binding := factory.Get(nonExistentID)
	if binding != nil {
		t.Error("Expected Get to return nil for non-existent binding")
	}

	factory.Add(pipz.Transform(step1ID, func(_ context.Context, d TestData) TestData {
		d.Value += "_1"
		return d
	}))

	factory.Add(pipz.Transform(step2ID, func(_ context.Context, d TestData) TestData {
		d.Value += "_2"
		return d
	}))

	// Register initial schema
	schema1 := flume.Schema{
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "step1"},
			},
		},
	}

	if err := factory.SetSchema("dynamic-schema", schema1); err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	id := dynamicID
	binding, err := factory.Bind(id, "dynamic-schema", flume.WithAutoSync[TestData]())
	if err != nil {
		t.Fatalf("Failed to bind schema: %v", err)
	}

	// Test initial schema via binding
	ctx := context.Background()
	result, pErr := binding.Process(ctx, TestData{Value: "test"})
	if pErr != nil {
		t.Fatalf("Process error: %v", pErr)
	}
	if result.Value != "test_1" {
		t.Errorf("Expected 'test_1', got '%s'", result.Value)
	}

	// Update schema via SetSchema (triggers auto-sync)
	schema2 := flume.Schema{
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "step1"},
				{Ref: "step2"},
			},
		},
	}

	err = factory.SetSchema("dynamic-schema", schema2)
	if err != nil {
		t.Fatalf("Failed to update schema: %v", err)
	}

	// Test updated schema
	result, pErr = binding.Process(ctx, TestData{Value: "test"})
	if pErr != nil {
		t.Fatalf("Process error: %v", pErr)
	}
	if result.Value != "test_1_2" {
		t.Errorf("Expected 'test_1_2', got '%s'", result.Value)
	}
}

func TestFactoryUtilities(t *testing.T) {
	factory := flume.New[TestData]()

	// Test checking for nonexistent components
	if factory.HasProcessor("nonexistent") {
		t.Error("Expected HasProcessor to return false for nonexistent processor")
	}
	if factory.HasPredicate("nonexistent") {
		t.Error("Expected HasPredicate to return false for nonexistent predicate")
	}
	if factory.HasCondition("nonexistent") {
		t.Error("Expected HasCondition to return false for nonexistent condition")
	}

	// Test empty lists
	if len(factory.ListProcessors()) != 0 {
		t.Error("Expected empty processor list")
	}
	if len(factory.ListPredicates()) != 0 {
		t.Error("Expected empty predicate list")
	}
	if len(factory.ListConditions()) != 0 {
		t.Error("Expected empty condition list")
	}

	// Define and register components
	testProcessorID := factory.Identity("test-processor", "Test processor for registration")
	testPredicateID := factory.Identity("test-predicate", "Test predicate for registration")
	testConditionID := factory.Identity("test-condition", "Test condition for registration")

	factory.Add(pipz.Transform(testProcessorID, func(_ context.Context, d TestData) TestData {
		return d
	}))
	factory.AddPredicate(flume.Predicate[TestData]{
		Identity: testPredicateID,
		Predicate: func(_ context.Context, _ TestData) bool {
			return true
		},
	})
	factory.AddCondition(flume.Condition[TestData]{
		Identity: testConditionID,
		Condition: func(_ context.Context, _ TestData) string {
			return "test"
		},
	})

	// Test has methods
	if !factory.HasProcessor("test-processor") {
		t.Error("Expected HasProcessor to return true for registered processor")
	}
	if !factory.HasPredicate("test-predicate") {
		t.Error("Expected HasPredicate to return true for registered predicate")
	}
	if !factory.HasCondition("test-condition") {
		t.Error("Expected HasCondition to return true for registered condition")
	}

	// Test list methods
	processors := factory.ListProcessors()
	if len(processors) != 1 || processors[0] != "test-processor" {
		t.Error("Expected processor list to contain 'test-processor'")
	}

	predicates := factory.ListPredicates()
	if len(predicates) != 1 || predicates[0] != "test-predicate" {
		t.Error("Expected predicate list to contain 'test-predicate'")
	}

	conditions := factory.ListConditions()
	if len(conditions) != 1 || conditions[0] != "test-condition" {
		t.Error("Expected condition list to contain 'test-condition'")
	}
}

func TestVariadicRegistration(t *testing.T) {
	factory := flume.New[TestData]()

	// Define processor identities
	step1ID := factory.Identity("step1", "First step appends _1")
	step2ID := factory.Identity("step2", "Second step appends _2")
	step3ID := factory.Identity("step3", "Third step appends _3")
	step4ID := factory.Identity("step4", "Fourth step appends _4")
	step5ID := factory.Identity("step5", "Fifth step appends _5")

	// Define predicate identities
	isPositiveID := factory.Identity("is-positive", "Checks if counter is positive")
	isHighID := factory.Identity("is-high", "Checks if counter exceeds 10")
	hasValueID := factory.Identity("has-value", "Checks if value is non-empty")

	// Define condition identities
	counterRangeID := factory.Identity("counter-range", "Categorizes counter into low/medium/high")
	valueTypeID := factory.Identity("value-type", "Categorizes value as empty or non-empty")

	// Test variadic processor registration
	factory.Add(
		pipz.Transform(step1ID, func(_ context.Context, d TestData) TestData {
			d.Value += "_1"
			return d
		}),
		pipz.Transform(step2ID, func(_ context.Context, d TestData) TestData {
			d.Value += "_2"
			return d
		}),
		pipz.Transform(step3ID, func(_ context.Context, d TestData) TestData {
			d.Value += "_3"
			return d
		}),
		pipz.Transform(step4ID, func(_ context.Context, d TestData) TestData {
			d.Value += "_4"
			return d
		}),
		pipz.Transform(step5ID, func(_ context.Context, d TestData) TestData {
			d.Value += "_5"
			return d
		}),
	)

	// Test variadic predicate registration
	factory.AddPredicate(
		flume.Predicate[TestData]{
			Identity: isPositiveID,
			Predicate: func(_ context.Context, d TestData) bool {
				return d.Counter > 0
			},
		},
		flume.Predicate[TestData]{
			Identity: isHighID,
			Predicate: func(_ context.Context, d TestData) bool {
				return d.Counter > 10
			},
		},
		flume.Predicate[TestData]{
			Identity: hasValueID,
			Predicate: func(_ context.Context, d TestData) bool {
				return d.Value != ""
			},
		},
	)

	// Test variadic condition registration
	factory.AddCondition(
		flume.Condition[TestData]{
			Identity: counterRangeID,
			Condition: func(_ context.Context, d TestData) string {
				if d.Counter < 5 {
					return "low"
				} else if d.Counter < 10 {
					return "medium"
				}
				return "high"
			},
		},
		flume.Condition[TestData]{
			Identity: valueTypeID,
			Condition: func(_ context.Context, d TestData) string {
				if d.Value == "" {
					return "empty"
				}
				return "non-empty"
			},
		},
	)

	// Verify all processors are registered
	if !factory.HasProcessor("step1") || !factory.HasProcessor("step2") || !factory.HasProcessor("step3") {
		t.Error("Not all processors were registered")
	}

	// Verify all predicates are registered
	if !factory.HasPredicate("is-positive") || !factory.HasPredicate("is-high") || !factory.HasPredicate("has-value") {
		t.Error("Not all predicates were registered")
	}

	// Verify all conditions are registered
	if !factory.HasCondition("counter-range") || !factory.HasCondition("value-type") {
		t.Error("Not all conditions were registered")
	}

	// Test a pipeline using the registered components
	schema := flume.Schema{
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "step1"},
				{
					Type:      "filter",
					Predicate: "has-value",
					Then:      &flume.Node{Ref: "step2"},
				},
				{
					Type:      "switch",
					Condition: "counter-range",
					Routes: map[string]flume.Node{
						"low":    {Ref: "step3"},
						"medium": {Ref: "step4"},
						"high":   {Ref: "step5"},
					},
				},
			},
		},
	}

	pipeline, err := factory.Build(schema)
	if err != nil {
		t.Fatalf("Failed to build pipeline: %v", err)
	}

	// Test the pipeline
	ctx := context.Background()
	result, pErr := pipeline.Process(ctx, TestData{Value: "test", Counter: 3})

	if pErr != nil {
		t.Fatalf("Pipeline error: %v", pErr)
	}

	// Expected: test_1 (step1) -> test_1_2 (step2 from filter) -> test_1_2_3 (step3 from switch low route)
	if result.Value != "test_1_2_3" {
		t.Errorf("Expected 'test_1_2_3', got '%s'", result.Value)
	}
}

func TestSchemaManagement(t *testing.T) {
	factory := flume.New[TestData]()

	// Define identities
	testID := factory.Identity("test", "Test processor for schema management")
	bindingID := factory.Identity("schema1", "First schema binding")
	nonExistentID := factory.Identity("non-existent", "Non-existent binding for testing")

	// Add a processor for our schemas
	factory.Add(pipz.Transform(testID, func(_ context.Context, d TestData) TestData {
		d.Value = "processed"
		return d
	}))

	// Register schema first
	schema1 := flume.Schema{
		Node: flume.Node{Ref: "test"},
	}
	if err := factory.SetSchema("test-schema", schema1); err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	// Test Bind
	binding, err := factory.Bind(bindingID, "test-schema", flume.WithAutoSync[TestData]())
	if err != nil {
		t.Fatalf("Failed to bind schema: %v", err)
	}
	if binding == nil {
		t.Fatal("Expected non-nil binding")
	}

	// Test Get retrieves the same binding
	retrieved := factory.Get(bindingID)
	if retrieved == nil {
		t.Error("Expected to find binding")
	}
	if retrieved != binding {
		t.Error("Get should return the same binding instance")
	}

	// Test Get for non-existent
	missing := factory.Get(nonExistentID)
	if missing != nil {
		t.Error("Expected Get to return nil for non-existent binding")
	}

	// Verify process works
	ctx := context.Background()
	result, pErr := binding.Process(ctx, TestData{Value: "input"})
	if pErr != nil {
		t.Fatalf("Process failed: %v", pErr)
	}
	if result.Value != "processed" {
		t.Errorf("Expected 'processed', got '%s'", result.Value)
	}

	// Test schema registry functions
	if !factory.HasSchema("test-schema") {
		t.Error("Expected HasSchema to return true")
	}

	schemas := factory.ListSchemas()
	found := false
	for _, s := range schemas {
		if s == "test-schema" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected 'test-schema' in ListSchemas")
	}

	// Test GetSchema
	retrievedSchema, ok := factory.GetSchema("test-schema")
	if !ok {
		t.Error("Expected GetSchema to find schema")
	}
	if retrievedSchema.Ref != "test" {
		t.Errorf("Expected schema ref 'test', got '%s'", retrievedSchema.Ref)
	}
}

func TestComponentRemoval(t *testing.T) {
	factory := flume.New[TestData]()

	// Define identities
	proc1ID := factory.Identity("proc1", "First processor for removal test")
	proc2ID := factory.Identity("proc2", "Second processor for removal test")
	proc3ID := factory.Identity("proc3", "Third processor for removal test")
	pred1ID := factory.Identity("pred1", "First predicate for removal test")
	pred2ID := factory.Identity("pred2", "Second predicate for removal test")
	cond1ID := factory.Identity("cond1", "Condition for removal test")

	// Add multiple processors
	factory.Add(
		pipz.Transform(proc1ID, func(_ context.Context, d TestData) TestData { return d }),
		pipz.Transform(proc2ID, func(_ context.Context, d TestData) TestData { return d }),
		pipz.Transform(proc3ID, func(_ context.Context, d TestData) TestData { return d }),
	)

	// Remove one processor
	count := factory.Remove("proc1")
	if count != 1 {
		t.Errorf("Expected to remove 1 processor, removed %d", count)
	}

	// Remove multiple processors
	count = factory.Remove("proc2", "proc3", "non-existent")
	if count != 2 {
		t.Errorf("Expected to remove 2 processors, removed %d", count)
	}

	// Verify all removed
	if factory.HasProcessor("proc1") || factory.HasProcessor("proc2") || factory.HasProcessor("proc3") {
		t.Error("Processors should be removed")
	}

	// Test predicate removal
	factory.AddPredicate(
		flume.Predicate[TestData]{Identity: pred1ID, Predicate: func(_ context.Context, _ TestData) bool { return true }},
		flume.Predicate[TestData]{Identity: pred2ID, Predicate: func(_ context.Context, _ TestData) bool { return false }},
	)

	count = factory.RemovePredicate("pred1", "pred2")
	if count != 2 {
		t.Errorf("Expected to remove 2 predicates, removed %d", count)
	}

	// Test condition removal
	factory.AddCondition(
		flume.Condition[TestData]{Identity: cond1ID, Condition: func(_ context.Context, _ TestData) string { return "a" }},
	)

	count = factory.RemoveCondition("cond1")
	if count != 1 {
		t.Errorf("Expected to remove 1 condition, removed %d", count)
	}
}

func BenchmarkBuild(b *testing.B) {
	factory := flume.New[TestData]()

	// Define identities
	step1ID := factory.Identity("step1", "First benchmark step")
	step2ID := factory.Identity("step2", "Second benchmark step")

	// Register processors
	factory.Add(
		pipz.Transform(step1ID, func(_ context.Context, d TestData) TestData {
			d.Value += "_1"
			return d
		}),
		pipz.Transform(step2ID, func(_ context.Context, d TestData) TestData {
			d.Counter++
			return d
		}),
	)

	// Simple schema
	schema := flume.Schema{
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "step1"},
				{Ref: "step2"},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := factory.Build(schema)
		if err != nil {
			b.Fatalf("Build failed: %v", err)
		}
	}
}

func BenchmarkConcurrentOperations(b *testing.B) {
	factory := flume.New[TestData]()

	// Define identity once
	testID := factory.Identity("test", "Test processor for concurrent benchmark")

	// Simple schema
	schema := flume.Schema{
		Node: flume.Node{Ref: "test"},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Add a processor
			factory.Add(pipz.Transform(testID, func(_ context.Context, d TestData) TestData {
				return d
			}))

			// Build schema
			_, err := factory.Build(schema)
			if err == nil {
				// Only remove if build succeeded (processor exists)
				factory.Remove("test")
			}
		}
	})
}

func TestChannelFactory(t *testing.T) {
	t.Run("add and get channel", func(t *testing.T) {
		factory := flume.New[TestData]()
		channel := make(chan TestData, 10)

		// Add channel
		factory.AddChannel("test-channel", channel)

		// Check HasChannel
		if !factory.HasChannel("test-channel") {
			t.Error("expected channel to exist")
		}

		// Get channel
		retrieved, exists := factory.GetChannel("test-channel")
		if !exists {
			t.Error("expected channel to exist")
		}
		if retrieved == nil {
			t.Error("expected to get non-nil channel")
		}

		// List channels
		channels := factory.ListChannels()
		if len(channels) != 1 || channels[0] != "test-channel" {
			t.Errorf("expected ['test-channel'], got %v", channels)
		}
	})

	t.Run("remove channel", func(t *testing.T) {
		factory := flume.New[TestData]()
		channel := make(chan TestData, 10)

		factory.AddChannel("test-channel", channel)

		// Remove channel
		removed := factory.RemoveChannel("test-channel")
		if !removed {
			t.Error("expected channel to be removed")
		}

		// Verify it's gone
		if factory.HasChannel("test-channel") {
			t.Error("expected channel to not exist")
		}

		// Remove again should return false
		removed = factory.RemoveChannel("test-channel")
		if removed {
			t.Error("expected false for non-existent channel")
		}
	})
}
