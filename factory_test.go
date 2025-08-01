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

	// Register processors using variadic function
	factory.Add(
		pipz.Transform("uppercase", func(_ context.Context, d TestData) TestData {
			d.Value += "_UPPER"
			return d
		}),
		pipz.Apply("increment", func(_ context.Context, d TestData) (TestData, error) {
			d.Counter++
			return d, nil
		}),
		pipz.Effect("log", func(_ context.Context, _ TestData) error {
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

	// Register processors
	factory.Add(pipz.Transform("double", func(_ context.Context, d TestData) TestData {
		d.Counter *= 2
		return d
	}))

	factory.Add(pipz.Transform("add-ten", func(_ context.Context, d TestData) TestData {
		d.Counter += 10
		return d
	}))

	// Add predicate
	factory.AddPredicate(flume.Predicate[TestData]{
		Name: "is-high",
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

	// Register processors
	factory.Add(pipz.Transform("route-a", func(_ context.Context, d TestData) TestData {
		d.Value = "route-a"
		return d
	}))

	factory.Add(pipz.Transform("route-b", func(_ context.Context, d TestData) TestData {
		d.Value = "route-b"
		return d
	}))

	factory.Add(pipz.Transform("route-default", func(_ context.Context, d TestData) TestData {
		d.Value = "route-default"
		return d
	}))

	// Add condition
	factory.AddCondition(flume.Condition[TestData]{
		Name: "value-switch",
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

		// Register processor that fails first 2 times
		errorCount := 0
		factory.Add(pipz.Apply("may-fail", func(_ context.Context, d TestData) (TestData, error) {
			errorCount++
			if errorCount <= 2 {
				return d, errors.New("temporary error")
			}
			d.Value = "success"
			return d, nil
		}))

		factory.Add(pipz.Transform("fallback", func(_ context.Context, d TestData) TestData {
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

		// Register processor that always fails
		factory.Add(pipz.Apply("may-fail", func(_ context.Context, d TestData) (TestData, error) {
			return d, errors.New("permanent error")
		}))

		factory.Add(pipz.Transform("fallback", func(_ context.Context, d TestData) TestData {
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

	factory.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
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

	// Test getting non-existent pipeline
	_, ok := factory.Pipeline("non-existent")
	if ok {
		t.Error("Expected Pipeline to return false for non-existent schema")
	}

	factory.Add(pipz.Transform("step1", func(_ context.Context, d TestData) TestData {
		d.Value += "_1"
		return d
	}))

	factory.Add(pipz.Transform("step2", func(_ context.Context, d TestData) TestData {
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

	err := factory.SetSchema("dynamic", schema1)
	if err != nil {
		t.Fatalf("Failed to register schema: %v", err)
	}

	// Test initial schema
	pipeline, ok := factory.Pipeline("dynamic")
	if !ok {
		t.Fatal("Pipeline should exist")
	}

	ctx := context.Background()
	result, pErr := pipeline.Process(ctx, TestData{Value: "test"})
	if pErr != nil {
		t.Fatalf("Pipeline error: %v", pErr)
	}
	if result.Value != "test_1" {
		t.Errorf("Expected 'test_1', got '%s'", result.Value)
	}

	// Update schema
	schema2 := flume.Schema{
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "step1"},
				{Ref: "step2"},
			},
		},
	}

	err = factory.SetSchema("dynamic", schema2)
	if err != nil {
		t.Fatalf("Failed to update schema: %v", err)
	}

	// Test updated schema
	pipeline, ok = factory.Pipeline("dynamic")
	if !ok {
		t.Fatal("Pipeline should still exist after update")
	}
	result, pErr = pipeline.Process(ctx, TestData{Value: "test"})
	if pErr != nil {
		t.Fatalf("Pipeline error: %v", pErr)
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

	// Register components
	factory.Add(pipz.Transform("test-processor", func(_ context.Context, d TestData) TestData {
		return d
	}))
	factory.AddPredicate(flume.Predicate[TestData]{
		Name: "test-predicate",
		Predicate: func(_ context.Context, _ TestData) bool {
			return true
		},
	})
	factory.AddCondition(flume.Condition[TestData]{
		Name: "test-condition",
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

	// Test variadic processor registration
	factory.Add(
		pipz.Transform("step1", func(_ context.Context, d TestData) TestData {
			d.Value += "_1"
			return d
		}),
		pipz.Transform("step2", func(_ context.Context, d TestData) TestData {
			d.Value += "_2"
			return d
		}),
		pipz.Transform("step3", func(_ context.Context, d TestData) TestData {
			d.Value += "_3"
			return d
		}),
		pipz.Transform("step4", func(_ context.Context, d TestData) TestData {
			d.Value += "_4"
			return d
		}),
		pipz.Transform("step5", func(_ context.Context, d TestData) TestData {
			d.Value += "_5"
			return d
		}),
	)

	// Test variadic predicate registration
	factory.AddPredicate(
		flume.Predicate[TestData]{
			Name: "is-positive",
			Predicate: func(_ context.Context, d TestData) bool {
				return d.Counter > 0
			},
		},
		flume.Predicate[TestData]{
			Name: "is-high",
			Predicate: func(_ context.Context, d TestData) bool {
				return d.Counter > 10
			},
		},
		flume.Predicate[TestData]{
			Name: "has-value",
			Predicate: func(_ context.Context, d TestData) bool {
				return d.Value != ""
			},
		},
	)

	// Test variadic condition registration
	factory.AddCondition(
		flume.Condition[TestData]{
			Name: "counter-range",
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
			Name: "value-type",
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

	// Test ListSchemas on empty factory
	schemas := factory.ListSchemas()
	if len(schemas) != 0 {
		t.Error("Expected empty schema list")
	}

	// Add a processor for our schemas
	factory.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
		d.Value = "processed"
		return d
	}))

	// Test SetSchema (first time - should register)
	schema1 := flume.Schema{
		Node: flume.Node{Ref: "test"},
	}
	err := factory.SetSchema("schema1", schema1)
	if err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	// Test GetSchema
	retrieved, ok := factory.GetSchema("schema1")
	if !ok {
		t.Error("Expected to find schema1")
	}
	if retrieved.Node.Ref != "test" {
		t.Error("Retrieved schema doesn't match")
	}

	// Test GetSchema for non-existent
	_, ok = factory.GetSchema("non-existent")
	if ok {
		t.Error("Expected GetSchema to return false for non-existent schema")
	}

	// Test ListSchemas with one schema
	schemas = factory.ListSchemas()
	if len(schemas) != 1 || schemas[0] != "schema1" {
		t.Errorf("Expected [schema1], got %v", schemas)
	}

	// Test SetSchema (update existing)
	schema2 := flume.Schema{
		Node: flume.Node{
			Type:     "sequence",
			Children: []flume.Node{{Ref: "test"}},
		},
	}
	err = factory.SetSchema("schema1", schema2)
	if err != nil {
		t.Fatalf("Failed to update schema: %v", err)
	}

	// Verify update worked
	retrieved, _ = factory.GetSchema("schema1")
	if retrieved.Node.Type != "sequence" {
		t.Error("Schema was not updated")
	}

	// Test RemoveSchema
	removed := factory.RemoveSchema("schema1")
	if !removed {
		t.Error("Expected RemoveSchema to return true")
	}

	// Try to remove again
	removed = factory.RemoveSchema("schema1")
	if removed {
		t.Error("Expected RemoveSchema to return false for non-existent schema")
	}

	// Verify removal
	schemas = factory.ListSchemas()
	if len(schemas) != 0 {
		t.Error("Expected empty schema list after removal")
	}

	// Verify pipeline is also gone
	_, ok = factory.Pipeline("schema1")
	if ok {
		t.Error("Pipeline should be removed with schema")
	}
}

func TestComponentRemoval(t *testing.T) {
	factory := flume.New[TestData]()

	// Add multiple processors
	factory.Add(
		pipz.Transform("proc1", func(_ context.Context, d TestData) TestData { return d }),
		pipz.Transform("proc2", func(_ context.Context, d TestData) TestData { return d }),
		pipz.Transform("proc3", func(_ context.Context, d TestData) TestData { return d }),
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
		flume.Predicate[TestData]{Name: "pred1", Predicate: func(_ context.Context, _ TestData) bool { return true }},
		flume.Predicate[TestData]{Name: "pred2", Predicate: func(_ context.Context, _ TestData) bool { return false }},
	)

	count = factory.RemovePredicate("pred1", "pred2")
	if count != 2 {
		t.Errorf("Expected to remove 2 predicates, removed %d", count)
	}

	// Test condition removal
	factory.AddCondition(
		flume.Condition[TestData]{Name: "cond1", Condition: func(_ context.Context, _ TestData) string { return "a" }},
	)

	count = factory.RemoveCondition("cond1")
	if count != 1 {
		t.Errorf("Expected to remove 1 condition, removed %d", count)
	}
}

func BenchmarkBuild(b *testing.B) {
	factory := flume.New[TestData]()

	// Register processors
	factory.Add(
		pipz.Transform("step1", func(_ context.Context, d TestData) TestData {
			d.Value += "_1"
			return d
		}),
		pipz.Transform("step2", func(_ context.Context, d TestData) TestData {
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

	// Simple schema
	schema := flume.Schema{
		Node: flume.Node{Ref: "test"},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Add a processor
			factory.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
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
