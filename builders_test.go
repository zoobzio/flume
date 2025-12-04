package flume_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zoobzio/flume"
	"github.com/zoobzio/pipz"
)

func TestBuildSequence(t *testing.T) {
	factory := flume.New[TestData]()

	// Register test processors
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
	)

	tests := []struct {
		name        string
		schema      flume.Schema
		input       TestData
		expected    TestData
		expectError bool
		errorMsg    string
	}{
		{
			name: "basic sequence",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Ref: "step1"},
						{Ref: "step2"},
						{Ref: "step3"},
					},
				},
			},
			input:    TestData{Value: "test"},
			expected: TestData{Value: "test_1_2_3"},
		},
		{
			name: "sequence with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Name: "custom-sequence",
					Children: []flume.Node{
						{Ref: "step1"},
						{Ref: "step2"},
					},
				},
			},
			input:    TestData{Value: "test"},
			expected: TestData{Value: "test_1_2"},
		},
		{
			name: "empty sequence",
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "sequence",
					Children: []flume.Node{},
				},
			},
			expectError: true,
			errorMsg:    "sequence requires at least one child",
		},
		{
			name: "sequence with invalid child",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Ref: "step1"},
						{Ref: "non-existent"},
					},
				},
			},
			expectError: true,
			errorMsg:    "processor 'non-existent' not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				} else if tt.errorMsg != "" && !contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, tt.input)
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			if result.Value != tt.expected.Value {
				t.Errorf("Expected value '%s', got '%s'", tt.expected.Value, result.Value)
			}
		})
	}
}

func TestBuildConcurrent(t *testing.T) {
	factory := flume.New[TestData]()

	// Register test processors that modify different fields
	factory.Add(
		pipz.Transform("add-suffix", func(_ context.Context, d TestData) TestData {
			d.Value += "_concurrent"
			return d
		}),
		pipz.Transform("increment", func(_ context.Context, d TestData) TestData {
			d.Counter += 10
			return d
		}),
	)

	tests := []struct {
		name        string
		schema      flume.Schema
		expectError bool
		errorMsg    string
	}{
		{
			name: "basic concurrent",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "concurrent",
					Children: []flume.Node{
						{Ref: "add-suffix"},
						{Ref: "increment"},
					},
				},
			},
		},
		{
			name: "concurrent with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "concurrent",
					Name: "custom-concurrent",
					Children: []flume.Node{
						{Ref: "add-suffix"},
					},
				},
			},
		},
		{
			name: "empty concurrent",
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "concurrent",
					Children: []flume.Node{},
				},
			},
			expectError: true,
			errorMsg:    "concurrent requires at least one child",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Just verify it builds and runs without error
			ctx := context.Background()
			_, pErr := pipeline.Process(ctx, TestData{Value: "test", Counter: 5})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}
		})
	}
}

func TestBuildRace(t *testing.T) {
	factory := flume.New[TestData]()

	factory.Add(
		pipz.Transform("fast", func(_ context.Context, d TestData) TestData {
			d.Value = "fast"
			return d
		}),
		pipz.Transform("slow", func(_ context.Context, d TestData) TestData {
			d.Value = "slow"
			return d
		}),
	)

	tests := []struct {
		name        string
		schema      flume.Schema
		expectError bool
		errorMsg    string
	}{
		{
			name: "basic race",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "race",
					Children: []flume.Node{
						{Ref: "fast"},
						{Ref: "slow"},
					},
				},
			},
		},
		{
			name: "race with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "race",
					Name: "custom-race",
					Children: []flume.Node{
						{Ref: "fast"},
						{Ref: "slow"},
					},
				},
			},
		},
		{
			name: "empty race",
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "race",
					Children: []flume.Node{},
				},
			},
			expectError: true,
			errorMsg:    "race requires at least one child",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			// Result should be either "fast" or "slow"
			if result.Value != "fast" && result.Value != "slow" {
				t.Errorf("Expected 'fast' or 'slow', got '%s'", result.Value)
			}
		})
	}
}

func TestBuildFallback(t *testing.T) {
	factory := flume.New[TestData]()

	errorCount := 0
	factory.Add(
		pipz.Apply("failing", func(_ context.Context, d TestData) (TestData, error) {
			errorCount++
			return d, errors.New("primary failed")
		}),
		pipz.Transform("fallback", func(_ context.Context, d TestData) TestData {
			d.Value = "fallback"
			return d
		}),
		pipz.Transform("success", func(_ context.Context, d TestData) TestData {
			d.Value = "primary"
			return d
		}),
	)

	tests := []struct {
		name        string
		schema      flume.Schema
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name: "fallback activates on error",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Children: []flume.Node{
						{Ref: "failing"},
						{Ref: "fallback"},
					},
				},
			},
			expected: "fallback",
		},
		{
			name: "fallback not used on success",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Children: []flume.Node{
						{Ref: "success"},
						{Ref: "fallback"},
					},
				},
			},
			expected: "primary",
		},
		{
			name: "fallback with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Name: "custom-fallback",
					Children: []flume.Node{
						{Ref: "success"},
						{Ref: "fallback"},
					},
				},
			},
			expected: "primary",
		},
		{
			name: "fallback with wrong number of children",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Children: []flume.Node{
						{Ref: "success"},
					},
				},
			},
			expectError: true,
			errorMsg:    "fallback requires exactly 2 children",
		},
		{
			name: "fallback with three children",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Children: []flume.Node{
						{Ref: "success"},
						{Ref: "fallback"},
						{Ref: "success"},
					},
				},
			},
			expectError: true,
			errorMsg:    "fallback requires exactly 2 children",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorCount = 0
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			if result.Value != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result.Value)
			}
		})
	}
}

func TestBuildRetry(t *testing.T) {
	factory := flume.New[TestData]()

	tests := []struct {
		name          string
		schema        flume.Schema
		setupFactory  func()
		expected      string
		expectError   bool
		errorMsg      string
		checkAttempts int
	}{
		{
			name: "retry succeeds after failures",
			setupFactory: func() {
				attempts := 0
				factory.Add(pipz.Apply("flaky", func(_ context.Context, d TestData) (TestData, error) {
					attempts++
					if attempts < 3 {
						return d, errors.New("temporary failure")
					}
					d.Value = "success"
					return d, nil
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "retry",
					Attempts: 3,
					Child:    &flume.Node{Ref: "flaky"},
				},
			},
			expected: "success",
		},
		{
			name: "retry with default attempts",
			setupFactory: func() {
				factory.Add(pipz.Transform("stable", func(_ context.Context, d TestData) TestData {
					d.Value = "default"
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:  "retry",
					Child: &flume.Node{Ref: "stable"},
				},
			},
			expected: "default",
		},
		{
			name: "retry with custom name",
			setupFactory: func() {
				factory.Add(pipz.Transform("stable", func(_ context.Context, d TestData) TestData {
					d.Value = "custom"
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:  "retry",
					Name:  "custom-retry",
					Child: &flume.Node{Ref: "stable"},
				},
			},
			expected: "custom",
		},
		{
			name: "backoff retry",
			setupFactory: func() {
				factory.Add(pipz.Transform("backoff-proc", func(_ context.Context, d TestData) TestData {
					d.Value = "backoff"
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "retry",
					Attempts: 2,
					Backoff:  "10ms",
					Child:    &flume.Node{Ref: "backoff-proc"},
				},
			},
			expected: "backoff",
		},
		{
			name: "backoff with custom name",
			setupFactory: func() {
				factory.Add(pipz.Transform("backoff-proc", func(_ context.Context, d TestData) TestData {
					d.Value = "backoff-custom"
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "retry",
					Name:     "custom-backoff",
					Attempts: 2,
					Backoff:  "10ms",
					Child:    &flume.Node{Ref: "backoff-proc"},
				},
			},
			expected: "backoff-custom",
		},
		{
			name:         "retry without child",
			setupFactory: func() {},
			schema: flume.Schema{
				Node: flume.Node{
					Type: "retry",
				},
			},
			expectError: true,
			errorMsg:    "retry requires a child",
		},
		{
			name: "invalid backoff duration",
			setupFactory: func() {
				factory.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:    "retry",
					Backoff: "invalid",
					Child:   &flume.Node{Ref: "test"},
				},
			},
			expectError: true,
			errorMsg:    "invalid backoff duration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset factory for each test
			factory = flume.New[TestData]()
			if tt.setupFactory != nil {
				tt.setupFactory()
			}

			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			if result.Value != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result.Value)
			}
		})
	}
}

func TestBuildTimeout(t *testing.T) {
	factory := flume.New[TestData]()

	factory.Add(
		pipz.Transform("fast", func(_ context.Context, d TestData) TestData {
			d.Value = "completed"
			return d
		}),
		pipz.Apply("slow", func(ctx context.Context, d TestData) (TestData, error) {
			select {
			case <-time.After(2 * time.Second):
				d.Value = "should-timeout"
				return d, nil
			case <-ctx.Done():
				return d, ctx.Err()
			}
		}),
	)

	tests := []struct {
		name        string
		schema      flume.Schema
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name: "timeout with fast processor",
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "timeout",
					Duration: "1s",
					Child:    &flume.Node{Ref: "fast"},
				},
			},
			expected: "completed",
		},
		{
			name: "timeout with default duration",
			schema: flume.Schema{
				Node: flume.Node{
					Type:  "timeout",
					Child: &flume.Node{Ref: "fast"},
				},
			},
			expected: "completed",
		},
		{
			name: "timeout with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "timeout",
					Name:     "custom-timeout",
					Duration: "500ms",
					Child:    &flume.Node{Ref: "fast"},
				},
			},
			expected: "completed",
		},
		{
			name: "timeout without child",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "timeout",
				},
			},
			expectError: true,
			errorMsg:    "timeout requires a child",
		},
		{
			name: "invalid timeout duration",
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "timeout",
					Duration: "invalid",
					Child:    &flume.Node{Ref: "fast"},
				},
			},
			expectError: true,
			errorMsg:    "invalid duration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			if result.Value != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result.Value)
			}
		})
	}
}

func TestBuildFilter(t *testing.T) {
	factory := flume.New[TestData]()

	factory.Add(
		pipz.Transform("then-branch", func(_ context.Context, d TestData) TestData {
			d.Value = "then"
			return d
		}),
		pipz.Transform("else-branch", func(_ context.Context, d TestData) TestData {
			d.Value = "else"
			return d
		}),
	)

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
	)

	tests := []struct {
		name        string
		schema      flume.Schema
		input       TestData
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name: "filter with then only - true condition",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "is-positive",
					Then:      &flume.Node{Ref: "then-branch"},
				},
			},
			input:    TestData{Counter: 5},
			expected: "then",
		},
		{
			name: "filter with then only - false condition",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "is-positive",
					Then:      &flume.Node{Ref: "then-branch"},
				},
			},
			input:    TestData{Counter: -5},
			expected: "", // Passthrough when predicate is false and no else
		},
		{
			name: "filter with else - true condition",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "is-high",
					Then:      &flume.Node{Ref: "then-branch"},
					Else:      &flume.Node{Ref: "else-branch"},
				},
			},
			input:    TestData{Counter: 20},
			expected: "then",
		},
		{
			name: "filter with else - false condition",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "is-high",
					Then:      &flume.Node{Ref: "then-branch"},
					Else:      &flume.Node{Ref: "else-branch"},
				},
			},
			input:    TestData{Counter: 5},
			expected: "else",
		},
		{
			name: "filter with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Name:      "custom-filter",
					Predicate: "is-positive",
					Then:      &flume.Node{Ref: "then-branch"},
				},
			},
			input:    TestData{Counter: 1},
			expected: "then",
		},
		{
			name: "filter without predicate",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "filter",
					Then: &flume.Node{Ref: "then-branch"},
				},
			},
			expectError: true,
			errorMsg:    "filter requires a predicate",
		},
		{
			name: "filter without then",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "is-positive",
				},
			},
			expectError: true,
			errorMsg:    "filter requires a then branch",
		},
		{
			name: "filter with missing predicate",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "non-existent",
					Then:      &flume.Node{Ref: "then-branch"},
				},
			},
			expectError: true,
			errorMsg:    "predicate not found: non-existent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, tt.input)
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			if result.Value != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result.Value)
			}
		})
	}
}

func TestBuildSwitch(t *testing.T) {
	factory := flume.New[TestData]()

	factory.Add(
		pipz.Transform("route-a", func(_ context.Context, d TestData) TestData {
			d.Value = "route-a"
			return d
		}),
		pipz.Transform("route-b", func(_ context.Context, d TestData) TestData {
			d.Value = "route-b"
			return d
		}),
		pipz.Transform("route-default", func(_ context.Context, d TestData) TestData {
			d.Value = "route-default"
			return d
		}),
	)

	factory.AddCondition(
		flume.Condition[TestData]{
			Name: "value-router",
			Condition: func(_ context.Context, d TestData) string {
				switch d.Value {
				case "a":
					return "a"
				case "b":
					return "b"
				default:
					return "default"
				}
			},
		},
		flume.Condition[TestData]{
			Name: "counter-router",
			Condition: func(_ context.Context, d TestData) string {
				if d.Counter < 5 {
					return "low"
				} else if d.Counter < 10 {
					return "medium"
				}
				return "high"
			},
		},
	)

	tests := []struct {
		name        string
		schema      flume.Schema
		input       TestData
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name: "switch basic routing",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "value-router",
					Routes: map[string]flume.Node{
						"a":       {Ref: "route-a"},
						"b":       {Ref: "route-b"},
						"default": {Ref: "route-default"},
					},
				},
			},
			input:    TestData{Value: "a"},
			expected: "route-a",
		},
		{
			name: "switch default route",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "value-router",
					Routes: map[string]flume.Node{
						"a":       {Ref: "route-a"},
						"b":       {Ref: "route-b"},
						"default": {Ref: "route-default"},
					},
				},
			},
			input:    TestData{Value: "unknown"},
			expected: "route-default",
		},
		{
			name: "switch with Default field",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "value-router",
					Routes: map[string]flume.Node{
						"a": {Ref: "route-a"},
						"b": {Ref: "route-b"},
					},
					Default: &flume.Node{Ref: "route-default"},
				},
			},
			input:    TestData{Value: "unknown"},
			expected: "route-default",
		},
		{
			name: "switch with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Name:      "custom-switch",
					Condition: "value-router",
					Routes: map[string]flume.Node{
						"a": {Ref: "route-a"},
					},
				},
			},
			input:    TestData{Value: "a"},
			expected: "route-a",
		},
		{
			name: "switch without condition",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "switch",
					Routes: map[string]flume.Node{
						"a": {Ref: "route-a"},
					},
				},
			},
			expectError: true,
			errorMsg:    "switch requires a condition",
		},
		{
			name: "switch without routes",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "value-router",
				},
			},
			expectError: true,
			errorMsg:    "switch requires at least one route",
		},
		{
			name: "switch with missing condition",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "non-existent",
					Routes: map[string]flume.Node{
						"a": {Ref: "route-a"},
					},
				},
			},
			expectError: true,
			errorMsg:    "condition not found: non-existent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, tt.input)
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			if result.Value != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result.Value)
			}
		})
	}
}

func TestBuildComplexNesting(t *testing.T) {
	factory := flume.New[TestData]()

	// Register processors
	factory.Add(
		pipz.Transform("prep", func(_ context.Context, d TestData) TestData {
			d.Value = "prepared"
			d.Counter = 15
			return d
		}),
		pipz.Transform("process-high", func(_ context.Context, d TestData) TestData {
			d.Value += "_high"
			return d
		}),
		pipz.Transform("process-low", func(_ context.Context, d TestData) TestData {
			d.Value += "_low"
			return d
		}),
		pipz.Transform("finalize", func(_ context.Context, d TestData) TestData {
			d.Value += "_done"
			return d
		}),
		pipz.Apply("may-fail", func(_ context.Context, d TestData) (TestData, error) {
			if d.Counter > 20 {
				return d, errors.New("too high")
			}
			d.Value += "_checked"
			return d, nil
		}),
		pipz.Transform("fallback-handler", func(_ context.Context, d TestData) TestData {
			d.Value += "_fallback"
			return d
		}),
	)

	// Register predicates
	factory.AddPredicate(flume.Predicate[TestData]{
		Name: "is-high",
		Predicate: func(_ context.Context, d TestData) bool {
			return d.Counter > 10
		},
	})

	// Complex nested schema
	schema := flume.Schema{
		Node: flume.Node{
			Type: "sequence",
			Name: "main-pipeline",
			Children: []flume.Node{
				{Ref: "prep"},
				{
					Type:      "filter",
					Predicate: "is-high",
					Then: &flume.Node{
						Type: "fallback",
						Children: []flume.Node{
							{
								Type:     "retry",
								Attempts: 2,
								Child:    &flume.Node{Ref: "may-fail"},
							},
							{Ref: "fallback-handler"},
						},
					},
					Else: &flume.Node{Ref: "process-low"},
				},
				{
					Type:     "timeout",
					Duration: "100ms",
					Child:    &flume.Node{Ref: "finalize"},
				},
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
		t.Fatalf("Process error: %v", pErr)
	}

	// With Counter=15 from prep, is-high is true, may-fail succeeds
	expected := "prepared_checked_done"
	if result.Value != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result.Value)
	}
}

func TestBuildNodeErrors(t *testing.T) {
	factory := flume.New[TestData]()

	tests := []struct {
		name          string
		schema        flume.Schema
		errorContains string
	}{
		{
			name: "unknown node type",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "invalid-type",
				},
			},
			errorContains: "unknown node type 'invalid-type'",
		},
		{
			name: "sequence with invalid child",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Type: "invalid-child-type"},
					},
				},
			},
			errorContains: "unknown node type 'invalid-child-type'",
		},
		{
			name: "filter with invalid then branch",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "test",
					Then:      &flume.Node{Type: "invalid-then"},
				},
			},
			errorContains: "unknown node type 'invalid-then'",
		},
		{
			name: "filter with invalid else branch - add predicate first",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "test-pred",
					Then:      &flume.Node{Ref: "test"},
					Else:      &flume.Node{Type: "invalid-else"},
				},
			},
			errorContains: "unknown node type 'invalid-else'",
		},
		{
			name: "switch with invalid route",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "test",
					Routes: map[string]flume.Node{
						"a": {Type: "invalid-route"},
					},
				},
			},
			errorContains: "unknown node type 'invalid-route'",
		},
		{
			name: "switch with invalid default - add condition first",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "test-cond",
					Routes: map[string]flume.Node{
						"a": {Ref: "test"},
					},
					Default: &flume.Node{Type: "invalid-default"},
				},
			},
			errorContains: "unknown node type 'invalid-default'",
		},
		{
			name: "retry with invalid child",
			schema: flume.Schema{
				Node: flume.Node{
					Type:  "retry",
					Child: &flume.Node{Type: "invalid-retry-child"},
				},
			},
			errorContains: "unknown node type 'invalid-retry-child'",
		},
		{
			name: "timeout with invalid child",
			schema: flume.Schema{
				Node: flume.Node{
					Type:  "timeout",
					Child: &flume.Node{Type: "invalid-timeout-child"},
				},
			},
			errorContains: "unknown node type 'invalid-timeout-child'",
		},
		{
			name: "fallback with invalid primary",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Children: []flume.Node{
						{Type: "invalid-primary"},
						{Ref: "test"},
					},
				},
			},
			errorContains: "unknown node type 'invalid-primary'",
		},
		{
			name: "fallback with invalid fallback branch - add processor first",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Children: []flume.Node{
						{Ref: "test-proc"},
						{Type: "invalid-fallback"},
					},
				},
			},
			errorContains: "unknown node type 'invalid-fallback'",
		},
	}

	// Add test predicate, condition and processor for some tests
	factory.AddPredicate(flume.Predicate[TestData]{
		Name: "test-pred",
		Predicate: func(_ context.Context, _ TestData) bool {
			return true
		},
	})
	factory.AddCondition(flume.Condition[TestData]{
		Name: "test-cond",
		Condition: func(_ context.Context, _ TestData) string {
			return "a"
		},
	})
	factory.Add(pipz.Transform("test-proc", func(_ context.Context, d TestData) TestData {
		return d
	}))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := factory.Build(tt.schema)
			if err == nil {
				t.Error("Expected error but got none")
			} else if !contains(err.Error(), tt.errorContains) {
				t.Errorf("Expected error containing '%s', got '%s'", tt.errorContains, err.Error())
			}
		})
	}
}

func TestBuildCircuitBreaker(t *testing.T) {
	factory := flume.New[TestData]()

	failureCount := 0
	factory.Add(
		pipz.Apply("failing", func(_ context.Context, d TestData) (TestData, error) {
			failureCount++
			if failureCount < 3 {
				return d, errors.New("service failure")
			}
			d.Value = "success"
			return d, nil
		}),
		pipz.Transform("stable", func(_ context.Context, d TestData) TestData {
			d.Value = "stable"
			return d
		}),
	)

	tests := []struct {
		name        string
		schema      flume.Schema
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name: "circuit breaker with stable processor",
			schema: flume.Schema{
				Node: flume.Node{
					Type:             "circuit-breaker",
					FailureThreshold: 3,
					RecoveryTimeout:  "30s",
					Child:            &flume.Node{Ref: "stable"},
				},
			},
			expected: "stable",
		},
		{
			name: "circuit breaker with default values",
			schema: flume.Schema{
				Node: flume.Node{
					Type:  "circuit-breaker",
					Child: &flume.Node{Ref: "stable"},
				},
			},
			expected: "stable",
		},
		{
			name: "circuit breaker with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type:             "circuit-breaker",
					Name:             "custom-breaker",
					FailureThreshold: 2,
					Child:            &flume.Node{Ref: "stable"},
				},
			},
			expected: "stable",
		},
		{
			name: "circuit breaker without child",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "circuit-breaker",
				},
			},
			expectError: true,
			errorMsg:    "circuit-breaker requires a child",
		},
		{
			name: "circuit breaker with invalid recovery timeout",
			schema: flume.Schema{
				Node: flume.Node{
					Type:            "circuit-breaker",
					RecoveryTimeout: "invalid",
					Child:           &flume.Node{Ref: "stable"},
				},
			},
			expectError: true,
			errorMsg:    "invalid recovery timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failureCount = 0
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			if result.Value != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result.Value)
			}
		})
	}
}

func TestBuildRateLimit(t *testing.T) {
	factory := flume.New[TestData]()

	factory.Add(
		pipz.Transform("processor", func(_ context.Context, d TestData) TestData {
			d.Value = "processed"
			return d
		}),
	)

	tests := []struct {
		name        string
		schema      flume.Schema
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name: "rate limit with custom values",
			schema: flume.Schema{
				Node: flume.Node{
					Type:              "rate-limit",
					RequestsPerSecond: 100.0,
					BurstSize:         10,
					Child:             &flume.Node{Ref: "processor"},
				},
			},
			expected: "processed",
		},
		{
			name: "rate limit with default values",
			schema: flume.Schema{
				Node: flume.Node{
					Type:  "rate-limit",
					Child: &flume.Node{Ref: "processor"},
				},
			},
			expected: "processed",
		},
		{
			name: "rate limit with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type:              "rate-limit",
					Name:              "custom-limiter",
					RequestsPerSecond: 50.0,
					BurstSize:         5,
					Child:             &flume.Node{Ref: "processor"},
				},
			},
			expected: "processed",
		},
		{
			name: "rate limit without child",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "rate-limit",
				},
			},
			expectError: true,
			errorMsg:    "rate-limit requires a child",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			if result.Value != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result.Value)
			}
		})
	}
}

func TestBuildConcurrentWithReducer(t *testing.T) {
	factory := flume.New[TestData]()

	factory.Add(
		pipz.Transform("double", func(_ context.Context, d TestData) TestData {
			d.Counter *= 2
			return d
		}),
		pipz.Transform("triple", func(_ context.Context, d TestData) TestData {
			d.Counter *= 3
			return d
		}),
	)

	factory.AddReducer(flume.Reducer[TestData]{
		Name:        "sum-reducer",
		Description: "Sums counter values from all results",
		Reducer: func(original TestData, results map[pipz.Name]TestData, _ map[pipz.Name]error) TestData {
			total := 0
			for _, r := range results {
				total += r.Counter
			}
			original.Counter = total
			return original
		},
	})

	tests := []struct {
		name        string
		schema      flume.Schema
		input       TestData
		expected    int
		expectError bool
		errorMsg    string
	}{
		{
			name: "concurrent with reducer",
			schema: flume.Schema{
				Node: flume.Node{
					Type:    "concurrent",
					Reducer: "sum-reducer",
					Children: []flume.Node{
						{Ref: "double"},
						{Ref: "triple"},
					},
				},
			},
			input:    TestData{Counter: 10},
			expected: 50, // 10*2 + 10*3 = 20 + 30 = 50
		},
		{
			name: "concurrent with reducer and custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type:    "concurrent",
					Name:    "custom-concurrent",
					Reducer: "sum-reducer",
					Children: []flume.Node{
						{Ref: "double"},
					},
				},
			},
			input:    TestData{Counter: 5},
			expected: 10, // 5*2 = 10
		},
		{
			name: "concurrent with missing reducer",
			schema: flume.Schema{
				Node: flume.Node{
					Type:    "concurrent",
					Reducer: "non-existent",
					Children: []flume.Node{
						{Ref: "double"},
					},
				},
			},
			expectError: true,
			errorMsg:    "reducer not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, tt.input)
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			if result.Counter != tt.expected {
				t.Errorf("Expected counter %d, got %d", tt.expected, result.Counter)
			}
		})
	}
}

func TestBuildContest(t *testing.T) {
	factory := flume.New[TestData]()

	factory.Add(
		pipz.Transform("winner", func(_ context.Context, d TestData) TestData {
			d.Value = "winner"
			d.Counter = 100
			return d
		}),
		pipz.Transform("loser", func(_ context.Context, d TestData) TestData {
			d.Value = "loser"
			d.Counter = 50
			return d
		}),
	)

	factory.AddPredicate(flume.Predicate[TestData]{
		Name: "highest-counter",
		Predicate: func(_ context.Context, d TestData) bool {
			return d.Counter >= 100
		},
	})

	tests := []struct {
		name        string
		schema      flume.Schema
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name: "contest basic",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "contest",
					Predicate: "highest-counter",
					Children: []flume.Node{
						{Ref: "winner"},
						{Ref: "loser"},
					},
				},
			},
			expected: "winner",
		},
		{
			name: "contest with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "contest",
					Name:      "custom-contest",
					Predicate: "highest-counter",
					Children: []flume.Node{
						{Ref: "winner"},
					},
				},
			},
			expected: "winner",
		},
		{
			name: "contest without predicate",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "contest",
					Children: []flume.Node{
						{Ref: "winner"},
					},
				},
			},
			expectError: true,
			errorMsg:    "contest requires a predicate",
		},
		{
			name: "contest without children",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "contest",
					Predicate: "highest-counter",
				},
			},
			expectError: true,
			errorMsg:    "contest requires at least one child",
		},
		{
			name: "contest with missing predicate",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "contest",
					Predicate: "non-existent",
					Children: []flume.Node{
						{Ref: "winner"},
					},
				},
			},
			expectError: true,
			errorMsg:    "predicate not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			if result.Value != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result.Value)
			}
		})
	}
}

func TestBuildHandle(t *testing.T) {
	factory := flume.New[TestData]()

	factory.Add(
		pipz.Apply("failing", func(_ context.Context, d TestData) (TestData, error) {
			return d, errors.New("intentional failure")
		}),
		pipz.Transform("stable", func(_ context.Context, d TestData) TestData {
			d.Value = "stable"
			return d
		}),
	)

	factory.AddErrorHandler(flume.ErrorHandler[TestData]{
		Name:        "log-handler",
		Description: "Logs errors and marks as handled",
		Handler: pipz.Transform("error-handler", func(_ context.Context, e *pipz.Error[TestData]) *pipz.Error[TestData] {
			e.InputData.Value = "handled"
			return e
		}),
	})

	tests := []struct {
		name        string
		schema      flume.Schema
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name: "handle with stable processor",
			schema: flume.Schema{
				Node: flume.Node{
					Type:         "handle",
					ErrorHandler: "log-handler",
					Child:        &flume.Node{Ref: "stable"},
				},
			},
			expected: "stable",
		},
		{
			name: "handle with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type:         "handle",
					Name:         "custom-handle",
					ErrorHandler: "log-handler",
					Child:        &flume.Node{Ref: "stable"},
				},
			},
			expected: "stable",
		},
		{
			name: "handle without error handler",
			schema: flume.Schema{
				Node: flume.Node{
					Type:  "handle",
					Child: &flume.Node{Ref: "stable"},
				},
			},
			expectError: true,
			errorMsg:    "handle requires an error handler",
		},
		{
			name: "handle without child",
			schema: flume.Schema{
				Node: flume.Node{
					Type:         "handle",
					ErrorHandler: "log-handler",
				},
			},
			expectError: true,
			errorMsg:    "handle requires a child",
		},
		{
			name: "handle with missing error handler",
			schema: flume.Schema{
				Node: flume.Node{
					Type:         "handle",
					ErrorHandler: "non-existent",
					Child:        &flume.Node{Ref: "stable"},
				},
			},
			expectError: true,
			errorMsg:    "error handler not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			if result.Value != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result.Value)
			}
		})
	}
}

func TestBuildScaffold(t *testing.T) {
	factory := flume.New[TestData]()

	executed := make(chan bool, 2)
	factory.Add(
		pipz.Transform("background-task", func(_ context.Context, d TestData) TestData {
			executed <- true
			d.Value = "background"
			return d
		}),
		pipz.Transform("another-task", func(_ context.Context, d TestData) TestData {
			executed <- true
			d.Value = "another"
			return d
		}),
	)

	tests := []struct {
		name        string
		schema      flume.Schema
		expectError bool
		errorMsg    string
	}{
		{
			name: "scaffold basic",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "scaffold",
					Children: []flume.Node{
						{Ref: "background-task"},
						{Ref: "another-task"},
					},
				},
			},
		},
		{
			name: "scaffold with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "scaffold",
					Name: "custom-scaffold",
					Children: []flume.Node{
						{Ref: "background-task"},
					},
				},
			},
		},
		{
			name: "scaffold without children",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "scaffold",
				},
			},
			expectError: true,
			errorMsg:    "scaffold requires at least one child",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{Value: "original"})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			// Scaffold returns original input unchanged
			if result.Value != "original" {
				t.Errorf("Expected 'original', got '%s'", result.Value)
			}
		})
	}
}

func TestBuildWorkerPool(t *testing.T) {
	factory := flume.New[TestData]()

	factory.Add(
		pipz.Transform("worker-task", func(_ context.Context, d TestData) TestData {
			d.Value = "processed"
			d.Counter++
			return d
		}),
	)

	tests := []struct {
		name        string
		schema      flume.Schema
		expectError bool
		errorMsg    string
	}{
		{
			name: "worker pool basic",
			schema: flume.Schema{
				Node: flume.Node{
					Type:    "worker-pool",
					Workers: 4,
					Children: []flume.Node{
						{Ref: "worker-task"},
					},
				},
			},
		},
		{
			name: "worker pool with default workers",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "worker-pool",
					Children: []flume.Node{
						{Ref: "worker-task"},
					},
				},
			},
		},
		{
			name: "worker pool with custom name",
			schema: flume.Schema{
				Node: flume.Node{
					Type:    "worker-pool",
					Name:    "custom-pool",
					Workers: 2,
					Children: []flume.Node{
						{Ref: "worker-task"},
					},
				},
			},
		},
		{
			name: "worker pool without children",
			schema: flume.Schema{
				Node: flume.Node{
					Type:    "worker-pool",
					Workers: 4,
				},
			},
			expectError: true,
			errorMsg:    "worker-pool requires at least one child",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.Build(tt.schema)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// WorkerPool returns the original input (fan-out pattern)
			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{Value: "original"})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}

			// Verify original input is returned unchanged
			if result.Value != "original" {
				t.Errorf("Expected 'original', got '%s'", result.Value)
			}
		})
	}
}

func TestReducerRegistration(t *testing.T) {
	factory := flume.New[TestData]()

	// Test AddReducer
	factory.AddReducer(
		flume.Reducer[TestData]{
			Name:        "test-reducer",
			Description: "Test reducer",
			Reducer: func(original TestData, _ map[pipz.Name]TestData, _ map[pipz.Name]error) TestData {
				return original
			},
		},
		flume.Reducer[TestData]{
			Name:        "another-reducer",
			Description: "Another reducer",
			Reducer: func(original TestData, _ map[pipz.Name]TestData, _ map[pipz.Name]error) TestData {
				return original
			},
		},
	)

	// Test HasReducer
	if !factory.HasReducer("test-reducer") {
		t.Error("Expected HasReducer to return true for registered reducer")
	}
	if factory.HasReducer("non-existent") {
		t.Error("Expected HasReducer to return false for non-existent reducer")
	}

	// Test ListReducers
	reducers := factory.ListReducers()
	if len(reducers) != 2 {
		t.Errorf("Expected 2 reducers, got %d", len(reducers))
	}

	// Test RemoveReducer
	factory.RemoveReducer("test-reducer")
	if factory.HasReducer("test-reducer") {
		t.Error("Expected reducer to be removed")
	}
}

func TestErrorHandlerRegistration(t *testing.T) {
	factory := flume.New[TestData]()

	// Test AddErrorHandler
	factory.AddErrorHandler(
		flume.ErrorHandler[TestData]{
			Name:        "test-handler",
			Description: "Test handler",
			Handler: pipz.Transform("handler", func(_ context.Context, e *pipz.Error[TestData]) *pipz.Error[TestData] {
				return e
			}),
		},
		flume.ErrorHandler[TestData]{
			Name:        "another-handler",
			Description: "Another handler",
			Handler: pipz.Transform("handler2", func(_ context.Context, e *pipz.Error[TestData]) *pipz.Error[TestData] {
				return e
			}),
		},
	)

	// Test HasErrorHandler
	if !factory.HasErrorHandler("test-handler") {
		t.Error("Expected HasErrorHandler to return true for registered handler")
	}
	if factory.HasErrorHandler("non-existent") {
		t.Error("Expected HasErrorHandler to return false for non-existent handler")
	}

	// Test ListErrorHandlers
	handlers := factory.ListErrorHandlers()
	if len(handlers) != 2 {
		t.Errorf("Expected 2 handlers, got %d", len(handlers))
	}

	// Test RemoveErrorHandler
	factory.RemoveErrorHandler("test-handler")
	if factory.HasErrorHandler("test-handler") {
		t.Error("Expected handler to be removed")
	}
}

func TestBuildErrorPaths(t *testing.T) {
	factory := flume.New[TestData]()

	// Register minimal components for testing
	factory.Add(
		pipz.Transform("exists", func(_ context.Context, d TestData) TestData {
			return d
		}),
	)
	factory.AddPredicate(flume.Predicate[TestData]{
		Name:      "exists",
		Predicate: func(_ context.Context, _ TestData) bool { return true },
	})
	factory.AddCondition(flume.Condition[TestData]{
		Name:      "exists",
		Condition: func(_ context.Context, _ TestData) string { return "a" },
	})

	tests := []struct {
		name         string
		schema       flume.Schema
		expectedPath string
	}{
		{
			name: "missing processor at root",
			schema: flume.Schema{
				Node: flume.Node{Ref: "missing"},
			},
			expectedPath: "root: processor 'missing' not found",
		},
		{
			name: "missing processor in sequence child",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Ref: "exists"},
						{Ref: "missing"},
					},
				},
			},
			expectedPath: "root.children[1]: processor 'missing' not found",
		},
		{
			name: "missing processor in nested sequence",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{
							Type: "sequence",
							Children: []flume.Node{
								{Ref: "missing"},
							},
						},
					},
				},
			},
			expectedPath: "root.children[0].children[0]: processor 'missing' not found",
		},
		{
			name: "missing processor in filter then",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "exists",
					Then:      &flume.Node{Ref: "missing"},
				},
			},
			expectedPath: "root.then: processor 'missing' not found",
		},
		{
			name: "missing processor in filter else",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "exists",
					Then:      &flume.Node{Ref: "exists"},
					Else:      &flume.Node{Ref: "missing"},
				},
			},
			expectedPath: "root.else: processor 'missing' not found",
		},
		{
			name: "missing processor in switch route",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "exists",
					Routes: map[string]flume.Node{
						"a": {Ref: "missing"},
					},
				},
			},
			expectedPath: "root.routes.a: processor 'missing' not found",
		},
		{
			name: "missing processor in switch default",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "exists",
					Routes: map[string]flume.Node{
						"a": {Ref: "exists"},
					},
					Default: &flume.Node{Ref: "missing"},
				},
			},
			expectedPath: "root.default: processor 'missing' not found",
		},
		{
			name: "missing processor in retry child",
			schema: flume.Schema{
				Node: flume.Node{
					Type:  "retry",
					Child: &flume.Node{Ref: "missing"},
				},
			},
			expectedPath: "root.child: processor 'missing' not found",
		},
		{
			name: "missing processor in timeout child",
			schema: flume.Schema{
				Node: flume.Node{
					Type:  "timeout",
					Child: &flume.Node{Ref: "missing"},
				},
			},
			expectedPath: "root.child: processor 'missing' not found",
		},
		{
			name: "missing processor in fallback primary",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Children: []flume.Node{
						{Ref: "missing"},
						{Ref: "exists"},
					},
				},
			},
			expectedPath: "root.children[0](primary): processor 'missing' not found",
		},
		{
			name: "missing processor in fallback secondary",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Children: []flume.Node{
						{Ref: "exists"},
						{Ref: "missing"},
					},
				},
			},
			expectedPath: "root.children[1](fallback): processor 'missing' not found",
		},
		{
			name: "error includes connector type context",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{
							Type: "concurrent",
							Children: []flume.Node{
								{Ref: "exists"},
								{
									Type: "race",
									Children: []flume.Node{
										{Ref: "missing"},
									},
								},
							},
						},
					},
				},
			},
			expectedPath: "root.children[0].children[1].children[0]: processor 'missing' not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := factory.Build(tt.schema)
			if err == nil {
				t.Fatal("expected error but got none")
			}
			if err.Error() != tt.expectedPath {
				t.Errorf("expected error:\n  %s\ngot:\n  %s", tt.expectedPath, err.Error())
			}
		})
	}
}
