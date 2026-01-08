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

	// Define identities upfront
	step1ID := factory.Identity("step1", "Appends _1 suffix to value")
	step2ID := factory.Identity("step2", "Appends _2 suffix to value")
	step3ID := factory.Identity("step3", "Appends _3 suffix to value")

	// Register test processors
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
	)

	tests := []struct {
		name        string
		errorMsg    string
		schema      flume.Schema
		input       TestData
		expected    TestData
		expectError bool
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

	// Define identities upfront
	addSuffixID := factory.Identity("add-suffix", "Appends _concurrent suffix to value")
	incrementID := factory.Identity("increment", "Increments counter by 10")

	// Register test processors that modify different fields
	factory.Add(
		pipz.Transform(addSuffixID, func(_ context.Context, d TestData) TestData {
			d.Value += "_concurrent"
			return d
		}),
		pipz.Transform(incrementID, func(_ context.Context, d TestData) TestData {
			d.Counter += 10
			return d
		}),
	)

	tests := []struct {
		name        string
		errorMsg    string
		schema      flume.Schema
		expectError bool
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

	// Define identities upfront
	fastID := factory.Identity("fast", "Sets value to fast")
	slowID := factory.Identity("slow", "Sets value to slow")

	factory.Add(
		pipz.Transform(fastID, func(_ context.Context, d TestData) TestData {
			d.Value = "fast"
			return d
		}),
		pipz.Transform(slowID, func(_ context.Context, d TestData) TestData {
			d.Value = "slow"
			return d
		}),
	)

	tests := []struct {
		name        string
		errorMsg    string
		schema      flume.Schema
		expectError bool
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

	// Define identities upfront
	failingID := factory.Identity("failing", "Always fails with error")
	fallbackID := factory.Identity("fallback", "Fallback processor sets value to fallback")
	successID := factory.Identity("success", "Sets value to primary")

	errorCount := 0
	factory.Add(
		pipz.Apply(failingID, func(_ context.Context, d TestData) (TestData, error) {
			errorCount++
			return d, errors.New("primary failed")
		}),
		pipz.Transform(fallbackID, func(_ context.Context, d TestData) TestData {
			d.Value = "fallback"
			return d
		}),
		pipz.Transform(successID, func(_ context.Context, d TestData) TestData {
			d.Value = "primary"
			return d
		}),
	)

	tests := []struct {
		name        string
		expected    string
		errorMsg    string
		schema      flume.Schema
		expectError bool
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
		setupFactory  func()
		name          string
		expected      string
		errorMsg      string
		schema        flume.Schema
		checkAttempts int
		expectError   bool
	}{
		{
			name: "retry succeeds after failures",
			setupFactory: func() {
				flakyID := factory.Identity("flaky", "Fails twice then succeeds")
				attempts := 0
				factory.Add(pipz.Apply(flakyID, func(_ context.Context, d TestData) (TestData, error) {
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
				stableID := factory.Identity("stable", "Stable processor for retry tests")
				factory.Add(pipz.Transform(stableID, func(_ context.Context, d TestData) TestData {
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
				stableID := factory.Identity("stable", "Stable processor for retry tests")
				factory.Add(pipz.Transform(stableID, func(_ context.Context, d TestData) TestData {
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
				backoffProcID := factory.Identity("backoff-proc", "Processor with backoff delay")
				factory.Add(pipz.Transform(backoffProcID, func(_ context.Context, d TestData) TestData {
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
				backoffProcID := factory.Identity("backoff-proc", "Processor with backoff delay")
				factory.Add(pipz.Transform(backoffProcID, func(_ context.Context, d TestData) TestData {
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
				testID := factory.Identity("test", "Test processor for invalid backoff")
				factory.Add(pipz.Transform(testID, func(_ context.Context, d TestData) TestData {
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

	// Define identities upfront
	fastID := factory.Identity("fast", "Fast processor that completes immediately")
	slowID := factory.Identity("slow", "Slow processor that takes 2 seconds")

	factory.Add(
		pipz.Transform(fastID, func(_ context.Context, d TestData) TestData {
			d.Value = "completed"
			return d
		}),
		pipz.Apply(slowID, func(ctx context.Context, d TestData) (TestData, error) {
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
		expected    string
		errorMsg    string
		schema      flume.Schema
		expectError bool
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

	// Define identities upfront
	thenBranchID := factory.Identity("then-branch", "Executes when predicate is true")
	elseBranchID := factory.Identity("else-branch", "Executes when predicate is false")
	isPositiveID := factory.Identity("is-positive", "Returns true when counter is positive")
	isHighID := factory.Identity("is-high", "Returns true when counter exceeds 10")

	factory.Add(
		pipz.Transform(thenBranchID, func(_ context.Context, d TestData) TestData {
			d.Value = "then"
			return d
		}),
		pipz.Transform(elseBranchID, func(_ context.Context, d TestData) TestData {
			d.Value = "else"
			return d
		}),
	)

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
	)

	tests := []struct {
		name        string
		expected    string
		errorMsg    string
		schema      flume.Schema
		input       TestData
		expectError bool
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

	// Define identities upfront
	routeAID := factory.Identity("route-a", "Handler for route A")
	routeBID := factory.Identity("route-b", "Handler for route B")
	routeDefaultID := factory.Identity("route-default", "Default route handler")
	valueRouterID := factory.Identity("value-router", "Routes based on value field")
	counterRouterID := factory.Identity("counter-router", "Routes based on counter thresholds")

	factory.Add(
		pipz.Transform(routeAID, func(_ context.Context, d TestData) TestData {
			d.Value = "route-a"
			return d
		}),
		pipz.Transform(routeBID, func(_ context.Context, d TestData) TestData {
			d.Value = "route-b"
			return d
		}),
		pipz.Transform(routeDefaultID, func(_ context.Context, d TestData) TestData {
			d.Value = "route-default"
			return d
		}),
	)

	factory.AddCondition(
		flume.Condition[TestData]{
			Identity: valueRouterID,
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
			Identity: counterRouterID,
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
		expected    string
		errorMsg    string
		schema      flume.Schema
		input       TestData
		expectError bool
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

	// Define identities upfront
	prepID := factory.Identity("prep", "Prepares data for processing")
	processHighID := factory.Identity("process-high", "Handles high counter values")
	processLowID := factory.Identity("process-low", "Handles low counter values")
	finalizeID := factory.Identity("finalize", "Finalizes the processed data")
	mayFailID := factory.Identity("may-fail", "May fail if counter exceeds 20")
	fallbackHandlerID := factory.Identity("fallback-handler", "Handles failures with fallback")
	isHighID := factory.Identity("is-high", "Returns true when counter exceeds 10")

	// Register processors
	factory.Add(
		pipz.Transform(prepID, func(_ context.Context, d TestData) TestData {
			d.Value = "prepared"
			d.Counter = 15
			return d
		}),
		pipz.Transform(processHighID, func(_ context.Context, d TestData) TestData {
			d.Value += "_high"
			return d
		}),
		pipz.Transform(processLowID, func(_ context.Context, d TestData) TestData {
			d.Value += "_low"
			return d
		}),
		pipz.Transform(finalizeID, func(_ context.Context, d TestData) TestData {
			d.Value += "_done"
			return d
		}),
		pipz.Apply(mayFailID, func(_ context.Context, d TestData) (TestData, error) {
			if d.Counter > 20 {
				return d, errors.New("too high")
			}
			d.Value += "_checked"
			return d, nil
		}),
		pipz.Transform(fallbackHandlerID, func(_ context.Context, d TestData) TestData {
			d.Value += "_fallback"
			return d
		}),
	)

	// Register predicates
	factory.AddPredicate(flume.Predicate[TestData]{
		Identity: isHighID,
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
		errorContains string
		schema        flume.Schema
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
	testPredID := factory.Identity("test-pred", "Test predicate for error path tests")
	testCondID := factory.Identity("test-cond", "Test condition for error path tests")
	testProcID := factory.Identity("test-proc", "Test processor for error path tests")

	factory.AddPredicate(flume.Predicate[TestData]{
		Identity: testPredID,
		Predicate: func(_ context.Context, _ TestData) bool {
			return true
		},
	})
	factory.AddCondition(flume.Condition[TestData]{
		Identity: testCondID,
		Condition: func(_ context.Context, _ TestData) string {
			return "a"
		},
	})
	factory.Add(pipz.Transform(testProcID, func(_ context.Context, d TestData) TestData {
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

	// Define identities upfront
	failingID := factory.Identity("failing", "Fails first two attempts then succeeds")
	stableID := factory.Identity("stable", "Stable processor for circuit breaker tests")

	failureCount := 0
	factory.Add(
		pipz.Apply(failingID, func(_ context.Context, d TestData) (TestData, error) {
			failureCount++
			if failureCount < 3 {
				return d, errors.New("service failure")
			}
			d.Value = "success"
			return d, nil
		}),
		pipz.Transform(stableID, func(_ context.Context, d TestData) TestData {
			d.Value = "stable"
			return d
		}),
	)

	tests := []struct {
		name        string
		expected    string
		errorMsg    string
		schema      flume.Schema
		expectError bool
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

	// Define identities upfront
	processorID := factory.Identity("processor", "Processor for rate limit tests")

	factory.Add(
		pipz.Transform(processorID, func(_ context.Context, d TestData) TestData {
			d.Value = "processed"
			return d
		}),
	)

	tests := []struct {
		name        string
		expected    string
		errorMsg    string
		schema      flume.Schema
		expectError bool
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

	// Define identities upfront
	doubleID := factory.Identity("double", "Doubles the counter value")
	tripleID := factory.Identity("triple", "Triples the counter value")
	sumReducerID := factory.Identity("sum-reducer", "Sums counter values from all results")

	factory.Add(
		pipz.Transform(doubleID, func(_ context.Context, d TestData) TestData {
			d.Counter *= 2
			return d
		}),
		pipz.Transform(tripleID, func(_ context.Context, d TestData) TestData {
			d.Counter *= 3
			return d
		}),
	)

	factory.AddReducer(flume.Reducer[TestData]{
		Identity: sumReducerID,
		Reducer: func(original TestData, results map[pipz.Identity]TestData, _ map[pipz.Identity]error) TestData {
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
		errorMsg    string
		schema      flume.Schema
		input       TestData
		expected    int
		expectError bool
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

	// Define identities upfront
	winnerID := factory.Identity("winner", "Sets counter to 100")
	loserID := factory.Identity("loser", "Sets counter to 50")
	highestCounterID := factory.Identity("highest-counter", "Returns true when counter is 100 or more")

	factory.Add(
		pipz.Transform(winnerID, func(_ context.Context, d TestData) TestData {
			d.Value = "winner"
			d.Counter = 100
			return d
		}),
		pipz.Transform(loserID, func(_ context.Context, d TestData) TestData {
			d.Value = "loser"
			d.Counter = 50
			return d
		}),
	)

	factory.AddPredicate(flume.Predicate[TestData]{
		Identity: highestCounterID,
		Predicate: func(_ context.Context, d TestData) bool {
			return d.Counter >= 100
		},
	})

	tests := []struct {
		name        string
		expected    string
		errorMsg    string
		schema      flume.Schema
		expectError bool
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

	// Define identities upfront
	failingID := factory.Identity("failing", "Always fails with intentional error")
	stableID := factory.Identity("stable", "Stable processor for handle tests")
	logHandlerID := factory.Identity("log-handler", "Logs errors and marks as handled")
	errorHandlerID := factory.Identity("error-handler", "Internal error handler processor")

	factory.Add(
		pipz.Apply(failingID, func(_ context.Context, d TestData) (TestData, error) {
			return d, errors.New("intentional failure")
		}),
		pipz.Transform(stableID, func(_ context.Context, d TestData) TestData {
			d.Value = "stable"
			return d
		}),
	)

	factory.AddErrorHandler(flume.ErrorHandler[TestData]{
		Identity: logHandlerID,
		Handler: pipz.Transform(errorHandlerID, func(_ context.Context, e *pipz.Error[TestData]) *pipz.Error[TestData] {
			e.InputData.Value = "handled"
			return e
		}),
	})

	tests := []struct {
		name        string
		expected    string
		errorMsg    string
		schema      flume.Schema
		expectError bool
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

	// Define identities upfront
	backgroundTaskID := factory.Identity("background-task", "Background task processor")
	anotherTaskID := factory.Identity("another-task", "Another background task")

	executed := make(chan bool, 2)
	factory.Add(
		pipz.Transform(backgroundTaskID, func(_ context.Context, d TestData) TestData {
			executed <- true
			d.Value = "background"
			return d
		}),
		pipz.Transform(anotherTaskID, func(_ context.Context, d TestData) TestData {
			executed <- true
			d.Value = "another"
			return d
		}),
	)

	tests := []struct {
		name        string
		errorMsg    string
		schema      flume.Schema
		expectError bool
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

	// Define identities upfront
	workerTaskID := factory.Identity("worker-task", "Worker task processor")

	factory.Add(
		pipz.Transform(workerTaskID, func(_ context.Context, d TestData) TestData {
			d.Value = "processed"
			d.Counter++
			return d
		}),
	)

	tests := []struct {
		name        string
		errorMsg    string
		schema      flume.Schema
		expectError bool
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

	// Define identities upfront
	testReducerID := factory.Identity("test-reducer", "Test reducer for registration tests")
	anotherReducerID := factory.Identity("another-reducer", "Another reducer for registration tests")

	// Test AddReducer
	factory.AddReducer(
		flume.Reducer[TestData]{
			Identity: testReducerID,
			Reducer: func(original TestData, _ map[pipz.Identity]TestData, _ map[pipz.Identity]error) TestData {
				return original
			},
		},
		flume.Reducer[TestData]{
			Identity: anotherReducerID,
			Reducer: func(original TestData, _ map[pipz.Identity]TestData, _ map[pipz.Identity]error) TestData {
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

	// Define identities upfront
	testHandlerID := factory.Identity("test-handler", "Test handler for registration tests")
	handlerID := factory.Identity("handler", "Internal handler processor")
	anotherHandlerID := factory.Identity("another-handler", "Another handler for registration tests")
	handler2ID := factory.Identity("handler2", "Internal handler processor 2")

	// Test AddErrorHandler
	factory.AddErrorHandler(
		flume.ErrorHandler[TestData]{
			Identity: testHandlerID,
			Handler: pipz.Transform(handlerID, func(_ context.Context, e *pipz.Error[TestData]) *pipz.Error[TestData] {
				return e
			}),
		},
		flume.ErrorHandler[TestData]{
			Identity: anotherHandlerID,
			Handler: pipz.Transform(handler2ID, func(_ context.Context, e *pipz.Error[TestData]) *pipz.Error[TestData] {
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

func TestBuildStream(t *testing.T) {
	t.Run("stream with timeout succeeds when channel has capacity", func(t *testing.T) {
		factory := flume.New[TestData]()
		channel := make(chan TestData, 10)
		factory.AddChannel("test-channel", channel)

		schema := flume.Schema{
			Node: flume.Node{
				Stream:        "test-channel",
				StreamTimeout: "1s",
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "test"}

		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result.Value != "test" {
			t.Errorf("expected 'test', got '%s'", result.Value)
		}

		// Verify channel received data
		select {
		case received := <-channel:
			if received.Value != "test" {
				t.Errorf("expected 'test', got '%s'", received.Value)
			}
		default:
			t.Error("channel didn't receive expected data")
		}
	})

	t.Run("stream with timeout fails when channel is full", func(t *testing.T) {
		factory := flume.New[TestData]()
		channel := make(chan TestData) // unbuffered, will block
		factory.AddChannel("blocked-channel", channel)

		schema := flume.Schema{
			Node: flume.Node{
				Stream:        "blocked-channel",
				StreamTimeout: "50ms",
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "test"}

		_, err = pipeline.Process(ctx, input)
		if err == nil {
			t.Fatal("expected timeout error but got none")
		}
		if !contains(err.Error(), "write timeout") {
			t.Errorf("expected timeout error, got: %v", err)
		}
	})

	t.Run("stream without timeout respects context cancellation", func(t *testing.T) {
		factory := flume.New[TestData]()
		channel := make(chan TestData) // unbuffered, will block
		factory.AddChannel("blocked-channel", channel)

		schema := flume.Schema{
			Node: flume.Node{
				Stream: "blocked-channel",
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		input := TestData{Value: "test"}

		_, err = pipeline.Process(ctx, input)
		if err == nil {
			t.Fatal("expected context error but got none")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("stream timeout validation", func(t *testing.T) {
		factory := flume.New[TestData]()
		channel := make(chan TestData, 10)
		factory.AddChannel("test-channel", channel)

		schema := flume.Schema{
			Node: flume.Node{
				Stream:        "test-channel",
				StreamTimeout: "invalid",
			},
		}

		err := factory.ValidateSchema(schema)
		if err == nil {
			t.Fatal("expected validation error but got none")
		}
		if !contains(err.Error(), "invalid stream_timeout") {
			t.Errorf("expected stream_timeout validation error, got: %v", err)
		}
	})
}

func TestStreamIntegration(t *testing.T) {
	t.Run("pipeline ending with stream", func(t *testing.T) {
		factory := flume.New[TestData]()

		// Define identities
		validateID := factory.Identity("validate", "Validates data is not empty")
		enrichID := factory.Identity("enrich", "Enriches data with prefix")

		// Add processors
		factory.Add(
			pipz.Apply(validateID, func(_ context.Context, d TestData) (TestData, error) {
				if d.Value == "" {
					return d, errors.New("empty value")
				}
				return d, nil
			}),
			pipz.Transform(enrichID, func(_ context.Context, d TestData) TestData {
				d.Value = "enriched-" + d.Value
				return d
			}),
		)

		// Add channel
		channel := make(chan TestData, 10)
		factory.AddChannel("output-channel", channel)

		// Build pipeline that ends with stream
		schema := flume.Schema{
			Node: flume.Node{
				Type: "sequence",
				Children: []flume.Node{
					{Ref: "validate"},
					{Ref: "enrich"},
					{Stream: "output-channel"},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		// Process data
		ctx := context.Background()
		input := TestData{Value: "test"}

		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Pipeline should return the enriched result
		if result.Value != "enriched-test" {
			t.Errorf("expected 'enriched-test', got '%s'", result.Value)
		}

		// Check channel output
		select {
		case received := <-channel:
			if received.Value != "enriched-test" {
				t.Errorf("expected 'enriched-test', got '%s'", received.Value)
			}
		default:
			t.Error("channel didn't receive expected data")
		}
	})

	t.Run("switch routing to different channels", func(t *testing.T) {
		factory := flume.New[TestData]()

		// Define identities
		routeByValueID := factory.Identity("route-by-value", "Routes data by value to priority queues")

		// Add condition
		factory.AddCondition(flume.Condition[TestData]{
			Identity: routeByValueID,
			Condition: func(_ context.Context, d TestData) string {
				if d.Value == "high" {
					return "high-priority"
				}
				return "low-priority"
			},
		})

		// Add channels
		highChannel := make(chan TestData, 10)
		lowChannel := make(chan TestData, 10)
		factory.AddChannel("high-channel", highChannel)
		factory.AddChannel("low-channel", lowChannel)

		// Build switch that routes to different channels
		schema := flume.Schema{
			Node: flume.Node{
				Type:      "switch",
				Condition: "route-by-value",
				Routes: map[string]flume.Node{
					"high-priority": {Stream: "high-channel"},
					"low-priority":  {Stream: "low-channel"},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		// Process different values
		ctx := context.Background()

		// Send high priority
		highData := TestData{Value: "high"}
		_, err = pipeline.Process(ctx, highData)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Send low priority
		lowData := TestData{Value: "low"}
		_, err = pipeline.Process(ctx, lowData)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify routing
		select {
		case received := <-highChannel:
			if received.Value != "high" {
				t.Errorf("high channel: expected 'high', got '%s'", received.Value)
			}
		default:
			t.Error("high channel didn't receive expected data")
		}

		select {
		case received := <-lowChannel:
			if received.Value != "low" {
				t.Errorf("low channel: expected 'low', got '%s'", received.Value)
			}
		default:
			t.Error("low channel didn't receive expected data")
		}
	})
}

func TestBuildErrorPaths(t *testing.T) {
	factory := flume.New[TestData]()

	// Define identities upfront - note: these use the same "exists" name for processor/predicate/condition
	// to test error paths when components exist
	existsProcessorID := factory.Identity("exists", "Processor for error path tests")
	// Note: predicates and conditions can share names with processors as they are in separate registries
	existsPredicateID := pipz.NewIdentity("exists", "Predicate for error path tests")
	existsConditionID := pipz.NewIdentity("exists", "Condition for error path tests")

	// Register minimal components for testing
	factory.Add(
		pipz.Transform(existsProcessorID, func(_ context.Context, d TestData) TestData {
			return d
		}),
	)
	factory.AddPredicate(flume.Predicate[TestData]{
		Identity:  existsPredicateID,
		Predicate: func(_ context.Context, _ TestData) bool { return true },
	})
	factory.AddCondition(flume.Condition[TestData]{
		Identity:  existsConditionID,
		Condition: func(_ context.Context, _ TestData) string { return "a" },
	})

	tests := []struct {
		name         string
		expectedPath string
		schema       flume.Schema
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

// TestBuildStreamWithChild tests stream with child continuation after sending to channel.
func TestBuildStreamWithChild(t *testing.T) {
	factory := flume.New[TestData]()

	// Define identities
	transformID := factory.Identity("transform-after-stream", "Transforms data after stream send")

	factory.Add(
		pipz.Transform(transformID, func(_ context.Context, d TestData) TestData {
			d.Value = "transformed-" + d.Value
			return d
		}),
	)

	channel := make(chan TestData, 10)
	factory.AddChannel("stream-channel", channel)

	t.Run("stream with single child continues processing", func(t *testing.T) {
		schema := flume.Schema{
			Node: flume.Node{
				Stream:        "stream-channel",
				StreamTimeout: "1s",
				Child:         &flume.Node{Ref: "transform-after-stream"},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "original"}

		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Result should be transformed
		if result.Value != "transformed-original" {
			t.Errorf("expected 'transformed-original', got '%s'", result.Value)
		}

		// Channel should have received original data (before transform)
		select {
		case received := <-channel:
			if received.Value != "original" {
				t.Errorf("channel expected 'original', got '%s'", received.Value)
			}
		default:
			t.Error("channel didn't receive expected data")
		}
	})

	t.Run("stream with multiple children continues processing", func(t *testing.T) {
		step1ID := factory.Identity("step-1", "First step after stream")
		step2ID := factory.Identity("step-2", "Second step after stream")

		factory.Add(
			pipz.Transform(step1ID, func(_ context.Context, d TestData) TestData {
				d.Value += "_step1"
				return d
			}),
			pipz.Transform(step2ID, func(_ context.Context, d TestData) TestData {
				d.Value += "_step2"
				return d
			}),
		)

		schema := flume.Schema{
			Node: flume.Node{
				Stream:        "stream-channel",
				StreamTimeout: "1s",
				Children: []flume.Node{
					{Ref: "step-1"},
					{Ref: "step-2"},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "base"}

		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Result should have both steps applied
		if result.Value != "base_step1_step2" {
			t.Errorf("expected 'base_step1_step2', got '%s'", result.Value)
		}

		// Channel should have received original data
		select {
		case received := <-channel:
			if received.Value != "base" {
				t.Errorf("channel expected 'base', got '%s'", received.Value)
			}
		default:
			t.Error("channel didn't receive expected data")
		}
	})

	t.Run("stream with custom name", func(t *testing.T) {
		schema := flume.Schema{
			Node: flume.Node{
				Name:          "custom-stream-name",
				Stream:        "stream-channel",
				StreamTimeout: "1s",
				Child:         &flume.Node{Ref: "transform-after-stream"},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "test"}

		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Value != "transformed-test" {
			t.Errorf("expected 'transformed-test', got '%s'", result.Value)
		}

		// Drain channel
		<-channel
	})
}

// TestBuildHandleWithActualErrors tests handle connector with actual error scenarios.
// Note: The handle connector processes errors via the handler but still returns the error
// (it's for observability/logging, not recovery). Use fallback for error recovery.
func TestBuildHandleWithActualErrors(t *testing.T) {
	factory := flume.New[TestData]()

	// Define identities
	failingID := factory.Identity("failing-processor", "Always fails with error")
	observerHandlerID := factory.Identity("observer-handler", "Observes errors")
	observerProcessorID := factory.Identity("observer-processor", "Internal observer processor")
	conditionalFailID := factory.Identity("conditional-fail", "Fails based on input")

	var handlerCalled bool
	var capturedError string

	factory.Add(
		pipz.Apply(failingID, func(_ context.Context, d TestData) (TestData, error) {
			return d, errors.New("intentional failure from child")
		}),
		pipz.Apply(conditionalFailID, func(_ context.Context, d TestData) (TestData, error) {
			if d.Value == "fail" {
				return d, errors.New("conditional failure")
			}
			return d, nil
		}),
	)

	// Error handler that observes errors (handle doesn't suppress errors, just processes them)
	factory.AddErrorHandler(flume.ErrorHandler[TestData]{
		Identity: observerHandlerID,
		Handler: pipz.Transform(observerProcessorID, func(_ context.Context, e *pipz.Error[TestData]) *pipz.Error[TestData] {
			handlerCalled = true
			capturedError = e.Err.Error()
			return e
		}),
	})

	t.Run("handle invokes error handler on child error", func(t *testing.T) {
		handlerCalled = false
		capturedError = ""

		schema := flume.Schema{
			Node: flume.Node{
				Type:         "handle",
				ErrorHandler: "observer-handler",
				Child:        &flume.Node{Ref: "failing-processor"},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "original"}

		_, err = pipeline.Process(ctx, input)
		// Handle still returns the error (it's for observability, not recovery)
		if err == nil {
			t.Fatal("expected error to be returned")
		}

		if !handlerCalled {
			t.Error("expected error handler to be called")
		}

		if capturedError != "intentional failure from child" {
			t.Errorf("expected captured error 'intentional failure from child', got '%s'", capturedError)
		}
	})

	t.Run("handle with nested sequence invokes handler on failure", func(t *testing.T) {
		handlerCalled = false
		capturedError = ""

		step1ID := factory.Identity("step-before-fail", "Step before failure")
		factory.Add(
			pipz.Transform(step1ID, func(_ context.Context, d TestData) TestData {
				d.Value = "step1-" + d.Value
				return d
			}),
		)

		schema := flume.Schema{
			Node: flume.Node{
				Type:         "handle",
				ErrorHandler: "observer-handler",
				Child: &flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Ref: "step-before-fail"},
						{Ref: "failing-processor"},
					},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "original"}

		_, err = pipeline.Process(ctx, input)
		if err == nil {
			t.Fatal("expected error to be returned")
		}

		if !handlerCalled {
			t.Error("expected error handler to be called for nested failure")
		}
	})

	t.Run("handle passes through when no error", func(t *testing.T) {
		handlerCalled = false

		schema := flume.Schema{
			Node: flume.Node{
				Type:         "handle",
				ErrorHandler: "observer-handler",
				Child:        &flume.Node{Ref: "conditional-fail"},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "success"} // Won't trigger failure

		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Value should pass through unchanged since no error occurred
		if result.Value != "success" {
			t.Errorf("expected 'success', got '%s'", result.Value)
		}

		if handlerCalled {
			t.Error("expected error handler NOT to be called when no error")
		}
	})

	t.Run("handle with error invocation", func(t *testing.T) {
		handlerCalled = false

		schema := flume.Schema{
			Node: flume.Node{
				Type:         "handle",
				ErrorHandler: "observer-handler",
				Child:        &flume.Node{Ref: "conditional-fail"},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "fail"} // Will trigger failure

		_, err = pipeline.Process(ctx, input)
		if err == nil {
			t.Fatal("expected error to be returned")
		}

		if !handlerCalled {
			t.Error("expected error handler to be called when error occurs")
		}

		if capturedError != "conditional failure" {
			t.Errorf("expected 'conditional failure', got '%s'", capturedError)
		}
	})
}

// TestBuildFallbackComplex tests fallback connector with complex scenarios.
func TestBuildFallbackComplex(t *testing.T) {
	factory := flume.New[TestData]()

	// Define identities
	failingPrimaryID := factory.Identity("failing-primary", "Primary that always fails")
	failingFallbackID := factory.Identity("failing-fallback", "Fallback that also fails")
	successFallbackID := factory.Identity("success-fallback", "Fallback that succeeds")
	nestedFailID := factory.Identity("nested-fail", "Nested processor that fails")

	factory.Add(
		pipz.Apply(failingPrimaryID, func(_ context.Context, d TestData) (TestData, error) {
			return d, errors.New("primary failure")
		}),
		pipz.Apply(failingFallbackID, func(_ context.Context, d TestData) (TestData, error) {
			return d, errors.New("fallback also failed")
		}),
		pipz.Transform(successFallbackID, func(_ context.Context, d TestData) TestData {
			d.Value = "fallback-success"
			return d
		}),
		pipz.Apply(nestedFailID, func(_ context.Context, d TestData) (TestData, error) {
			return d, errors.New("nested failure")
		}),
	)

	t.Run("fallback with nested sequence primary that fails", func(t *testing.T) {
		step1ID := factory.Identity("seq-step1", "First step in sequence")
		factory.Add(
			pipz.Transform(step1ID, func(_ context.Context, d TestData) TestData {
				d.Value = "step1-" + d.Value
				return d
			}),
		)

		schema := flume.Schema{
			Node: flume.Node{
				Type: "fallback",
				Children: []flume.Node{
					{
						Type: "sequence",
						Children: []flume.Node{
							{Ref: "seq-step1"},
							{Ref: "nested-fail"},
						},
					},
					{Ref: "success-fallback"},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "original"}

		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Value != "fallback-success" {
			t.Errorf("expected 'fallback-success', got '%s'", result.Value)
		}
	})

	t.Run("fallback when both primary and fallback fail", func(t *testing.T) {
		schema := flume.Schema{
			Node: flume.Node{
				Type: "fallback",
				Children: []flume.Node{
					{Ref: "failing-primary"},
					{Ref: "failing-fallback"},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "original"}

		_, err = pipeline.Process(ctx, input)
		if err == nil {
			t.Fatal("expected error when both primary and fallback fail")
		}
		if !contains(err.Error(), "fallback also failed") {
			t.Errorf("expected fallback error, got: %v", err)
		}
	})

	t.Run("nested fallback recovery", func(t *testing.T) {
		deepFallbackID := factory.Identity("deep-fallback", "Deep fallback processor")
		factory.Add(
			pipz.Transform(deepFallbackID, func(_ context.Context, d TestData) TestData {
				d.Value = "deep-recovery"
				return d
			}),
		)

		schema := flume.Schema{
			Node: flume.Node{
				Type: "fallback",
				Children: []flume.Node{
					{Ref: "failing-primary"},
					{
						Type: "fallback",
						Children: []flume.Node{
							{Ref: "failing-fallback"},
							{Ref: "deep-fallback"},
						},
					},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := TestData{Value: "original"}

		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Value != "deep-recovery" {
			t.Errorf("expected 'deep-recovery', got '%s'", result.Value)
		}
	})
}
