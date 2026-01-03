package flume_test

import (
	"context"
	"strings"
	"testing"

	"github.com/zoobzio/flume"
	"github.com/zoobzio/pipz"
)

func TestValidation(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(*flume.Factory[TestData])
		schema         flume.Schema
		expectedErrors []string
	}{
		{
			name: "missing processor reference",
			setup: func(_ *flume.Factory[TestData]) {
				// Don't register the processor
			},
			schema: flume.Schema{
				Node: flume.Node{
					Ref: "non-existent",
				},
			},
			expectedErrors: []string{
				"processor 'non-existent' not found",
			},
		},
		{
			name: "missing predicate",
			setup: func(f *flume.Factory[TestData]) {
				testID := f.Identity("test", "Test processor")
				f.Add(pipz.Transform(testID, func(_ context.Context, d TestData) TestData {
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "missing-predicate",
					Then:      &flume.Node{Ref: "test"},
				},
			},
			expectedErrors: []string{
				"predicate 'missing-predicate' not found",
			},
		},
		{
			name: "missing condition",
			setup: func(f *flume.Factory[TestData]) {
				testID := f.Identity("test", "Test processor")
				f.Add(pipz.Transform(testID, func(_ context.Context, d TestData) TestData {
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "missing-condition",
					Routes: map[string]flume.Node{
						"a": {Ref: "test"},
					},
				},
			},
			expectedErrors: []string{
				"condition 'missing-condition' not found",
			},
		},
		{
			name:  "empty node",
			setup: func(_ *flume.Factory[TestData]) {},
			schema: flume.Schema{
				Node: flume.Node{
					// Neither ref nor type
				},
			},
			expectedErrors: []string{
				"empty node - must have either ref, type, or stream",
			},
		},
		{
			name: "both ref and type",
			setup: func(f *flume.Factory[TestData]) {
				testID := f.Identity("test", "Test processor")
				f.Add(pipz.Transform(testID, func(_ context.Context, d TestData) TestData {
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Ref:  "test",
					Type: "sequence",
				},
			},
			expectedErrors: []string{
				"node cannot have both 'ref' and 'type'",
			},
		},
		{
			name:  "sequence without children",
			setup: func(_ *flume.Factory[TestData]) {},
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
				},
			},
			expectedErrors: []string{
				"sequence requires at least one child",
			},
		},
		{
			name: "fallback with wrong number of children",
			setup: func(f *flume.Factory[TestData]) {
				testID := f.Identity("test", "Test processor")
				f.Add(pipz.Transform(testID, func(_ context.Context, d TestData) TestData {
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Children: []flume.Node{
						{Ref: "test"},
						// Missing second child
					},
				},
			},
			expectedErrors: []string{
				"fallback requires exactly 2 children",
			},
		},
		{
			name:  "retry without child",
			setup: func(_ *flume.Factory[TestData]) {},
			schema: flume.Schema{
				Node: flume.Node{
					Type: "retry",
				},
			},
			expectedErrors: []string{
				"retry requires a child",
			},
		},
		{
			name: "negative retry attempts",
			setup: func(f *flume.Factory[TestData]) {
				testID := f.Identity("test", "Test processor")
				f.Add(pipz.Transform(testID, func(_ context.Context, d TestData) TestData {
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "retry",
					Attempts: -1,
					Child:    &flume.Node{Ref: "test"},
				},
			},
			expectedErrors: []string{
				"attempts must be positive",
			},
		},
		{
			name: "invalid timeout duration",
			setup: func(f *flume.Factory[TestData]) {
				testID := f.Identity("test", "Test processor")
				f.Add(pipz.Transform(testID, func(_ context.Context, d TestData) TestData {
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "timeout",
					Duration: "invalid",
					Child:    &flume.Node{Ref: "test"},
				},
			},
			expectedErrors: []string{
				"invalid duration",
			},
		},
		{
			name: "invalid backoff duration",
			setup: func(f *flume.Factory[TestData]) {
				testID := f.Identity("test", "Test processor")
				f.Add(pipz.Transform(testID, func(_ context.Context, d TestData) TestData {
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:    "retry",
					Backoff: "invalid-backoff",
					Child:   &flume.Node{Ref: "test"},
				},
			},
			expectedErrors: []string{
				"invalid backoff duration",
			},
		},
		{
			name: "filter without then",
			setup: func(f *flume.Factory[TestData]) {
				testPredID := f.Identity("test-pred", "Test predicate")
				f.AddPredicate(flume.Predicate[TestData]{
					Identity: testPredID,
					Predicate: func(_ context.Context, _ TestData) bool {
						return true
					},
				})
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "test-pred",
					// Missing Then
				},
			},
			expectedErrors: []string{
				"filter requires a 'then' branch",
			},
		},
		{
			name: "switch without routes",
			setup: func(f *flume.Factory[TestData]) {
				testCondID := f.Identity("test-cond", "Test condition")
				f.AddCondition(flume.Condition[TestData]{
					Identity: testCondID,
					Condition: func(_ context.Context, _ TestData) string {
						return "a"
					},
				})
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "test-cond",
					// Missing Routes
				},
			},
			expectedErrors: []string{
				"switch requires at least one route",
			},
		},
		{
			name:  "empty concurrent children",
			setup: func(_ *flume.Factory[TestData]) {},
			schema: flume.Schema{
				Node: flume.Node{
					Type: "concurrent",
				},
			},
			expectedErrors: []string{
				"concurrent requires at least one child",
			},
		},
		{
			name:  "empty race children",
			setup: func(_ *flume.Factory[TestData]) {},
			schema: flume.Schema{
				Node: flume.Node{
					Type: "race",
				},
			},
			expectedErrors: []string{
				"race requires at least one child",
			},
		},
		{
			name:  "timeout without child",
			setup: func(_ *flume.Factory[TestData]) {},
			schema: flume.Schema{
				Node: flume.Node{
					Type: "timeout",
				},
			},
			expectedErrors: []string{
				"timeout requires a child",
			},
		},
		{
			name: "multiple errors",
			setup: func(f *flume.Factory[TestData]) {
				// Only register one processor
				existsID := f.Identity("exists", "Existing processor")
				f.Add(pipz.Transform(existsID, func(_ context.Context, d TestData) TestData {
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Ref: "missing1"},
						{Ref: "exists"},
						{Ref: "missing2"},
						{
							Type:      "filter",
							Predicate: "missing-pred",
							Then:      &flume.Node{Ref: "missing3"},
						},
					},
				},
			},
			expectedErrors: []string{
				"processor 'missing1' not found",
				"processor 'missing2' not found",
				"predicate 'missing-pred' not found",
				"processor 'missing3' not found",
			},
		},
		{
			name: "nested validation errors",
			setup: func(f *flume.Factory[TestData]) {
				routeID := f.Identity("route", "Routing condition")
				f.AddCondition(flume.Condition[TestData]{
					Identity: routeID,
					Condition: func(_ context.Context, _ TestData) string {
						return "a"
					},
				})
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "route",
					Routes: map[string]flume.Node{
						"a": {
							Type: "sequence",
							Children: []flume.Node{
								{Ref: "missing-in-route"},
							},
						},
					},
				},
			},
			expectedErrors: []string{
				"routes.a.children[0]: processor 'missing-in-route' not found",
			},
		},
		{
			name: "circular reference in sequence",
			setup: func(f *flume.Factory[TestData]) {
				// Register a processor that exists
				proc1ID := f.Identity("proc1", "First processor")
				f.Add(pipz.Transform(proc1ID, func(_ context.Context, d TestData) TestData {
					return d
				}))
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Ref: "proc1"},
						{Ref: "proc1"}, // Same ref again creates a logical cycle
					},
				},
			},
			expectedErrors: []string{
				"circular reference detected: 'proc1' creates a cycle",
			},
		},
		{
			name: "circular reference in nested structure",
			setup: func(f *flume.Factory[TestData]) {
				proc1ID := f.Identity("proc1", "First processor")
				proc2ID := f.Identity("proc2", "Second processor")
				f.Add(
					pipz.Transform(proc1ID, func(_ context.Context, d TestData) TestData {
						return d
					}),
					pipz.Transform(proc2ID, func(_ context.Context, d TestData) TestData {
						return d
					}),
				)
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Ref: "proc1"},
						{
							Type: "concurrent",
							Children: []flume.Node{
								{Ref: "proc2"},
								{Ref: "proc1"}, // Cycle: proc1 already in path
							},
						},
					},
				},
			},
			expectedErrors: []string{
				"circular reference detected: 'proc1' creates a cycle",
			},
		},
		{
			name: "same processor in switch routes is valid",
			setup: func(f *flume.Factory[TestData]) {
				approveID := f.Identity("approve", "Approval processor")
				rejectID := f.Identity("reject", "Rejection processor")
				routeID := f.Identity("route", "Routing condition")
				f.Add(
					pipz.Transform(approveID, func(_ context.Context, d TestData) TestData {
						return d
					}),
					pipz.Transform(rejectID, func(_ context.Context, d TestData) TestData {
						return d
					}),
				)
				f.AddCondition(flume.Condition[TestData]{
					Identity: routeID,
					Condition: func(_ context.Context, _ TestData) string {
						return "a"
					},
				})
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "route",
					Routes: map[string]flume.Node{
						"high":   {Ref: "reject"},
						"medium": {Ref: "approve"},
						"low":    {Ref: "approve"}, // Same as medium - valid in switch
					},
				},
			},
			expectedErrors: []string{}, // No errors expected
		},
		{
			name: "same processor in filter branches is valid",
			setup: func(f *flume.Factory[TestData]) {
				processID := f.Identity("process", "Processing handler")
				checkID := f.Identity("check", "Check predicate")
				f.Add(
					pipz.Transform(processID, func(_ context.Context, d TestData) TestData {
						return d
					}),
				)
				f.AddPredicate(flume.Predicate[TestData]{
					Identity: checkID,
					Predicate: func(_ context.Context, _ TestData) bool {
						return true
					},
				})
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "check",
					Then:      &flume.Node{Ref: "process"},
					Else:      &flume.Node{Ref: "process"}, // Same as then - valid in filter
				},
			},
			expectedErrors: []string{}, // No errors expected
		},
		{
			name: "same processor in fallback branches is valid",
			setup: func(f *flume.Factory[TestData]) {
				handlerID := f.Identity("handler", "Fallback handler")
				f.Add(
					pipz.Transform(handlerID, func(_ context.Context, d TestData) TestData {
						return d
					}),
				)
			},
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Children: []flume.Node{
						{Ref: "handler"}, // Primary
						{Ref: "handler"}, // Fallback - same processor, valid
					},
				},
			},
			expectedErrors: []string{}, // No errors expected
		},
		{
			name: "valid stream with registered channel",
			setup: func(f *flume.Factory[TestData]) {
				channel := make(chan TestData, 10)
				f.AddChannel("test-channel", channel)
			},
			schema: flume.Schema{
				Node: flume.Node{
					Stream: "test-channel",
				},
			},
			expectedErrors: []string{}, // No errors expected
		},
		{
			name: "missing channel",
			setup: func(_ *flume.Factory[TestData]) {
				// Don't register any channel
			},
			schema: flume.Schema{
				Node: flume.Node{
					Stream: "missing",
				},
			},
			expectedErrors: []string{
				"channel 'missing' not found",
			},
		},
		{
			name: "stream with invalid child processor",
			setup: func(f *flume.Factory[TestData]) {
				channel := make(chan TestData, 10)
				f.AddChannel("test-channel", channel)
			},
			schema: flume.Schema{
				Node: flume.Node{
					Stream: "test-channel",
					Child:  &flume.Node{Ref: "missing-processor"},
				},
			},
			expectedErrors: []string{
				"processor 'missing-processor' not found",
			},
		},
		{
			name: "stream with invalid children processor",
			setup: func(f *flume.Factory[TestData]) {
				channel := make(chan TestData, 10)
				f.AddChannel("test-channel", channel)
			},
			schema: flume.Schema{
				Node: flume.Node{
					Stream:   "test-channel",
					Children: []flume.Node{{Ref: "missing-processor"}},
				},
			},
			expectedErrors: []string{
				"processor 'missing-processor' not found",
			},
		},
		{
			name: "stream with type field",
			setup: func(f *flume.Factory[TestData]) {
				channel := make(chan TestData, 10)
				f.AddChannel("test-channel", channel)
			},
			schema: flume.Schema{
				Node: flume.Node{
					Stream: "test-channel",
					Type:   "sequence",
				},
			},
			expectedErrors: []string{
				"stream node should not have type or ref fields",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := flume.New[TestData]()
			tt.setup(factory)

			err := factory.ValidateSchema(tt.schema)

			if len(tt.expectedErrors) == 0 {
				if err != nil {
					t.Errorf("Expected no errors, got: %v", err)
				}
				return
			}

			if err == nil {
				t.Errorf("Expected errors but got none")
				return
			}

			// Check that all expected errors are present
			errStr := err.Error()
			for _, expected := range tt.expectedErrors {
				if !strings.Contains(errStr, expected) {
					t.Errorf("Expected error containing '%s', got: %v", expected, err)
				}
			}
		})
	}
}

func TestValidationWithBuild(t *testing.T) {
	factory := flume.New[TestData]()

	// Try to build invalid schema
	schema := flume.Schema{
		Node: flume.Node{
			Ref: "non-existent",
		},
	}

	_, err := factory.Build(schema)
	if err == nil {
		t.Error("Expected build to fail with validation error")
	}

	if !strings.Contains(err.Error(), "processor 'non-existent' not found") {
		t.Errorf("Expected validation error, got: %v", err)
	}
}

func TestValidationErrorFormatting(t *testing.T) {
	factory := flume.New[TestData]()

	// Create a schema with multiple errors
	schema := flume.Schema{
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "missing1"},
				{Ref: "missing2"},
			},
		},
	}

	err := factory.ValidateSchema(schema)
	if err == nil {
		t.Fatal("Expected validation errors")
	}

	// Check error formatting
	errStr := err.Error()
	if !strings.Contains(errStr, "2 validation errors:") {
		t.Errorf("Expected formatted error count, got: %v", err)
	}
}

func TestSchemaVersion(t *testing.T) {
	factory := flume.New[TestData]()

	// Define identities
	proc1ID := factory.Identity("proc1", "Test processor for versioning")
	testID := factory.Identity("test", "Test binding")

	// Create a simple processor
	factory.Add(pipz.Apply(proc1ID, func(_ context.Context, data TestData) (TestData, error) {
		return data, nil
	}))

	// Test schema with version
	schemaV1 := flume.Schema{
		Version: "1.0.0",
		Node: flume.Node{
			Ref: "proc1",
		},
	}

	// Register and bind initial schema
	if err := factory.SetSchema("versioned-schema", schemaV1); err != nil {
		t.Fatalf("Failed to set schema v1: %v", err)
	}

	binding, err := factory.Bind(testID, "versioned-schema", flume.WithAutoSync[TestData]())
	if err != nil {
		t.Fatalf("Failed to bind schema v1: %v", err)
	}

	// Verify binding works
	ctx := context.Background()
	_, pErr := binding.Process(ctx, TestData{})
	if pErr != nil {
		t.Fatalf("Failed to process with v1: %v", pErr)
	}

	// Update to version 2 via SetSchema (auto-sync)
	schemaV2 := flume.Schema{
		Version: "2.0.0",
		Node: flume.Node{
			Ref: "proc1",
		},
	}

	err = factory.SetSchema("versioned-schema", schemaV2)
	if err != nil {
		t.Fatalf("Failed to update schema to v2: %v", err)
	}

	// Verify updated binding works
	_, pErr = binding.Process(ctx, TestData{})
	if pErr != nil {
		t.Fatalf("Failed to process with v2: %v", pErr)
	}

	// Test BuildFromJSON with version
	jsonWithVersion := `{
		"version": "3.0.0",
		"ref": "proc1"
	}`

	pipeline, err := factory.BuildFromJSON(jsonWithVersion)
	if err != nil {
		t.Fatalf("Failed to build from JSON with version: %v", err)
	}
	if pipeline == nil {
		t.Fatal("Expected non-nil pipeline")
	}

	// Test BuildFromYAML with version
	// Since Schema embeds Node, we need to verify the YAML structure
	yamlWithVersion := `version: "4.0.0"
ref: "proc1"`

	pipeline, err = factory.BuildFromYAML(yamlWithVersion)
	if err != nil {
		t.Fatalf("Failed to build from YAML with version: %v", err)
	}
	if pipeline == nil {
		t.Fatal("Expected non-nil pipeline")
	}

	// Test schema without version works too
	schemaNoVersion := flume.Schema{
		Node: flume.Node{
			Ref: "proc1",
		},
	}

	if err := factory.SetSchema("no-version-schema", schemaNoVersion); err != nil {
		t.Fatalf("Failed to set schema without version: %v", err)
	}

	noVersionID := factory.Identity("no-version", "Binding without version")
	binding2, err := factory.Bind(noVersionID, "no-version-schema")
	if err != nil {
		t.Fatalf("Failed to bind schema without version: %v", err)
	}

	// Verify it works
	_, pErr = binding2.Process(ctx, TestData{})
	if pErr != nil {
		t.Fatalf("Failed to process schema without version: %v", pErr)
	}
}

func TestValidateSchemaStructure(t *testing.T) {
	tests := []struct {
		name           string
		schema         flume.Schema
		expectedErrors []string
	}{
		{
			name: "valid simple ref",
			schema: flume.Schema{
				Node: flume.Node{
					Ref: "any-processor", // Reference existence not checked
				},
			},
			expectedErrors: []string{},
		},
		{
			name: "valid sequence with refs",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Ref: "proc1"},
						{Ref: "proc2"},
					},
				},
			},
			expectedErrors: []string{},
		},
		{
			name: "valid complex nested structure",
			schema: flume.Schema{
				Version: "1.0.0",
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Ref: "validate"},
						{
							Type:      "filter",
							Predicate: "is-valid", // Reference existence not checked
							Then:      &flume.Node{Ref: "process"},
							Else:      &flume.Node{Ref: "reject"},
						},
						{
							Type:     "retry",
							Attempts: 3,
							Backoff:  "100ms",
							Child:    &flume.Node{Ref: "save"},
						},
					},
				},
			},
			expectedErrors: []string{},
		},
		{
			name: "empty node",
			schema: flume.Schema{
				Node: flume.Node{},
			},
			expectedErrors: []string{
				"empty node - must have either ref, type, or stream",
			},
		},
		{
			name: "both ref and type",
			schema: flume.Schema{
				Node: flume.Node{
					Ref:  "proc",
					Type: "sequence",
				},
			},
			expectedErrors: []string{
				"node cannot have both 'ref' and 'type'",
			},
		},
		{
			name: "unknown node type",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "unknown-type",
				},
			},
			expectedErrors: []string{
				"unknown node type 'unknown-type'",
			},
		},
		{
			name: "sequence without children",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
				},
			},
			expectedErrors: []string{
				"sequence requires at least one child",
			},
		},
		{
			name: "concurrent without children",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "concurrent",
				},
			},
			expectedErrors: []string{
				"concurrent requires at least one child",
			},
		},
		{
			name: "race without children",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "race",
				},
			},
			expectedErrors: []string{
				"race requires at least one child",
			},
		},
		{
			name: "fallback with wrong child count",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "fallback",
					Children: []flume.Node{
						{Ref: "only-one"},
					},
				},
			},
			expectedErrors: []string{
				"fallback requires exactly 2 children",
			},
		},
		{
			name: "retry without child",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "retry",
				},
			},
			expectedErrors: []string{
				"retry requires a child",
			},
		},
		{
			name: "retry with negative attempts",
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "retry",
					Attempts: -1,
					Child:    &flume.Node{Ref: "proc"},
				},
			},
			expectedErrors: []string{
				"attempts must be positive",
			},
		},
		{
			name: "retry with invalid backoff",
			schema: flume.Schema{
				Node: flume.Node{
					Type:    "retry",
					Backoff: "invalid",
					Child:   &flume.Node{Ref: "proc"},
				},
			},
			expectedErrors: []string{
				"invalid backoff duration",
			},
		},
		{
			name: "timeout without child",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "timeout",
				},
			},
			expectedErrors: []string{
				"timeout requires a child",
			},
		},
		{
			name: "timeout with invalid duration",
			schema: flume.Schema{
				Node: flume.Node{
					Type:     "timeout",
					Duration: "not-a-duration",
					Child:    &flume.Node{Ref: "proc"},
				},
			},
			expectedErrors: []string{
				"invalid duration",
			},
		},
		{
			name: "filter without predicate",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "filter",
					Then: &flume.Node{Ref: "proc"},
				},
			},
			expectedErrors: []string{
				"filter requires a predicate",
			},
		},
		{
			name: "filter without then",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "check",
				},
			},
			expectedErrors: []string{
				"filter requires a 'then' branch",
			},
		},
		{
			name: "switch without condition",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "switch",
					Routes: map[string]flume.Node{
						"a": {Ref: "proc"},
					},
				},
			},
			expectedErrors: []string{
				"switch requires a condition",
			},
		},
		{
			name: "switch without routes",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "route",
				},
			},
			expectedErrors: []string{
				"switch requires at least one route",
			},
		},
		{
			name: "circuit-breaker without child",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "circuit-breaker",
				},
			},
			expectedErrors: []string{
				"circuit-breaker requires a child",
			},
		},
		{
			name: "circuit-breaker with invalid recovery timeout",
			schema: flume.Schema{
				Node: flume.Node{
					Type:            "circuit-breaker",
					RecoveryTimeout: "bad",
					Child:           &flume.Node{Ref: "proc"},
				},
			},
			expectedErrors: []string{
				"invalid recovery timeout",
			},
		},
		{
			name: "rate-limit without child",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "rate-limit",
				},
			},
			expectedErrors: []string{
				"rate-limit requires a child",
			},
		},
		{
			name: "rate-limit with negative rps",
			schema: flume.Schema{
				Node: flume.Node{
					Type:              "rate-limit",
					RequestsPerSecond: -1,
					Child:             &flume.Node{Ref: "proc"},
				},
			},
			expectedErrors: []string{
				"requests_per_second must be non-negative",
			},
		},
		{
			name: "rate-limit with negative burst",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "rate-limit",
					BurstSize: -1,
					Child:     &flume.Node{Ref: "proc"},
				},
			},
			expectedErrors: []string{
				"burst_size must be non-negative",
			},
		},
		{
			name: "contest without predicate",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "contest",
					Children: []flume.Node{
						{Ref: "proc"},
					},
				},
			},
			expectedErrors: []string{
				"contest requires a predicate",
			},
		},
		{
			name: "contest without children",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "contest",
					Predicate: "check",
				},
			},
			expectedErrors: []string{
				"contest requires at least one child",
			},
		},
		{
			name: "handle without child",
			schema: flume.Schema{
				Node: flume.Node{
					Type:         "handle",
					ErrorHandler: "handler",
				},
			},
			expectedErrors: []string{
				"handle requires a child",
			},
		},
		{
			name: "handle without error handler",
			schema: flume.Schema{
				Node: flume.Node{
					Type:  "handle",
					Child: &flume.Node{Ref: "proc"},
				},
			},
			expectedErrors: []string{
				"handle requires an error_handler",
			},
		},
		{
			name: "scaffold without children",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "scaffold",
				},
			},
			expectedErrors: []string{
				"scaffold requires at least one child",
			},
		},
		{
			name: "worker-pool without children",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "worker-pool",
				},
			},
			expectedErrors: []string{
				"worker-pool requires at least one child",
			},
		},
		{
			name: "worker-pool with negative workers",
			schema: flume.Schema{
				Node: flume.Node{
					Type:    "worker-pool",
					Workers: -1,
					Children: []flume.Node{
						{Ref: "proc"},
					},
				},
			},
			expectedErrors: []string{
				"workers must be non-negative",
			},
		},
		{
			name: "stream with invalid timeout",
			schema: flume.Schema{
				Node: flume.Node{
					Stream:        "output",
					StreamTimeout: "bad-duration",
				},
			},
			expectedErrors: []string{
				"invalid stream_timeout",
			},
		},
		{
			name: "stream with both child and children",
			schema: flume.Schema{
				Node: flume.Node{
					Stream: "output",
					Child:  &flume.Node{Ref: "proc1"},
					Children: []flume.Node{
						{Ref: "proc2"},
					},
				},
			},
			expectedErrors: []string{
				"stream node has both 'child' and 'children' specified",
			},
		},
		{
			name: "stream with type field",
			schema: flume.Schema{
				Node: flume.Node{
					Stream: "output",
					Type:   "sequence",
				},
			},
			expectedErrors: []string{
				"stream node should not have type or ref fields",
			},
		},
		{
			name: "multiple structural errors",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{}, // empty
						{
							Type: "retry",
							// missing child - returns early, doesn't check attempts
						},
						{
							Type:     "timeout",
							Duration: "bad",
							Child:    &flume.Node{Ref: "proc"},
						},
					},
				},
			},
			expectedErrors: []string{
				"empty node",
				"retry requires a child",
				"invalid duration",
			},
		},
		{
			name: "nested errors in routes",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "route",
					Routes: map[string]flume.Node{
						"a": {
							Type: "sequence",
							// missing children
						},
					},
				},
			},
			expectedErrors: []string{
				"sequence requires at least one child",
			},
		},
		{
			name: "does not check processor references",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Ref: "nonexistent-processor-1"},
						{Ref: "nonexistent-processor-2"},
					},
				},
			},
			expectedErrors: []string{}, // No errors - refs are not validated
		},
		{
			name: "does not check predicate references",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Predicate: "nonexistent-predicate",
					Then:      &flume.Node{Ref: "proc"},
				},
			},
			expectedErrors: []string{}, // No errors - predicate refs are not validated
		},
		{
			name: "does not check condition references",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "switch",
					Condition: "nonexistent-condition",
					Routes: map[string]flume.Node{
						"a": {Ref: "proc"},
					},
				},
			},
			expectedErrors: []string{}, // No errors - condition refs are not validated
		},
		{
			name: "does not check reducer references",
			schema: flume.Schema{
				Node: flume.Node{
					Type:    "concurrent",
					Reducer: "nonexistent-reducer",
					Children: []flume.Node{
						{Ref: "proc"},
					},
				},
			},
			expectedErrors: []string{}, // No errors - reducer refs are not validated
		},
		{
			name: "does not check error handler references",
			schema: flume.Schema{
				Node: flume.Node{
					Type:         "handle",
					ErrorHandler: "nonexistent-handler",
					Child:        &flume.Node{Ref: "proc"},
				},
			},
			expectedErrors: []string{}, // No errors - error handler refs are not validated
		},
		{
			name: "does not check channel references",
			schema: flume.Schema{
				Node: flume.Node{
					Stream: "nonexistent-channel",
				},
			},
			expectedErrors: []string{}, // No errors - channel refs are not validated
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := flume.ValidateSchemaStructure(tt.schema)

			if len(tt.expectedErrors) == 0 {
				if err != nil {
					t.Errorf("Expected no errors, got: %v", err)
				}
				return
			}

			if err == nil {
				t.Errorf("Expected errors but got none")
				return
			}

			errStr := err.Error()
			for _, expected := range tt.expectedErrors {
				if !strings.Contains(errStr, expected) {
					t.Errorf("Expected error containing '%s', got: %v", expected, err)
				}
			}
		})
	}
}

func TestValidateSchemaStructureComplexValid(t *testing.T) {
	// Test a complex but structurally valid schema
	schema := flume.Schema{
		Version: "1.0.0",
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "validate"},
				{
					Type:      "filter",
					Predicate: "is-valid",
					Then:      &flume.Node{Ref: "process"},
					Else: &flume.Node{
						Type:     "retry",
						Attempts: 3,
						Backoff:  "100ms",
						Child:    &flume.Node{Ref: "fallback-process"},
					},
				},
				{
					Type:     "timeout",
					Duration: "5s",
					Child: &flume.Node{
						Type: "circuit-breaker",
						Child: &flume.Node{
							Type:              "rate-limit",
							RequestsPerSecond: 10,
							BurstSize:         5,
							Child:             &flume.Node{Ref: "save"},
						},
					},
				},
			},
		},
	}

	err := flume.ValidateSchemaStructure(schema)
	if err != nil {
		t.Errorf("Expected valid schema structure, got: %v", err)
	}
}

func TestValidateSchemaStructureVsValidateSchema(t *testing.T) {
	// Demonstrate the difference between structural and full validation
	schema := flume.Schema{
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "nonexistent-processor"},
			},
		},
	}

	// Structural validation passes (doesn't check references)
	err := flume.ValidateSchemaStructure(schema)
	if err != nil {
		t.Errorf("ValidateSchemaStructure should pass for structurally valid schema: %v", err)
	}

	// Full validation fails (processor not registered)
	factory := flume.New[TestData]()
	err = factory.ValidateSchema(schema)
	if err == nil {
		t.Error("ValidateSchema should fail for missing processor")
	}
	if !strings.Contains(err.Error(), "processor 'nonexistent-processor' not found") {
		t.Errorf("Expected processor not found error, got: %v", err)
	}
}
