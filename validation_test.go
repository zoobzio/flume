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
				f.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
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
				f.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
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
				f.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
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
				f.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
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
				f.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
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
				f.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
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
				f.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
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
				f.AddPredicate(flume.Predicate[TestData]{
					Name: "test-pred",
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
				f.AddCondition(flume.Condition[TestData]{
					Name: "test-cond",
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
				f.Add(pipz.Transform("exists", func(_ context.Context, d TestData) TestData {
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
				f.AddCondition(flume.Condition[TestData]{
					Name: "route",
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
				f.Add(pipz.Transform("proc1", func(_ context.Context, d TestData) TestData {
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
				f.Add(
					pipz.Transform("proc1", func(_ context.Context, d TestData) TestData {
						return d
					}),
					pipz.Transform("proc2", func(_ context.Context, d TestData) TestData {
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

	// Create a simple processor
	factory.Add(pipz.Apply("proc1", func(_ context.Context, data TestData) (TestData, error) {
		return data, nil
	}))

	// Test schema with version
	schemaV1 := flume.Schema{
		Version: "1.0.0",
		Node: flume.Node{
			Ref: "proc1",
		},
	}

	// Set initial schema
	err := factory.SetSchema("test", schemaV1)
	if err != nil {
		t.Fatalf("Failed to set schema v1: %v", err)
	}

	// Get schema and verify version
	retrieved, ok := factory.GetSchema("test")
	if !ok {
		t.Fatal("Failed to retrieve schema")
	}
	if retrieved.Version != "1.0.0" {
		t.Errorf("Expected version 1.0.0, got %s", retrieved.Version)
	}

	// Update to version 2
	schemaV2 := flume.Schema{
		Version: "2.0.0",
		Node: flume.Node{
			Ref: "proc1",
		},
	}

	err = factory.SetSchema("test", schemaV2)
	if err != nil {
		t.Fatalf("Failed to update schema to v2: %v", err)
	}

	// Get updated schema
	retrieved, ok = factory.GetSchema("test")
	if !ok {
		t.Fatal("Failed to retrieve updated schema")
	}
	if retrieved.Version != "2.0.0" {
		t.Errorf("Expected version 2.0.0, got %s", retrieved.Version)
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

	err = factory.SetSchema("no-version", schemaNoVersion)
	if err != nil {
		t.Fatalf("Failed to set schema without version: %v", err)
	}

	retrieved, ok = factory.GetSchema("no-version")
	if !ok {
		t.Fatal("Failed to retrieve schema without version")
	}
	if retrieved.Version != "" {
		t.Errorf("Expected empty version, got %s", retrieved.Version)
	}
}
