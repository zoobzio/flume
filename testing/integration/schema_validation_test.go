package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/zoobzio/flume"
	flumetesting "github.com/zoobzio/flume/testing"
	"github.com/zoobzio/pipz"
)

func TestSchemaValidation_ComprehensiveErrors(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	existingProcessorID := factory.Identity("existing-processor", "Pass-through processor for validation error tests")

	// Register some processors for partial validation
	factory.Add(pipz.Transform(existingProcessorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		return d
	}))

	tests := []struct {
		name           string
		schema         string
		expectedErrors []string
	}{
		{
			name: "multiple_missing_processors",
			schema: `
type: sequence
children:
  - ref: missing-one
  - ref: missing-two
  - ref: existing-processor
  - ref: missing-three
`,
			expectedErrors: []string{
				"missing-one",
				"missing-two",
				"missing-three",
			},
		},
		{
			name: "missing_predicate_in_filter",
			schema: `
type: filter
predicate: nonexistent-predicate
then:
  ref: existing-processor
`,
			expectedErrors: []string{
				"predicate 'nonexistent-predicate' not found",
			},
		},
		{
			name: "missing_condition_in_switch",
			schema: `
type: switch
condition: nonexistent-condition
routes:
  active:
    ref: existing-processor
default:
  ref: existing-processor
`,
			expectedErrors: []string{
				"condition 'nonexistent-condition' not found",
			},
		},
		{
			name: "invalid_retry_attempts",
			schema: `
type: retry
attempts: -1
child:
  ref: existing-processor
`,
			expectedErrors: []string{
				"attempts",
			},
		},
		{
			name: "invalid_timeout_duration",
			schema: `
type: timeout
duration: "invalid"
child:
  ref: existing-processor
`,
			expectedErrors: []string{
				"duration",
			},
		},
		{
			name: "fallback_wrong_children_count",
			schema: `
type: fallback
children:
  - ref: existing-processor
`,
			expectedErrors: []string{
				"exactly 2 children",
			},
		},
		{
			name: "sequence_no_children",
			schema: `
type: sequence
children: []
`,
			expectedErrors: []string{
				"at least one child",
			},
		},
		{
			name: "deeply_nested_errors",
			schema: `
type: sequence
children:
  - type: concurrent
    children:
      - type: filter
        predicate: missing-pred
        then:
          ref: missing-proc
      - type: retry
        attempts: 0
        child:
          ref: another-missing
`,
			expectedErrors: []string{
				"missing-pred",
				"missing-proc",
				"another-missing",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := factory.BuildFromYAML(tt.schema)

			if err == nil {
				t.Fatal("expected validation error but got none")
			}

			errStr := err.Error()
			for _, expected := range tt.expectedErrors {
				if !strings.Contains(errStr, expected) {
					t.Errorf("expected error to contain %q, got: %s", expected, errStr)
				}
			}
		})
	}
}

func TestSchemaValidation_ValidSchemas(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	validateID := factory.Identity("validate", "Validates data by passing through")
	enrichID := factory.Identity("enrich", "Enriches data by incrementing value")
	formatID := factory.Identity("format", "Formats data by uppercasing name")
	isValidID := factory.Identity("is-valid", "Predicate that checks if ID is positive")
	getStatusID := factory.Identity("get-status", "Condition that returns high/low based on value")

	// Register processors
	factory.Add(
		pipz.Transform(validateID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}),
		pipz.Transform(enrichID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Value += 1.0
			return d
		}),
		pipz.Transform(formatID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Name = strings.ToUpper(d.Name)
			return d
		}),
	)

	// Register predicates
	factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
		Identity: isValidID,
		Predicate: func(_ context.Context, d flumetesting.TestData) bool {
			return d.ID > 0
		},
	})

	// Register conditions
	factory.AddCondition(flume.Condition[flumetesting.TestData]{
		Identity: getStatusID,
		Condition: func(_ context.Context, d flumetesting.TestData) string {
			if d.Value > 100 {
				return "high"
			}
			return "low"
		},
	})

	tests := []struct {
		name   string
		schema string
	}{
		{
			name:   "simple_ref",
			schema: `ref: validate`,
		},
		{
			name: "sequence",
			schema: `
type: sequence
children:
  - ref: validate
  - ref: enrich
  - ref: format
`,
		},
		{
			name: "filter",
			schema: `
type: filter
predicate: is-valid
then:
  ref: enrich
`,
		},
		{
			name: "filter_with_else",
			schema: `
type: filter
predicate: is-valid
then:
  ref: enrich
else:
  ref: format
`,
		},
		{
			name: "switch",
			schema: `
type: switch
condition: get-status
routes:
  high:
    ref: format
  low:
    ref: validate
default:
  ref: enrich
`,
		},
		{
			name: "retry",
			schema: `
type: retry
attempts: 3
child:
  ref: validate
`,
		},
		{
			name: "timeout",
			schema: `
type: timeout
duration: "30s"
child:
  ref: validate
`,
		},
		{
			name: "fallback",
			schema: `
type: fallback
children:
  - ref: validate
  - ref: enrich
`,
		},
		{
			name: "circuit_breaker",
			schema: `
type: circuit-breaker
failure_threshold: 5
recovery_timeout: "60s"
child:
  ref: validate
`,
		},
		{
			name: "rate_limit",
			schema: `
type: rate-limit
requests_per_second: 10.0
burst_size: 5
child:
  ref: validate
`,
		},
		{
			name: "complex_nested",
			schema: `
type: sequence
children:
  - ref: validate
  - type: filter
    predicate: is-valid
    then:
      type: retry
      attempts: 3
      child:
        type: timeout
        duration: "5s"
        child:
          ref: enrich
  - ref: format
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.BuildFromYAML(tt.schema)
			if err != nil {
				t.Fatalf("expected no error but got: %v", err)
			}

			if pipeline == nil {
				t.Fatal("expected pipeline to be non-nil")
			}

			// Verify pipeline can process data
			ctx := context.Background()
			input := flumetesting.TestData{ID: 1, Name: "test", Value: 50.0}
			_, err = pipeline.Process(ctx, input)
			if err != nil {
				t.Fatalf("pipeline processing failed: %v", err)
			}
		})
	}
}

func TestSchemaValidation_CycleDetection(t *testing.T) {
	// This test verifies that cycle detection works properly
	// flume detects circular references when the same processor is used multiple times

	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	processorAID := factory.Identity("processor-a", "Processor A for cycle detection test")
	processorBID := factory.Identity("processor-b", "Processor B for cycle detection test")
	processorCID := factory.Identity("processor-c", "Processor C for cycle detection test")

	factory.Add(
		pipz.Transform(processorAID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}),
		pipz.Transform(processorBID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}),
		pipz.Transform(processorCID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}),
	)

	// Sequence with different processors should work
	schema := `
type: sequence
children:
  - ref: processor-a
  - ref: processor-b
  - ref: processor-c
`
	_, err := factory.BuildFromYAML(schema)
	if err != nil {
		t.Fatalf("expected no error for valid schema, got: %v", err)
	}

	// Schema with repeated processor should be detected as circular reference
	repeatedSchema := `
type: sequence
children:
  - ref: processor-a
  - ref: processor-a
`
	_, err = factory.BuildFromYAML(repeatedSchema)
	if err == nil {
		t.Error("expected circular reference error for repeated processor")
	}
	if !strings.Contains(err.Error(), "circular reference") {
		t.Errorf("expected circular reference error, got: %v", err)
	}
}

func TestSchemaValidation_ChannelIntegration(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	processID := factory.Identity("process", "Processor that doubles the value")

	// Register processor
	factory.Add(pipz.Transform(processID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		d.Value *= 2
		return d
	}))

	// Register channel
	outputChan := make(chan flumetesting.TestData, 10)
	factory.AddChannel("output", outputChan)

	tests := []struct {
		name        string
		schema      string
		expectError bool
	}{
		{
			name: "valid_stream_reference",
			schema: `
type: sequence
children:
  - ref: process
  - stream: output
`,
			expectError: false,
		},
		{
			name: "missing_channel",
			schema: `
type: sequence
children:
  - ref: process
  - stream: nonexistent-channel
`,
			expectError: true,
		},
		{
			name: "stream_with_child",
			schema: `
type: sequence
children:
  - stream: output
    child:
      ref: process
`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := factory.BuildFromYAML(tt.schema)

			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

func TestSchemaValidation_ErrorPathAccuracy(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	validID := factory.Identity("valid", "Valid processor for error path accuracy test")

	factory.Add(pipz.Transform(validID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		return d
	}))

	// Schema with error deep in the structure
	schema := `
type: sequence
children:
  - ref: valid
  - type: concurrent
    children:
      - ref: valid
      - type: sequence
        children:
          - ref: valid
          - ref: deeply-missing
`

	_, err := factory.BuildFromYAML(schema)
	if err == nil {
		t.Fatal("expected error but got none")
	}

	errStr := err.Error()

	// Verify the error path is accurate
	if !strings.Contains(errStr, "deeply-missing") {
		t.Errorf("expected error to reference 'deeply-missing', got: %s", errStr)
	}
}

func TestSchemaValidation_JSONFormat(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	processAID := factory.Identity("process-a", "Processor A for JSON format test")
	processBID := factory.Identity("process-b", "Processor B for JSON format test")

	factory.Add(
		pipz.Transform(processAID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}),
		pipz.Transform(processBID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}),
	)

	jsonSchema := `{
		"type": "sequence",
		"children": [
			{"ref": "process-a"},
			{"ref": "process-b"}
		]
	}`

	pipeline, err := factory.BuildFromJSON(jsonSchema)
	if err != nil {
		t.Fatalf("failed to build from JSON: %v", err)
	}

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Name: "test"}
	_, err = pipeline.Process(ctx, input)
	if err != nil {
		t.Fatalf("pipeline processing failed: %v", err)
	}
}

func TestSchemaValidation_VersionTracking(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	processID := factory.Identity("process", "Processor for version tracking test")

	factory.Add(pipz.Transform(processID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		return d
	}))

	schema := `
version: "1.0.0"
type: sequence
children:
  - ref: process
`

	pipeline, err := factory.BuildFromYAML(schema)
	if err != nil {
		t.Fatalf("failed to build schema with version: %v", err)
	}

	if pipeline == nil {
		t.Fatal("expected pipeline to be non-nil")
	}
}
