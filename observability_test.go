package flume_test

import (
	"testing"

	"github.com/zoobzio/flume"
	"github.com/zoobzio/zlog"
)

func TestObservabilitySignals(t *testing.T) {
	// Test that all signals are defined and can be used with zlog.Emit
	signals := []struct {
		name   string
		signal zlog.Signal
	}{
		// Factory lifecycle events
		{"FactoryCreated", flume.FactoryCreated},
		{"ProcessorRegistered", flume.ProcessorRegistered},
		{"PredicateRegistered", flume.PredicateRegistered},
		{"ConditionRegistered", flume.ConditionRegistered},

		// Schema operations
		{"SchemaValidationStarted", flume.SchemaValidationStarted},
		{"SchemaValidationCompleted", flume.SchemaValidationCompleted},
		{"SchemaValidationFailed", flume.SchemaValidationFailed},
		{"SchemaBuildStarted", flume.SchemaBuildStarted},
		{"SchemaBuildCompleted", flume.SchemaBuildCompleted},
		{"SchemaBuildFailed", flume.SchemaBuildFailed},

		// Dynamic schema management
		{"SchemaRegistered", flume.SchemaRegistered},
		{"SchemaUpdated", flume.SchemaUpdated},
		{"SchemaUpdateFailed", flume.SchemaUpdateFailed},
		{"SchemaRemoved", flume.SchemaRemoved},
		{"PipelineRetrieved", flume.PipelineRetrieved},

		// Component removal
		{"ProcessorRemoved", flume.ProcessorRemoved},
		{"PredicateRemoved", flume.PredicateRemoved},
		{"ConditionRemoved", flume.ConditionRemoved},

		// File operations
		{"SchemaFileLoaded", flume.SchemaFileLoaded},
		{"SchemaFileFailed", flume.SchemaFileFailed},
		{"SchemaYAMLParsed", flume.SchemaYAMLParsed},
		{"SchemaJSONParsed", flume.SchemaJSONParsed},
		{"SchemaParseFailed", flume.SchemaParseFailed},

		// Performance and metrics
		{"FactoryOperationDuration", flume.FactoryOperationDuration},
		{"PipelineExecutionStarted", flume.PipelineExecutionStarted},
		{"PipelineExecutionCompleted", flume.PipelineExecutionCompleted},
	}

	for _, tt := range signals {
		t.Run(tt.name, func(t *testing.T) {
			// Verify the signal is not empty
			if tt.signal == "" {
				t.Errorf("Signal %s is empty", tt.name)
			}

			// Verify the signal has the expected prefix
			signalStr := string(tt.signal)
			if len(signalStr) < 6 || signalStr[:6] != "FLUME_" {
				t.Errorf("Signal %s does not have FLUME_ prefix: %s", tt.name, signalStr)
			}

			// Test that the signal can be used with zlog.Emit
			// This won't actually emit anything without a configured handler,
			// but it verifies the types are compatible
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("Signal %s caused panic when used with zlog.Emit: %v", tt.name, r)
					}
				}()
				zlog.Emit(tt.signal, "Test message")
			}()
		})
	}
}

func TestSignalUniqueness(t *testing.T) {
	// Verify all signals have unique values
	signals := map[zlog.Signal]string{
		flume.FactoryCreated:             "FactoryCreated",
		flume.ProcessorRegistered:        "ProcessorRegistered",
		flume.PredicateRegistered:        "PredicateRegistered",
		flume.ConditionRegistered:        "ConditionRegistered",
		flume.SchemaValidationStarted:    "SchemaValidationStarted",
		flume.SchemaValidationCompleted:  "SchemaValidationCompleted",
		flume.SchemaValidationFailed:     "SchemaValidationFailed",
		flume.SchemaBuildStarted:         "SchemaBuildStarted",
		flume.SchemaBuildCompleted:       "SchemaBuildCompleted",
		flume.SchemaBuildFailed:          "SchemaBuildFailed",
		flume.SchemaRegistered:           "SchemaRegistered",
		flume.SchemaUpdated:              "SchemaUpdated",
		flume.SchemaUpdateFailed:         "SchemaUpdateFailed",
		flume.SchemaRemoved:              "SchemaRemoved",
		flume.PipelineRetrieved:          "PipelineRetrieved",
		flume.ProcessorRemoved:           "ProcessorRemoved",
		flume.PredicateRemoved:           "PredicateRemoved",
		flume.ConditionRemoved:           "ConditionRemoved",
		flume.SchemaFileLoaded:           "SchemaFileLoaded",
		flume.SchemaFileFailed:           "SchemaFileFailed",
		flume.SchemaYAMLParsed:           "SchemaYAMLParsed",
		flume.SchemaJSONParsed:           "SchemaJSONParsed",
		flume.SchemaParseFailed:          "SchemaParseFailed",
		flume.FactoryOperationDuration:   "FactoryOperationDuration",
		flume.PipelineExecutionStarted:   "PipelineExecutionStarted",
		flume.PipelineExecutionCompleted: "PipelineExecutionCompleted",
	}

	seen := make(map[zlog.Signal]string)
	for signal, name := range signals {
		if existingName, exists := seen[signal]; exists {
			t.Errorf("Duplicate signal value: %s and %s both have value %s",
				existingName, name, string(signal))
		}
		seen[signal] = name
	}

	// Verify we tested all signals (26 total)
	if len(signals) != 26 {
		t.Errorf("Expected 26 signals, got %d", len(signals))
	}
}
