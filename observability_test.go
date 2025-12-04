package flume_test

import (
	"context"
	"testing"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/flume"
)

func TestObservabilitySignals(t *testing.T) {
	// Test that all signals are defined and can be used with capitan.Emit
	signals := []struct {
		name   string
		signal capitan.Signal
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

		// Channel operations
		{"ChannelRegistered", flume.ChannelRegistered},
		{"ChannelRemoved", flume.ChannelRemoved},

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
			// Verify the signal has a name
			if tt.signal.Name() == "" {
				t.Errorf("Signal %s has empty name", tt.name)
			}

			// Verify the signal has the expected prefix
			signalName := tt.signal.Name()
			if len(signalName) < 6 || signalName[:6] != "flume." {
				t.Errorf("Signal %s does not have flume. prefix: %s", tt.name, signalName)
			}

			// Verify the signal has a description
			if tt.signal.Description() == "" {
				t.Errorf("Signal %s has empty description", tt.name)
			}

			// Test that the signal can be used with capitan.Emit
			// This won't actually emit anything without a configured listener,
			// but it verifies the types are compatible
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("Signal %s caused panic when used with capitan.Emit: %v", tt.name, r)
					}
				}()
				capitan.Emit(context.Background(), tt.signal)
			}()
		})
	}
}

func TestSignalUniqueness(t *testing.T) {
	// Verify all signals have unique values
	signals := map[string]string{
		flume.FactoryCreated.Name():             "FactoryCreated",
		flume.ProcessorRegistered.Name():        "ProcessorRegistered",
		flume.PredicateRegistered.Name():        "PredicateRegistered",
		flume.ConditionRegistered.Name():        "ConditionRegistered",
		flume.SchemaValidationStarted.Name():    "SchemaValidationStarted",
		flume.SchemaValidationCompleted.Name():  "SchemaValidationCompleted",
		flume.SchemaValidationFailed.Name():     "SchemaValidationFailed",
		flume.SchemaBuildStarted.Name():         "SchemaBuildStarted",
		flume.SchemaBuildCompleted.Name():       "SchemaBuildCompleted",
		flume.SchemaBuildFailed.Name():          "SchemaBuildFailed",
		flume.SchemaRegistered.Name():           "SchemaRegistered",
		flume.SchemaUpdated.Name():              "SchemaUpdated",
		flume.SchemaUpdateFailed.Name():         "SchemaUpdateFailed",
		flume.SchemaRemoved.Name():              "SchemaRemoved",
		flume.PipelineRetrieved.Name():          "PipelineRetrieved",
		flume.ChannelRegistered.Name():          "ChannelRegistered",
		flume.ChannelRemoved.Name():             "ChannelRemoved",
		flume.ProcessorRemoved.Name():           "ProcessorRemoved",
		flume.PredicateRemoved.Name():           "PredicateRemoved",
		flume.ConditionRemoved.Name():           "ConditionRemoved",
		flume.SchemaFileLoaded.Name():           "SchemaFileLoaded",
		flume.SchemaFileFailed.Name():           "SchemaFileFailed",
		flume.SchemaYAMLParsed.Name():           "SchemaYAMLParsed",
		flume.SchemaJSONParsed.Name():           "SchemaJSONParsed",
		flume.SchemaParseFailed.Name():          "SchemaParseFailed",
		flume.FactoryOperationDuration.Name():   "FactoryOperationDuration",
		flume.PipelineExecutionStarted.Name():   "PipelineExecutionStarted",
		flume.PipelineExecutionCompleted.Name(): "PipelineExecutionCompleted",
	}

	// Verify we tested all signals (28 total)
	if len(signals) != 28 {
		t.Errorf("Expected 28 signals, got %d", len(signals))
	}
}

func TestFieldKeys(t *testing.T) {
	// Test that all field keys are defined
	keys := []struct {
		name string
		key  capitan.Key
	}{
		{"KeyName", flume.KeyName},
		{"KeyType", flume.KeyType},
		{"KeyVersion", flume.KeyVersion},
		{"KeyOldVersion", flume.KeyOldVersion},
		{"KeyNewVersion", flume.KeyNewVersion},
		{"KeyPath", flume.KeyPath},
		{"KeyError", flume.KeyError},
		{"KeyDuration", flume.KeyDuration},
		{"KeyErrorCount", flume.KeyErrorCount},
		{"KeySizeBytes", flume.KeySizeBytes},
		{"KeyFound", flume.KeyFound},
	}

	for _, tt := range keys {
		t.Run(tt.name, func(t *testing.T) {
			if tt.key.Name() == "" {
				t.Errorf("Key %s has empty name", tt.name)
			}
		})
	}
}
