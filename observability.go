package flume

import (
	"github.com/zoobzio/capitan"
)

// Flume-specific signals for observability.
var (
	// Factory lifecycle events.
	FactoryCreated         = capitan.NewSignal("flume.factory.created", "Flume factory created")
	ProcessorRegistered    = capitan.NewSignal("flume.processor.registered", "Processor registered")
	PredicateRegistered    = capitan.NewSignal("flume.predicate.registered", "Predicate registered")
	ConditionRegistered    = capitan.NewSignal("flume.condition.registered", "Condition registered")
	ReducerRegistered      = capitan.NewSignal("flume.reducer.registered", "Reducer registered")
	ErrorHandlerRegistered = capitan.NewSignal("flume.errorhandler.registered", "Error handler registered")

	// Schema operations.
	SchemaValidationStarted   = capitan.NewSignal("flume.schema.validation.started", "Schema validation started")
	SchemaValidationCompleted = capitan.NewSignal("flume.schema.validation.completed", "Schema validation completed")
	SchemaValidationFailed    = capitan.NewSignal("flume.schema.validation.failed", "Schema validation failed")
	SchemaBuildStarted        = capitan.NewSignal("flume.schema.build.started", "Schema build started")
	SchemaBuildCompleted      = capitan.NewSignal("flume.schema.build.completed", "Schema build completed")
	SchemaBuildFailed         = capitan.NewSignal("flume.schema.build.failed", "Schema build failed")

	// Dynamic schema management.
	SchemaRegistered   = capitan.NewSignal("flume.schema.registered", "Schema registered")
	SchemaUpdated      = capitan.NewSignal("flume.schema.updated", "Schema updated")
	SchemaUpdateFailed = capitan.NewSignal("flume.schema.update.failed", "Schema update failed")
	SchemaRemoved      = capitan.NewSignal("flume.schema.removed", "Schema removed")
	PipelineRetrieved  = capitan.NewSignal("flume.pipeline.retrieved", "Pipeline retrieved")

	// Channel operations.
	ChannelRegistered = capitan.NewSignal("flume.channel.registered", "Channel registered")
	ChannelRemoved    = capitan.NewSignal("flume.channel.removed", "Channel removed")

	// Component removal.
	ProcessorRemoved    = capitan.NewSignal("flume.processor.removed", "Processor removed")
	PredicateRemoved    = capitan.NewSignal("flume.predicate.removed", "Predicate removed")
	ConditionRemoved    = capitan.NewSignal("flume.condition.removed", "Condition removed")
	ReducerRemoved      = capitan.NewSignal("flume.reducer.removed", "Reducer removed")
	ErrorHandlerRemoved = capitan.NewSignal("flume.errorhandler.removed", "Error handler removed")

	// File operations.
	SchemaFileLoaded  = capitan.NewSignal("flume.schema.file.loaded", "Schema file loaded")
	SchemaFileFailed  = capitan.NewSignal("flume.schema.file.failed", "Schema file failed")
	SchemaYAMLParsed  = capitan.NewSignal("flume.schema.yaml.parsed", "YAML schema parsed")
	SchemaJSONParsed  = capitan.NewSignal("flume.schema.json.parsed", "JSON schema parsed")
	SchemaParseFailed = capitan.NewSignal("flume.schema.parse.failed", "Schema parse failed")

	// Performance and metrics.
	FactoryOperationDuration   = capitan.NewSignal("flume.factory.operation.duration", "Factory operation duration")
	PipelineExecutionStarted   = capitan.NewSignal("flume.pipeline.execution.started", "Pipeline execution started")
	PipelineExecutionCompleted = capitan.NewSignal("flume.pipeline.execution.completed", "Pipeline execution completed")
)

// Field keys for event data.
var (
	KeyName       = capitan.NewStringKey("name")
	KeyType       = capitan.NewStringKey("type")
	KeySchema     = capitan.NewStringKey("schema")
	KeyVersion    = capitan.NewStringKey("version")
	KeyOldVersion = capitan.NewStringKey("old_version")
	KeyNewVersion = capitan.NewStringKey("new_version")
	KeyPath       = capitan.NewStringKey("path")
	KeyError      = capitan.NewStringKey("error")
	KeyDuration   = capitan.NewDurationKey("duration")
	KeyErrorCount = capitan.NewIntKey("error_count")
	KeySizeBytes  = capitan.NewIntKey("size_bytes")
	KeyFound      = capitan.NewBoolKey("found")
)
