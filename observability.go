package flume

import (
	"github.com/zoobzio/zlog"
)

// Flume-specific signals for observability.
const (
	// Factory lifecycle events.
	FactoryCreated      = zlog.Signal("FLUME_FACTORY_CREATED")
	ProcessorRegistered = zlog.Signal("FLUME_PROCESSOR_REGISTERED")
	PredicateRegistered = zlog.Signal("FLUME_PREDICATE_REGISTERED")
	ConditionRegistered = zlog.Signal("FLUME_CONDITION_REGISTERED")

	// Schema operations.
	SchemaValidationStarted   = zlog.Signal("FLUME_SCHEMA_VALIDATION_STARTED")
	SchemaValidationCompleted = zlog.Signal("FLUME_SCHEMA_VALIDATION_COMPLETED")
	SchemaValidationFailed    = zlog.Signal("FLUME_SCHEMA_VALIDATION_FAILED")
	SchemaBuildStarted        = zlog.Signal("FLUME_SCHEMA_BUILD_STARTED")
	SchemaBuildCompleted      = zlog.Signal("FLUME_SCHEMA_BUILD_COMPLETED")
	SchemaBuildFailed         = zlog.Signal("FLUME_SCHEMA_BUILD_FAILED")

	// Dynamic schema management.
	SchemaRegistered   = zlog.Signal("FLUME_SCHEMA_REGISTERED")
	SchemaUpdated      = zlog.Signal("FLUME_SCHEMA_UPDATED")
	SchemaUpdateFailed = zlog.Signal("FLUME_SCHEMA_UPDATE_FAILED")
	SchemaRemoved      = zlog.Signal("FLUME_SCHEMA_REMOVED")
	PipelineRetrieved  = zlog.Signal("FLUME_PIPELINE_RETRIEVED")

	// Component removal.
	ProcessorRemoved = zlog.Signal("FLUME_PROCESSOR_REMOVED")
	PredicateRemoved = zlog.Signal("FLUME_PREDICATE_REMOVED")
	ConditionRemoved = zlog.Signal("FLUME_CONDITION_REMOVED")

	// File operations.
	SchemaFileLoaded  = zlog.Signal("FLUME_SCHEMA_FILE_LOADED")
	SchemaFileFailed  = zlog.Signal("FLUME_SCHEMA_FILE_FAILED")
	SchemaYAMLParsed  = zlog.Signal("FLUME_SCHEMA_YAML_PARSED")
	SchemaJSONParsed  = zlog.Signal("FLUME_SCHEMA_JSON_PARSED")
	SchemaParseFailed = zlog.Signal("FLUME_SCHEMA_PARSE_FAILED")

	// Performance and metrics.
	FactoryOperationDuration   = zlog.Signal("FLUME_FACTORY_OPERATION_DURATION")
	PipelineExecutionStarted   = zlog.Signal("FLUME_PIPELINE_EXECUTION_STARTED")
	PipelineExecutionCompleted = zlog.Signal("FLUME_PIPELINE_EXECUTION_COMPLETED")
)
