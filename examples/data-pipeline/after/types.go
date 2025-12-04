//go:build examples

// Data Pipeline Factory - Types
// ==============================
// Shared types for the data pipeline example.
// Note: Identical to 'before' - the types don't change.

package main

import (
	"time"
)

// Record represents a data record flowing through the pipeline.
type Record struct {
	ID        string
	Source    string
	Timestamp time.Time
	Payload   map[string]any

	// Processing state
	Validated   bool
	Transformed bool
	Enriched    bool

	// Routing metadata
	Priority    Priority
	Destination string

	// Processing metadata
	ProcessLog []string
	Errors     []string
}

// Clone implements pipz.Cloner for concurrent processing.
func (r Record) Clone() Record {
	payload := make(map[string]any, len(r.Payload))
	for k, v := range r.Payload {
		payload[k] = v
	}

	processLog := make([]string, len(r.ProcessLog))
	copy(processLog, r.ProcessLog)

	errors := make([]string, len(r.Errors))
	copy(errors, r.Errors)

	return Record{
		ID:          r.ID,
		Source:      r.Source,
		Timestamp:   r.Timestamp,
		Payload:     payload,
		Validated:   r.Validated,
		Transformed: r.Transformed,
		Enriched:    r.Enriched,
		Priority:    r.Priority,
		Destination: r.Destination,
		ProcessLog:  processLog,
		Errors:      errors,
	}
}

// Priority levels.
type Priority string

const (
	PriorityHigh    Priority = "high"
	PriorityNormal  Priority = "normal"
	PriorityLow     Priority = "low"
	PriorityUnknown Priority = "unknown"
)

// Processor names.
const (
	ProcessorValidate  = "validate"
	ProcessorTransform = "transform"
	ProcessorEnrich    = "enrich"
)

// Stream/channel names.
const (
	StreamStorage   = "storage"
	StreamAnalytics = "analytics"
	StreamAlerts    = "alerts"
	StreamArchive   = "archive"
	StreamAll       = "all" // For subscribers who want everything
)

// Condition names.
const (
	ConditionRoutePriority = "route-priority"
)
