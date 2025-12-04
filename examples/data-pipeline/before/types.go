//go:build examples

// Data Pipeline - Types
// ======================
// Shared types for the data ingestion example.

package main

import (
	"time"
)

// Record represents a data record flowing through the pipeline.
type Record struct {
	ID        string
	Source    string // where the data came from
	Timestamp time.Time
	Payload   map[string]any

	// Processing state
	Validated   bool
	Transformed bool
	Enriched    bool

	// Routing metadata
	Priority    Priority
	Destination string // where to send this record

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

// Priority levels for routing.
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
	ProcessorStore     = "store"
	ProcessorAnalytics = "analytics"
	ProcessorAlert     = "alert"
	ProcessorArchive   = "archive"
)

// Predicate names.
const (
	PredicateIsHighPriority = "is-high-priority"
	PredicateNeedsAlert     = "needs-alert"
)

// Condition names.
const (
	ConditionRoutePriority = "route-priority"
)

// Destination names.
const (
	DestinationStorage   = "storage"
	DestinationAnalytics = "analytics"
	DestinationAlerts    = "alerts"
	DestinationArchive   = "archive"
)
