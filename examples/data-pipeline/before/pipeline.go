//go:build examples

// Data Pipeline - pipz Implementation
// ====================================
// A data ingestion pipeline that processes records and sends them
// to multiple destinations (storage, analytics, alerts, archive).
//
// This works, but the fanout logic is hardcoded. What if you want
// to change which destinations receive data without code changes?

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/zoobzio/pipz"
)

// ============================================================================
// PROCESSORS
// ============================================================================

// Validate ensures the record has required fields.
var Validate = pipz.Apply(ProcessorValidate, func(_ context.Context, r Record) (Record, error) {
	if r.ID == "" {
		return r, errors.New("record ID required")
	}
	if r.Source == "" {
		return r, errors.New("record source required")
	}
	r.Validated = true
	r.ProcessLog = append(r.ProcessLog, "validated")
	return r, nil
})

// Transform normalizes the record format.
var Transform = pipz.Apply(ProcessorTransform, func(_ context.Context, r Record) (Record, error) {
	// Simulate transformation
	if r.Payload == nil {
		r.Payload = make(map[string]any)
	}
	r.Payload["transformed_at"] = time.Now().Format(time.RFC3339)
	r.Transformed = true
	r.ProcessLog = append(r.ProcessLog, "transformed")
	return r, nil
})

// Enrich adds metadata to the record.
var Enrich = pipz.Apply(ProcessorEnrich, func(ctx context.Context, r Record) (Record, error) {
	r = EnrichmentService.Enrich(ctx, r)
	r.Enriched = true
	r.ProcessLog = append(r.ProcessLog, fmt.Sprintf("enriched (priority: %s)", r.Priority))
	return r, nil
})

// Store saves the record to primary storage.
var Store = pipz.Effect(ProcessorStore, func(ctx context.Context, r Record) error {
	return StorageService.Store(ctx, r)
})

// Analytics sends the record to analytics.
var Analytics = pipz.Effect(ProcessorAnalytics, func(ctx context.Context, r Record) error {
	return AnalyticsService.Track(ctx, r)
})

// Alert sends alerts for high-priority records.
var Alert = pipz.Effect(ProcessorAlert, func(ctx context.Context, r Record) error {
	if r.Priority != PriorityHigh {
		return nil // Only alert on high priority
	}
	return AlertService.SendAlert(ctx, r)
})

// Archive sends records to cold storage.
var Archive = pipz.Effect(ProcessorArchive, func(ctx context.Context, r Record) error {
	return ArchiveService.Archive(ctx, r)
})

// ============================================================================
// PIPELINE CONSTRUCTION
// ============================================================================
// THE PROBLEM: Fanout destinations are hardcoded.
//
// Current flow:
//   validate -> transform -> enrich -> [storage, analytics, alert, archive]
//
// What if:
// - You want to add a new destination (webhook, kafka, etc.)
// - You want to disable analytics for certain data
// - You want different fanout patterns for different data types
//
// With pure pipz, every change requires code modification and redeploy.

// DataPipeline is THE pipeline - fanout destinations hardcoded.
var DataPipeline = pipz.NewSequence[Record](
	"data-pipeline",
	Validate,
	Transform,
	Enrich,
	// Hardcoded fanout to all destinations
	pipz.NewConcurrent[Record](
		"fanout",
		nil, // no reducer
		Store,
		Analytics,
		Alert,
		Archive,
	),
)

// ProcessRecord runs a record through the pipeline.
func ProcessRecord(ctx context.Context, record Record) (Record, error) {
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now()
	}
	if record.ProcessLog == nil {
		record.ProcessLog = make([]string, 0)
	}
	if record.Priority == "" {
		record.Priority = PriorityUnknown
	}

	return DataPipeline.Process(ctx, record)
}
