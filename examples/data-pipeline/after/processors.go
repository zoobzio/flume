//go:build examples

// Data Pipeline Factory - Processors
// ====================================
// The same processors from 'before', registered with flume.
// Note: No "Store", "Analytics", "Alert", "Archive" processors here -
// those are now channel consumers, not pipeline processors.

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
// Same core processors as before.
// The key difference: fanout is now via channels, not processors.

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

// AllProcessors returns processors for factory registration.
func AllProcessors() []pipz.Chainable[Record] {
	return []pipz.Chainable[Record]{
		Validate,
		Transform,
		Enrich,
	}
}
