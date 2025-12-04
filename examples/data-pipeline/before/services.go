//go:build examples

// Data Pipeline - Mock Services
// ==============================
// Simulated external services for demonstration.

package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// StorageService simulates a data store.
var StorageService = &storageService{
	records: make(map[string]Record),
}

type storageService struct {
	mu      sync.RWMutex
	records map[string]Record
}

func (s *storageService) Store(_ context.Context, r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[r.ID] = r
	fmt.Printf("  [Storage] Stored record %s\n", r.ID)
	return nil
}

func (s *storageService) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}

// AnalyticsService simulates an analytics system.
var AnalyticsService = &analyticsService{
	events: make([]Record, 0),
}

type analyticsService struct {
	mu     sync.Mutex
	events []Record
}

func (a *analyticsService) Track(_ context.Context, r Record) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, r)
	fmt.Printf("  [Analytics] Tracked record %s (priority: %s)\n", r.ID, r.Priority)
	return nil
}

func (a *analyticsService) Count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.events)
}

// AlertService simulates an alerting system.
var AlertService = &alertService{
	alerts: make([]Record, 0),
}

type alertService struct {
	mu     sync.Mutex
	alerts []Record
}

func (a *alertService) SendAlert(_ context.Context, r Record) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.alerts = append(a.alerts, r)
	fmt.Printf("  [Alert] Alert sent for record %s: high priority data!\n", r.ID)
	return nil
}

func (a *alertService) Count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.alerts)
}

// ArchiveService simulates a cold storage archive.
var ArchiveService = &archiveService{
	archived: make([]Record, 0),
}

type archiveService struct {
	mu       sync.Mutex
	archived []Record
}

func (a *archiveService) Archive(_ context.Context, r Record) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.archived = append(a.archived, r)
	fmt.Printf("  [Archive] Archived record %s\n", r.ID)
	return nil
}

func (a *archiveService) Count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.archived)
}

// EnrichmentService simulates data enrichment.
var EnrichmentService = &enrichmentService{}

type enrichmentService struct{}

func (e *enrichmentService) Enrich(_ context.Context, r Record) Record {
	// Simulate enrichment by adding metadata
	r.Payload["enriched_at"] = time.Now().Format(time.RFC3339)
	r.Payload["enrichment_version"] = "1.0"

	// Determine priority based on payload
	if val, ok := r.Payload["critical"]; ok && val == true {
		r.Priority = PriorityHigh
	} else if val, ok := r.Payload["batch"]; ok && val == true {
		r.Priority = PriorityLow
	} else {
		r.Priority = PriorityNormal
	}

	return r
}

// ResetServices clears all service state.
func ResetServices() {
	StorageService.mu.Lock()
	StorageService.records = make(map[string]Record)
	StorageService.mu.Unlock()

	AnalyticsService.mu.Lock()
	AnalyticsService.events = make([]Record, 0)
	AnalyticsService.mu.Unlock()

	AlertService.mu.Lock()
	AlertService.alerts = make([]Record, 0)
	AlertService.mu.Unlock()

	ArchiveService.mu.Lock()
	ArchiveService.archived = make([]Record, 0)
	ArchiveService.mu.Unlock()
}
