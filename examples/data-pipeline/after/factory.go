//go:build examples

// Data Pipeline Factory - flume Implementation
// =============================================
// Same processors, but fanout is now via channels.
// Consumers subscribe to streams they care about.

package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zoobzio/flume"
)

// DataFactory manages the data pipeline.
var DataFactory *flume.Factory[Record]

// Channel references for consumers.
var (
	StorageChannel   chan Record
	AnalyticsChannel chan Record
	AlertsChannel    chan Record
	ArchiveChannel   chan Record
	AllChannel       chan Record // For consumers who want everything
)

// InitFactory creates the factory and registers components.
func InitFactory() {
	DataFactory = flume.New[Record]()

	// Register processors
	DataFactory.Add(AllProcessors()...)

	// Register predicates for conditional routing
	DataFactory.AddPredicate(
		flume.Predicate[Record]{
			Name:        "is-high-priority",
			Description: "Record has high priority",
			Predicate: func(_ context.Context, r Record) bool {
				return r.Priority == PriorityHigh
			},
		},
		flume.Predicate[Record]{
			Name:        "is-batch",
			Description: "Record is from batch processing",
			Predicate: func(_ context.Context, r Record) bool {
				return r.Priority == PriorityLow
			},
		},
	)

	// Create channels for stream integration
	StorageChannel = make(chan Record, 100)
	AnalyticsChannel = make(chan Record, 100)
	AlertsChannel = make(chan Record, 100)
	ArchiveChannel = make(chan Record, 100)
	AllChannel = make(chan Record, 100)

	// Register channels with the factory
	DataFactory.AddChannel(StreamStorage, StorageChannel)
	DataFactory.AddChannel(StreamAnalytics, AnalyticsChannel)
	DataFactory.AddChannel(StreamAlerts, AlertsChannel)
	DataFactory.AddChannel(StreamArchive, ArchiveChannel)
	DataFactory.AddChannel(StreamAll, AllChannel)
}

// ============================================================================
// PIPELINE SCHEMAS
// ============================================================================
// Different fanout patterns defined declaratively.

// StandardPipelineSchema - standard fanout to all destinations.
var StandardPipelineSchema = flume.Schema{
	Version: "1.0.0",
	Node: flume.Node{
		Type: "sequence",
		Name: "standard-pipeline",
		Children: []flume.Node{
			{Ref: ProcessorValidate},
			{Ref: ProcessorTransform},
			{Ref: ProcessorEnrich},
			// Fan out to multiple streams
			{
				Type: "concurrent",
				Name: "fanout",
				Children: []flume.Node{
					{Stream: StreamStorage},
					{Stream: StreamAnalytics},
					// Only alert on high priority
					{
						Type:      "filter",
						Predicate: "is-high-priority",
						Then:      &flume.Node{Stream: StreamAlerts},
					},
					{Stream: StreamArchive},
					{Stream: StreamAll},
				},
			},
		},
	},
}

// AnalyticsOnlySchema - just analytics, skip storage.
// Useful for real-time dashboards that don't need persistence.
var AnalyticsOnlySchema = flume.Schema{
	Version: "1.0.0",
	Node: flume.Node{
		Type: "sequence",
		Name: "analytics-only",
		Children: []flume.Node{
			{Ref: ProcessorValidate},
			{Ref: ProcessorTransform},
			{Ref: ProcessorEnrich},
			{Stream: StreamAnalytics},
			{Stream: StreamAll},
		},
	},
}

// HighPriorityOnlySchema - storage + alerts only.
// For critical data that needs immediate attention.
var HighPriorityOnlySchema = flume.Schema{
	Version: "1.0.0",
	Node: flume.Node{
		Type: "sequence",
		Name: "high-priority-pipeline",
		Children: []flume.Node{
			{Ref: ProcessorValidate},
			{Ref: ProcessorTransform},
			{Ref: ProcessorEnrich},
			{
				Type: "concurrent",
				Children: []flume.Node{
					{Stream: StreamStorage},
					{Stream: StreamAlerts},
					{Stream: StreamAll},
				},
			},
		},
	},
}

// BatchArchiveSchema - archive only for batch processing.
// Skip real-time systems, just store for later analysis.
var BatchArchiveSchema = flume.Schema{
	Version: "1.0.0",
	Node: flume.Node{
		Type: "sequence",
		Name: "batch-archive",
		Children: []flume.Node{
			{Ref: ProcessorValidate},
			{Ref: ProcessorTransform},
			// Skip enrichment for batch - just archive raw
			{Stream: StreamArchive},
			{Stream: StreamAll},
		},
	},
}

// LoadSchemas registers all pipeline schemas.
func LoadSchemas() error {
	schemas := map[string]flume.Schema{
		"standard":      StandardPipelineSchema,
		"analytics":     AnalyticsOnlySchema,
		"high-priority": HighPriorityOnlySchema,
		"batch":         BatchArchiveSchema,
	}

	for name, schema := range schemas {
		if err := DataFactory.SetSchema(name, schema); err != nil {
			return fmt.Errorf("failed to load schema %s: %w", name, err)
		}
	}

	return nil
}

// ============================================================================
// CONSUMERS
// ============================================================================
// Goroutines that consume from channels and call services.
// This is the "same data, three destinations, zero code duplication" part.

var consumerWg sync.WaitGroup

// StartConsumers launches goroutines to consume from channels.
func StartConsumers(ctx context.Context) {
	// Storage consumer
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for {
			select {
			case r := <-StorageChannel:
				_ = StorageService.Store(ctx, r)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Analytics consumer
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for {
			select {
			case r := <-AnalyticsChannel:
				_ = AnalyticsService.Track(ctx, r)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Alerts consumer
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for {
			select {
			case r := <-AlertsChannel:
				_ = AlertService.SendAlert(ctx, r)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Archive consumer
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for {
			select {
			case r := <-ArchiveChannel:
				_ = ArchiveService.Archive(ctx, r)
			case <-ctx.Done():
				return
			}
		}
	}()

	// "All" consumer - could be used for logging, metrics, etc.
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for {
			select {
			case <-AllChannel:
				// Just drain - in production this might go to a log aggregator
			case <-ctx.Done():
				return
			}
		}
	}()
}

// DrainChannels waits for channels to empty.
func DrainChannels() {
	// Give consumers time to process
	time.Sleep(100 * time.Millisecond)
}

// ProcessRecord runs a record through the specified pipeline.
func ProcessRecord(ctx context.Context, pipelineName string, record Record) (Record, error) {
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now()
	}
	if record.ProcessLog == nil {
		record.ProcessLog = make([]string, 0)
	}
	if record.Priority == "" {
		record.Priority = PriorityUnknown
	}

	pipeline, ok := DataFactory.Pipeline(pipelineName)
	if !ok {
		return record, fmt.Errorf("pipeline %q not found", pipelineName)
	}

	return pipeline.Process(ctx, record)
}
