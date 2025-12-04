//go:build examples

// Data Pipeline Factory - After (flume + channels)
// =================================================
// Same processors, but fanout is now schema-driven.
// Different pipelines for different data types.
//
// Run: go run -tags examples .

package main

import (
	"context"
	"fmt"
)

func main() {
	fmt.Println("=== Data Pipeline Factory (flume + channels) ===")
	fmt.Println("Same processors, different fanout patterns per use case.")
	fmt.Println()

	// Initialize factory and consumers
	InitFactory()
	if err := LoadSchemas(); err != nil {
		fmt.Printf("Failed to load schemas: %v\n", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ResetServices()
	StartConsumers(ctx)

	// Show available pipelines
	fmt.Println("Available pipelines:")
	for _, name := range DataFactory.ListSchemas() {
		schema, _ := DataFactory.GetSchema(name)
		fmt.Printf("  - %s (v%s)\n", name, schema.Version)
	}
	fmt.Println()

	// Demo: Different records through different pipelines
	demos := []struct {
		name     string
		pipeline string
		record   Record
	}{
		{
			name:     "Normal API data",
			pipeline: "standard",
			record: Record{
				ID:     "record-001",
				Source: "api",
				Payload: map[string]any{
					"user_id": "user-123",
					"action":  "page_view",
				},
			},
		},
		{
			name:     "Critical alert data",
			pipeline: "high-priority",
			record: Record{
				ID:     "record-002",
				Source: "monitoring",
				Payload: map[string]any{
					"critical": true,
					"alert":    "CPU > 95%",
				},
			},
		},
		{
			name:     "Real-time dashboard data",
			pipeline: "analytics",
			record: Record{
				ID:     "record-003",
				Source: "metrics",
				Payload: map[string]any{
					"metric": "requests_per_second",
					"value":  1234,
				},
			},
		},
		{
			name:     "Batch import data",
			pipeline: "batch",
			record: Record{
				ID:     "record-004",
				Source: "batch-import",
				Payload: map[string]any{
					"batch":    true,
					"batch_id": "import-20240101",
				},
			},
		},
	}

	for _, demo := range demos {
		fmt.Printf("--- %s ---\n", demo.name)
		fmt.Printf("Pipeline: %s\n", demo.pipeline)

		result, err := ProcessRecord(ctx, demo.pipeline, demo.record)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("Processed: %s (priority: %s)\n", result.ID, result.Priority)
			fmt.Printf("Log: %v\n", result.ProcessLog)
		}
		fmt.Println()
	}

	// Let consumers process
	DrainChannels()

	// Show results
	fmt.Println("=== Destination Summary ===")
	fmt.Printf("Storage:   %d records\n", StorageService.Count())
	fmt.Printf("Analytics: %d events\n", AnalyticsService.Count())
	fmt.Printf("Alerts:    %d alerts\n", AlertService.Count())
	fmt.Printf("Archive:   %d archived\n", ArchiveService.Count())
	fmt.Println()

	// Explain what happened
	fmt.Println("=== WHAT HAPPENED ===")
	fmt.Println()
	fmt.Println("Same 4 records, different routing:")
	fmt.Println()
	fmt.Println("  record-001 (standard):      storage + analytics + archive")
	fmt.Println("  record-002 (high-priority): storage + alerts (critical!)")
	fmt.Println("  record-003 (analytics):     analytics only (real-time)")
	fmt.Println("  record-004 (batch):         archive only (cold storage)")
	fmt.Println()

	// Show the key benefits
	fmt.Println("=== THE SOLUTION ===")
	fmt.Println()
	fmt.Println("Fanout is now schema-driven:")
	fmt.Println()
	fmt.Println("  // Standard pipeline schema")
	fmt.Println("  Children: []flume.Node{")
	fmt.Println("      {Stream: StreamStorage},")
	fmt.Println("      {Stream: StreamAnalytics},")
	fmt.Println("      {Type: \"filter\", Predicate: \"is-high-priority\",")
	fmt.Println("          Then: &flume.Node{Stream: StreamAlerts}},")
	fmt.Println("      {Stream: StreamArchive},")
	fmt.Println("  }")
	fmt.Println()
	fmt.Println("Benefits:")
	fmt.Println("  1. Add destination = add channel + update schema")
	fmt.Println("  2. Change routing = update schema (no code change)")
	fmt.Println("  3. Multiple pipelines = multiple schemas")
	fmt.Println("  4. Consumers subscribe to what they need")
	fmt.Println("  5. Hot-reload supported for all schemas")
	fmt.Println()
	fmt.Println("The SAME data, THREE destinations, ZERO code duplication.")
}
