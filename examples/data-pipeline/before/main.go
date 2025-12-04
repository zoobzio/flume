//go:build examples

// Data Pipeline - Before (pipz only)
// ===================================
// A data ingestion pipeline with hardcoded fanout destinations.
//
// Run: go run -tags examples .

package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	fmt.Println("=== Data Pipeline (pipz only) ===")
	fmt.Println("Process data and fan out to multiple destinations.")
	fmt.Println()

	ctx := context.Background()
	ResetServices()

	// Process some records
	records := []Record{
		{
			ID:     "record-001",
			Source: "api",
			Payload: map[string]any{
				"user_id": "user-123",
				"action":  "login",
			},
		},
		{
			ID:     "record-002",
			Source: "webhook",
			Payload: map[string]any{
				"critical": true, // Will become high priority
				"alert":    "system overload",
			},
		},
		{
			ID:     "record-003",
			Source: "batch",
			Payload: map[string]any{
				"batch":      true, // Will become low priority
				"batch_id":   "batch-456",
				"item_count": 1000,
			},
		},
	}

	fmt.Println("Processing records through single pipeline...")
	fmt.Println()

	for _, record := range records {
		fmt.Printf("--- Processing %s ---\n", record.ID)

		result, err := ProcessRecord(ctx, record)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("Processed: priority=%s\n", result.Priority)
			fmt.Printf("Log: %v\n", result.ProcessLog)
		}
		fmt.Println()
	}

	// Show destination counts
	fmt.Println("=== Destination Summary ===")
	fmt.Printf("Storage:   %d records\n", StorageService.Count())
	fmt.Printf("Analytics: %d events\n", AnalyticsService.Count())
	fmt.Printf("Alerts:    %d alerts\n", AlertService.Count())
	fmt.Printf("Archive:   %d archived\n", ArchiveService.Count())
	fmt.Println()

	// Show the limitation
	fmt.Println("=== THE PROBLEM ===")
	fmt.Println()
	fmt.Println("The fanout destinations are hardcoded:")
	fmt.Println()
	fmt.Println("  pipz.NewConcurrent[Record](")
	fmt.Println("      \"fanout\",")
	fmt.Println("      nil,")
	fmt.Println("      Store,      // Always runs")
	fmt.Println("      Analytics,  // Always runs")
	fmt.Println("      Alert,      // Always runs (checks priority internally)")
	fmt.Println("      Archive,    // Always runs")
	fmt.Println("  )")
	fmt.Println()
	fmt.Println("What if you want to:")
	fmt.Println("  - Add a new destination (Kafka, webhook)?")
	fmt.Println("  - Disable analytics for batch data?")
	fmt.Println("  - Route different data types to different destinations?")
	fmt.Println("  - Let consumers subscribe to specific data streams?")
	fmt.Println()
	fmt.Println("With pure pipz, every change requires code + redeploy.")
	fmt.Println()
	fmt.Println("See the 'after' example to see how flume + channels solve this.")
}

func init() {
	// Suppress timestamp drift in output
	_ = time.Now()
}
