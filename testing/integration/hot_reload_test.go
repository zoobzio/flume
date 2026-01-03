package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zoobzio/flume"
	flumetesting "github.com/zoobzio/flume/testing"
	"github.com/zoobzio/pipz"
)

func TestAutoSync_BasicSchemaUpdate(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	v1ProcessorID := factory.Identity("processor-v1", "Version 1 processor that prefixes name with v1")
	v2ProcessorID := factory.Identity("processor-v2", "Version 2 processor that prefixes name with v2")
	pipelineID := factory.Identity("my-pipeline", "Pipeline for basic schema update test")

	// Register processors
	var v1Calls, v2Calls int64

	factory.Add(
		pipz.Transform(v1ProcessorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&v1Calls, 1)
			d.Name = "v1-" + d.Name
			return d
		}),
		pipz.Transform(v2ProcessorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&v2Calls, 1)
			d.Name = "v2-" + d.Name
			return d
		}),
	)

	// Set initial schema
	schemaV1 := flume.Schema{
		Version: "1.0.0",
		Node: flume.Node{
			Ref: "processor-v1",
		},
	}

	if err := factory.SetSchema("my-schema", schemaV1); err != nil {
		t.Fatalf("failed to set schema v1: %v", err)
	}

	binding, err := factory.Bind(pipelineID, "my-schema", flume.WithAutoSync[flumetesting.TestData]())
	if err != nil {
		t.Fatalf("failed to bind schema v1: %v", err)
	}

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Name: "test"}

	result, pErr := binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("v1 processing failed: %v", pErr)
	}

	if result.Name != "v1-test" {
		t.Errorf("expected 'v1-test', got %q", result.Name)
	}

	// Update schema via SetSchema (triggers auto-sync)
	schemaV2 := flume.Schema{
		Version: "2.0.0",
		Node: flume.Node{
			Ref: "processor-v2",
		},
	}

	err = factory.SetSchema("my-schema", schemaV2)
	if err != nil {
		t.Fatalf("failed to update to schema v2: %v", err)
	}

	result, pErr = binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("v2 processing failed: %v", pErr)
	}

	if result.Name != "v2-test" {
		t.Errorf("expected 'v2-test', got %q", result.Name)
	}

	// Verify call counts
	if atomic.LoadInt64(&v1Calls) != 1 {
		t.Errorf("expected v1 to be called once, got %d", v1Calls)
	}
	if atomic.LoadInt64(&v2Calls) != 1 {
		t.Errorf("expected v2 to be called once, got %d", v2Calls)
	}
}

func TestAutoSync_ConcurrentAccess(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	processorID := factory.Identity("processor", "Simple processor for concurrent access testing")
	pipelineID := factory.Identity("concurrent-pipeline", "Pipeline for concurrent access test")

	var processCount int64

	factory.Add(pipz.Transform(processorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		atomic.AddInt64(&processCount, 1)
		d.Name = "processed-" + d.Name
		return d
	}))

	schema := flume.Schema{
		Version: "1.0.0",
		Node: flume.Node{
			Ref: "processor",
		},
	}

	if err := factory.SetSchema("concurrent-schema", schema); err != nil {
		t.Fatalf("failed to set schema: %v", err)
	}

	binding, err := factory.Bind(pipelineID, "concurrent-schema", flume.WithAutoSync[flumetesting.TestData]())
	if err != nil {
		t.Fatalf("failed to bind schema: %v", err)
	}

	const numGoroutines = 50
	const numOperations = 100

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines*numOperations)

	// Start concurrent processors
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				ctx := context.Background()
				input := flumetesting.TestData{ID: id*1000 + j, Name: "test"}
				_, pErr := binding.Process(ctx, input)
				if pErr != nil {
					errChan <- pErr
				}
			}
		}(i)
	}

	// Concurrent schema updates (errors logged but not fatal in concurrent context)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			time.Sleep(time.Duration(v) * 10 * time.Millisecond)
			newSchema := flume.Schema{
				Version: "update-" + string(rune('A'+v)),
				Node: flume.Node{
					Ref: "processor",
				},
			}
			if err := factory.SetSchema("concurrent-schema", newSchema); err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for pErr := range errChan {
		t.Errorf("concurrent processing error: %v", pErr)
	}

	expectedCalls := int64(numGoroutines * numOperations)
	actualCalls := atomic.LoadInt64(&processCount)
	if actualCalls != expectedCalls {
		t.Errorf("expected %d process calls, got %d", expectedCalls, actualCalls)
	}
}

func TestAutoSync_MultipleBindings(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities
	v1ProcessorID := factory.Identity("processor-v1", "Version 1 processor")
	v2ProcessorID := factory.Identity("processor-v2", "Version 2 processor")
	binding1ID := factory.Identity("binding-1", "First binding")
	binding2ID := factory.Identity("binding-2", "Second binding")
	binding3ID := factory.Identity("binding-3", "Third binding (no auto-sync)")

	factory.Add(
		pipz.Transform(v1ProcessorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Name = "v1-" + d.Name
			return d
		}),
		pipz.Transform(v2ProcessorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Name = "v2-" + d.Name
			return d
		}),
	)

	schemaV1 := flume.Schema{
		Version: "1.0.0",
		Node:    flume.Node{Ref: "processor-v1"},
	}

	if err := factory.SetSchema("shared-schema", schemaV1); err != nil {
		t.Fatalf("failed to set schema: %v", err)
	}

	// Create bindings - two with auto-sync, one without
	binding1, err := factory.Bind(binding1ID, "shared-schema", flume.WithAutoSync[flumetesting.TestData]())
	if err != nil {
		t.Fatalf("failed to bind binding1: %v", err)
	}

	binding2, err := factory.Bind(binding2ID, "shared-schema", flume.WithAutoSync[flumetesting.TestData]())
	if err != nil {
		t.Fatalf("failed to bind binding2: %v", err)
	}

	binding3, err := factory.Bind(binding3ID, "shared-schema") // No auto-sync
	if err != nil {
		t.Fatalf("failed to bind binding3: %v", err)
	}

	ctx := context.Background()
	input := flumetesting.TestData{Name: "test"}

	// Verify all use v1 initially
	for i, b := range []*flume.Binding[flumetesting.TestData]{binding1, binding2, binding3} {
		result, pErr := b.Process(ctx, input)
		if pErr != nil {
			t.Fatalf("binding%d process failed: %v", i+1, pErr)
		}
		if result.Name != "v1-test" {
			t.Errorf("binding%d: expected 'v1-test', got %q", i+1, result.Name)
		}
	}

	// Update schema
	schemaV2 := flume.Schema{
		Version: "2.0.0",
		Node:    flume.Node{Ref: "processor-v2"},
	}

	if err := factory.SetSchema("shared-schema", schemaV2); err != nil {
		t.Fatalf("failed to update schema: %v", err)
	}

	// Verify auto-sync bindings updated, non-auto-sync didn't
	result1, pErr := binding1.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("binding1 process after update failed: %v", pErr)
	}
	result2, pErr := binding2.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("binding2 process after update failed: %v", pErr)
	}
	result3, pErr := binding3.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("binding3 process after update failed: %v", pErr)
	}

	if result1.Name != "v2-test" {
		t.Errorf("binding1 (auto-sync): expected 'v2-test', got %q", result1.Name)
	}
	if result2.Name != "v2-test" {
		t.Errorf("binding2 (auto-sync): expected 'v2-test', got %q", result2.Name)
	}
	if result3.Name != "v1-test" {
		t.Errorf("binding3 (no auto-sync): expected 'v1-test' (unchanged), got %q", result3.Name)
	}
}

func TestAutoSync_ErrorHandling(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities
	validProcessorID := factory.Identity("valid-processor", "Valid processor")
	pipelineID := factory.Identity("error-test-pipeline", "Pipeline for error handling test")

	factory.Add(pipz.Transform(validProcessorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		d.Name = "valid-" + d.Name
		return d
	}))

	validSchema := flume.Schema{
		Version: "1.0.0",
		Node:    flume.Node{Ref: "valid-processor"},
	}

	if err := factory.SetSchema("error-schema", validSchema); err != nil {
		t.Fatalf("failed to set valid schema: %v", err)
	}

	binding, err := factory.Bind(pipelineID, "error-schema", flume.WithAutoSync[flumetesting.TestData]())
	if err != nil {
		t.Fatalf("failed to bind: %v", err)
	}

	ctx := context.Background()
	input := flumetesting.TestData{Name: "test"}

	// Verify working
	result, pErr := binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("process failed: %v", pErr)
	}
	if result.Name != "valid-test" {
		t.Errorf("expected 'valid-test', got %q", result.Name)
	}

	// Try to set invalid schema (references non-existent processor)
	invalidSchema := flume.Schema{
		Version: "2.0.0",
		Node:    flume.Node{Ref: "nonexistent-processor"},
	}

	err = factory.SetSchema("error-schema", invalidSchema)
	if err == nil {
		t.Fatal("expected error for invalid schema, got nil")
	}

	// Binding should still work with previous schema
	result, pErr = binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("process failed after invalid schema attempt: %v", pErr)
	}
	if result.Name != "valid-test" {
		t.Errorf("expected binding to still use valid schema, got %q", result.Name)
	}
}

func TestAutoSync_ProcessorMetadata(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities
	processorID := factory.Identity("metadata-processor", "Processor with metadata")
	pipelineID := factory.Identity("metadata-pipeline", "Pipeline for metadata test")

	factory.Add(pipz.Transform(processorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		d.Name = "processed"
		return d
	}))

	schema := flume.Schema{
		Version: "1.0.0",
		Node:    flume.Node{Ref: "metadata-processor"},
	}

	if err := factory.SetSchema("metadata-schema", schema); err != nil {
		t.Fatalf("failed to set schema: %v", err)
	}

	binding, err := factory.Bind(pipelineID, "metadata-schema", flume.WithAutoSync[flumetesting.TestData]())
	if err != nil {
		t.Fatalf("failed to bind: %v", err)
	}

	// Verify accessors
	if binding.Identity().Name() != "metadata-pipeline" {
		t.Errorf("expected identity name 'metadata-pipeline', got %q", binding.Identity().Name())
	}

	if binding.SchemaID() != "metadata-schema" {
		t.Errorf("expected schema ID 'metadata-schema', got %q", binding.SchemaID())
	}

	if !binding.AutoSync() {
		t.Error("expected AutoSync to be true")
	}

	if binding.Pipeline() == nil {
		t.Error("expected non-nil pipeline")
	}
}

func TestAutoSync_SchemaRegistry(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	processorID := factory.Identity("registry-processor", "Processor for registry test")
	factory.Add(pipz.Transform(processorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		return d
	}))

	// Test schema registry operations
	schema := flume.Schema{
		Version: "1.0.0",
		Node:    flume.Node{Ref: "registry-processor"},
	}

	// Initially no schemas
	if factory.HasSchema("test-schema") {
		t.Error("expected no schema initially")
	}

	// Set schema
	if err := factory.SetSchema("test-schema", schema); err != nil {
		t.Fatalf("failed to set schema: %v", err)
	}

	// Verify schema exists
	if !factory.HasSchema("test-schema") {
		t.Error("expected schema to exist after SetSchema")
	}

	// Get schema
	retrieved, ok := factory.GetSchema("test-schema")
	if !ok {
		t.Error("expected GetSchema to find schema")
	}
	if retrieved.Version != "1.0.0" {
		t.Errorf("expected version '1.0.0', got %q", retrieved.Version)
	}

	// List schemas
	schemas := factory.ListSchemas()
	found := false
	for _, s := range schemas {
		if s == "test-schema" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'test-schema' in ListSchemas")
	}

	// Remove schema
	if !factory.RemoveSchema("test-schema") {
		t.Error("expected RemoveSchema to return true")
	}

	if factory.HasSchema("test-schema") {
		t.Error("expected schema to be removed")
	}
}

func TestAutoSync_HighLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high load test in short mode")
	}

	factory := flume.New[flumetesting.TestData]()

	processorID := factory.Identity("load-processor", "Processor for high load test")
	pipelineID := factory.Identity("load-pipeline", "Pipeline for high load test")

	var processCount int64
	factory.Add(pipz.Transform(processorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		atomic.AddInt64(&processCount, 1)
		return d
	}))

	schema := flume.Schema{
		Node: flume.Node{Ref: "load-processor"},
	}

	if err := factory.SetSchema("load-schema", schema); err != nil {
		t.Fatalf("failed to set schema: %v", err)
	}

	binding, err := factory.Bind(pipelineID, "load-schema", flume.WithAutoSync[flumetesting.TestData]())
	if err != nil {
		t.Fatalf("failed to bind: %v", err)
	}

	const numGoroutines = 100
	const numOperations = 1000

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				//nolint:errcheck // benchmark - errors not relevant
				binding.Process(context.Background(), flumetesting.TestData{})
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	expected := int64(numGoroutines * numOperations)
	actual := atomic.LoadInt64(&processCount)

	if actual != expected {
		t.Errorf("expected %d operations, got %d", expected, actual)
	}

	t.Logf("Completed %d operations in %v (%.0f ops/sec)",
		actual, elapsed, float64(actual)/elapsed.Seconds())
}
