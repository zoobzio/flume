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

func TestHotReload_BasicSchemaUpdate(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	var v1Calls, v2Calls int64

	factory.Add(
		pipz.Transform("processor-v1", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&v1Calls, 1)
			d.Name = "v1-" + d.Name
			return d
		}),
		pipz.Transform("processor-v2", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
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

	err := factory.SetSchema("my-pipeline", schemaV1)
	if err != nil {
		t.Fatalf("failed to set schema v1: %v", err)
	}

	// Get and use pipeline
	pipeline, ok := factory.Pipeline("my-pipeline")
	if !ok {
		t.Fatal("failed to get pipeline")
	}

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Name: "test"}

	result, err := pipeline.Process(ctx, input)
	if err != nil {
		t.Fatalf("v1 processing failed: %v", err)
	}

	if result.Name != "v1-test" {
		t.Errorf("expected 'v1-test', got %q", result.Name)
	}

	// Update schema
	schemaV2 := flume.Schema{
		Version: "2.0.0",
		Node: flume.Node{
			Ref: "processor-v2",
		},
	}

	err = factory.SetSchema("my-pipeline", schemaV2)
	if err != nil {
		t.Fatalf("failed to set schema v2: %v", err)
	}

	// Get updated pipeline
	pipeline, ok = factory.Pipeline("my-pipeline")
	if !ok {
		t.Fatal("failed to get updated pipeline")
	}

	result, err = pipeline.Process(ctx, input)
	if err != nil {
		t.Fatalf("v2 processing failed: %v", err)
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

func TestHotReload_ConcurrentAccess(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	var callCount int64

	factory.Add(pipz.Transform("processor", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		atomic.AddInt64(&callCount, 1)
		return d
	}))

	schema := flume.Schema{
		Node: flume.Node{Ref: "processor"},
	}

	err := factory.SetSchema("concurrent-pipeline", schema)
	if err != nil {
		t.Fatalf("failed to set schema: %v", err)
	}

	// Run concurrent readers and writers
	const numReaders = 10
	const numIterations = 100

	var wg sync.WaitGroup
	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Name: "test"}

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				pipeline, ok := factory.Pipeline("concurrent-pipeline")
				if !ok {
					t.Error("failed to get pipeline during concurrent access")
					return
				}
				_, err := pipeline.Process(ctx, input)
				if err != nil {
					t.Errorf("processing failed: %v", err)
					return
				}
			}
		}()
	}

	// Start writer (updating schema periodically)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 10; j++ {
			newSchema := flume.Schema{
				Version: string(rune('A' + j)),
				Node:    flume.Node{Ref: "processor"},
			}
			if err := factory.SetSchema("concurrent-pipeline", newSchema); err != nil {
				t.Errorf("failed to update schema: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()

	expectedCalls := int64(numReaders * numIterations)
	actualCalls := atomic.LoadInt64(&callCount)
	if actualCalls != expectedCalls {
		t.Errorf("expected %d calls, got %d", expectedCalls, actualCalls)
	}
}

func TestHotReload_SchemaRemoval(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	factory.Add(pipz.Transform("processor", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		return d
	}))

	schema := flume.Schema{
		Node: flume.Node{Ref: "processor"},
	}

	// Set schema
	err := factory.SetSchema("removable", schema)
	if err != nil {
		t.Fatalf("failed to set schema: %v", err)
	}

	// Verify it exists
	_, ok := factory.Pipeline("removable")
	if !ok {
		t.Fatal("expected pipeline to exist")
	}

	// Verify it's in the list
	schemas := factory.ListSchemas()
	found := false
	for _, name := range schemas {
		if name == "removable" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'removable' in schema list")
	}

	// Remove schema
	removed := factory.RemoveSchema("removable")
	if !removed {
		t.Error("expected RemoveSchema to return true")
	}

	// Verify it no longer exists
	_, ok = factory.Pipeline("removable")
	if ok {
		t.Error("expected pipeline to be removed")
	}

	// Removing again should return false
	removed = factory.RemoveSchema("removable")
	if removed {
		t.Error("expected RemoveSchema to return false for non-existent schema")
	}
}

func TestHotReload_MultipleSchemas(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	factory.Add(
		pipz.Transform("double", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Value *= 2
			return d
		}),
		pipz.Transform("triple", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Value *= 3
			return d
		}),
		pipz.Transform("add-ten", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Value += 10
			return d
		}),
	)

	// Register multiple schemas
	schemas := map[string]flume.Schema{
		"double-pipeline": {Node: flume.Node{Ref: "double"}},
		"triple-pipeline": {Node: flume.Node{Ref: "triple"}},
		"add-ten-pipeline": {Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "double"},
				{Ref: "add-ten"},
			},
		}},
	}

	for name, schema := range schemas {
		if err := factory.SetSchema(name, schema); err != nil {
			t.Fatalf("failed to set schema %q: %v", name, err)
		}
	}

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Value: 5.0}

	// Test each pipeline
	tests := []struct {
		pipelineName string
		expected     float64
	}{
		{"double-pipeline", 10.0},
		{"triple-pipeline", 15.0},
		{"add-ten-pipeline", 20.0}, // (5 * 2) + 10
	}

	for _, tt := range tests {
		t.Run(tt.pipelineName, func(t *testing.T) {
			pipeline, ok := factory.Pipeline(tt.pipelineName)
			if !ok {
				t.Fatalf("failed to get pipeline %q", tt.pipelineName)
			}

			result, err := pipeline.Process(ctx, input)
			if err != nil {
				t.Fatalf("processing failed: %v", err)
			}

			if result.Value != tt.expected {
				t.Errorf("expected Value=%f, got %f", tt.expected, result.Value)
			}
		})
	}

	// Verify list includes all schemas
	schemaList := factory.ListSchemas()
	if len(schemaList) != len(schemas) {
		t.Errorf("expected %d schemas, got %d", len(schemas), len(schemaList))
	}
}

func TestHotReload_GetSchema(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	factory.Add(pipz.Transform("processor", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		return d
	}))

	originalSchema := flume.Schema{
		Version: "1.2.3",
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "processor"},
			},
		},
	}

	err := factory.SetSchema("test-schema", originalSchema)
	if err != nil {
		t.Fatalf("failed to set schema: %v", err)
	}

	// Retrieve schema
	retrieved, ok := factory.GetSchema("test-schema")
	if !ok {
		t.Fatal("expected to retrieve schema")
	}

	// Verify version is preserved
	if retrieved.Version != "1.2.3" {
		t.Errorf("expected version '1.2.3', got %q", retrieved.Version)
	}

	// Verify structure is preserved
	if retrieved.Type != "sequence" {
		t.Errorf("expected type 'sequence', got %q", retrieved.Type)
	}

	if len(retrieved.Children) != 1 {
		t.Errorf("expected 1 child, got %d", len(retrieved.Children))
	}

	// Non-existent schema
	_, ok = factory.GetSchema("nonexistent")
	if ok {
		t.Error("expected GetSchema to return false for non-existent schema")
	}
}

func TestHotReload_ValidationOnUpdate(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	factory.Add(pipz.Transform("valid-processor", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		return d
	}))

	// Set valid schema
	validSchema := flume.Schema{
		Node: flume.Node{Ref: "valid-processor"},
	}

	err := factory.SetSchema("validated", validSchema)
	if err != nil {
		t.Fatalf("failed to set valid schema: %v", err)
	}

	// Attempt to update with invalid schema
	invalidSchema := flume.Schema{
		Node: flume.Node{Ref: "nonexistent-processor"},
	}

	err = factory.SetSchema("validated", invalidSchema)
	if err == nil {
		t.Error("expected error when setting invalid schema")
	}

	// Verify original schema still works
	pipeline, ok := factory.Pipeline("validated")
	if !ok {
		t.Fatal("expected pipeline to still exist after failed update")
	}

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1}
	_, err = pipeline.Process(ctx, input)
	if err != nil {
		t.Errorf("original pipeline should still work: %v", err)
	}
}

func TestHotReload_ProcessorUpdate(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	var version int64

	// Initial processor
	factory.Add(pipz.Transform("versioned", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		d.ID = int(atomic.LoadInt64(&version))
		return d
	}))

	schema := flume.Schema{
		Node: flume.Node{Ref: "versioned"},
	}

	err := factory.SetSchema("processor-update", schema)
	if err != nil {
		t.Fatalf("failed to set schema: %v", err)
	}

	ctx := context.Background()
	input := flumetesting.TestData{Name: "test"}

	// Process with v1
	atomic.StoreInt64(&version, 1)
	pipeline, _ := factory.Pipeline("processor-update")
	result, err := pipeline.Process(ctx, input)
	if err != nil {
		t.Fatalf("v1 processing failed: %v", err)
	}
	if result.ID != 1 {
		t.Errorf("expected ID=1, got %d", result.ID)
	}

	// Update processor version
	atomic.StoreInt64(&version, 2)
	result, err = pipeline.Process(ctx, input)
	if err != nil {
		t.Fatalf("v2 processing failed: %v", err)
	}
	if result.ID != 2 {
		t.Errorf("expected ID=2, got %d", result.ID)
	}
}

func TestHotReload_ChannelPersistence(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	factory.Add(pipz.Transform("process", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		return d
	}))

	outputChan := make(chan flumetesting.TestData, 10)
	factory.AddChannel("output", outputChan)

	schema := flume.Schema{
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "process"},
				{Stream: "output"},
			},
		},
	}

	err := factory.SetSchema("channel-pipeline", schema)
	if err != nil {
		t.Fatalf("failed to set schema: %v", err)
	}

	// Process data
	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Name: "test"}

	pipeline, _ := factory.Pipeline("channel-pipeline")
	_, err = pipeline.Process(ctx, input)
	if err != nil {
		t.Fatalf("processing failed: %v", err)
	}

	// Check channel received data
	select {
	case received := <-outputChan:
		if received.ID != 1 {
			t.Errorf("expected ID=1, got %d", received.ID)
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for channel data")
	}

	// Update schema (channel should still work)
	schema.Version = "2.0.0"
	err = factory.SetSchema("channel-pipeline", schema)
	if err != nil {
		t.Fatalf("failed to update schema: %v", err)
	}

	// Process again
	input.ID = 2
	pipeline, _ = factory.Pipeline("channel-pipeline")
	_, err = pipeline.Process(ctx, input)
	if err != nil {
		t.Fatalf("processing failed after update: %v", err)
	}

	select {
	case received := <-outputChan:
		if received.ID != 2 {
			t.Errorf("expected ID=2, got %d", received.ID)
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for channel data after update")
	}
}
