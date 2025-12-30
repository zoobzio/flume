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

	// Set initial schema via Bind
	schemaV1 := flume.Schema{
		Version: "1.0.0",
		Node: flume.Node{
			Ref: "processor-v1",
		},
	}

	binding, err := factory.Bind(pipelineID, schemaV1)
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

	// Update schema via binding.Update
	schemaV2 := flume.Schema{
		Version: "2.0.0",
		Node: flume.Node{
			Ref: "processor-v2",
		},
	}

	err = binding.Update(schemaV2)
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

func TestHotReload_ConcurrentAccess(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	processorID := factory.Identity("processor", "Simple processor for concurrent access testing")
	pipelineID := factory.Identity("concurrent-pipeline", "Pipeline for concurrent access test")

	var callCount int64

	factory.Add(pipz.Transform(processorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		atomic.AddInt64(&callCount, 1)
		return d
	}))

	schema := flume.Schema{
		Node: flume.Node{Ref: "processor"},
	}

	binding, err := factory.Bind(pipelineID, schema)
	if err != nil {
		t.Fatalf("failed to bind schema: %v", err)
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
				_, pErr := binding.Process(ctx, input)
				if pErr != nil {
					t.Errorf("processing failed: %v", pErr)
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
			if err := binding.Update(newSchema); err != nil {
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

func TestHotReload_BindingRollback(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	v1ID := factory.Identity("v1", "Version 1 processor that prefixes name with v1")
	v2ID := factory.Identity("v2", "Version 2 processor that prefixes name with v2")
	pipelineID := factory.Identity("rollback-test", "Pipeline for rollback testing")

	factory.Add(
		pipz.Transform(v1ID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Name = "v1-" + d.Name
			return d
		}),
		pipz.Transform(v2ID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Name = "v2-" + d.Name
			return d
		}),
	)

	schemaV1 := flume.Schema{
		Node: flume.Node{Ref: "v1"},
	}

	binding, err := factory.Bind(pipelineID, schemaV1)
	if err != nil {
		t.Fatalf("failed to bind schema: %v", err)
	}

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Name: "test"}

	// Verify v1 works
	result, pErr := binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("v1 processing failed: %v", pErr)
	}
	if result.Name != "v1-test" {
		t.Errorf("expected 'v1-test', got %q", result.Name)
	}

	// Update to v2
	schemaV2 := flume.Schema{
		Node: flume.Node{Ref: "v2"},
	}
	err = binding.Update(schemaV2)
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	// Verify v2 works
	result, pErr = binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("v2 processing failed: %v", pErr)
	}
	if result.Name != "v2-test" {
		t.Errorf("expected 'v2-test', got %q", result.Name)
	}

	// Rollback to v1
	err = binding.Rollback()
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	// Verify rollback restored v1
	result, pErr = binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("v1 processing after rollback failed: %v", pErr)
	}
	if result.Name != "v1-test" {
		t.Errorf("expected 'v1-test' after rollback, got %q", result.Name)
	}
}

func TestHotReload_MultipleBindings(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	doubleID := factory.Identity("double", "Doubles the value")
	tripleID := factory.Identity("triple", "Triples the value")
	addTenID := factory.Identity("add-ten", "Adds ten to the value")
	doublePipelineID := factory.Identity("double-pipeline", "Pipeline that doubles values")
	triplePipelineID := factory.Identity("triple-pipeline", "Pipeline that triples values")
	addTenPipelineID := factory.Identity("add-ten-pipeline", "Pipeline that doubles then adds ten")

	factory.Add(
		pipz.Transform(doubleID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Value *= 2
			return d
		}),
		pipz.Transform(tripleID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Value *= 3
			return d
		}),
		pipz.Transform(addTenID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Value += 10
			return d
		}),
	)

	// Create multiple bindings
	type bindingCase struct {
		id       pipz.Identity
		schema   flume.Schema
		expected float64
	}

	cases := []bindingCase{
		{doublePipelineID, flume.Schema{Node: flume.Node{Ref: "double"}}, 10.0},
		{triplePipelineID, flume.Schema{Node: flume.Node{Ref: "triple"}}, 15.0},
		{addTenPipelineID, flume.Schema{Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "double"},
				{Ref: "add-ten"},
			},
		}}, 20.0}, // (5 * 2) + 10
	}

	bindings := make(map[string]*flume.Binding[flumetesting.TestData])
	for _, c := range cases {
		binding, err := factory.Bind(c.id, c.schema)
		if err != nil {
			t.Fatalf("failed to bind %q: %v", c.id.Name(), err)
		}
		bindings[c.id.Name()] = binding
	}

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Value: 5.0}

	// Test each binding
	for _, c := range cases {
		t.Run(c.id.Name(), func(t *testing.T) {
			binding := bindings[c.id.Name()]
			result, pErr := binding.Process(ctx, input)
			if pErr != nil {
				t.Fatalf("processing failed: %v", pErr)
			}

			if result.Value != c.expected {
				t.Errorf("expected Value=%f, got %f", c.expected, result.Value)
			}
		})
	}
}

func TestHotReload_BindingHistory(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	processorID := factory.Identity("processor", "Pass-through processor for history test")
	pipelineID := factory.Identity("test-schema", "Pipeline for binding history test")

	factory.Add(pipz.Transform(processorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		return d
	}))

	schema := flume.Schema{
		Version: "1.2.3",
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "processor"},
			},
		},
	}

	binding, err := factory.Bind(pipelineID, schema)
	if err != nil {
		t.Fatalf("failed to bind schema: %v", err)
	}

	// Process to verify it works
	ctx := context.Background()
	input := flumetesting.TestData{ID: 1}

	_, pErr := binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("processing failed: %v", pErr)
	}

	// Update schema
	schema2 := flume.Schema{
		Version: "2.0.0",
		Node:    flume.Node{Ref: "processor"},
	}
	err = binding.Update(schema2)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	// Verify we can roll back
	err = binding.Rollback()
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	// Non-existent binding
	nonexistentID := factory.Identity("nonexistent", "Identity for testing non-existent binding lookup")
	missing := factory.Get(nonexistentID)
	if missing != nil {
		t.Error("expected Get to return nil for non-existent binding")
	}
}

func TestHotReload_ValidationOnUpdate(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	processorID := factory.Identity("valid-processor", "Valid processor for validation test")
	pipelineID := factory.Identity("validated", "Pipeline for validation on update test")

	factory.Add(pipz.Transform(processorID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		return d
	}))

	// Bind valid schema
	validSchema := flume.Schema{
		Node: flume.Node{Ref: "valid-processor"},
	}

	binding, err := factory.Bind(pipelineID, validSchema)
	if err != nil {
		t.Fatalf("failed to bind valid schema: %v", err)
	}

	// Attempt to update with invalid schema
	invalidSchema := flume.Schema{
		Node: flume.Node{Ref: "nonexistent-processor"},
	}

	err = binding.Update(invalidSchema)
	if err == nil {
		t.Error("expected error when updating with invalid schema")
	}

	// Verify original schema still works
	ctx := context.Background()
	input := flumetesting.TestData{ID: 1}
	_, pErr := binding.Process(ctx, input)
	if pErr != nil {
		t.Errorf("original binding should still work: %v", pErr)
	}
}

func TestHotReload_ProcessorUpdate(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	versionedID := factory.Identity("versioned", "Versioned processor that sets ID from atomic version")
	pipelineID := factory.Identity("processor-update", "Pipeline for processor update test")

	var version int64

	// Initial processor
	factory.Add(pipz.Transform(versionedID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		d.ID = int(atomic.LoadInt64(&version))
		return d
	}))

	schema := flume.Schema{
		Node: flume.Node{Ref: "versioned"},
	}

	binding, err := factory.Bind(pipelineID, schema)
	if err != nil {
		t.Fatalf("failed to bind schema: %v", err)
	}

	ctx := context.Background()
	input := flumetesting.TestData{Name: "test"}

	// Process with v1
	atomic.StoreInt64(&version, 1)
	result, pErr := binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("v1 processing failed: %v", pErr)
	}
	if result.ID != 1 {
		t.Errorf("expected ID=1, got %d", result.ID)
	}

	// Update processor version
	atomic.StoreInt64(&version, 2)
	result, pErr = binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("v2 processing failed: %v", pErr)
	}
	if result.ID != 2 {
		t.Errorf("expected ID=2, got %d", result.ID)
	}
}

func TestHotReload_ChannelPersistence(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Define identities upfront
	processID := factory.Identity("process", "Pass-through processor for channel persistence test")
	pipelineID := factory.Identity("channel-pipeline", "Pipeline for channel persistence test")

	factory.Add(pipz.Transform(processID, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
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

	binding, err := factory.Bind(pipelineID, schema)
	if err != nil {
		t.Fatalf("failed to bind schema: %v", err)
	}

	// Process data
	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Name: "test"}

	_, pErr := binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("processing failed: %v", pErr)
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
	err = binding.Update(schema)
	if err != nil {
		t.Fatalf("failed to update binding: %v", err)
	}

	// Process again
	input.ID = 2
	_, pErr = binding.Process(ctx, input)
	if pErr != nil {
		t.Fatalf("processing failed after update: %v", pErr)
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
