package flume_test

import (
	"context"
	"sync"
	"testing"

	"github.com/zoobzio/flume"
	"github.com/zoobzio/pipz"
)

// bindingTestData implements pipz.Cloner for binding tests.
type bindingTestData struct {
	Value   string
	Counter int
}

func (d bindingTestData) Clone() bindingTestData {
	return bindingTestData{
		Value:   d.Value,
		Counter: d.Counter,
	}
}

func TestBindingBasic(t *testing.T) {
	factory := flume.New[bindingTestData]()

	// Register a processor
	procID := factory.Identity("process", "Appends suffix to value")
	factory.Add(pipz.Transform(procID, func(_ context.Context, d bindingTestData) bindingTestData {
		d.Value += "_processed"
		return d
	}))

	// Register schema
	schema := flume.Schema{
		Version: "1.0",
		Node:    flume.Node{Ref: "process"},
	}
	if err := factory.SetSchema("test-schema", schema); err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	// Create binding
	bindingID := factory.Identity("test-binding", "Test binding")
	binding, err := factory.Bind(bindingID, "test-schema")
	if err != nil {
		t.Fatalf("Failed to create binding: %v", err)
	}

	// Test Process
	result, err := binding.Process(context.Background(), bindingTestData{Value: "test"})
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if result.Value != "test_processed" {
		t.Errorf("Expected 'test_processed', got '%s'", result.Value)
	}
}

func TestBindingAccessors(t *testing.T) {
	factory := flume.New[bindingTestData]()

	procID := factory.Identity("noop", "No-op processor")
	factory.Add(pipz.Transform(procID, func(_ context.Context, d bindingTestData) bindingTestData {
		return d
	}))

	schema := flume.Schema{
		Version: "2.5",
		Node:    flume.Node{Ref: "noop"},
	}
	if err := factory.SetSchema("accessor-schema", schema); err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	bindingID := factory.Identity("accessor-test", "Test binding for accessors")
	binding, err := factory.Bind(bindingID, "accessor-schema")
	if err != nil {
		t.Fatalf("Failed to create binding: %v", err)
	}

	t.Run("Identity", func(t *testing.T) {
		id := binding.Identity()
		if id.Name() != "accessor-test" {
			t.Errorf("Expected identity name 'accessor-test', got '%s'", id.Name())
		}
	})

	t.Run("SchemaID", func(t *testing.T) {
		if schemaID := binding.SchemaID(); schemaID != "accessor-schema" {
			t.Errorf("Expected schema ID 'accessor-schema', got '%s'", schemaID)
		}
	})

	t.Run("AutoSync", func(t *testing.T) {
		if binding.AutoSync() {
			t.Error("Expected AutoSync to be false by default")
		}
	})

	t.Run("Pipeline", func(t *testing.T) {
		pipeline := binding.Pipeline()
		if pipeline == nil {
			t.Error("Expected non-nil pipeline")
		}
	})
}

func TestBindingAutoSync(t *testing.T) {
	factory := flume.New[bindingTestData]()

	proc1ID := factory.Identity("proc1", "Processor 1")
	proc2ID := factory.Identity("proc2", "Processor 2")

	factory.Add(pipz.Transform(proc1ID, func(_ context.Context, d bindingTestData) bindingTestData {
		d.Value = "v1"
		return d
	}))
	factory.Add(pipz.Transform(proc2ID, func(_ context.Context, d bindingTestData) bindingTestData {
		d.Value = "v2"
		return d
	}))

	schema1 := flume.Schema{
		Version: "1",
		Node:    flume.Node{Ref: "proc1"},
	}
	if err := factory.SetSchema("auto-sync-schema", schema1); err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	bindingID := factory.Identity("auto-sync-test", "Test binding for auto-sync")
	binding, err := factory.Bind(bindingID, "auto-sync-schema", flume.WithAutoSync[bindingTestData]())
	if err != nil {
		t.Fatalf("Failed to create binding: %v", err)
	}

	if !binding.AutoSync() {
		t.Error("Expected AutoSync to be true")
	}

	t.Run("initial_version", func(t *testing.T) {
		result, err := binding.Process(context.Background(), bindingTestData{})
		if err != nil {
			t.Fatalf("Process failed: %v", err)
		}
		if result.Value != "v1" {
			t.Errorf("Expected 'v1', got '%s'", result.Value)
		}
	})

	t.Run("auto_sync_on_schema_update", func(t *testing.T) {
		schema2 := flume.Schema{
			Version: "2",
			Node:    flume.Node{Ref: "proc2"},
		}
		if err := factory.SetSchema("auto-sync-schema", schema2); err != nil {
			t.Fatalf("Failed to update schema: %v", err)
		}

		result, err := binding.Process(context.Background(), bindingTestData{})
		if err != nil {
			t.Fatalf("Process failed: %v", err)
		}
		if result.Value != "v2" {
			t.Errorf("Expected 'v2' after auto-sync, got '%s'", result.Value)
		}
	})
}

func TestBindingNoAutoSync(t *testing.T) {
	factory := flume.New[bindingTestData]()

	proc1ID := factory.Identity("proc1", "Processor 1")
	proc2ID := factory.Identity("proc2", "Processor 2")

	factory.Add(pipz.Transform(proc1ID, func(_ context.Context, d bindingTestData) bindingTestData {
		d.Value = "v1"
		return d
	}))
	factory.Add(pipz.Transform(proc2ID, func(_ context.Context, d bindingTestData) bindingTestData {
		d.Value = "v2"
		return d
	}))

	schema1 := flume.Schema{
		Version: "1",
		Node:    flume.Node{Ref: "proc1"},
	}
	if err := factory.SetSchema("no-sync-schema", schema1); err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	bindingID := factory.Identity("no-sync-test", "Test binding without auto-sync")
	binding, err := factory.Bind(bindingID, "no-sync-schema") // No WithAutoSync
	if err != nil {
		t.Fatalf("Failed to create binding: %v", err)
	}

	// Update schema
	schema2 := flume.Schema{
		Version: "2",
		Node:    flume.Node{Ref: "proc2"},
	}
	if err := factory.SetSchema("no-sync-schema", schema2); err != nil {
		t.Fatalf("Failed to update schema: %v", err)
	}

	// Binding should still use original schema
	result, err := binding.Process(context.Background(), bindingTestData{})
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	if result.Value != "v1" {
		t.Errorf("Expected 'v1' (no auto-sync), got '%s'", result.Value)
	}
}

func TestBindingSchemaNotFound(t *testing.T) {
	factory := flume.New[bindingTestData]()

	bindingID := factory.Identity("missing-schema-test", "Test binding for missing schema")
	_, err := factory.Bind(bindingID, "nonexistent-schema")
	if err == nil {
		t.Error("Expected error for nonexistent schema, got nil")
	}
}

func TestBindingIdempotent(t *testing.T) {
	factory := flume.New[bindingTestData]()

	procID := factory.Identity("proc", "Test processor")
	factory.Add(pipz.Transform(procID, func(_ context.Context, d bindingTestData) bindingTestData {
		return d
	}))

	schema := flume.Schema{
		Node: flume.Node{Ref: "proc"},
	}
	if err := factory.SetSchema("idempotent-schema", schema); err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	bindingID := factory.Identity("idempotent-test", "Test binding for idempotency")

	binding1, err := factory.Bind(bindingID, "idempotent-schema")
	if err != nil {
		t.Fatalf("First Bind failed: %v", err)
	}

	// Second Bind with same identity should return same binding
	binding2, err := factory.Bind(bindingID, "idempotent-schema")
	if err != nil {
		t.Fatalf("Second Bind failed: %v", err)
	}

	if binding1 != binding2 {
		t.Error("Expected Bind to be idempotent and return same binding")
	}
}

func TestBindingGet(t *testing.T) {
	factory := flume.New[bindingTestData]()

	procID := factory.Identity("proc", "Test processor")
	factory.Add(pipz.Transform(procID, func(_ context.Context, d bindingTestData) bindingTestData {
		return d
	}))

	t.Run("get_nonexistent", func(t *testing.T) {
		unknownID := factory.Identity("unknown", "Unknown binding")
		binding := factory.Get(unknownID)
		if binding != nil {
			t.Error("Expected nil for nonexistent binding")
		}
	})

	t.Run("get_existing", func(t *testing.T) {
		schema := flume.Schema{
			Node: flume.Node{Ref: "proc"},
		}
		if err := factory.SetSchema("get-schema", schema); err != nil {
			t.Fatalf("Failed to set schema: %v", err)
		}

		bindingID := factory.Identity("existing", "Existing binding")
		created, err := factory.Bind(bindingID, "get-schema")
		if err != nil {
			t.Fatalf("Bind failed: %v", err)
		}

		retrieved := factory.Get(bindingID)
		if retrieved != created {
			t.Error("Expected Get to return the same binding")
		}
	})
}

func TestBindingListBindings(t *testing.T) {
	factory := flume.New[bindingTestData]()

	procID := factory.Identity("proc", "Test processor")
	factory.Add(pipz.Transform(procID, func(_ context.Context, d bindingTestData) bindingTestData {
		return d
	}))

	schema := flume.Schema{
		Node: flume.Node{Ref: "proc"},
	}
	if err := factory.SetSchema("list-schema", schema); err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	// Create multiple bindings
	names := []string{"alpha", "beta", "gamma"}
	for _, name := range names {
		id := factory.Identity(name, "Binding "+name)
		if _, err := factory.Bind(id, "list-schema"); err != nil {
			t.Fatalf("Bind failed for %s: %v", name, err)
		}
	}

	listed := factory.ListBindings()
	if len(listed) != len(names) {
		t.Errorf("Expected %d bindings, got %d", len(names), len(listed))
	}

	// Check all names are present (order may vary)
	nameSet := make(map[string]bool)
	for _, n := range listed {
		nameSet[n] = true
	}
	for _, name := range names {
		if !nameSet[name] {
			t.Errorf("Expected binding '%s' in list", name)
		}
	}
}

func TestBindingConcurrentAccess(t *testing.T) {
	factory := flume.New[bindingTestData]()

	procID := factory.Identity("proc", "Test processor")
	factory.Add(pipz.Transform(procID, func(_ context.Context, d bindingTestData) bindingTestData {
		d.Counter++
		return d
	}))

	schema := flume.Schema{
		Node: flume.Node{Ref: "proc"},
	}
	if err := factory.SetSchema("concurrent-schema", schema); err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	bindingID := factory.Identity("concurrent-test", "Test binding for concurrency")
	binding, err := factory.Bind(bindingID, "concurrent-schema", flume.WithAutoSync[bindingTestData]())
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}

	// Run concurrent operations
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// Concurrent Process calls
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := binding.Process(context.Background(), bindingTestData{})
			if err != nil {
				errChan <- err
			}
		}()
	}

	// Concurrent reads
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = binding.Identity()
			_ = binding.SchemaID()
			_ = binding.AutoSync()
			_ = binding.Pipeline()
		}()
	}

	// Some schema updates (triggers auto-sync rebuilds)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			s := flume.Schema{
				Version: string(rune('A' + v)),
				Node:    flume.Node{Ref: "proc"},
			}
			if err := factory.SetSchema("concurrent-schema", s); err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrent operation error: %v", err)
	}
}

func TestSchemaRegistry(t *testing.T) {
	factory := flume.New[bindingTestData]()

	procID := factory.Identity("proc", "Test processor")
	factory.Add(pipz.Transform(procID, func(_ context.Context, d bindingTestData) bindingTestData {
		return d
	}))

	t.Run("SetSchema", func(t *testing.T) {
		schema := flume.Schema{
			Version: "1.0",
			Node:    flume.Node{Ref: "proc"},
		}
		if err := factory.SetSchema("test-schema", schema); err != nil {
			t.Fatalf("SetSchema failed: %v", err)
		}
	})

	t.Run("GetSchema", func(t *testing.T) {
		schema, ok := factory.GetSchema("test-schema")
		if !ok {
			t.Fatal("Expected schema to exist")
		}
		if schema.Version != "1.0" {
			t.Errorf("Expected version '1.0', got '%s'", schema.Version)
		}
	})

	t.Run("HasSchema", func(t *testing.T) {
		if !factory.HasSchema("test-schema") {
			t.Error("Expected HasSchema to return true")
		}
		if factory.HasSchema("nonexistent") {
			t.Error("Expected HasSchema to return false for nonexistent")
		}
	})

	t.Run("ListSchemas", func(t *testing.T) {
		schemas := factory.ListSchemas()
		found := false
		for _, id := range schemas {
			if id == "test-schema" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected 'test-schema' in ListSchemas")
		}
	})

	t.Run("RemoveSchema", func(t *testing.T) {
		if !factory.RemoveSchema("test-schema") {
			t.Error("Expected RemoveSchema to return true")
		}
		if factory.HasSchema("test-schema") {
			t.Error("Expected schema to be removed")
		}
		if factory.RemoveSchema("test-schema") {
			t.Error("Expected RemoveSchema to return false for already removed")
		}
	})
}

func TestSetSchemaValidation(t *testing.T) {
	factory := flume.New[bindingTestData]()

	// Try to set invalid schema (references non-existent processor)
	invalidSchema := flume.Schema{
		Node: flume.Node{Ref: "nonexistent"},
	}

	err := factory.SetSchema("invalid", invalidSchema)
	if err == nil {
		t.Error("Expected error for invalid schema, got nil")
	}
}

func TestMultipleBindingsAutoSync(t *testing.T) {
	factory := flume.New[bindingTestData]()

	proc1ID := factory.Identity("proc1", "Processor 1")
	proc2ID := factory.Identity("proc2", "Processor 2")

	factory.Add(pipz.Transform(proc1ID, func(_ context.Context, d bindingTestData) bindingTestData {
		d.Value = "v1"
		return d
	}))
	factory.Add(pipz.Transform(proc2ID, func(_ context.Context, d bindingTestData) bindingTestData {
		d.Value = "v2"
		return d
	}))

	schema1 := flume.Schema{
		Version: "1",
		Node:    flume.Node{Ref: "proc1"},
	}
	if err := factory.SetSchema("shared-schema", schema1); err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	// Create multiple bindings with auto-sync
	binding1ID := factory.Identity("multi-1", "Multi binding 1")
	binding2ID := factory.Identity("multi-2", "Multi binding 2")

	binding1, err := factory.Bind(binding1ID, "shared-schema", flume.WithAutoSync[bindingTestData]())
	if err != nil {
		t.Fatalf("Failed to create binding1: %v", err)
	}

	binding2, err := factory.Bind(binding2ID, "shared-schema", flume.WithAutoSync[bindingTestData]())
	if err != nil {
		t.Fatalf("Failed to create binding2: %v", err)
	}

	// Both should use v1
	r1, err := binding1.Process(context.Background(), bindingTestData{})
	if err != nil {
		t.Fatalf("binding1 process failed: %v", err)
	}
	r2, err := binding2.Process(context.Background(), bindingTestData{})
	if err != nil {
		t.Fatalf("binding2 process failed: %v", err)
	}
	if r1.Value != "v1" || r2.Value != "v1" {
		t.Errorf("Expected both bindings to use 'v1'")
	}

	// Update schema
	schema2 := flume.Schema{
		Version: "2",
		Node:    flume.Node{Ref: "proc2"},
	}
	if err := factory.SetSchema("shared-schema", schema2); err != nil {
		t.Fatalf("Failed to update schema: %v", err)
	}

	// Both should auto-sync to v2
	r1, err = binding1.Process(context.Background(), bindingTestData{})
	if err != nil {
		t.Fatalf("binding1 process after update failed: %v", err)
	}
	r2, err = binding2.Process(context.Background(), bindingTestData{})
	if err != nil {
		t.Fatalf("binding2 process after update failed: %v", err)
	}
	if r1.Value != "v2" || r2.Value != "v2" {
		t.Errorf("Expected both bindings to auto-sync to 'v2', got '%s' and '%s'", r1.Value, r2.Value)
	}
}
