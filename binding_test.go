package flume_test

import (
	"context"
	"errors"
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

	// Create binding
	bindingID := factory.Identity("test-binding", "Test binding")
	schema := flume.Schema{
		Version: "1.0",
		Node:    flume.Node{Ref: "process"},
	}

	binding, err := factory.Bind(bindingID, schema)
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

	bindingID := factory.Identity("accessor-test", "Test binding for accessors")
	schema := flume.Schema{
		Version: "2.5",
		Node:    flume.Node{Ref: "noop"},
	}

	binding, err := factory.Bind(bindingID, schema)
	if err != nil {
		t.Fatalf("Failed to create binding: %v", err)
	}

	t.Run("Version", func(t *testing.T) {
		if v := binding.Version(); v != "2.5" {
			t.Errorf("Expected version '2.5', got '%s'", v)
		}
	})

	t.Run("Schema", func(t *testing.T) {
		s := binding.Schema()
		if s.Version != "2.5" {
			t.Errorf("Expected schema version '2.5', got '%s'", s.Version)
		}
	})

	t.Run("Identity", func(t *testing.T) {
		id := binding.Identity()
		if id.Name() != "accessor-test" {
			t.Errorf("Expected identity name 'accessor-test', got '%s'", id.Name())
		}
	})

	t.Run("HistoryCap", func(t *testing.T) {
		if hCap := binding.HistoryCap(); hCap != flume.DefaultHistoryCap {
			t.Errorf("Expected history cap %d, got %d", flume.DefaultHistoryCap, hCap)
		}
	})

	t.Run("History_empty_initially", func(t *testing.T) {
		history := binding.History()
		if len(history) != 0 {
			t.Errorf("Expected empty history, got %d entries", len(history))
		}
	})

	t.Run("Pipeline", func(t *testing.T) {
		pipeline := binding.Pipeline()
		if pipeline == nil {
			t.Error("Expected non-nil pipeline")
		}
	})
}

func TestBindingUpdate(t *testing.T) {
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

	bindingID := factory.Identity("update-test", "Test binding for updates")
	schema1 := flume.Schema{
		Version: "1",
		Node:    flume.Node{Ref: "proc1"},
	}

	binding, err := factory.Bind(bindingID, schema1)
	if err != nil {
		t.Fatalf("Failed to create binding: %v", err)
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

	t.Run("update_with_explicit_version", func(t *testing.T) {
		schema2 := flume.Schema{
			Version: "2",
			Node:    flume.Node{Ref: "proc2"},
		}

		if err := binding.Update(schema2); err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		if v := binding.Version(); v != "2" {
			t.Errorf("Expected version '2', got '%s'", v)
		}

		result, err := binding.Process(context.Background(), bindingTestData{})
		if err != nil {
			t.Fatalf("Process failed: %v", err)
		}
		if result.Value != "v2" {
			t.Errorf("Expected 'v2', got '%s'", result.Value)
		}

		// Check history
		history := binding.History()
		if len(history) != 1 {
			t.Errorf("Expected 1 history entry, got %d", len(history))
		}
		if history[0].Version != "1" {
			t.Errorf("Expected history version '1', got '%s'", history[0].Version)
		}
	})

	t.Run("update_with_auto_version_numeric", func(t *testing.T) {
		schema3 := flume.Schema{
			Node: flume.Node{Ref: "proc1"},
		}

		if err := binding.Update(schema3); err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		// Should auto-increment from "2" to "3"
		if v := binding.Version(); v != "3" {
			t.Errorf("Expected auto-incremented version '3', got '%s'", v)
		}
	})

	t.Run("update_with_auto_version_non_numeric", func(t *testing.T) {
		// First set a non-numeric version
		schemaAlpha := flume.Schema{
			Version: "alpha",
			Node:    flume.Node{Ref: "proc2"},
		}
		if err := binding.Update(schemaAlpha); err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		// Now update without version - should append ".1"
		schemaNext := flume.Schema{
			Node: flume.Node{Ref: "proc1"},
		}
		if err := binding.Update(schemaNext); err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		if v := binding.Version(); v != "alpha.1" {
			t.Errorf("Expected version 'alpha.1', got '%s'", v)
		}
	})
}

func TestBindingUpdateError(t *testing.T) {
	factory := flume.New[bindingTestData]()

	procID := factory.Identity("valid", "Valid processor")
	factory.Add(pipz.Transform(procID, func(_ context.Context, d bindingTestData) bindingTestData {
		return d
	}))

	bindingID := factory.Identity("error-test", "Test binding for errors")
	schema := flume.Schema{
		Node: flume.Node{Ref: "valid"},
	}

	binding, err := factory.Bind(bindingID, schema)
	if err != nil {
		t.Fatalf("Failed to create binding: %v", err)
	}

	// Try to update with invalid schema (references non-existent processor)
	invalidSchema := flume.Schema{
		Node: flume.Node{Ref: "nonexistent"},
	}

	err = binding.Update(invalidSchema)
	if err == nil {
		t.Error("Expected error for invalid schema, got nil")
	}

	// Verify binding still works with original schema
	result, err := binding.Process(context.Background(), bindingTestData{Value: "test"})
	if err != nil {
		t.Fatalf("Process failed after invalid update: %v", err)
	}
	if result.Value != "test" {
		t.Errorf("Expected 'test', got '%s'", result.Value)
	}
}

func TestBindingRollback(t *testing.T) {
	factory := flume.New[bindingTestData]()

	proc1ID := factory.Identity("v1-proc", "Version 1 processor")
	proc2ID := factory.Identity("v2-proc", "Version 2 processor")

	factory.Add(pipz.Transform(proc1ID, func(_ context.Context, d bindingTestData) bindingTestData {
		d.Value = "v1"
		return d
	}))
	factory.Add(pipz.Transform(proc2ID, func(_ context.Context, d bindingTestData) bindingTestData {
		d.Value = "v2"
		return d
	}))

	bindingID := factory.Identity("rollback-test", "Test binding for rollback")
	schema1 := flume.Schema{
		Version: "1",
		Node:    flume.Node{Ref: "v1-proc"},
	}

	binding, err := factory.Bind(bindingID, schema1)
	if err != nil {
		t.Fatalf("Failed to create binding: %v", err)
	}

	t.Run("rollback_with_no_history", func(t *testing.T) {
		err := binding.Rollback()
		if !errors.Is(err, flume.ErrNoHistory) {
			t.Errorf("Expected ErrNoHistory, got %v", err)
		}
	})

	// Update to v2
	schema2 := flume.Schema{
		Version: "2",
		Node:    flume.Node{Ref: "v2-proc"},
	}
	if err := binding.Update(schema2); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	t.Run("rollback_to_previous", func(t *testing.T) {
		if v := binding.Version(); v != "2" {
			t.Errorf("Expected version '2' before rollback, got '%s'", v)
		}

		err := binding.Rollback()
		if err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		if v := binding.Version(); v != "1" {
			t.Errorf("Expected version '1' after rollback, got '%s'", v)
		}

		result, err := binding.Process(context.Background(), bindingTestData{})
		if err != nil {
			t.Fatalf("Process failed: %v", err)
		}
		if result.Value != "v1" {
			t.Errorf("Expected 'v1' after rollback, got '%s'", result.Value)
		}

		// History should now be empty
		if len(binding.History()) != 0 {
			t.Errorf("Expected empty history after rollback, got %d", len(binding.History()))
		}
	})
}

func TestBindingRollbackTo(t *testing.T) {
	factory := flume.New[bindingTestData]()

	for _, v := range []string{"v1", "v2", "v3", "v4"} {
		id := factory.Identity(v+"-proc", "Processor for "+v)
		val := v
		factory.Add(pipz.Transform(id, func(_ context.Context, d bindingTestData) bindingTestData {
			d.Value = val
			return d
		}))
	}

	bindingID := factory.Identity("rollback-to-test", "Test binding for rollback-to")
	schema := flume.Schema{
		Version: "v1",
		Node:    flume.Node{Ref: "v1-proc"},
	}

	binding, err := factory.Bind(bindingID, schema)
	if err != nil {
		t.Fatalf("Failed to create binding: %v", err)
	}

	// Create versions v2, v3, v4
	for _, v := range []string{"v2", "v3", "v4"} {
		s := flume.Schema{
			Version: v,
			Node:    flume.Node{Ref: v + "-proc"},
		}
		if err := binding.Update(s); err != nil {
			t.Fatalf("Update to %s failed: %v", v, err)
		}
	}

	t.Run("rollback_to_specific_version", func(t *testing.T) {
		// Current is v4, history is [v1, v2, v3]
		if v := binding.Version(); v != "v4" {
			t.Errorf("Expected version 'v4', got '%s'", v)
		}

		err := binding.RollbackTo("v2")
		if err != nil {
			t.Fatalf("RollbackTo failed: %v", err)
		}

		if v := binding.Version(); v != "v2" {
			t.Errorf("Expected version 'v2' after rollback, got '%s'", v)
		}

		// History should be [v1] (v2, v3 are discarded, v4 was current)
		history := binding.History()
		if len(history) != 1 {
			t.Errorf("Expected 1 history entry, got %d", len(history))
		}
		if history[0].Version != "v1" {
			t.Errorf("Expected history[0] to be 'v1', got '%s'", history[0].Version)
		}

		result, err := binding.Process(context.Background(), bindingTestData{})
		if err != nil {
			t.Fatalf("Process failed: %v", err)
		}
		if result.Value != "v2" {
			t.Errorf("Expected 'v2', got '%s'", result.Value)
		}
	})

	t.Run("rollback_to_nonexistent_version", func(t *testing.T) {
		err := binding.RollbackTo("nonexistent")
		if !errors.Is(err, flume.ErrVersionNotFound) {
			t.Errorf("Expected ErrVersionNotFound, got %v", err)
		}
	})
}

func TestBindingHistoryCap(t *testing.T) {
	factory := flume.New[bindingTestData]()

	procID := factory.Identity("proc", "Test processor")
	factory.Add(pipz.Transform(procID, func(_ context.Context, d bindingTestData) bindingTestData {
		return d
	}))

	bindingID := factory.Identity("history-cap-test", "Test binding for history cap")
	schema := flume.Schema{
		Version: "1",
		Node:    flume.Node{Ref: "proc"},
	}

	binding, err := factory.Bind(bindingID, schema)
	if err != nil {
		t.Fatalf("Failed to create binding: %v", err)
	}

	t.Run("history_trimmed_on_update", func(t *testing.T) {
		// Set small history cap
		binding.SetHistoryCap(3)

		// Create 5 updates (so history would be 5 entries without cap)
		for i := 2; i <= 6; i++ {
			s := flume.Schema{
				Version: string(rune('0' + i)),
				Node:    flume.Node{Ref: "proc"},
			}
			if err := binding.Update(s); err != nil {
				t.Fatalf("Update failed: %v", err)
			}
		}

		// History should be trimmed to 3 entries
		history := binding.History()
		if len(history) != 3 {
			t.Errorf("Expected 3 history entries, got %d", len(history))
		}

		// Oldest entries should be trimmed (1, 2 removed; 3, 4, 5 remain)
		if history[0].Version != "3" {
			t.Errorf("Expected oldest history to be '3', got '%s'", history[0].Version)
		}
	})

	t.Run("set_history_cap_trims_existing", func(t *testing.T) {
		// Currently have 3 entries, set cap to 1
		binding.SetHistoryCap(1)

		history := binding.History()
		if len(history) != 1 {
			t.Errorf("Expected 1 history entry after cap reduction, got %d", len(history))
		}

		// Should keep the most recent
		if history[0].Version != "5" {
			t.Errorf("Expected remaining entry to be '5', got '%s'", history[0].Version)
		}
	})

	t.Run("set_history_cap_zero_ignored", func(t *testing.T) {
		currentCap := binding.HistoryCap()
		binding.SetHistoryCap(0)

		if hCap := binding.HistoryCap(); hCap != currentCap {
			t.Errorf("Expected cap unchanged at %d, got %d", currentCap, hCap)
		}
	})

	t.Run("set_history_cap_negative_ignored", func(t *testing.T) {
		currentCap := binding.HistoryCap()
		binding.SetHistoryCap(-5)

		if hCap := binding.HistoryCap(); hCap != currentCap {
			t.Errorf("Expected cap unchanged at %d, got %d", currentCap, hCap)
		}
	})
}

func TestBindingIdempotent(t *testing.T) {
	factory := flume.New[bindingTestData]()

	procID := factory.Identity("proc", "Test processor")
	factory.Add(pipz.Transform(procID, func(_ context.Context, d bindingTestData) bindingTestData {
		return d
	}))

	bindingID := factory.Identity("idempotent-test", "Test binding for idempotency")
	schema := flume.Schema{
		Node: flume.Node{Ref: "proc"},
	}

	binding1, err := factory.Bind(bindingID, schema)
	if err != nil {
		t.Fatalf("First Bind failed: %v", err)
	}

	// Second Bind with same identity should return same binding
	binding2, err := factory.Bind(bindingID, schema)
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
		bindingID := factory.Identity("existing", "Existing binding")
		schema := flume.Schema{
			Node: flume.Node{Ref: "proc"},
		}

		created, err := factory.Bind(bindingID, schema)
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

	// Create multiple bindings
	names := []string{"alpha", "beta", "gamma"}
	for _, name := range names {
		id := factory.Identity(name, "Binding "+name)
		if _, err := factory.Bind(id, schema); err != nil {
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

	bindingID := factory.Identity("concurrent-test", "Test binding for concurrency")
	schema := flume.Schema{
		Node: flume.Node{Ref: "proc"},
	}

	binding, err := factory.Bind(bindingID, schema)
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
			_ = binding.Version()
			_ = binding.Schema()
			_ = binding.History()
			_ = binding.HistoryCap()
		}()
	}

	// Some updates
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			s := flume.Schema{
				Version: string(rune('A' + v)),
				Node:    flume.Node{Ref: "proc"},
			}
			if err := binding.Update(s); err != nil {
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

func TestBindingHistoryInfo(t *testing.T) {
	factory := flume.New[bindingTestData]()

	procID := factory.Identity("proc", "Test processor")
	factory.Add(pipz.Transform(procID, func(_ context.Context, d bindingTestData) bindingTestData {
		return d
	}))

	bindingID := factory.Identity("history-info-test", "Test binding for history info")
	schema := flume.Schema{
		Version: "initial",
		Node:    flume.Node{Ref: "proc"},
	}

	binding, err := factory.Bind(bindingID, schema)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}

	// Update a few times
	if err := binding.Update(flume.Schema{Version: "second", Node: flume.Node{Ref: "proc"}}); err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if err := binding.Update(flume.Schema{Version: "third", Node: flume.Node{Ref: "proc"}}); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	history := binding.History()
	if len(history) != 2 {
		t.Fatalf("Expected 2 history entries, got %d", len(history))
	}

	// Check versions are in order (oldest first)
	if history[0].Version != "initial" {
		t.Errorf("Expected history[0].Version to be 'initial', got '%s'", history[0].Version)
	}
	if history[1].Version != "second" {
		t.Errorf("Expected history[1].Version to be 'second', got '%s'", history[1].Version)
	}

	// Check BuiltAt times are set and in order
	if history[0].BuiltAt.IsZero() {
		t.Error("Expected history[0].BuiltAt to be set")
	}
	if history[1].BuiltAt.IsZero() {
		t.Error("Expected history[1].BuiltAt to be set")
	}
	if history[1].BuiltAt.Before(history[0].BuiltAt) {
		t.Error("Expected history entries to be in chronological order")
	}
}
