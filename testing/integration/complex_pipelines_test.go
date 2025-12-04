package integration

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zoobzio/flume"
	flumetesting "github.com/zoobzio/flume/testing"
	"github.com/zoobzio/pipz"
)

func TestComplexPipeline_OrderProcessing(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	var (
		validated  int64
		enriched   int64
		processed  int64
		discounted int64
	)

	// Register processors
	factory.Add(
		pipz.Apply("validate", func(_ context.Context, d flumetesting.TestData) (flumetesting.TestData, error) {
			atomic.AddInt64(&validated, 1)
			if d.ID <= 0 {
				return d, errors.New("invalid ID")
			}
			return d, nil
		}),
		pipz.Transform("enrich", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&enriched, 1)
			if d.Metadata == nil {
				d.Metadata = make(map[string]any)
			}
			d.Metadata["enriched_at"] = time.Now().Unix()
			return d
		}),
		pipz.Transform("process", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&processed, 1)
			d.Name = strings.ToUpper(d.Name)
			return d
		}),
		pipz.Transform("apply-discount", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&discounted, 1)
			d.Value *= 0.9 // 10% discount
			return d
		}),
	)

	// Register predicates
	factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
		Name: "is-premium",
		Predicate: func(_ context.Context, d flumetesting.TestData) bool {
			return d.Value > 100
		},
	})

	// Complex schema: validate -> enrich -> (if premium: discount) -> process
	schema := `
type: sequence
children:
  - ref: validate
  - ref: enrich
  - type: filter
    predicate: is-premium
    then:
      ref: apply-discount
  - ref: process
`

	pipeline, err := factory.BuildFromYAML(schema)
	if err != nil {
		t.Fatalf("failed to build pipeline: %v", err)
	}

	ctx := context.Background()

	// Test premium order (gets discount)
	premiumOrder := flumetesting.TestData{
		ID:    1,
		Name:  "premium order",
		Value: 200.0,
	}

	result, err := pipeline.Process(ctx, premiumOrder)
	if err != nil {
		t.Fatalf("premium order failed: %v", err)
	}

	if result.Value != 180.0 { // 200 * 0.9
		t.Errorf("expected Value=180.0, got %f", result.Value)
	}
	if result.Name != "PREMIUM ORDER" {
		t.Errorf("expected uppercase name, got %q", result.Name)
	}

	// Test regular order (no discount)
	regularOrder := flumetesting.TestData{
		ID:    2,
		Name:  "regular order",
		Value: 50.0,
	}

	result, err = pipeline.Process(ctx, regularOrder)
	if err != nil {
		t.Fatalf("regular order failed: %v", err)
	}

	if result.Value != 50.0 { // No discount applied
		t.Errorf("expected Value=50.0, got %f", result.Value)
	}

	// Verify call counts
	if atomic.LoadInt64(&validated) != 2 {
		t.Errorf("expected 2 validations, got %d", validated)
	}
	if atomic.LoadInt64(&enriched) != 2 {
		t.Errorf("expected 2 enrichments, got %d", enriched)
	}
	if atomic.LoadInt64(&discounted) != 1 { // Only premium
		t.Errorf("expected 1 discount, got %d", discounted)
	}
	if atomic.LoadInt64(&processed) != 2 {
		t.Errorf("expected 2 processes, got %d", processed)
	}
}

func TestComplexPipeline_MultiRouting(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	var (
		lowCalls    int64
		mediumCalls int64
		highCalls   int64
		defaultCall int64
	)

	factory.Add(
		pipz.Transform("handle-low", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&lowCalls, 1)
			d.Tags = append(d.Tags, "low-priority")
			return d
		}),
		pipz.Transform("handle-medium", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&mediumCalls, 1)
			d.Tags = append(d.Tags, "medium-priority")
			return d
		}),
		pipz.Transform("handle-high", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&highCalls, 1)
			d.Tags = append(d.Tags, "high-priority")
			return d
		}),
		pipz.Transform("handle-default", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&defaultCall, 1)
			d.Tags = append(d.Tags, "default")
			return d
		}),
	)

	factory.AddCondition(flume.Condition[flumetesting.TestData]{
		Name: "get-priority",
		Condition: func(_ context.Context, d flumetesting.TestData) string {
			switch {
			case d.Value < 10:
				return "low"
			case d.Value < 50:
				return "medium"
			case d.Value < 100:
				return "high"
			default:
				// For values >= 100, explicitly return "default" to trigger the default route
				return "default"
			}
		},
	})

	schema := `
type: switch
condition: get-priority
routes:
  low:
    ref: handle-low
  medium:
    ref: handle-medium
  high:
    ref: handle-high
default:
  ref: handle-default
`

	pipeline, err := factory.BuildFromYAML(schema)
	if err != nil {
		t.Fatalf("failed to build pipeline: %v", err)
	}

	ctx := context.Background()

	tests := []struct {
		value       float64
		expectedTag string
	}{
		{5.0, "low-priority"},
		{25.0, "medium-priority"},
		{75.0, "high-priority"},
		{150.0, "default"},
	}

	for _, tt := range tests {
		input := flumetesting.TestData{ID: 1, Value: tt.value, Tags: []string{}}
		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Fatalf("processing failed for value %f: %v", tt.value, err)
		}

		found := false
		for _, tag := range result.Tags {
			if tag == tt.expectedTag {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected tag %q for value %f, got tags: %v", tt.expectedTag, tt.value, result.Tags)
		}
	}
}

func TestComplexPipeline_ResilienceStack(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	var attempts int64

	factory.Add(
		pipz.Apply("flaky-service", func(_ context.Context, d flumetesting.TestData) (flumetesting.TestData, error) {
			count := atomic.AddInt64(&attempts, 1)
			// Fail first 2 attempts
			if count <= 2 {
				return d, errors.New("temporary failure")
			}
			d.Name = "processed"
			return d, nil
		}),
		pipz.Transform("fallback-handler", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Name = "fallback"
			return d
		}),
	)

	// Resilience stack: timeout -> circuit-breaker -> retry -> service
	schema := `
type: timeout
duration: "5s"
child:
  type: circuit-breaker
  failure_threshold: 10
  recovery_timeout: "60s"
  child:
    type: retry
    attempts: 3
    child:
      ref: flaky-service
`

	pipeline, err := factory.BuildFromYAML(schema)
	if err != nil {
		t.Fatalf("failed to build pipeline: %v", err)
	}

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1}

	result, err := pipeline.Process(ctx, input)
	if err != nil {
		t.Fatalf("resilient pipeline failed: %v", err)
	}

	if result.Name != "processed" {
		t.Errorf("expected 'processed', got %q", result.Name)
	}

	// Verify retries happened
	if atomic.LoadInt64(&attempts) != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestComplexPipeline_ConcurrentProcessing(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	var (
		step1Calls int64
		step2Calls int64
	)

	factory.Add(
		pipz.Transform("concurrent-step1", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&step1Calls, 1)
			time.Sleep(10 * time.Millisecond) // Simulate work
			d.Value++
			return d
		}),
		pipz.Transform("concurrent-step2", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			atomic.AddInt64(&step2Calls, 1)
			time.Sleep(10 * time.Millisecond) // Simulate work
			d.Value += 2
			return d
		}),
		pipz.Transform("finalize", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Name = "finalized"
			return d
		}),
	)

	schema := `
type: sequence
children:
  - type: concurrent
    children:
      - ref: concurrent-step1
      - ref: concurrent-step2
  - ref: finalize
`

	pipeline, err := factory.BuildFromYAML(schema)
	if err != nil {
		t.Fatalf("failed to build pipeline: %v", err)
	}

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Value: 0}

	start := time.Now()
	result, err := pipeline.Process(ctx, input)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("concurrent pipeline failed: %v", err)
	}

	// Concurrent should complete in ~10ms, not ~20ms
	if elapsed > 50*time.Millisecond {
		t.Errorf("expected concurrent execution, took %v", elapsed)
	}

	if result.Name != "finalized" {
		t.Errorf("expected 'finalized', got %q", result.Name)
	}

	// Both steps should be called
	if atomic.LoadInt64(&step1Calls) != 1 {
		t.Errorf("expected step1 called once, got %d", step1Calls)
	}
	if atomic.LoadInt64(&step2Calls) != 1 {
		t.Errorf("expected step2 called once, got %d", step2Calls)
	}
}

func TestComplexPipeline_ChannelFanOut(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	factory.Add(
		pipz.Transform("process", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Value *= 2
			return d
		}),
	)

	factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
		Name: "is-high-value",
		Predicate: func(_ context.Context, d flumetesting.TestData) bool {
			return d.Value > 100
		},
	})

	highValueChan := make(chan flumetesting.TestData, 10)
	lowValueChan := make(chan flumetesting.TestData, 10)

	factory.AddChannel("high-value-stream", highValueChan)
	factory.AddChannel("low-value-stream", lowValueChan)

	// Fan-out based on value
	schema := `
type: sequence
children:
  - ref: process
  - type: filter
    predicate: is-high-value
    then:
      stream: high-value-stream
    else:
      stream: low-value-stream
`

	pipeline, err := factory.BuildFromYAML(schema)
	if err != nil {
		t.Fatalf("failed to build pipeline: %v", err)
	}

	ctx := context.Background()

	// Process high-value item
	highInput := flumetesting.TestData{ID: 1, Value: 100}
	_, err = pipeline.Process(ctx, highInput)
	if err != nil {
		t.Fatalf("high-value processing failed: %v", err)
	}

	// Process low-value item
	lowInput := flumetesting.TestData{ID: 2, Value: 25}
	_, err = pipeline.Process(ctx, lowInput)
	if err != nil {
		t.Fatalf("low-value processing failed: %v", err)
	}

	// Check high-value channel
	select {
	case received := <-highValueChan:
		if received.Value != 200 { // 100 * 2
			t.Errorf("expected high-value channel to receive 200, got %f", received.Value)
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for high-value channel")
	}

	// Check low-value channel
	select {
	case received := <-lowValueChan:
		if received.Value != 50 { // 25 * 2
			t.Errorf("expected low-value channel to receive 50, got %f", received.Value)
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for low-value channel")
	}
}

func TestComplexPipeline_RaceBehavior(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	factory.Add(
		pipz.Apply("slow-service", func(ctx context.Context, d flumetesting.TestData) (flumetesting.TestData, error) {
			select {
			case <-time.After(100 * time.Millisecond):
				d.Name = "slow"
				return d, nil
			case <-ctx.Done():
				return d, ctx.Err()
			}
		}),
		pipz.Transform("fast-service", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Name = "fast"
			return d
		}),
	)

	schema := `
type: race
children:
  - ref: slow-service
  - ref: fast-service
`

	pipeline, err := factory.BuildFromYAML(schema)
	if err != nil {
		t.Fatalf("failed to build pipeline: %v", err)
	}

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1}

	start := time.Now()
	result, err := pipeline.Process(ctx, input)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("race pipeline failed: %v", err)
	}

	// Fast service should win
	if result.Name != "fast" {
		t.Errorf("expected 'fast' to win race, got %q", result.Name)
	}

	// Should complete quickly
	if elapsed > 50*time.Millisecond {
		t.Errorf("expected quick completion, took %v", elapsed)
	}
}

func TestComplexPipeline_DeeplyNested(t *testing.T) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors that track call depth
	factory.Add(
		pipz.Transform("level-1", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Tags = append(d.Tags, "L1")
			return d
		}),
		pipz.Transform("level-2", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Tags = append(d.Tags, "L2")
			return d
		}),
		pipz.Transform("level-3", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Tags = append(d.Tags, "L3")
			return d
		}),
		pipz.Transform("level-4", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Tags = append(d.Tags, "L4")
			return d
		}),
	)

	factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
		Name: "always-true",
		Predicate: func(_ context.Context, _ flumetesting.TestData) bool {
			return true
		},
	})

	// Deeply nested structure
	schema := `
type: sequence
children:
  - ref: level-1
  - type: filter
    predicate: always-true
    then:
      type: sequence
      children:
        - ref: level-2
        - type: retry
          attempts: 1
          child:
            type: sequence
            children:
              - ref: level-3
              - type: timeout
                duration: "10s"
                child:
                  ref: level-4
`

	pipeline, err := factory.BuildFromYAML(schema)
	if err != nil {
		t.Fatalf("failed to build deeply nested pipeline: %v", err)
	}

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Tags: []string{}}

	result, err := pipeline.Process(ctx, input)
	if err != nil {
		t.Fatalf("deeply nested pipeline failed: %v", err)
	}

	// All levels should be called in order
	expected := []string{"L1", "L2", "L3", "L4"}
	if len(result.Tags) != len(expected) {
		t.Fatalf("expected %d tags, got %d: %v", len(expected), len(result.Tags), result.Tags)
	}

	for i, tag := range expected {
		if result.Tags[i] != tag {
			t.Errorf("expected tag[%d]=%q, got %q", i, tag, result.Tags[i])
		}
	}
}
