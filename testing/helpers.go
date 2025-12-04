// Package testing provides test utilities and helpers for flume-based applications.
//
// This package includes mock processors, schema generators, and assertion helpers
// to make testing flume factories and pipelines easier and more comprehensive.
//
// Example usage:
//
//	func TestMyFactory(t *testing.T) {
//		factory := testing.NewTestFactory[string](t)
//		factory.AddProcessor("validate", func(ctx context.Context, s string) (string, error) {
//			if s == "" {
//				return "", errors.New("empty string")
//			}
//			return s, nil
//		})
//
//		schema := testing.SchemaBuilder{}.
//			Sequence().
//			Ref("validate").
//			Build()
//
//		pipeline, err := factory.BuildFromYAML(schema)
//		require.NoError(t, err)
//
//		result, err := pipeline.Process(context.Background(), "test")
//		assert.Equal(t, "test", result)
//	}
package testing

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zoobzio/flume"
	"github.com/zoobzio/pipz"
)

// TestData is a simple cloneable test data type for use in tests.
type TestData struct {
	Metadata map[string]any
	Name     string
	Tags     []string
	ID       int
	Value    float64
}

// Clone implements pipz.Cloner for TestData.
func (d TestData) Clone() TestData {
	tags := make([]string, len(d.Tags))
	copy(tags, d.Tags)

	metadata := make(map[string]any, len(d.Metadata))
	for k, v := range d.Metadata {
		metadata[k] = v
	}

	return TestData{
		ID:       d.ID,
		Name:     d.Name,
		Value:    d.Value,
		Tags:     tags,
		Metadata: metadata,
	}
}

// TestFactory wraps a flume.Factory with additional test utilities.
type TestFactory[T pipz.Cloner[T]] struct {
	*flume.Factory[T]
	t              *testing.T
	processorCalls map[pipz.Name]*int64
	mu             sync.RWMutex
}

// NewTestFactory creates a new test factory with tracking capabilities.
func NewTestFactory[T pipz.Cloner[T]](t *testing.T) *TestFactory[T] {
	return &TestFactory[T]{
		Factory:        flume.New[T](),
		t:              t,
		processorCalls: make(map[pipz.Name]*int64),
	}
}

// AddProcessor adds a processor with call tracking.
func (f *TestFactory[T]) AddProcessor(name string, fn func(context.Context, T) (T, error)) {
	f.mu.Lock()
	counter := new(int64)
	f.processorCalls[pipz.Name(name)] = counter
	f.mu.Unlock()

	processor := pipz.Apply(pipz.Name(name), func(ctx context.Context, data T) (T, error) {
		atomic.AddInt64(counter, 1)
		return fn(ctx, data)
	})
	f.Factory.Add(processor)
}

// AddTransform adds a transform processor with call tracking.
func (f *TestFactory[T]) AddTransform(name string, fn func(context.Context, T) T) {
	f.mu.Lock()
	counter := new(int64)
	f.processorCalls[pipz.Name(name)] = counter
	f.mu.Unlock()

	processor := pipz.Transform(pipz.Name(name), func(ctx context.Context, data T) T {
		atomic.AddInt64(counter, 1)
		return fn(ctx, data)
	})
	f.Factory.Add(processor)
}

// AddEffect adds an effect processor with call tracking.
func (f *TestFactory[T]) AddEffect(name string, fn func(context.Context, T) error) {
	f.mu.Lock()
	counter := new(int64)
	f.processorCalls[pipz.Name(name)] = counter
	f.mu.Unlock()

	processor := pipz.Effect(pipz.Name(name), func(ctx context.Context, data T) error {
		atomic.AddInt64(counter, 1)
		return fn(ctx, data)
	})
	f.Factory.Add(processor)
}

// CallCount returns the number of times a processor was called.
func (f *TestFactory[T]) CallCount(name string) int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if counter, ok := f.processorCalls[pipz.Name(name)]; ok {
		return atomic.LoadInt64(counter)
	}
	return 0
}

// ResetCounts resets all processor call counts.
func (f *TestFactory[T]) ResetCounts() {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, counter := range f.processorCalls {
		atomic.StoreInt64(counter, 0)
	}
}

// AssertCalled verifies a processor was called at least once.
func (f *TestFactory[T]) AssertCalled(name string) {
	f.t.Helper()
	if f.CallCount(name) == 0 {
		f.t.Errorf("expected processor %q to be called, but it was not", name)
	}
}

// AssertNotCalled verifies a processor was never called.
func (f *TestFactory[T]) AssertNotCalled(name string) {
	f.t.Helper()
	if count := f.CallCount(name); count > 0 {
		f.t.Errorf("expected processor %q to not be called, but it was called %d times", name, count)
	}
}

// AssertCallCount verifies a processor was called exactly n times.
func (f *TestFactory[T]) AssertCallCount(name string, expected int64) {
	f.t.Helper()
	actual := f.CallCount(name)
	if actual != expected {
		f.t.Errorf("expected processor %q to be called %d times, but it was called %d times", name, expected, actual)
	}
}

// SchemaBuilder provides a fluent API for building test schemas.
type SchemaBuilder struct {
	lines []string
	depth int
}

// NewSchemaBuilder creates a new schema builder.
func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		lines: []string{},
		depth: 0,
	}
}

// Version adds a version to the schema.
func (b *SchemaBuilder) Version(v string) *SchemaBuilder {
	b.lines = append(b.lines, fmt.Sprintf("version: %q", v))
	return b
}

// Ref creates a simple processor reference schema.
func (b *SchemaBuilder) Ref(name string) *SchemaBuilder {
	b.lines = append(b.lines, b.indent()+fmt.Sprintf("ref: %s", name))
	return b
}

// Sequence starts a sequence connector.
func (b *SchemaBuilder) Sequence() *SchemaBuilder {
	b.lines = append(b.lines, b.indent()+"type: sequence")
	b.lines = append(b.lines, b.indent()+"children:")
	b.depth++
	return b
}

// Concurrent starts a concurrent connector.
func (b *SchemaBuilder) Concurrent() *SchemaBuilder {
	b.lines = append(b.lines, b.indent()+"type: concurrent")
	b.lines = append(b.lines, b.indent()+"children:")
	b.depth++
	return b
}

// Child adds a child reference.
func (b *SchemaBuilder) Child(name string) *SchemaBuilder {
	b.lines = append(b.lines, b.indent()+fmt.Sprintf("- ref: %s", name))
	return b
}

// Filter adds a filter node.
func (b *SchemaBuilder) Filter(predicate, thenRef string) *SchemaBuilder {
	b.lines = append(b.lines, b.indent()+"- type: filter")
	b.lines = append(b.lines, b.indent()+"  predicate: "+predicate)
	b.lines = append(b.lines, b.indent()+"  then:")
	b.lines = append(b.lines, b.indent()+"    ref: "+thenRef)
	return b
}

// Retry adds a retry node.
func (b *SchemaBuilder) Retry(attempts int, childRef string) *SchemaBuilder {
	b.lines = append(b.lines, b.indent()+"- type: retry")
	b.lines = append(b.lines, b.indent()+fmt.Sprintf("  attempts: %d", attempts))
	b.lines = append(b.lines, b.indent()+"  child:")
	b.lines = append(b.lines, b.indent()+"    ref: "+childRef)
	return b
}

// Timeout adds a timeout node.
func (b *SchemaBuilder) Timeout(duration string, childRef string) *SchemaBuilder {
	b.lines = append(b.lines, b.indent()+"- type: timeout")
	b.lines = append(b.lines, b.indent()+fmt.Sprintf("  duration: %q", duration))
	b.lines = append(b.lines, b.indent()+"  child:")
	b.lines = append(b.lines, b.indent()+"    ref: "+childRef)
	return b
}

// Fallback adds a fallback node.
func (b *SchemaBuilder) Fallback(primaryRef, fallbackRef string) *SchemaBuilder {
	b.lines = append(b.lines, b.indent()+"- type: fallback")
	b.lines = append(b.lines, b.indent()+"  children:")
	b.lines = append(b.lines, b.indent()+"    - ref: "+primaryRef)
	b.lines = append(b.lines, b.indent()+"    - ref: "+fallbackRef)
	return b
}

// Stream adds a stream terminal node.
func (b *SchemaBuilder) Stream(channelName string) *SchemaBuilder {
	b.lines = append(b.lines, b.indent()+fmt.Sprintf("- stream: %s", channelName))
	return b
}

// End closes the current depth level.
func (b *SchemaBuilder) End() *SchemaBuilder {
	if b.depth > 0 {
		b.depth--
	}
	return b
}

// Build returns the complete YAML schema.
func (b *SchemaBuilder) Build() string {
	return strings.Join(b.lines, "\n")
}

func (b *SchemaBuilder) indent() string {
	return strings.Repeat("  ", b.depth)
}

// MockProcessor provides a configurable mock implementation of pipz.Chainable[T].
type MockProcessor[T any] struct {
	returnVal   T
	returnErr   error
	name        pipz.Name
	panicMsg    string
	callHistory []MockCall[T]
	callCount   int64
	delay       time.Duration
	maxHistory  int
	mu          sync.RWMutex
}

// MockCall represents a single call to the mock processor.
type MockCall[T any] struct {
	Input     T
	Timestamp time.Time
	Context   context.Context
}

// NewMockProcessor creates a new mock processor for testing.
func NewMockProcessor[T any](name string) *MockProcessor[T] {
	return &MockProcessor[T]{
		name:       pipz.Name(name),
		maxHistory: 100,
	}
}

// WithReturn configures the mock to return specific values.
func (m *MockProcessor[T]) WithReturn(val T, err error) *MockProcessor[T] {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.returnVal = val
	m.returnErr = err
	return m
}

// WithDelay configures the mock to delay execution.
func (m *MockProcessor[T]) WithDelay(d time.Duration) *MockProcessor[T] {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delay = d
	return m
}

// WithPanic configures the mock to panic with a specific message.
func (m *MockProcessor[T]) WithPanic(msg string) *MockProcessor[T] {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.panicMsg = msg
	return m
}

// Name returns the name of the mock processor.
func (m *MockProcessor[T]) Name() pipz.Name {
	return m.name
}

// Process implements pipz.Chainable[T].
func (m *MockProcessor[T]) Process(ctx context.Context, data T) (T, error) {
	atomic.AddInt64(&m.callCount, 1)

	m.mu.Lock()
	if m.maxHistory > 0 {
		call := MockCall[T]{
			Input:     data,
			Timestamp: time.Now(),
			Context:   ctx,
		}
		m.callHistory = append(m.callHistory, call)
		if len(m.callHistory) > m.maxHistory {
			m.callHistory = m.callHistory[1:]
		}
	}

	delay := m.delay
	returnVal := m.returnVal
	returnErr := m.returnErr
	panicMsg := m.panicMsg
	m.mu.Unlock()

	if panicMsg != "" {
		panic(panicMsg)
	}

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return data, ctx.Err()
		}
	}

	return returnVal, returnErr
}

// CallCount returns the number of times Process has been called.
func (m *MockProcessor[T]) CallCount() int {
	return int(atomic.LoadInt64(&m.callCount))
}

// Reset clears all call tracking.
func (m *MockProcessor[T]) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	atomic.StoreInt64(&m.callCount, 0)
	m.callHistory = nil
}

// BenchmarkHelper provides utilities for benchmarking flume operations.
type BenchmarkHelper[T pipz.Cloner[T]] struct {
	factory *flume.Factory[T]
}

// NewBenchmarkHelper creates a new benchmark helper.
func NewBenchmarkHelper[T pipz.Cloner[T]]() *BenchmarkHelper[T] {
	return &BenchmarkHelper[T]{
		factory: flume.New[T](),
	}
}

// Factory returns the underlying factory.
func (h *BenchmarkHelper[T]) Factory() *flume.Factory[T] {
	return h.factory
}

// RegisterNoOpProcessors registers n no-op processors for benchmarking registration overhead.
func (h *BenchmarkHelper[T]) RegisterNoOpProcessors(n int) {
	for i := 0; i < n; i++ {
		name := pipz.Name(fmt.Sprintf("noop-%d", i))
		processor := pipz.Transform(name, func(_ context.Context, data T) T {
			return data
		})
		h.factory.Add(processor)
	}
}

// GenerateSequenceSchema generates a sequence schema with n processor references.
func (*BenchmarkHelper[T]) GenerateSequenceSchema(n int) string {
	var sb strings.Builder
	sb.WriteString("type: sequence\nchildren:\n")
	for i := 0; i < n; i++ {
		sb.WriteString(fmt.Sprintf("  - ref: noop-%d\n", i))
	}
	return sb.String()
}

// GenerateNestedSchema generates a nested schema with specified depth.
func (*BenchmarkHelper[T]) GenerateNestedSchema(depth int) string {
	if depth <= 0 {
		return "ref: noop-0"
	}

	var sb strings.Builder
	for i := 0; i < depth; i++ {
		sb.WriteString(strings.Repeat("  ", i))
		sb.WriteString("type: sequence\n")
		sb.WriteString(strings.Repeat("  ", i))
		sb.WriteString("children:\n")
		sb.WriteString(strings.Repeat("  ", i))
		sb.WriteString("  - ref: noop-0\n")
		if i < depth-1 {
			sb.WriteString(strings.Repeat("  ", i))
			sb.WriteString("  - ")
		}
	}
	return sb.String()
}

// MeasureLatency measures the latency of a function call.
func MeasureLatency(fn func()) time.Duration {
	start := time.Now()
	fn()
	return time.Since(start)
}

// MeasureLatencyWithResult measures the latency and returns both result and duration.
func MeasureLatencyWithResult[T any](fn func() T) (T, time.Duration) {
	start := time.Now()
	result := fn()
	return result, time.Since(start)
}

// ParallelTest runs a test function in parallel with multiple goroutines.
func ParallelTest(t *testing.T, goroutines int, testFunc func(id int)) {
	t.Helper()

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			testFunc(id)
		}(i)
	}

	wg.Wait()
}

// WaitForCondition waits for a condition to be true with a timeout.
func WaitForCondition(timeout time.Duration, condition func() bool) bool {
	start := time.Now()
	for time.Since(start) < timeout {
		if condition() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
