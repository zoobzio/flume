// Package flume provides a dynamic pipeline factory for pipz that enables
// schema-driven pipeline construction with hot-reloading capabilities.
//
// Flume allows you to define pipelines using declarative YAML/JSON schemas
// instead of imperative code. It supports registering reusable processors,
// predicates, and conditions that can be composed into complex pipelines
// through configuration rather than compilation.
//
// Key features:
//   - Schema-driven pipeline construction (YAML/JSON)
//   - Hot-reloading of pipeline definitions without restarts
//   - Type-safe pipeline building through Go generics
//   - Integration with streamz for terminal stream processing
//   - Comprehensive validation with detailed error reporting
//   - Support for all pipz connector types (sequence, parallel, retry, etc.)
//
// Basic usage:
//
//	factory := flume.New[MyData]()
//	factory.Add(pipz.Apply("validate", validateFunc))
//	factory.AddPredicate(flume.Predicate[MyData]{
//	    Name: "is-valid",
//	    Predicate: func(ctx context.Context, d MyData) bool { return d.Valid },
//	})
//
//	schema := `
//	type: sequence
//	children:
//	  - ref: validate
//	  - type: filter
//	    predicate: is-valid
//	    then:
//	      ref: process
//	`
//
//	pipeline, err := factory.BuildFromYAML(schema)
//	result, err := pipeline.Process(ctx, data)
//
// Stream Integration:
//
// Flume integrates with streamz to support stream termination nodes.
// Streams act as terminal endpoints for synchronous pipelines:
//
//	stream := streamz.NewFlumeStream("output", 100, myStreamPipeline)
//	factory.AddStream("output", stream)
//
//	schema := `
//	type: sequence
//	children:
//	  - ref: process
//	  - stream: output  # Terminal node sends to stream
//	`
package flume

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zoobzio/pipz"
	"github.com/zoobzio/zlog"
)

// Predicate combines a name with a predicate function for batch registration.
type Predicate[T any] struct { //nolint:govet
	Name      pipz.Name
	Predicate func(context.Context, T) bool
}

// Condition combines a name with a condition function for batch registration.
type Condition[T any] struct { //nolint:govet
	Name      pipz.Name
	Condition func(context.Context, T) string
}

// Factory creates dynamic pipelines from schemas using registered components.
// It maintains three registries: processors, predicates, and conditions.
// T must implement pipz.Cloner[T] to support parallel processing.
type Factory[T pipz.Cloner[T]] struct {
	processors map[pipz.Name]pipz.Chainable[T]
	predicates map[pipz.Name]func(context.Context, T) bool
	conditions map[pipz.Name]func(context.Context, T) string
	schemas    map[string]*Schema
	pipelines  map[string]*atomic.Pointer[pipz.Chainable[T]]
	channels   map[string]chan<- T
	mu         sync.RWMutex
}

// New creates a new Factory for type T.
// T must implement pipz.Cloner[T] to support parallel processing.
func New[T pipz.Cloner[T]]() *Factory[T] {
	factory := &Factory[T]{
		processors: make(map[pipz.Name]pipz.Chainable[T]),
		predicates: make(map[pipz.Name]func(context.Context, T) bool),
		conditions: make(map[pipz.Name]func(context.Context, T) string),
		schemas:    make(map[string]*Schema),
		pipelines:  make(map[string]*atomic.Pointer[pipz.Chainable[T]]),
		channels:   make(map[string]chan<- T),
	}

	zlog.Emit(FactoryCreated, "Flume factory created",
		zlog.String("type", fmt.Sprintf("%T", *new(T))))

	return factory
}

// Add registers one or more processors to the factory using their intrinsic names.
func (f *Factory[T]) Add(processors ...pipz.Chainable[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, processor := range processors {
		f.processors[processor.Name()] = processor

		zlog.Emit(ProcessorRegistered, "Processor registered",
			zlog.String("name", string(processor.Name()))) //nolint:unconvert
	}
}

// AddPredicate registers one or more boolean predicates for use in filter conditions.
func (f *Factory[T]) AddPredicate(predicates ...Predicate[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, p := range predicates {
		f.predicates[p.Name] = p.Predicate

		zlog.Emit(PredicateRegistered, "Predicate registered",
			zlog.String("name", string(p.Name))) //nolint:unconvert
	}
}

// AddCondition registers one or more string-returning conditions for use in switch routing.
func (f *Factory[T]) AddCondition(conditions ...Condition[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, c := range conditions {
		f.conditions[c.Name] = c.Condition

		zlog.Emit(ConditionRegistered, "Condition registered",
			zlog.String("name", string(c.Name))) //nolint:unconvert
	}
}

// Build creates a pipeline from a schema.
func (f *Factory[T]) Build(schema Schema) (pipz.Chainable[T], error) {
	start := time.Now()

	// Log build start with version if present
	startFields := []zlog.Field{}
	if schema.Version != "" {
		startFields = append(startFields, zlog.String("version", schema.Version))
	}
	zlog.Emit(SchemaBuildStarted, "Schema build started", startFields...)

	// Validate first
	if err := f.ValidateSchema(schema); err != nil {
		failFields := []zlog.Field{
			zlog.String("error", err.Error()),
			zlog.Duration("duration", time.Since(start)),
		}
		if schema.Version != "" {
			failFields = append(failFields, zlog.String("version", schema.Version))
		}
		zlog.Emit(SchemaBuildFailed, "Schema build failed during validation", failFields...)
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	pipeline, err := f.buildNode(&schema.Node)
	if err != nil {
		failFields := []zlog.Field{
			zlog.String("error", err.Error()),
			zlog.Duration("duration", time.Since(start)),
		}
		if schema.Version != "" {
			failFields = append(failFields, zlog.String("version", schema.Version))
		}
		zlog.Emit(SchemaBuildFailed, "Schema build failed during construction", failFields...)
		return nil, err
	}

	completeFields := []zlog.Field{
		zlog.Duration("duration", time.Since(start)),
	}
	if schema.Version != "" {
		completeFields = append(completeFields, zlog.String("version", schema.Version))
	}
	zlog.Emit(SchemaBuildCompleted, "Schema build completed", completeFields...)
	return pipeline, nil
}

// buildNode recursively builds a pipeline node.
func (f *Factory[T]) buildNode(node *Node) (pipz.Chainable[T], error) {
	// Handle processor reference
	if node.Ref != "" {
		processor, exists := f.processors[pipz.Name(node.Ref)] //nolint:unconvert
		if !exists {
			return nil, fmt.Errorf("processor not found: %s", node.Ref)
		}
		return processor, nil
	}

	// Handle connector types
	switch node.Type {
	case "sequence":
		return f.buildSequence(node)
	case "parallel", "concurrent":
		return f.buildConcurrent(node)
	case "race":
		return f.buildRace(node)
	case "fallback":
		return f.buildFallback(node)
	case "retry":
		return f.buildRetry(node)
	case "timeout":
		return f.buildTimeout(node)
	case "filter":
		return f.buildFilter(node)
	case "switch":
		return f.buildSwitch(node)
	case "circuit-breaker":
		return f.buildCircuitBreaker(node)
	case "rate-limit":
		return f.buildRateLimit(node)
	default:
		// Check if it's a stream reference
		if node.Stream != "" {
			return f.buildStream(node)
		}
		return nil, fmt.Errorf("unknown node type: %s", node.Type)
	}
}

// SetSchema adds or updates a named schema and builds its pipeline.
func (f *Factory[T]) SetSchema(name string, schema Schema) error {
	// Validate first (Build will also validate, but this gives clearer error context)
	if err := f.ValidateSchema(schema); err != nil {
		return fmt.Errorf("invalid schema %s: %w", name, err)
	}

	pipeline, err := f.Build(schema)
	if err != nil {
		return fmt.Errorf("failed to build schema %s: %w", name, err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Check if updating existing schema
	oldSchema := f.schemas[name]
	isUpdate := oldSchema != nil

	f.schemas[name] = &schema
	if ptr, exists := f.pipelines[name]; exists {
		ptr.Store(&pipeline)
	} else {
		ptr := &atomic.Pointer[pipz.Chainable[T]]{}
		ptr.Store(&pipeline)
		f.pipelines[name] = ptr
	}

	if isUpdate {
		fields := []zlog.Field{
			zlog.String("name", name),
		}
		if oldSchema.Version != "" {
			fields = append(fields, zlog.String("old_version", oldSchema.Version))
		}
		if schema.Version != "" {
			fields = append(fields, zlog.String("new_version", schema.Version))
		}
		zlog.Emit(SchemaUpdated, "Schema updated", fields...)
	} else {
		fields := []zlog.Field{
			zlog.String("name", name),
		}
		if schema.Version != "" {
			fields = append(fields, zlog.String("version", schema.Version))
		}
		zlog.Emit(SchemaRegistered, "Schema registered", fields...)
	}
	return nil
}

// Pipeline returns the current pipeline for a named schema.
// Returns the pipeline and true if found, or nil and false if not found.
func (f *Factory[T]) Pipeline(name string) (pipz.Chainable[T], bool) {
	f.mu.RLock()
	ptr := f.pipelines[name]
	f.mu.RUnlock()

	if ptr == nil {
		zlog.Emit(PipelineRetrieved, "Pipeline retrieved",
			zlog.String("name", name),
			zlog.Bool("found", false))
		return nil, false
	}

	zlog.Emit(PipelineRetrieved, "Pipeline retrieved",
		zlog.String("name", name),
		zlog.Bool("found", true))
	return *ptr.Load(), true
}

// GetSchema returns a schema by name.
// Returns the schema and true if found, or an empty schema and false if not found.
func (f *Factory[T]) GetSchema(name string) (Schema, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if schema, exists := f.schemas[name]; exists {
		return *schema, true
	}
	return Schema{}, false
}

// RemoveSchema removes a named schema and its pipeline.
// Returns true if the schema was removed, false if it didn't exist.
func (f *Factory[T]) RemoveSchema(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.schemas[name]; !exists {
		return false
	}

	delete(f.schemas, name)
	delete(f.pipelines, name)

	zlog.Emit(SchemaRemoved, "Schema removed",
		zlog.String("name", name))
	return true
}

// ListSchemas returns a list of all registered schema names.
func (f *Factory[T]) ListSchemas() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.schemas))
	for name := range f.schemas {
		names = append(names, name)
	}
	return names
}

// HasProcessor checks if a processor is registered.
func (f *Factory[T]) HasProcessor(name pipz.Name) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.processors[name]
	return exists
}

// HasPredicate checks if a predicate is registered.
func (f *Factory[T]) HasPredicate(name pipz.Name) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.predicates[name]
	return exists
}

// HasCondition checks if a condition is registered.
func (f *Factory[T]) HasCondition(name pipz.Name) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.conditions[name]
	return exists
}

// ListProcessors returns a slice of all registered processor names.
func (f *Factory[T]) ListProcessors() []pipz.Name {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]pipz.Name, 0, len(f.processors))
	for name := range f.processors {
		names = append(names, name)
	}
	return names
}

// ListPredicates returns a slice of all registered predicate names.
func (f *Factory[T]) ListPredicates() []pipz.Name {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]pipz.Name, 0, len(f.predicates))
	for name := range f.predicates {
		names = append(names, name)
	}
	return names
}

// ListConditions returns a slice of all registered condition names.
func (f *Factory[T]) ListConditions() []pipz.Name {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]pipz.Name, 0, len(f.conditions))
	for name := range f.conditions {
		names = append(names, name)
	}
	return names
}

// Remove removes one or more processors from the factory.
// Returns the number of processors actually removed.
func (f *Factory[T]) Remove(names ...pipz.Name) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	removed := 0
	for _, name := range names {
		if _, exists := f.processors[name]; exists {
			delete(f.processors, name)
			removed++

			zlog.Emit(ProcessorRemoved, "Processor removed",
				zlog.String("name", string(name))) //nolint:unconvert
		}
	}
	return removed
}

// AddChannel registers a channel with the factory.
// Channels can then be referenced by name in schemas as stream nodes.
func (f *Factory[T]) AddChannel(name string, channel chan<- T) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.channels[name] = channel

	zlog.Emit(ProcessorRegistered, "Channel registered",
		zlog.String("name", name))
}

// GetChannel retrieves a registered channel by name.
func (f *Factory[T]) GetChannel(name string) (chan<- T, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	channel, exists := f.channels[name]
	return channel, exists
}

// HasChannel checks if a channel is registered.
func (f *Factory[T]) HasChannel(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	_, exists := f.channels[name]
	return exists
}

// ListChannels returns a list of all registered channel names.
func (f *Factory[T]) ListChannels() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.channels))
	for name := range f.channels {
		names = append(names, name)
	}
	return names
}

// RemoveChannel removes a channel from the factory.
func (f *Factory[T]) RemoveChannel(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.channels[name]; exists {
		delete(f.channels, name)

		zlog.Emit(ProcessorRemoved, "Channel removed",
			zlog.String("name", name))
		return true
	}
	return false
}

// RemovePredicate removes one or more predicates from the factory.
// Returns the number of predicates actually removed.
func (f *Factory[T]) RemovePredicate(names ...pipz.Name) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	removed := 0
	for _, name := range names {
		if _, exists := f.predicates[name]; exists {
			delete(f.predicates, name)
			removed++

			zlog.Emit(PredicateRemoved, "Predicate removed",
				zlog.String("name", string(name))) //nolint:unconvert
		}
	}
	return removed
}

// RemoveCondition removes one or more conditions from the factory.
// Returns the number of conditions actually removed.
func (f *Factory[T]) RemoveCondition(names ...pipz.Name) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	removed := 0
	for _, name := range names {
		if _, exists := f.conditions[name]; exists {
			delete(f.conditions, name)
			removed++

			zlog.Emit(ConditionRemoved, "Condition removed",
				zlog.String("name", string(name))) //nolint:unconvert
		}
	}
	return removed
}
