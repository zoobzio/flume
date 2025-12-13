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
//   - Channel integration for terminal stream processing
//   - Comprehensive validation with detailed error reporting
//   - Support for all pipz connector types (sequence, concurrent, retry, etc.)
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
// Channel Integration:
//
// Flume supports channel integration for stream termination nodes.
// Channels act as terminal endpoints for synchronous pipelines:
//
//	ch := make(chan MyData, 100)
//	factory.AddChannel("output", ch)
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

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/pipz"
)

// Predicate combines a name with a predicate function for batch registration.
type Predicate[T any] struct { //nolint:govet
	Name        pipz.Name
	Description string // Human-readable description of what this predicate checks
	Predicate   func(context.Context, T) bool
}

// Condition combines a name with a condition function for batch registration.
type Condition[T any] struct { //nolint:govet
	Name        pipz.Name
	Description string   // Human-readable description of what this condition evaluates
	Values      []string // Possible return values (for schema generation/validation)
	Condition   func(context.Context, T) string
}

// Reducer combines a name with a reducer function for concurrent result merging.
type Reducer[T any] struct { //nolint:govet
	Name        pipz.Name
	Description string // Human-readable description of what this reducer does
	Reducer     func(original T, results map[pipz.Name]T, errors map[pipz.Name]error) T
}

// ErrorHandler combines a name with an error handler processor.
type ErrorHandler[T any] struct { //nolint:govet
	Name        pipz.Name
	Description string // Human-readable description of what this error handler does
	Handler     pipz.Chainable[*pipz.Error[T]]
}

// ProcessorMeta wraps a processor with metadata for introspection.
type ProcessorMeta[T any] struct { //nolint:govet
	Processor   pipz.Chainable[T]
	Description string   // Human-readable description of what this processor does
	Tags        []string // Categorization tags for discovery
}

// predicateMeta stores a predicate function with its metadata.
type predicateMeta[T any] struct { //nolint:govet
	description string
	predicate   func(context.Context, T) bool
}

// conditionMeta stores a condition function with its metadata.
type conditionMeta[T any] struct { //nolint:govet
	description string
	values      []string
	condition   func(context.Context, T) string
}

// reducerMeta stores a reducer function with its metadata.
type reducerMeta[T any] struct {
	reducer     func(original T, results map[pipz.Name]T, errors map[pipz.Name]error) T
	description string
}

// errorHandlerMeta stores an error handler with its metadata.
type errorHandlerMeta[T any] struct {
	handler     pipz.Chainable[*pipz.Error[T]]
	description string
}

// processorMeta stores a processor with its metadata.
type processorMeta[T any] struct {
	processor   pipz.Chainable[T]
	description string
	tags        []string
}

// Factory creates dynamic pipelines from schemas using registered components.
// It maintains registries for processors, predicates, conditions, reducers, and error handlers.
// T must implement pipz.Cloner[T] to support parallel processing.
type Factory[T pipz.Cloner[T]] struct {
	processors    map[pipz.Name]processorMeta[T]
	predicates    map[pipz.Name]predicateMeta[T]
	conditions    map[pipz.Name]conditionMeta[T]
	reducers      map[pipz.Name]reducerMeta[T]
	errorHandlers map[pipz.Name]errorHandlerMeta[T]
	schemas       map[string]*Schema
	pipelines     map[string]*atomic.Pointer[pipz.Chainable[T]]
	channels      map[string]chan<- T
	mu            sync.RWMutex
}

// New creates a new Factory for type T.
// T must implement pipz.Cloner[T] to support parallel processing.
func New[T pipz.Cloner[T]]() *Factory[T] {
	factory := &Factory[T]{
		processors:    make(map[pipz.Name]processorMeta[T]),
		predicates:    make(map[pipz.Name]predicateMeta[T]),
		conditions:    make(map[pipz.Name]conditionMeta[T]),
		reducers:      make(map[pipz.Name]reducerMeta[T]),
		errorHandlers: make(map[pipz.Name]errorHandlerMeta[T]),
		schemas:       make(map[string]*Schema),
		pipelines:     make(map[string]*atomic.Pointer[pipz.Chainable[T]]),
		channels:      make(map[string]chan<- T),
	}

	capitan.Emit(context.Background(), FactoryCreated,
		KeyType.Field(fmt.Sprintf("%T", *new(T))))

	return factory
}

// Add registers one or more processors to the factory using their intrinsic names.
// For processors with metadata, use AddWithMeta instead.
func (f *Factory[T]) Add(processors ...pipz.Chainable[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, processor := range processors {
		f.processors[processor.Name()] = processorMeta[T]{
			processor: processor,
		}

		capitan.Emit(context.Background(), ProcessorRegistered,
			KeyName.Field(string(processor.Name()))) //nolint:unconvert
	}
}

// AddWithMeta registers one or more processors with metadata for introspection.
func (f *Factory[T]) AddWithMeta(processors ...ProcessorMeta[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, pm := range processors {
		f.processors[pm.Processor.Name()] = processorMeta[T]{
			processor:   pm.Processor,
			description: pm.Description,
			tags:        pm.Tags,
		}

		capitan.Emit(context.Background(), ProcessorRegistered,
			KeyName.Field(string(pm.Processor.Name()))) //nolint:unconvert
	}
}

// AddPredicate registers one or more boolean predicates for use in filter conditions.
func (f *Factory[T]) AddPredicate(predicates ...Predicate[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, p := range predicates {
		f.predicates[p.Name] = predicateMeta[T]{
			description: p.Description,
			predicate:   p.Predicate,
		}

		capitan.Emit(context.Background(), PredicateRegistered,
			KeyName.Field(string(p.Name))) //nolint:unconvert
	}
}

// AddCondition registers one or more string-returning conditions for use in switch routing.
func (f *Factory[T]) AddCondition(conditions ...Condition[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, c := range conditions {
		f.conditions[c.Name] = conditionMeta[T]{
			description: c.Description,
			values:      c.Values,
			condition:   c.Condition,
		}

		capitan.Emit(context.Background(), ConditionRegistered,
			KeyName.Field(string(c.Name))) //nolint:unconvert
	}
}

// AddReducer registers one or more reducer functions for use in concurrent result merging.
func (f *Factory[T]) AddReducer(reducers ...Reducer[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, r := range reducers {
		f.reducers[r.Name] = reducerMeta[T]{
			description: r.Description,
			reducer:     r.Reducer,
		}

		capitan.Emit(context.Background(), ReducerRegistered,
			KeyName.Field(string(r.Name))) //nolint:unconvert
	}
}

// AddErrorHandler registers one or more error handlers for use in handle nodes.
func (f *Factory[T]) AddErrorHandler(handlers ...ErrorHandler[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, h := range handlers {
		f.errorHandlers[h.Name] = errorHandlerMeta[T]{
			description: h.Description,
			handler:     h.Handler,
		}

		capitan.Emit(context.Background(), ErrorHandlerRegistered,
			KeyName.Field(string(h.Name))) //nolint:unconvert
	}
}

// Build creates a pipeline from a schema.
func (f *Factory[T]) Build(schema Schema) (pipz.Chainable[T], error) {
	start := time.Now()

	// Log build start with version if present
	startFields := []capitan.Field{}
	if schema.Version != "" {
		startFields = append(startFields, KeyVersion.Field(schema.Version))
	}
	capitan.Emit(context.Background(), SchemaBuildStarted, startFields...)

	// Validate first
	if err := f.ValidateSchema(schema); err != nil {
		failFields := []capitan.Field{
			KeyError.Field(err.Error()),
			KeyDuration.Field(time.Since(start)),
		}
		if schema.Version != "" {
			failFields = append(failFields, KeyVersion.Field(schema.Version))
		}
		capitan.Emit(context.Background(), SchemaBuildFailed, failFields...)
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	pipeline, err := f.buildNode(&schema.Node, "root")
	if err != nil {
		failFields := []capitan.Field{
			KeyError.Field(err.Error()),
			KeyDuration.Field(time.Since(start)),
		}
		if schema.Version != "" {
			failFields = append(failFields, KeyVersion.Field(schema.Version))
		}
		capitan.Emit(context.Background(), SchemaBuildFailed, failFields...)
		return nil, err
	}

	completeFields := []capitan.Field{
		KeyDuration.Field(time.Since(start)),
	}
	if schema.Version != "" {
		completeFields = append(completeFields, KeyVersion.Field(schema.Version))
	}
	capitan.Emit(context.Background(), SchemaBuildCompleted, completeFields...)
	return pipeline, nil
}

// buildNode recursively builds a pipeline node.
func (f *Factory[T]) buildNode(node *Node, path string) (pipz.Chainable[T], error) {
	// Handle processor reference
	if node.Ref != "" {
		pm, exists := f.processors[pipz.Name(node.Ref)] //nolint:unconvert
		if !exists {
			return nil, fmt.Errorf("%s: processor '%s' not found", path, node.Ref)
		}
		return pm.processor, nil
	}

	// Handle connector types
	switch node.Type {
	case connectorSequence:
		return f.buildSequence(node, path)
	case connectorConcurrent:
		return f.buildConcurrent(node, path)
	case connectorRace:
		return f.buildRace(node, path)
	case connectorFallback:
		return f.buildFallback(node, path)
	case connectorRetry:
		return f.buildRetry(node, path)
	case connectorTimeout:
		return f.buildTimeout(node, path)
	case connectorFilter:
		return f.buildFilter(node, path)
	case connectorSwitch:
		return f.buildSwitch(node, path)
	case connectorCircuitBreaker:
		return f.buildCircuitBreaker(node, path)
	case connectorRateLimit:
		return f.buildRateLimit(node, path)
	case connectorContest:
		return f.buildContest(node, path)
	case connectorHandle:
		return f.buildHandle(node, path)
	case connectorScaffold:
		return f.buildScaffold(node, path)
	case connectorWorkerPool:
		return f.buildWorkerPool(node, path)
	default:
		// Check if it's a stream reference
		if node.Stream != "" {
			return f.buildStream(node, path)
		}
		return nil, fmt.Errorf("%s: unknown node type '%s'", path, node.Type)
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
		fields := []capitan.Field{
			KeyName.Field(name),
		}
		if oldSchema.Version != "" {
			fields = append(fields, KeyOldVersion.Field(oldSchema.Version))
		}
		if schema.Version != "" {
			fields = append(fields, KeyNewVersion.Field(schema.Version))
		}
		capitan.Emit(context.Background(), SchemaUpdated, fields...)
	} else {
		fields := []capitan.Field{
			KeyName.Field(name),
		}
		if schema.Version != "" {
			fields = append(fields, KeyVersion.Field(schema.Version))
		}
		capitan.Emit(context.Background(), SchemaRegistered, fields...)
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
		capitan.Emit(context.Background(), PipelineRetrieved,
			KeyName.Field(name),
			KeyFound.Field(false))
		return nil, false
	}

	capitan.Emit(context.Background(), PipelineRetrieved,
		KeyName.Field(name),
		KeyFound.Field(true))
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

	capitan.Emit(context.Background(), SchemaRemoved,
		KeyName.Field(name))
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

// HasReducer checks if a reducer is registered.
func (f *Factory[T]) HasReducer(name pipz.Name) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.reducers[name]
	return exists
}

// HasErrorHandler checks if an error handler is registered.
func (f *Factory[T]) HasErrorHandler(name pipz.Name) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.errorHandlers[name]
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

// ListReducers returns a slice of all registered reducer names.
func (f *Factory[T]) ListReducers() []pipz.Name {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]pipz.Name, 0, len(f.reducers))
	for name := range f.reducers {
		names = append(names, name)
	}
	return names
}

// ListErrorHandlers returns a slice of all registered error handler names.
func (f *Factory[T]) ListErrorHandlers() []pipz.Name {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]pipz.Name, 0, len(f.errorHandlers))
	for name := range f.errorHandlers {
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

			capitan.Emit(context.Background(), ProcessorRemoved,
				KeyName.Field(string(name))) //nolint:unconvert
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

	capitan.Emit(context.Background(), ChannelRegistered,
		KeyName.Field(name))
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

		capitan.Emit(context.Background(), ChannelRemoved,
			KeyName.Field(name))
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

			capitan.Emit(context.Background(), PredicateRemoved,
				KeyName.Field(string(name))) //nolint:unconvert
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

			capitan.Emit(context.Background(), ConditionRemoved,
				KeyName.Field(string(name))) //nolint:unconvert
		}
	}
	return removed
}

// RemoveReducer removes one or more reducers from the factory.
// Returns the number of reducers actually removed.
func (f *Factory[T]) RemoveReducer(names ...pipz.Name) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	removed := 0
	for _, name := range names {
		if _, exists := f.reducers[name]; exists {
			delete(f.reducers, name)
			removed++

			capitan.Emit(context.Background(), ReducerRemoved,
				KeyName.Field(string(name))) //nolint:unconvert
		}
	}
	return removed
}

// RemoveErrorHandler removes one or more error handlers from the factory.
// Returns the number of error handlers actually removed.
func (f *Factory[T]) RemoveErrorHandler(names ...pipz.Name) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	removed := 0
	for _, name := range names {
		if _, exists := f.errorHandlers[name]; exists {
			delete(f.errorHandlers, name)
			removed++

			capitan.Emit(context.Background(), ErrorHandlerRemoved,
				KeyName.Field(string(name))) //nolint:unconvert
		}
	}
	return removed
}
