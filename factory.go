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
//
//	// Define managed identities with meaningful descriptions
//	validateID := factory.Identity("validate", "Validates incoming data")
//	isValidID := factory.Identity("is-valid", "Checks if data passes validation rules")
//
//	// Register processors using managed identities
//	factory.Add(pipz.Apply(validateID, validateFunc))
//	factory.AddPredicate(flume.Predicate[MyData]{
//	    Identity: isValidID,
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
	"time"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/pipz"
)

// Predicate combines an identity with a predicate function for batch registration.
type Predicate[T any] struct {
	Predicate func(context.Context, T) bool
	Identity  pipz.Identity
}

// Condition combines an identity with a condition function for batch registration.
type Condition[T any] struct {
	Condition func(context.Context, T) string
	Identity  pipz.Identity
	Values    []string
}

// Reducer combines an identity with a reducer function for concurrent result merging.
type Reducer[T any] struct {
	Reducer  func(original T, results map[pipz.Identity]T, errors map[pipz.Identity]error) T
	Identity pipz.Identity
}

// ErrorHandler combines an identity with an error handler processor.
type ErrorHandler[T any] struct {
	Handler  pipz.Chainable[*pipz.Error[T]]
	Identity pipz.Identity
}

// ProcessorMeta wraps a processor with metadata for introspection.
type ProcessorMeta[T any] struct { //nolint:govet
	Processor pipz.Chainable[T]
	Tags      []string // Categorization tags for discovery
}

// predicateMeta stores a predicate function with its metadata.
type predicateMeta[T any] struct {
	predicate func(context.Context, T) bool
	identity  pipz.Identity
}

// conditionMeta stores a condition function with its metadata.
type conditionMeta[T any] struct {
	condition func(context.Context, T) string
	identity  pipz.Identity
	values    []string
}

// reducerMeta stores a reducer function with its metadata.
type reducerMeta[T any] struct {
	reducer  func(original T, results map[pipz.Identity]T, errors map[pipz.Identity]error) T
	identity pipz.Identity
}

// errorHandlerMeta stores an error handler with its metadata.
type errorHandlerMeta[T any] struct {
	handler  pipz.Chainable[*pipz.Error[T]]
	identity pipz.Identity
}

// processorMeta stores a processor with its metadata.
type processorMeta[T any] struct {
	processor pipz.Chainable[T]
	tags      []string
}

// Factory creates dynamic pipelines from schemas using registered components.
// It maintains registries for processors, predicates, conditions, reducers, and error handlers.
// T must implement pipz.Cloner[T] to support parallel processing.
type Factory[T pipz.Cloner[T]] struct {
	processors    map[string]processorMeta[T]
	predicates    map[string]predicateMeta[T]
	conditions    map[string]conditionMeta[T]
	reducers      map[string]reducerMeta[T]
	errorHandlers map[string]errorHandlerMeta[T]
	bindings      map[string]*Binding[T]   // Keyed by identity.ID().String()
	identities    map[string]pipz.Identity // Cached identities by name
	channels      map[string]chan<- T
	mu            sync.RWMutex
}

// New creates a new Factory for type T.
// T must implement pipz.Cloner[T] to support parallel processing.
func New[T pipz.Cloner[T]]() *Factory[T] {
	factory := &Factory[T]{
		processors:    make(map[string]processorMeta[T]),
		predicates:    make(map[string]predicateMeta[T]),
		conditions:    make(map[string]conditionMeta[T]),
		reducers:      make(map[string]reducerMeta[T]),
		errorHandlers: make(map[string]errorHandlerMeta[T]),
		bindings:      make(map[string]*Binding[T]),
		identities:    make(map[string]pipz.Identity),
		channels:      make(map[string]chan<- T),
	}

	capitan.Emit(context.Background(), FactoryCreated,
		KeyType.Field(fmt.Sprintf("%T", *new(T))))

	return factory
}

// Identity creates or retrieves a managed identity for the given name.
// Identities are cached - calling with the same name returns the same instance.
// Description is required and should meaningfully describe the component.
// Panics if called with the same name but different description.
func (f *Factory[T]) Identity(name, description string) pipz.Identity {
	f.mu.Lock()
	defer f.mu.Unlock()

	if id, ok := f.identities[name]; ok {
		if id.Description() != description {
			panic(fmt.Sprintf("identity %q already registered with different description: %q vs %q",
				name, id.Description(), description))
		}
		return id
	}
	id := pipz.NewIdentity(name, description)
	f.identities[name] = id
	return id
}

// internalIdentity returns a cached Identity for internal connector use.
// Used by builders for connectors that need identities (sequence, concurrent, etc).
func (f *Factory[T]) internalIdentity(name string) pipz.Identity {
	if id, ok := f.identities[name]; ok {
		return id
	}
	id := pipz.NewIdentity(name, "internal connector")
	f.identities[name] = id
	return id
}

// Add registers one or more processors to the factory using their intrinsic names.
// For processors with metadata, use AddWithMeta instead.
func (f *Factory[T]) Add(processors ...pipz.Chainable[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, processor := range processors {
		name := processor.Identity().Name()
		f.processors[name] = processorMeta[T]{
			processor: processor,
		}

		capitan.Emit(context.Background(), ProcessorRegistered,
			KeyName.Field(name))
	}
}

// AddWithMeta registers one or more processors with metadata for introspection.
func (f *Factory[T]) AddWithMeta(processors ...ProcessorMeta[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, pm := range processors {
		name := pm.Processor.Identity().Name()
		f.processors[name] = processorMeta[T]{
			processor: pm.Processor,
			tags:      pm.Tags,
		}

		capitan.Emit(context.Background(), ProcessorRegistered,
			KeyName.Field(name))
	}
}

// AddPredicate registers one or more boolean predicates for use in filter conditions.
func (f *Factory[T]) AddPredicate(predicates ...Predicate[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, p := range predicates {
		name := p.Identity.Name()
		f.predicates[name] = predicateMeta[T]{
			identity:  p.Identity,
			predicate: p.Predicate,
		}

		capitan.Emit(context.Background(), PredicateRegistered,
			KeyName.Field(name))
	}
}

// AddCondition registers one or more string-returning conditions for use in switch routing.
func (f *Factory[T]) AddCondition(conditions ...Condition[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, c := range conditions {
		name := c.Identity.Name()
		f.conditions[name] = conditionMeta[T]{
			identity:  c.Identity,
			values:    c.Values,
			condition: c.Condition,
		}

		capitan.Emit(context.Background(), ConditionRegistered,
			KeyName.Field(name))
	}
}

// AddReducer registers one or more reducer functions for use in concurrent result merging.
func (f *Factory[T]) AddReducer(reducers ...Reducer[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, r := range reducers {
		name := r.Identity.Name()
		f.reducers[name] = reducerMeta[T]{
			identity: r.Identity,
			reducer:  r.Reducer,
		}

		capitan.Emit(context.Background(), ReducerRegistered,
			KeyName.Field(name))
	}
}

// AddErrorHandler registers one or more error handlers for use in handle nodes.
func (f *Factory[T]) AddErrorHandler(handlers ...ErrorHandler[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, h := range handlers {
		name := h.Identity.Name()
		f.errorHandlers[name] = errorHandlerMeta[T]{
			identity: h.Identity,
			handler:  h.Handler,
		}

		capitan.Emit(context.Background(), ErrorHandlerRegistered,
			KeyName.Field(name))
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
		pm, exists := f.processors[node.Ref]
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

// HasProcessor checks if a processor is registered.
func (f *Factory[T]) HasProcessor(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.processors[name]
	return exists
}

// HasPredicate checks if a predicate is registered.
func (f *Factory[T]) HasPredicate(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.predicates[name]
	return exists
}

// HasCondition checks if a condition is registered.
func (f *Factory[T]) HasCondition(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.conditions[name]
	return exists
}

// HasReducer checks if a reducer is registered.
func (f *Factory[T]) HasReducer(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.reducers[name]
	return exists
}

// HasErrorHandler checks if an error handler is registered.
func (f *Factory[T]) HasErrorHandler(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.errorHandlers[name]
	return exists
}

// ListProcessors returns a slice of all registered processor names.
func (f *Factory[T]) ListProcessors() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.processors))
	for name := range f.processors {
		names = append(names, name)
	}
	return names
}

// ListPredicates returns a slice of all registered predicate names.
func (f *Factory[T]) ListPredicates() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.predicates))
	for name := range f.predicates {
		names = append(names, name)
	}
	return names
}

// ListConditions returns a slice of all registered condition names.
func (f *Factory[T]) ListConditions() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.conditions))
	for name := range f.conditions {
		names = append(names, name)
	}
	return names
}

// ListReducers returns a slice of all registered reducer names.
func (f *Factory[T]) ListReducers() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.reducers))
	for name := range f.reducers {
		names = append(names, name)
	}
	return names
}

// ListErrorHandlers returns a slice of all registered error handler names.
func (f *Factory[T]) ListErrorHandlers() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.errorHandlers))
	for name := range f.errorHandlers {
		names = append(names, name)
	}
	return names
}

// Remove removes one or more processors from the factory.
// Returns the number of processors actually removed.
func (f *Factory[T]) Remove(names ...string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	removed := 0
	for _, name := range names {
		if _, exists := f.processors[name]; exists {
			delete(f.processors, name)
			removed++

			capitan.Emit(context.Background(), ProcessorRemoved,
				KeyName.Field(name))
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
func (f *Factory[T]) RemovePredicate(names ...string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	removed := 0
	for _, name := range names {
		if _, exists := f.predicates[name]; exists {
			delete(f.predicates, name)
			removed++

			capitan.Emit(context.Background(), PredicateRemoved,
				KeyName.Field(name))
		}
	}
	return removed
}

// RemoveCondition removes one or more conditions from the factory.
// Returns the number of conditions actually removed.
func (f *Factory[T]) RemoveCondition(names ...string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	removed := 0
	for _, name := range names {
		if _, exists := f.conditions[name]; exists {
			delete(f.conditions, name)
			removed++

			capitan.Emit(context.Background(), ConditionRemoved,
				KeyName.Field(name))
		}
	}
	return removed
}

// RemoveReducer removes one or more reducers from the factory.
// Returns the number of reducers actually removed.
func (f *Factory[T]) RemoveReducer(names ...string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	removed := 0
	for _, name := range names {
		if _, exists := f.reducers[name]; exists {
			delete(f.reducers, name)
			removed++

			capitan.Emit(context.Background(), ReducerRemoved,
				KeyName.Field(name))
		}
	}
	return removed
}

// RemoveErrorHandler removes one or more error handlers from the factory.
// Returns the number of error handlers actually removed.
func (f *Factory[T]) RemoveErrorHandler(names ...string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	removed := 0
	for _, name := range names {
		if _, exists := f.errorHandlers[name]; exists {
			delete(f.errorHandlers, name)
			removed++

			capitan.Emit(context.Background(), ErrorHandlerRemoved,
				KeyName.Field(name))
		}
	}
	return removed
}

// Bind creates or retrieves a Binding for the given identity.
// If a Binding with this identity already exists, it is returned (idempotent).
// The schema is used to build the initial pipeline version.
func (f *Factory[T]) Bind(identity pipz.Identity, schema Schema) (*Binding[T], error) {
	key := identity.ID().String()

	f.mu.Lock()
	defer f.mu.Unlock()

	// Check for existing binding
	if binding, exists := f.bindings[key]; exists {
		return binding, nil
	}

	// Build the pipeline
	chainable, err := f.buildNode(&schema.Node, "root")
	if err != nil {
		return nil, fmt.Errorf("failed to build schema: %w", err)
	}

	// Wrap in Pipeline for tracing
	pipeline := pipz.NewPipeline(identity, chainable)

	// Create binding with default options
	binding := &Binding[T]{
		identity:   identity,
		factory:    f,
		historyCap: DefaultHistoryCap,
	}

	// Set initial version
	version := schema.Version
	if version == "" {
		version = "1"
	}

	binding.current = &pipelineVersion[T]{
		schema:   schema,
		pipeline: pipeline,
		version:  version,
		builtAt:  time.Now(),
	}

	f.bindings[key] = binding

	capitan.Emit(context.Background(), SchemaRegistered,
		KeyName.Field(identity.Name()),
		KeyVersion.Field(version))

	return binding, nil
}

// Get retrieves an existing Binding by identity.
// Returns nil if no binding exists for the given identity.
func (f *Factory[T]) Get(identity pipz.Identity) *Binding[T] {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.bindings[identity.ID().String()]
}

// ListBindings returns a slice of all registered binding names.
func (f *Factory[T]) ListBindings() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.bindings))
	for _, binding := range f.bindings {
		names = append(names, binding.identity.Name())
	}
	return names
}
