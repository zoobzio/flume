package flume

import (
	"context"
	"fmt"
	"time"

	"github.com/zoobzio/pipz"
)

// Connector type constants.
const (
	connectorSequence       = "sequence"
	connectorConcurrent     = "concurrent"
	connectorRace           = "race"
	connectorFallback       = "fallback"
	connectorRetry          = "retry"
	connectorTimeout        = "timeout"
	connectorFilter         = "filter"
	connectorSwitch         = "switch"
	connectorCircuitBreaker = "circuit-breaker"
	connectorRateLimit      = "rate-limit"
	connectorContest        = "contest"
	connectorHandle         = "handle"
	connectorScaffold       = "scaffold"
	connectorWorkerPool     = "worker-pool"
)

// Default configuration values.
const (
	DefaultRetryAttempts           = 3
	DefaultTimeoutDuration         = 30 * time.Second
	DefaultCircuitBreakerThreshold = 5
	DefaultRecoveryTimeout         = 60 * time.Second
	DefaultRequestsPerSecond       = 10.0
	DefaultBurstSize               = 1
	DefaultWorkerCount             = 4
)

// buildSequence creates a sequence connector from schema.
func (f *Factory[T]) buildSequence(node *Node, path string) (pipz.Chainable[T], error) {
	if len(node.Children) == 0 {
		return nil, fmt.Errorf("%s: sequence requires at least one child", path)
	}

	children := make([]pipz.Chainable[T], 0, len(node.Children))
	for i := range node.Children {
		processor, err := f.buildNode(&node.Children[i], fmt.Sprintf("%s.children[%d]", path, i))
		if err != nil {
			return nil, err
		}
		children = append(children, processor)
	}

	name := node.Name
	if name == "" {
		name = connectorSequence
	}

	return pipz.NewSequence(pipz.Name(name), children...), nil //nolint:unconvert
}

// buildConcurrent creates a concurrent connector from schema.
func (f *Factory[T]) buildConcurrent(node *Node, path string) (pipz.Chainable[T], error) {
	if len(node.Children) == 0 {
		return nil, fmt.Errorf("%s: concurrent requires at least one child", path)
	}

	children := make([]pipz.Chainable[T], 0, len(node.Children))
	for i := range node.Children {
		processor, err := f.buildNode(&node.Children[i], fmt.Sprintf("%s.children[%d]", path, i))
		if err != nil {
			return nil, err
		}
		children = append(children, processor)
	}

	name := node.Name
	if name == "" {
		name = connectorConcurrent
	}

	// Get reducer if specified
	var reducer func(original T, results map[pipz.Name]T, errors map[pipz.Name]error) T
	if node.Reducer != "" {
		rm, exists := f.reducers[pipz.Name(node.Reducer)] //nolint:unconvert
		if !exists {
			return nil, fmt.Errorf("%s: reducer '%s' not found", path, node.Reducer)
		}
		reducer = rm.reducer
	}

	return pipz.NewConcurrent[T](pipz.Name(name), reducer, children...), nil //nolint:unconvert
}

// buildRace creates a race connector from schema.
func (f *Factory[T]) buildRace(node *Node, path string) (pipz.Chainable[T], error) {
	if len(node.Children) == 0 {
		return nil, fmt.Errorf("%s: race requires at least one child", path)
	}

	children := make([]pipz.Chainable[T], 0, len(node.Children))
	for i := range node.Children {
		processor, err := f.buildNode(&node.Children[i], fmt.Sprintf("%s.children[%d]", path, i))
		if err != nil {
			return nil, err
		}
		children = append(children, processor)
	}

	name := node.Name
	if name == "" {
		name = connectorRace
	}

	return pipz.NewRace[T](pipz.Name(name), children...), nil //nolint:unconvert
}

// buildFallback creates a fallback connector from schema.
func (f *Factory[T]) buildFallback(node *Node, path string) (pipz.Chainable[T], error) {
	if len(node.Children) != 2 {
		return nil, fmt.Errorf("%s: fallback requires exactly 2 children", path)
	}

	primary, err := f.buildNode(&node.Children[0], fmt.Sprintf("%s.children[0]", path))
	if err != nil {
		return nil, err
	}

	fallback, err := f.buildNode(&node.Children[1], fmt.Sprintf("%s.children[1]", path))
	if err != nil {
		return nil, err
	}

	name := node.Name
	if name == "" {
		name = connectorFallback
	}

	return pipz.NewFallback(pipz.Name(name), primary, fallback), nil //nolint:unconvert
}

// buildRetry creates a retry connector from schema.
func (f *Factory[T]) buildRetry(node *Node, path string) (pipz.Chainable[T], error) {
	if node.Child == nil {
		return nil, fmt.Errorf("%s: retry requires a child", path)
	}

	child, err := f.buildNode(node.Child, fmt.Sprintf("%s.child", path))
	if err != nil {
		return nil, err
	}

	name := node.Name
	if name == "" {
		if node.Backoff != "" {
			name = "backoff"
		} else {
			name = connectorRetry
		}
	}

	// If backoff is specified, use NewBackoff instead of NewRetry
	if node.Backoff != "" {
		// Parse backoff duration
		backoff, err := time.ParseDuration(node.Backoff)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid backoff duration: %w", path, err)
		}

		// Use attempts field, default to DefaultRetryAttempts if not specified
		attempts := node.Attempts
		if attempts == 0 {
			attempts = DefaultRetryAttempts
		}

		return pipz.NewBackoff(pipz.Name(name), child, attempts, backoff), nil //nolint:unconvert
	}

	// Use attempts field, default to DefaultRetryAttempts if not specified
	attempts := node.Attempts
	if attempts == 0 {
		attempts = DefaultRetryAttempts
	}

	return pipz.NewRetry(pipz.Name(name), child, attempts), nil //nolint:unconvert
}

// buildTimeout creates a timeout connector from schema.
func (f *Factory[T]) buildTimeout(node *Node, path string) (pipz.Chainable[T], error) {
	if node.Child == nil {
		return nil, fmt.Errorf("%s: timeout requires a child", path)
	}

	child, err := f.buildNode(node.Child, fmt.Sprintf("%s.child", path))
	if err != nil {
		return nil, err
	}

	name := node.Name
	if name == "" {
		name = connectorTimeout
	}

	// Parse duration, default to DefaultTimeoutDuration if not specified
	duration := DefaultTimeoutDuration
	if node.Duration != "" {
		parsed, err := time.ParseDuration(node.Duration)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid duration: %w", path, err)
		}
		duration = parsed
	}

	return pipz.NewTimeout(pipz.Name(name), child, duration), nil //nolint:unconvert
}

// buildFilter creates a filter connector from schema.
func (f *Factory[T]) buildFilter(node *Node, path string) (pipz.Chainable[T], error) {
	if node.Predicate == "" {
		return nil, fmt.Errorf("%s: filter requires a predicate", path)
	}
	if node.Then == nil {
		return nil, fmt.Errorf("%s: filter requires a then branch", path)
	}

	pm, exists := f.predicates[pipz.Name(node.Predicate)] //nolint:unconvert
	if !exists {
		return nil, fmt.Errorf("%s: predicate '%s' not found", path, node.Predicate)
	}
	predicate := pm.predicate

	then, err := f.buildNode(node.Then, fmt.Sprintf("%s.then", path))
	if err != nil {
		return nil, err
	}

	name := node.Name
	if name == "" {
		name = fmt.Sprintf("filter-%s", node.Predicate)
	}

	// If no else branch, data passes through unchanged when predicate is false
	if node.Else == nil {
		return pipz.NewFilter[T](pipz.Name(name), predicate, then), nil //nolint:unconvert
	}

	// Build else branch and create a custom filter
	elseBranch, err := f.buildNode(node.Else, fmt.Sprintf("%s.else", path))
	if err != nil {
		return nil, err
	}

	// Create a processor that routes based on the predicate
	return pipz.Apply(pipz.Name(name), func(ctx context.Context, data T) (T, error) { //nolint:unconvert
		if predicate(ctx, data) {
			return then.Process(ctx, data)
		}
		return elseBranch.Process(ctx, data)
	}), nil
}

// buildSwitch creates a switch connector from schema.
func (f *Factory[T]) buildSwitch(node *Node, path string) (pipz.Chainable[T], error) {
	if node.Condition == "" {
		return nil, fmt.Errorf("%s: switch requires a condition", path)
	}
	if len(node.Routes) == 0 {
		return nil, fmt.Errorf("%s: switch requires at least one route", path)
	}

	cm, exists := f.conditions[pipz.Name(node.Condition)] //nolint:unconvert
	if !exists {
		return nil, fmt.Errorf("%s: condition '%s' not found", path, node.Condition)
	}
	condition := cm.condition

	name := node.Name
	if name == "" {
		name = fmt.Sprintf("switch-%s", node.Condition)
	}

	// Build all routes
	routes := make(map[string]pipz.Chainable[T])
	for key := range node.Routes {
		routeNode := node.Routes[key]
		route, err := f.buildNode(&routeNode, fmt.Sprintf("%s.routes[%s]", path, key))
		if err != nil {
			return nil, err
		}
		routes[key] = route
	}

	// Create switch
	sw := pipz.NewSwitch(pipz.Name(name), condition) //nolint:unconvert
	for key, route := range routes {
		sw.AddRoute(key, route)
	}

	// If there's a default route, add it as a special key
	if node.Default != nil {
		// For simplicity, we'll document that users should handle default in their condition function
		// by returning a special "default" key when no other condition matches
		defaultRoute, err := f.buildNode(node.Default, fmt.Sprintf("%s.default", path))
		if err != nil {
			return nil, err
		}
		sw.AddRoute("default", defaultRoute)
	}

	return sw, nil
}

// buildCircuitBreaker creates a circuit breaker connector from schema.
func (f *Factory[T]) buildCircuitBreaker(node *Node, path string) (pipz.Chainable[T], error) {
	if node.Child == nil {
		return nil, fmt.Errorf("%s: circuit-breaker requires a child", path)
	}

	child, err := f.buildNode(node.Child, fmt.Sprintf("%s.child", path))
	if err != nil {
		return nil, err
	}

	name := node.Name
	if name == "" {
		name = connectorCircuitBreaker
	}

	// Use failure threshold, default to DefaultCircuitBreakerThreshold if not specified
	failureThreshold := node.FailureThreshold
	if failureThreshold == 0 {
		failureThreshold = DefaultCircuitBreakerThreshold
	}

	// Parse recovery timeout, default to DefaultRecoveryTimeout if not specified
	recoveryTimeout := DefaultRecoveryTimeout
	if node.RecoveryTimeout != "" {
		parsed, err := time.ParseDuration(node.RecoveryTimeout)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid recovery timeout: %w", path, err)
		}
		recoveryTimeout = parsed
	}

	return pipz.NewCircuitBreaker(pipz.Name(name), child, failureThreshold, recoveryTimeout), nil //nolint:unconvert
}

// buildRateLimit creates a rate limiter connector from schema.
func (f *Factory[T]) buildRateLimit(node *Node, path string) (pipz.Chainable[T], error) {
	if node.Child == nil {
		return nil, fmt.Errorf("%s: rate-limit requires a child", path)
	}

	child, err := f.buildNode(node.Child, fmt.Sprintf("%s.child", path))
	if err != nil {
		return nil, err
	}

	name := node.Name
	if name == "" {
		name = connectorRateLimit
	}

	// Use requests per second, default to DefaultRequestsPerSecond if not specified
	requestsPerSecond := node.RequestsPerSecond
	if requestsPerSecond == 0 {
		requestsPerSecond = DefaultRequestsPerSecond
	}

	// Use burst size, default to DefaultBurstSize if not specified
	burstSize := node.BurstSize
	if burstSize == 0 {
		burstSize = DefaultBurstSize
	}

	// Create a sequence that chains rate limiter with the child
	rateLimiter := pipz.NewRateLimiter[T](pipz.Name(name+"_limiter"), requestsPerSecond, burstSize) //nolint:unconvert
	sequence := pipz.NewSequence(pipz.Name(name), rateLimiter, child)                               //nolint:unconvert
	return sequence, nil
}

// buildStream creates a stream effect from schema that can optionally continue processing.
func (f *Factory[T]) buildStream(node *Node, path string) (pipz.Chainable[T], error) {
	if node.Stream == "" {
		return nil, fmt.Errorf("%s: stream node requires a stream name", path)
	}

	channel, exists := f.channels[node.Stream]
	if !exists {
		return nil, fmt.Errorf("%s: channel '%s' not found", path, node.Stream)
	}

	// Parse optional stream timeout
	var streamTimeout time.Duration
	if node.StreamTimeout != "" {
		parsed, err := time.ParseDuration(node.StreamTimeout)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid stream_timeout: %w", path, err)
		}
		streamTimeout = parsed
	}

	// Create the effect that pushes to channel
	var streamEffect pipz.Chainable[T]
	if streamTimeout > 0 {
		streamEffect = pipz.Effect(pipz.Name(fmt.Sprintf("stream:%s", node.Stream)), func(ctx context.Context, item T) error { //nolint:unconvert
			select {
			case channel <- item:
				return nil
			case <-time.After(streamTimeout):
				return fmt.Errorf("stream '%s': write timeout after %v", node.Stream, streamTimeout)
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	} else {
		streamEffect = pipz.Effect(pipz.Name(fmt.Sprintf("stream:%s", node.Stream)), func(ctx context.Context, item T) error { //nolint:unconvert
			select {
			case channel <- item:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}

	// If there are no children, just return the effect
	if node.Child == nil && len(node.Children) == 0 {
		return streamEffect, nil
	}

	// If there are children, create a sequence starting with the stream effect
	children := []pipz.Chainable[T]{streamEffect}

	// Add single child if present
	if node.Child != nil {
		child, err := f.buildNode(node.Child, fmt.Sprintf("%s.child", path))
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}

	// Add multiple children if present
	for i := range node.Children {
		child, err := f.buildNode(&node.Children[i], fmt.Sprintf("%s.children[%d]", path, i))
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}

	name := node.Name
	if name == "" {
		name = fmt.Sprintf("stream:%s", node.Stream)
	}

	return pipz.NewSequence(pipz.Name(name), children...), nil //nolint:unconvert
}

// buildContest creates a contest connector from schema.
func (f *Factory[T]) buildContest(node *Node, path string) (pipz.Chainable[T], error) {
	if len(node.Children) == 0 {
		return nil, fmt.Errorf("%s: contest requires at least one child", path)
	}
	if node.Predicate == "" {
		return nil, fmt.Errorf("%s: contest requires a predicate", path)
	}

	pm, exists := f.predicates[pipz.Name(node.Predicate)] //nolint:unconvert
	if !exists {
		return nil, fmt.Errorf("%s: predicate '%s' not found", path, node.Predicate)
	}
	predicate := pm.predicate

	children := make([]pipz.Chainable[T], 0, len(node.Children))
	for i := range node.Children {
		processor, err := f.buildNode(&node.Children[i], fmt.Sprintf("%s.children[%d]", path, i))
		if err != nil {
			return nil, err
		}
		children = append(children, processor)
	}

	name := node.Name
	if name == "" {
		name = fmt.Sprintf("contest-%s", node.Predicate)
	}

	return pipz.NewContest[T](pipz.Name(name), predicate, children...), nil //nolint:unconvert
}

// buildHandle creates a handle connector from schema.
func (f *Factory[T]) buildHandle(node *Node, path string) (pipz.Chainable[T], error) {
	if node.Child == nil {
		return nil, fmt.Errorf("%s: handle requires a child", path)
	}
	if node.ErrorHandler == "" {
		return nil, fmt.Errorf("%s: handle requires an error_handler", path)
	}

	hm, exists := f.errorHandlers[pipz.Name(node.ErrorHandler)] //nolint:unconvert
	if !exists {
		return nil, fmt.Errorf("%s: error handler '%s' not found", path, node.ErrorHandler)
	}
	handler := hm.handler

	child, err := f.buildNode(node.Child, fmt.Sprintf("%s.child", path))
	if err != nil {
		return nil, err
	}

	name := node.Name
	if name == "" {
		name = fmt.Sprintf("handle-%s", node.ErrorHandler)
	}

	return pipz.NewHandle(pipz.Name(name), child, handler), nil //nolint:unconvert
}

// buildScaffold creates a scaffold connector from schema.
func (f *Factory[T]) buildScaffold(node *Node, path string) (pipz.Chainable[T], error) {
	if len(node.Children) == 0 {
		return nil, fmt.Errorf("%s: scaffold requires at least one child", path)
	}

	children := make([]pipz.Chainable[T], 0, len(node.Children))
	for i := range node.Children {
		processor, err := f.buildNode(&node.Children[i], fmt.Sprintf("%s.children[%d]", path, i))
		if err != nil {
			return nil, err
		}
		children = append(children, processor)
	}

	name := node.Name
	if name == "" {
		name = connectorScaffold
	}

	return pipz.NewScaffold[T](pipz.Name(name), children...), nil //nolint:unconvert
}

// buildWorkerPool creates a worker pool connector from schema.
func (f *Factory[T]) buildWorkerPool(node *Node, path string) (pipz.Chainable[T], error) {
	if len(node.Children) == 0 {
		return nil, fmt.Errorf("%s: worker-pool requires at least one child", path)
	}

	children := make([]pipz.Chainable[T], 0, len(node.Children))
	for i := range node.Children {
		processor, err := f.buildNode(&node.Children[i], fmt.Sprintf("%s.children[%d]", path, i))
		if err != nil {
			return nil, err
		}
		children = append(children, processor)
	}

	name := node.Name
	if name == "" {
		name = connectorWorkerPool
	}

	workers := node.Workers
	if workers == 0 {
		workers = DefaultWorkerCount
	}

	return pipz.NewWorkerPool[T](pipz.Name(name), workers, children...), nil //nolint:unconvert
}
