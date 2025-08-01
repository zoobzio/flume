package flume

import (
	"context"
	"fmt"
	"time"

	"github.com/zoobzio/pipz"
)

// Connector type constants.
const (
	connectorSequence   = "sequence"
	connectorConcurrent = "concurrent"
	connectorRace       = "race"
	connectorFallback   = "fallback"
	connectorRetry      = "retry"
	connectorTimeout    = "timeout"
)

// buildSequence creates a sequence connector from schema.
func (f *Factory[T]) buildSequence(node *Node) (pipz.Chainable[T], error) {
	if len(node.Children) == 0 {
		return nil, fmt.Errorf("sequence requires at least one child")
	}

	children := make([]pipz.Chainable[T], 0, len(node.Children))
	for i := range node.Children {
		processor, err := f.buildNode(&node.Children[i])
		if err != nil {
			return nil, fmt.Errorf("failed to build child %d: %w", i, err)
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
func (f *Factory[T]) buildConcurrent(node *Node) (pipz.Chainable[T], error) {
	if len(node.Children) == 0 {
		return nil, fmt.Errorf("concurrent requires at least one child")
	}

	children := make([]pipz.Chainable[T], 0, len(node.Children))
	for i := range node.Children {
		processor, err := f.buildNode(&node.Children[i])
		if err != nil {
			return nil, fmt.Errorf("failed to build child %d: %w", i, err)
		}
		children = append(children, processor)
	}

	name := node.Name
	if name == "" {
		name = connectorConcurrent
	}

	return pipz.NewConcurrent[T](pipz.Name(name), children...), nil //nolint:unconvert
}

// buildRace creates a race connector from schema.
func (f *Factory[T]) buildRace(node *Node) (pipz.Chainable[T], error) {
	if len(node.Children) == 0 {
		return nil, fmt.Errorf("race requires at least one child")
	}

	children := make([]pipz.Chainable[T], 0, len(node.Children))
	for i := range node.Children {
		processor, err := f.buildNode(&node.Children[i])
		if err != nil {
			return nil, fmt.Errorf("failed to build child %d: %w", i, err)
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
func (f *Factory[T]) buildFallback(node *Node) (pipz.Chainable[T], error) {
	if len(node.Children) != 2 {
		return nil, fmt.Errorf("fallback requires exactly 2 children")
	}

	primary, err := f.buildNode(&node.Children[0])
	if err != nil {
		return nil, fmt.Errorf("failed to build primary: %w", err)
	}

	fallback, err := f.buildNode(&node.Children[1])
	if err != nil {
		return nil, fmt.Errorf("failed to build fallback: %w", err)
	}

	name := node.Name
	if name == "" {
		name = connectorFallback
	}

	return pipz.NewFallback(pipz.Name(name), primary, fallback), nil //nolint:unconvert
}

// buildRetry creates a retry connector from schema.
func (f *Factory[T]) buildRetry(node *Node) (pipz.Chainable[T], error) {
	if node.Child == nil {
		return nil, fmt.Errorf("retry requires a child")
	}

	child, err := f.buildNode(node.Child)
	if err != nil {
		return nil, fmt.Errorf("failed to build child: %w", err)
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
			return nil, fmt.Errorf("invalid backoff duration: %w", err)
		}

		// Use attempts field, default to 3 if not specified
		attempts := node.Attempts
		if attempts == 0 {
			attempts = 3
		}

		return pipz.NewBackoff(pipz.Name(name), child, attempts, backoff), nil //nolint:unconvert
	}

	// Use attempts field, default to 3 if not specified
	attempts := node.Attempts
	if attempts == 0 {
		attempts = 3
	}

	return pipz.NewRetry(pipz.Name(name), child, attempts), nil //nolint:unconvert
}

// buildTimeout creates a timeout connector from schema.
func (f *Factory[T]) buildTimeout(node *Node) (pipz.Chainable[T], error) {
	if node.Child == nil {
		return nil, fmt.Errorf("timeout requires a child")
	}

	child, err := f.buildNode(node.Child)
	if err != nil {
		return nil, fmt.Errorf("failed to build child: %w", err)
	}

	name := node.Name
	if name == "" {
		name = connectorTimeout
	}

	// Parse duration, default to 30s if not specified
	duration := 30 * time.Second
	if node.Duration != "" {
		parsed, err := time.ParseDuration(node.Duration)
		if err != nil {
			return nil, fmt.Errorf("invalid duration: %w", err)
		}
		duration = parsed
	}

	return pipz.NewTimeout(pipz.Name(name), child, duration), nil //nolint:unconvert
}

// buildFilter creates a filter connector from schema.
func (f *Factory[T]) buildFilter(node *Node) (pipz.Chainable[T], error) {
	if node.Predicate == "" {
		return nil, fmt.Errorf("filter requires a predicate")
	}
	if node.Then == nil {
		return nil, fmt.Errorf("filter requires a then branch")
	}

	predicate, exists := f.predicates[pipz.Name(node.Predicate)] //nolint:unconvert
	if !exists {
		return nil, fmt.Errorf("predicate not found: %s", node.Predicate)
	}

	then, err := f.buildNode(node.Then)
	if err != nil {
		return nil, fmt.Errorf("failed to build then branch: %w", err)
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
	elseBranch, err := f.buildNode(node.Else)
	if err != nil {
		return nil, fmt.Errorf("failed to build else branch: %w", err)
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
func (f *Factory[T]) buildSwitch(node *Node) (pipz.Chainable[T], error) {
	if node.Condition == "" {
		return nil, fmt.Errorf("switch requires a condition")
	}
	if len(node.Routes) == 0 {
		return nil, fmt.Errorf("switch requires at least one route")
	}

	condition, exists := f.conditions[pipz.Name(node.Condition)] //nolint:unconvert
	if !exists {
		return nil, fmt.Errorf("condition not found: %s", node.Condition)
	}

	name := node.Name
	if name == "" {
		name = fmt.Sprintf("switch-%s", node.Condition)
	}

	// Build all routes
	routes := make(map[string]pipz.Chainable[T])
	for key := range node.Routes {
		routeNode := node.Routes[key]
		route, err := f.buildNode(&routeNode)
		if err != nil {
			return nil, fmt.Errorf("failed to build route %s: %w", key, err)
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
		defaultRoute, err := f.buildNode(node.Default)
		if err != nil {
			return nil, fmt.Errorf("failed to build default route: %w", err)
		}
		sw.AddRoute("default", defaultRoute)
	}

	return sw, nil
}
