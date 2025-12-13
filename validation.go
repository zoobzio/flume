package flume

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/pipz"
)

// ValidationError represents a schema validation error with detailed context.
type ValidationError struct { //nolint:govet
	Path    []string // Path to the error in the schema tree
	Message string   // Error message
}

func (e ValidationError) Error() string {
	if len(e.Path) == 0 {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", strings.Join(e.Path, "."), e.Message)
}

// ValidationErrors collects multiple validation errors.
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return "no validation errors"
	}
	if len(e) == 1 {
		return e[0].Error()
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d validation errors:\n", len(e)))
	for i, err := range e {
		sb.WriteString(fmt.Sprintf("  %d. %s\n", i+1, err.Error()))
	}
	return sb.String()
}

// ValidateSchema validates a schema without building it.
// Returns nil if valid, or ValidationErrors containing all issues found.
func (f *Factory[T]) ValidateSchema(schema Schema) error {
	start := time.Now()
	capitan.Emit(context.Background(), SchemaValidationStarted)

	f.mu.RLock()
	defer f.mu.RUnlock()

	var errors ValidationErrors
	f.validateNode(&schema.Node, []string{"root"}, &errors)

	if len(errors) == 0 {
		capitan.Emit(context.Background(), SchemaValidationCompleted,
			KeyDuration.Field(time.Since(start)))
		return nil
	}

	capitan.Emit(context.Background(), SchemaValidationFailed,
		KeyErrorCount.Field(len(errors)),
		KeyDuration.Field(time.Since(start)))
	return errors
}

// validateNode recursively validates a node and its children.
func (f *Factory[T]) validateNode(node *Node, path []string, errors *ValidationErrors) {
	f.validateNodeWithVisited(node, path, errors, make(map[string]bool))
}

// copyVisitedRefs creates a shallow copy of the visited refs map.
// Used for mutually exclusive branches (switch routes, filter then/else, fallback)
// where the same processor can validly appear in multiple branches.
func copyVisitedRefs(visited map[string]bool) map[string]bool {
	result := make(map[string]bool, len(visited))
	for k, v := range visited {
		result[k] = v
	}
	return result
}

// validateNodeWithVisited recursively validates a node with cycle detection.
func (f *Factory[T]) validateNodeWithVisited(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	// Check for empty node
	if node.Ref == "" && node.Type == "" && node.Stream == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "empty node - must have either ref, type, or stream",
		})
		return
	}

	// Check for conflicting ref and type
	if node.Ref != "" && node.Type != "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "node cannot have both 'ref' and 'type'",
		})
		return
	}

	// Handle stream nodes first
	if node.Stream != "" {
		f.validateStream(node, path, errors, visitedRefs)
		return
	}

	// Validate processor reference
	if node.Ref != "" {
		// Check for cycles
		if visitedRefs[node.Ref] {
			*errors = append(*errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("circular reference detected: '%s' creates a cycle", node.Ref),
			})
			return
		}

		if _, exists := f.processors[pipz.Name(node.Ref)]; !exists { //nolint:unconvert
			*errors = append(*errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("processor '%s' not found", node.Ref),
			})
		}

		// Mark this ref as visited for cycle detection
		visitedRefs[node.Ref] = true
		return // No more validation needed for refs
	}

	// Validate connector type
	switch node.Type {
	case connectorSequence:
		f.validateSequence(node, path, errors, visitedRefs)
	case connectorConcurrent:
		f.validateConcurrent(node, path, errors, visitedRefs)
	case connectorRace:
		f.validateRace(node, path, errors, visitedRefs)
	case connectorFallback:
		f.validateFallback(node, path, errors, visitedRefs)
	case connectorRetry:
		f.validateRetry(node, path, errors, visitedRefs)
	case connectorTimeout:
		f.validateTimeout(node, path, errors, visitedRefs)
	case connectorFilter:
		f.validateFilter(node, path, errors, visitedRefs)
	case connectorSwitch:
		f.validateSwitch(node, path, errors, visitedRefs)
	case connectorCircuitBreaker:
		f.validateCircuitBreaker(node, path, errors, visitedRefs)
	case connectorRateLimit:
		f.validateRateLimit(node, path, errors, visitedRefs)
	case connectorContest:
		f.validateContest(node, path, errors, visitedRefs)
	case connectorHandle:
		f.validateHandle(node, path, errors, visitedRefs)
	case connectorScaffold:
		f.validateScaffold(node, path, errors, visitedRefs)
	case connectorWorkerPool:
		f.validateWorkerPool(node, path, errors, visitedRefs)
	default:
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("unknown node type '%s'", node.Type),
		})
	}
}

// validateSequence validates a sequence node.
func (f *Factory[T]) validateSequence(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "sequence requires at least one child",
		})
		return
	}

	// Validate each child
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		f.validateNodeWithVisited(&node.Children[i], childPath, errors, visitedRefs)
	}
}

// validateConcurrent validates a concurrent node.
func (f *Factory[T]) validateConcurrent(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "concurrent requires at least one child",
		})
		return
	}

	// Validate reducer if specified
	if node.Reducer != "" {
		if _, exists := f.reducers[pipz.Name(node.Reducer)]; !exists { //nolint:unconvert
			*errors = append(*errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("reducer '%s' not found", node.Reducer),
			})
		}
	}

	// Validate each child
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		f.validateNodeWithVisited(&node.Children[i], childPath, errors, visitedRefs)
	}
}

// validateRace validates a race node.
func (f *Factory[T]) validateRace(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "race requires at least one child",
		})
		return
	}

	// Validate each child
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		f.validateNodeWithVisited(&node.Children[i], childPath, errors, visitedRefs)
	}
}

// validateFallback validates a fallback node.
func (f *Factory[T]) validateFallback(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if len(node.Children) != 2 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "fallback requires exactly 2 children",
		})
		return
	}

	// Validate primary with its own visited set copy.
	// Primary and fallback are mutually exclusive branches
	// (fallback only runs if primary fails).
	primaryPath := append(append([]string(nil), path...), "children[0](primary)")
	primaryCopy := node.Children[0]
	primaryVisited := copyVisitedRefs(visitedRefs)
	f.validateNodeWithVisited(&primaryCopy, primaryPath, errors, primaryVisited)

	// Validate fallback with its own visited set copy
	if len(node.Children) > 1 {
		fallbackPath := append(append([]string(nil), path...), "children[1](fallback)")
		fallbackCopy := node.Children[1]
		fallbackVisited := copyVisitedRefs(visitedRefs)
		f.validateNodeWithVisited(&fallbackCopy, fallbackPath, errors, fallbackVisited)
	}
}

// validateRetry validates a retry node.
func (f *Factory[T]) validateRetry(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if node.Child == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "retry requires a child",
		})
		return
	}

	// Validate attempts (0 means default, negative is error)
	if node.Attempts < 0 {
		*errors = append(*errors, ValidationError{
			Path:    append(append([]string(nil), path...), "attempts"),
			Message: "attempts must be positive",
		})
	}

	// Validate backoff duration if specified
	if node.Backoff != "" {
		if _, err := time.ParseDuration(node.Backoff); err != nil {
			*errors = append(*errors, ValidationError{
				Path:    append(append([]string(nil), path...), "backoff"),
				Message: fmt.Sprintf("invalid backoff duration: %s", err),
			})
		}
	}

	// Validate child
	childPath := append(append([]string(nil), path...), "child")
	f.validateNodeWithVisited(node.Child, childPath, errors, visitedRefs)
}

// validateTimeout validates a timeout node.
func (f *Factory[T]) validateTimeout(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if node.Child == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "timeout requires a child",
		})
		return
	}

	// Validate duration if specified
	if node.Duration != "" {
		if _, err := time.ParseDuration(node.Duration); err != nil {
			*errors = append(*errors, ValidationError{
				Path:    append(append([]string(nil), path...), "duration"),
				Message: fmt.Sprintf("invalid duration: %s", err),
			})
		}
	}

	// Validate child
	childPath := append(append([]string(nil), path...), "child")
	f.validateNodeWithVisited(node.Child, childPath, errors, visitedRefs)
}

// validateFilter validates a filter node.
func (f *Factory[T]) validateFilter(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	// Check predicate
	if node.Predicate == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "filter requires a predicate",
		})
	} else if _, exists := f.predicates[pipz.Name(node.Predicate)]; !exists { //nolint:unconvert
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("predicate '%s' not found", node.Predicate),
		})
	}

	// Check then branch with its own visited set copy.
	// Then and else are mutually exclusive branches.
	if node.Then == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "filter requires a 'then' branch",
		})
	} else {
		thenPath := append(append([]string(nil), path...), "then")
		thenVisited := copyVisitedRefs(visitedRefs)
		f.validateNodeWithVisited(node.Then, thenPath, errors, thenVisited)
	}

	// Validate optional else branch with its own visited set copy
	if node.Else != nil {
		elsePath := append(append([]string(nil), path...), "else")
		elseVisited := copyVisitedRefs(visitedRefs)
		f.validateNodeWithVisited(node.Else, elsePath, errors, elseVisited)
	}
}

// validateSwitch validates a switch node.
func (f *Factory[T]) validateSwitch(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	// Check condition
	if node.Condition == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "switch requires a condition",
		})
	} else if _, exists := f.conditions[pipz.Name(node.Condition)]; !exists { //nolint:unconvert
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("condition '%s' not found", node.Condition),
		})
	}

	// Check routes
	if len(node.Routes) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "switch requires at least one route",
		})
	} else {
		// Validate each route with its own visited set copy.
		// Routes are mutually exclusive branches, so the same processor
		// can validly appear in multiple routes without being a cycle.
		for key := range node.Routes {
			routePath := append(append([]string(nil), path...), fmt.Sprintf("routes.%s", key))
			route := node.Routes[key]
			routeVisited := copyVisitedRefs(visitedRefs)
			f.validateNodeWithVisited(&route, routePath, errors, routeVisited)
		}
	}

	// Validate optional default with its own visited set copy
	if node.Default != nil {
		defaultPath := append(append([]string(nil), path...), "default")
		defaultVisited := copyVisitedRefs(visitedRefs)
		f.validateNodeWithVisited(node.Default, defaultPath, errors, defaultVisited)
	}
}

// validateCircuitBreaker validates a circuit breaker node.
func (f *Factory[T]) validateCircuitBreaker(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if node.Child == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "circuit-breaker requires a child",
		})
		return
	}

	// Validate recovery timeout if specified
	if node.RecoveryTimeout != "" {
		if _, err := time.ParseDuration(node.RecoveryTimeout); err != nil {
			*errors = append(*errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("invalid recovery timeout: %s", err),
			})
		}
	}

	// Validate child
	childPath := append(append([]string(nil), path...), "child")
	f.validateNodeWithVisited(node.Child, childPath, errors, visitedRefs)
}

// validateRateLimit validates a rate limiter node.
func (f *Factory[T]) validateRateLimit(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if node.Child == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "rate-limit requires a child",
		})
		return
	}

	// Validate requests per second if specified
	if node.RequestsPerSecond < 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "requests_per_second must be non-negative",
		})
	}

	// Validate burst size if specified
	if node.BurstSize < 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "burst_size must be non-negative",
		})
	}

	// Validate child
	childPath := append(append([]string(nil), path...), "child")
	f.validateNodeWithVisited(node.Child, childPath, errors, visitedRefs)
}

// validateStream validates a stream node.
func (f *Factory[T]) validateStream(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	// Validate stream name
	if node.Stream == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "stream node requires a stream name",
		})
		return
	}

	// Check if channel is registered
	if !f.HasChannel(node.Stream) {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("channel '%s' not found", node.Stream),
		})
	}

	// Validate stream timeout if specified
	if node.StreamTimeout != "" {
		if _, err := time.ParseDuration(node.StreamTimeout); err != nil {
			*errors = append(*errors, ValidationError{
				Path:    append(append([]string(nil), path...), "stream_timeout"),
				Message: fmt.Sprintf("invalid stream_timeout: %s", err),
			})
		}
	}

	// Warn if both child and children are specified (ambiguous)
	if node.Child != nil && len(node.Children) > 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "stream node has both 'child' and 'children' specified; prefer using only one",
		})
	}

	// Validate optional children
	if node.Child != nil {
		childPath := append(append([]string(nil), path...), "child")
		f.validateNodeWithVisited(node.Child, childPath, errors, visitedRefs)
	}

	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		f.validateNodeWithVisited(&node.Children[i], childPath, errors, visitedRefs)
	}

	// Should not have both stream and other fields
	if node.Type != "" || node.Ref != "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "stream node should not have type or ref fields",
		})
	}
}

// validateContest validates a contest node.
func (f *Factory[T]) validateContest(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	// Check predicate
	if node.Predicate == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "contest requires a predicate",
		})
	} else if _, exists := f.predicates[pipz.Name(node.Predicate)]; !exists { //nolint:unconvert
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("predicate '%s' not found", node.Predicate),
		})
	}

	// Check children
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "contest requires at least one child",
		})
		return
	}

	// Validate each child with its own visited set copy (mutually exclusive branches)
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		childVisited := copyVisitedRefs(visitedRefs)
		f.validateNodeWithVisited(&node.Children[i], childPath, errors, childVisited)
	}
}

// validateHandle validates a handle node.
func (f *Factory[T]) validateHandle(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if node.Child == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "handle requires a child",
		})
		return
	}

	// Check error handler
	if node.ErrorHandler == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "handle requires an error_handler",
		})
	} else if _, exists := f.errorHandlers[pipz.Name(node.ErrorHandler)]; !exists { //nolint:unconvert
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("error handler '%s' not found", node.ErrorHandler),
		})
	}

	// Validate child
	childPath := append(append([]string(nil), path...), "child")
	f.validateNodeWithVisited(node.Child, childPath, errors, visitedRefs)
}

// validateScaffold validates a scaffold node.
func (f *Factory[T]) validateScaffold(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "scaffold requires at least one child",
		})
		return
	}

	// Validate each child
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		f.validateNodeWithVisited(&node.Children[i], childPath, errors, visitedRefs)
	}
}

// validateWorkerPool validates a worker pool node.
func (f *Factory[T]) validateWorkerPool(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "worker-pool requires at least one child",
		})
		return
	}

	// Validate workers count if specified
	if node.Workers < 0 {
		*errors = append(*errors, ValidationError{
			Path:    append(append([]string(nil), path...), "workers"),
			Message: "workers must be non-negative",
		})
	}

	// Validate each child
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		f.validateNodeWithVisited(&node.Children[i], childPath, errors, visitedRefs)
	}
}

// ValidateSchemaStructure validates schema syntax without requiring registered components.
// Use this for CI/CD schema linting where a configured factory is not available.
// Returns nil if valid, or ValidationErrors containing all structural issues found.
//
// This validates:
//   - Node structure (ref vs type exclusivity, non-empty nodes)
//   - Connector constraints (required children, child counts)
//   - Configuration values (valid durations, positive numbers)
//   - Unknown node types
//
// This does NOT validate:
//   - Processor references exist
//   - Predicate references exist
//   - Condition references exist
//   - Reducer references exist
//   - Error handler references exist
//   - Channel references exist
func ValidateSchemaStructure(schema Schema) error {
	var errors ValidationErrors
	validateNodeStructure(&schema.Node, []string{"root"}, &errors)

	if len(errors) == 0 {
		return nil
	}
	return errors
}

// validateNodeStructure recursively validates node structure without checking references.
func validateNodeStructure(node *Node, path []string, errors *ValidationErrors) {
	// Check for empty node
	if node.Ref == "" && node.Type == "" && node.Stream == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "empty node - must have either ref, type, or stream",
		})
		return
	}

	// Check for conflicting ref and type
	if node.Ref != "" && node.Type != "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "node cannot have both 'ref' and 'type'",
		})
		return
	}

	// Handle stream nodes
	if node.Stream != "" {
		validateStreamStructure(node, path, errors)
		return
	}

	// Processor references are structurally valid (existence checked elsewhere)
	if node.Ref != "" {
		return
	}

	// Validate connector type structure
	switch node.Type {
	case connectorSequence:
		validateSequenceStructure(node, path, errors)
	case connectorConcurrent:
		validateConcurrentStructure(node, path, errors)
	case connectorRace:
		validateRaceStructure(node, path, errors)
	case connectorFallback:
		validateFallbackStructure(node, path, errors)
	case connectorRetry:
		validateRetryStructure(node, path, errors)
	case connectorTimeout:
		validateTimeoutStructure(node, path, errors)
	case connectorFilter:
		validateFilterStructure(node, path, errors)
	case connectorSwitch:
		validateSwitchStructure(node, path, errors)
	case connectorCircuitBreaker:
		validateCircuitBreakerStructure(node, path, errors)
	case connectorRateLimit:
		validateRateLimitStructure(node, path, errors)
	case connectorContest:
		validateContestStructure(node, path, errors)
	case connectorHandle:
		validateHandleStructure(node, path, errors)
	case connectorScaffold:
		validateScaffoldStructure(node, path, errors)
	case connectorWorkerPool:
		validateWorkerPoolStructure(node, path, errors)
	default:
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("unknown node type '%s'", node.Type),
		})
	}
}

func validateSequenceStructure(node *Node, path []string, errors *ValidationErrors) {
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "sequence requires at least one child",
		})
		return
	}
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		validateNodeStructure(&node.Children[i], childPath, errors)
	}
}

func validateConcurrentStructure(node *Node, path []string, errors *ValidationErrors) {
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "concurrent requires at least one child",
		})
		return
	}
	// Reducer reference is not validated here (existence checked elsewhere)
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		validateNodeStructure(&node.Children[i], childPath, errors)
	}
}

func validateRaceStructure(node *Node, path []string, errors *ValidationErrors) {
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "race requires at least one child",
		})
		return
	}
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		validateNodeStructure(&node.Children[i], childPath, errors)
	}
}

func validateFallbackStructure(node *Node, path []string, errors *ValidationErrors) {
	if len(node.Children) != 2 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "fallback requires exactly 2 children",
		})
		return
	}
	primaryPath := append(append([]string(nil), path...), "children[0](primary)")
	validateNodeStructure(&node.Children[0], primaryPath, errors)
	fallbackPath := append(append([]string(nil), path...), "children[1](fallback)")
	validateNodeStructure(&node.Children[1], fallbackPath, errors)
}

func validateRetryStructure(node *Node, path []string, errors *ValidationErrors) {
	if node.Child == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "retry requires a child",
		})
		return
	}
	if node.Attempts < 0 {
		*errors = append(*errors, ValidationError{
			Path:    append(append([]string(nil), path...), "attempts"),
			Message: "attempts must be positive",
		})
	}
	if node.Backoff != "" {
		if _, err := time.ParseDuration(node.Backoff); err != nil {
			*errors = append(*errors, ValidationError{
				Path:    append(append([]string(nil), path...), "backoff"),
				Message: fmt.Sprintf("invalid backoff duration: %s", err),
			})
		}
	}
	childPath := append(append([]string(nil), path...), "child")
	validateNodeStructure(node.Child, childPath, errors)
}

func validateTimeoutStructure(node *Node, path []string, errors *ValidationErrors) {
	if node.Child == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "timeout requires a child",
		})
		return
	}
	if node.Duration != "" {
		if _, err := time.ParseDuration(node.Duration); err != nil {
			*errors = append(*errors, ValidationError{
				Path:    append(append([]string(nil), path...), "duration"),
				Message: fmt.Sprintf("invalid duration: %s", err),
			})
		}
	}
	childPath := append(append([]string(nil), path...), "child")
	validateNodeStructure(node.Child, childPath, errors)
}

func validateFilterStructure(node *Node, path []string, errors *ValidationErrors) {
	if node.Predicate == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "filter requires a predicate",
		})
	}
	// Predicate reference is not validated here (existence checked elsewhere)
	if node.Then == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "filter requires a 'then' branch",
		})
	} else {
		thenPath := append(append([]string(nil), path...), "then")
		validateNodeStructure(node.Then, thenPath, errors)
	}
	if node.Else != nil {
		elsePath := append(append([]string(nil), path...), "else")
		validateNodeStructure(node.Else, elsePath, errors)
	}
}

func validateSwitchStructure(node *Node, path []string, errors *ValidationErrors) {
	if node.Condition == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "switch requires a condition",
		})
	}
	// Condition reference is not validated here (existence checked elsewhere)
	if len(node.Routes) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "switch requires at least one route",
		})
	} else {
		for key := range node.Routes {
			routePath := append(append([]string(nil), path...), fmt.Sprintf("routes.%s", key))
			route := node.Routes[key]
			validateNodeStructure(&route, routePath, errors)
		}
	}
	if node.Default != nil {
		defaultPath := append(append([]string(nil), path...), "default")
		validateNodeStructure(node.Default, defaultPath, errors)
	}
}

func validateCircuitBreakerStructure(node *Node, path []string, errors *ValidationErrors) {
	if node.Child == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "circuit-breaker requires a child",
		})
		return
	}
	if node.RecoveryTimeout != "" {
		if _, err := time.ParseDuration(node.RecoveryTimeout); err != nil {
			*errors = append(*errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("invalid recovery timeout: %s", err),
			})
		}
	}
	childPath := append(append([]string(nil), path...), "child")
	validateNodeStructure(node.Child, childPath, errors)
}

func validateRateLimitStructure(node *Node, path []string, errors *ValidationErrors) {
	if node.Child == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "rate-limit requires a child",
		})
		return
	}
	if node.RequestsPerSecond < 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "requests_per_second must be non-negative",
		})
	}
	if node.BurstSize < 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "burst_size must be non-negative",
		})
	}
	childPath := append(append([]string(nil), path...), "child")
	validateNodeStructure(node.Child, childPath, errors)
}

func validateContestStructure(node *Node, path []string, errors *ValidationErrors) {
	if node.Predicate == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "contest requires a predicate",
		})
	}
	// Predicate reference is not validated here (existence checked elsewhere)
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "contest requires at least one child",
		})
		return
	}
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		validateNodeStructure(&node.Children[i], childPath, errors)
	}
}

func validateHandleStructure(node *Node, path []string, errors *ValidationErrors) {
	if node.Child == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "handle requires a child",
		})
		return
	}
	if node.ErrorHandler == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "handle requires an error_handler",
		})
	}
	// Error handler reference is not validated here (existence checked elsewhere)
	childPath := append(append([]string(nil), path...), "child")
	validateNodeStructure(node.Child, childPath, errors)
}

func validateScaffoldStructure(node *Node, path []string, errors *ValidationErrors) {
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "scaffold requires at least one child",
		})
		return
	}
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		validateNodeStructure(&node.Children[i], childPath, errors)
	}
}

func validateWorkerPoolStructure(node *Node, path []string, errors *ValidationErrors) {
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "worker-pool requires at least one child",
		})
		return
	}
	if node.Workers < 0 {
		*errors = append(*errors, ValidationError{
			Path:    append(append([]string(nil), path...), "workers"),
			Message: "workers must be non-negative",
		})
	}
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		validateNodeStructure(&node.Children[i], childPath, errors)
	}
}

func validateStreamStructure(node *Node, path []string, errors *ValidationErrors) {
	if node.Stream == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "stream node requires a stream name",
		})
		return
	}
	// Channel reference is not validated here (existence checked elsewhere)
	if node.StreamTimeout != "" {
		if _, err := time.ParseDuration(node.StreamTimeout); err != nil {
			*errors = append(*errors, ValidationError{
				Path:    append(append([]string(nil), path...), "stream_timeout"),
				Message: fmt.Sprintf("invalid stream_timeout: %s", err),
			})
		}
	}
	if node.Child != nil && len(node.Children) > 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "stream node has both 'child' and 'children' specified; prefer using only one",
		})
	}
	if node.Child != nil {
		childPath := append(append([]string(nil), path...), "child")
		validateNodeStructure(node.Child, childPath, errors)
	}
	for i := range node.Children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		validateNodeStructure(&node.Children[i], childPath, errors)
	}
	if node.Type != "" || node.Ref != "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "stream node should not have type or ref fields",
		})
	}
}
