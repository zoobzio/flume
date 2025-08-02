package flume

import (
	"fmt"
	"strings"
	"time"

	"github.com/zoobzio/pipz"
	"github.com/zoobzio/zlog"
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
	zlog.Emit(SchemaValidationStarted, "Schema validation started")

	f.mu.RLock()
	defer f.mu.RUnlock()

	var errors ValidationErrors
	f.validateNode(&schema.Node, []string{"root"}, &errors)

	if len(errors) == 0 {
		zlog.Emit(SchemaValidationCompleted, "Schema validation completed",
			zlog.Duration("duration", time.Since(start)))
		return nil
	}

	zlog.Emit(SchemaValidationFailed, "Schema validation failed",
		zlog.Int("error_count", len(errors)),
		zlog.Duration("duration", time.Since(start)))
	return errors
}

// validateNode recursively validates a node and its children.
func (f *Factory[T]) validateNode(node *Node, path []string, errors *ValidationErrors) {
	f.validateNodeWithVisited(node, path, errors, make(map[string]bool))
}

// validateNodeWithVisited recursively validates a node with cycle detection.
func (f *Factory[T]) validateNodeWithVisited(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	// Check for empty node
	if node.Ref == "" && node.Type == "" {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "node must have either 'ref' or 'type'",
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
	case "sequence":
		f.validateSequence(node, path, errors, visitedRefs)
	case "parallel", "concurrent":
		f.validateConcurrent(node, path, errors, visitedRefs)
	case "race":
		f.validateRace(node, path, errors, visitedRefs)
	case "fallback":
		f.validateFallback(node, path, errors, visitedRefs)
	case "retry":
		f.validateRetry(node, path, errors, visitedRefs)
	case "timeout":
		f.validateTimeout(node, path, errors, visitedRefs)
	case "filter":
		f.validateFilter(node, path, errors, visitedRefs)
	case "switch":
		f.validateSwitch(node, path, errors, visitedRefs)
	case "circuit-breaker":
		f.validateCircuitBreaker(node, path, errors, visitedRefs)
	case "rate-limit":
		f.validateRateLimit(node, path, errors, visitedRefs)
	default:
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("unknown node type: %s", node.Type),
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

// validateConcurrent validates a concurrent/parallel node.
func (f *Factory[T]) validateConcurrent(node *Node, path []string, errors *ValidationErrors, visitedRefs map[string]bool) {
	if len(node.Children) == 0 {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "concurrent requires at least one child",
		})
		return
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

	// Validate primary
	primaryPath := append(append([]string(nil), path...), "children[0](primary)")
	primaryCopy := node.Children[0]
	f.validateNodeWithVisited(&primaryCopy, primaryPath, errors, visitedRefs)

	// Validate fallback
	if len(node.Children) > 1 {
		fallbackPath := append(append([]string(nil), path...), "children[1](fallback)")
		fallbackCopy := node.Children[1]
		f.validateNodeWithVisited(&fallbackCopy, fallbackPath, errors, visitedRefs)
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

	// Check then branch
	if node.Then == nil {
		*errors = append(*errors, ValidationError{
			Path:    path,
			Message: "filter requires a 'then' branch",
		})
	} else {
		thenPath := append(append([]string(nil), path...), "then")
		f.validateNodeWithVisited(node.Then, thenPath, errors, visitedRefs)
	}

	// Validate optional else branch
	if node.Else != nil {
		elsePath := append(append([]string(nil), path...), "else")
		f.validateNodeWithVisited(node.Else, elsePath, errors, visitedRefs)
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
		// Validate each route
		for key := range node.Routes {
			routePath := append(append([]string(nil), path...), fmt.Sprintf("routes.%s", key))
			route := node.Routes[key]
			f.validateNodeWithVisited(&route, routePath, errors, visitedRefs)
		}
	}

	// Validate optional default
	if node.Default != nil {
		defaultPath := append(append([]string(nil), path...), "default")
		f.validateNodeWithVisited(node.Default, defaultPath, errors, visitedRefs)
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
				Message: fmt.Sprintf("invalid recovery timeout: %v", err),
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
