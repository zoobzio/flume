package flume

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/zoobzio/capitan"
)

// ValidationError represents a schema validation error with detailed context.
type ValidationError struct {
	Message string
	Path    []string
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

// refChecker provides optional reference validation callbacks.
// When a callback is nil, that reference type is not validated.
type refChecker struct {
	processor    func(string) bool
	predicate    func(string) bool
	condition    func(string) bool
	reducer      func(string) bool
	errorHandler func(string) bool
	channel      func(string) bool
}

// validationCtx holds state for recursive validation.
type validationCtx struct {
	errors      *ValidationErrors
	visitedRefs map[string]bool
	checker     refChecker
}

// addError appends a validation error with the given path and message.
func (vc *validationCtx) addError(path []string, message string) {
	*vc.errors = append(*vc.errors, ValidationError{Path: path, Message: message})
}

// copyForBranch creates a copy of the context for mutually exclusive branches.
func (vc *validationCtx) copyForBranch() *validationCtx {
	visited := make(map[string]bool, len(vc.visitedRefs))
	for k, v := range vc.visitedRefs {
		visited[k] = v
	}
	return &validationCtx{
		errors:      vc.errors,
		visitedRefs: visited,
		checker:     vc.checker,
	}
}

// ValidateSchema validates a schema without building it.
// Returns nil if valid, or ValidationErrors containing all issues found.
func (f *Factory[T]) ValidateSchema(schema Schema) error {
	start := time.Now()
	capitan.Emit(context.Background(), SchemaValidationStarted)

	f.mu.RLock()
	defer f.mu.RUnlock()

	var errors ValidationErrors
	ctx := &validationCtx{
		errors:      &errors,
		visitedRefs: make(map[string]bool),
		checker: refChecker{
			processor:    func(name string) bool { _, ok := f.processors[name]; return ok },
			predicate:    func(name string) bool { _, ok := f.predicates[name]; return ok },
			condition:    func(name string) bool { _, ok := f.conditions[name]; return ok },
			reducer:      func(name string) bool { _, ok := f.reducers[name]; return ok },
			errorHandler: func(name string) bool { _, ok := f.errorHandlers[name]; return ok },
			channel:      func(name string) bool { _, ok := f.channels[name]; return ok },
		},
	}

	validateNode(ctx, &schema.Node, []string{"root"})

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

// validateNode recursively validates a node with cycle detection.
func validateNode(ctx *validationCtx, node *Node, path []string) {
	// Check for empty node
	if node.Ref == "" && node.Type == "" && node.Stream == "" {
		ctx.addError(path, "empty node - must have either ref, type, or stream")
		return
	}

	// Check for conflicting ref and type
	if node.Ref != "" && node.Type != "" {
		ctx.addError(path, "node cannot have both 'ref' and 'type'")
		return
	}

	// Handle stream nodes first
	if node.Stream != "" {
		validateStream(ctx, node, path)
		return
	}

	// Validate processor reference
	if node.Ref != "" {
		// Check for cycles (only when doing full validation)
		if ctx.visitedRefs != nil && ctx.visitedRefs[node.Ref] {
			ctx.addError(path, fmt.Sprintf("circular reference detected: '%s' creates a cycle", node.Ref))
			return
		}

		// Check processor exists (only when checker is configured)
		if ctx.checker.processor != nil && !ctx.checker.processor(node.Ref) {
			ctx.addError(path, fmt.Sprintf("processor '%s' not found", node.Ref))
		}

		// Mark this ref as visited for cycle detection
		if ctx.visitedRefs != nil {
			ctx.visitedRefs[node.Ref] = true
		}
		return
	}

	// Validate connector type
	switch node.Type {
	case connectorSequence:
		validateSequence(ctx, node, path)
	case connectorConcurrent:
		validateConcurrent(ctx, node, path)
	case connectorRace:
		validateRace(ctx, node, path)
	case connectorFallback:
		validateFallback(ctx, node, path)
	case connectorRetry:
		validateRetry(ctx, node, path)
	case connectorTimeout:
		validateTimeout(ctx, node, path)
	case connectorFilter:
		validateFilter(ctx, node, path)
	case connectorSwitch:
		validateSwitch(ctx, node, path)
	case connectorCircuitBreaker:
		validateCircuitBreaker(ctx, node, path)
	case connectorRateLimit:
		validateRateLimit(ctx, node, path)
	case connectorContest:
		validateContest(ctx, node, path)
	case connectorHandle:
		validateHandle(ctx, node, path)
	case connectorScaffold:
		validateScaffold(ctx, node, path)
	case connectorWorkerPool:
		validateWorkerPool(ctx, node, path)
	default:
		ctx.addError(path, fmt.Sprintf("unknown node type '%s'", node.Type))
	}
}

// validateChildren validates each child node with shared visited refs.
func validateChildren(ctx *validationCtx, children []Node, path []string) {
	for i := range children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		validateNode(ctx, &children[i], childPath)
	}
}

// validateChildrenBranched validates each child with independent visited refs (mutually exclusive branches).
func validateChildrenBranched(ctx *validationCtx, children []Node, path []string) {
	for i := range children {
		childPath := append(append([]string(nil), path...), fmt.Sprintf("children[%d]", i))
		validateNode(ctx.copyForBranch(), &children[i], childPath)
	}
}

func validateSequence(ctx *validationCtx, node *Node, path []string) {
	if len(node.Children) == 0 {
		ctx.addError(path, "sequence requires at least one child")
		return
	}
	validateChildren(ctx, node.Children, path)
}

func validateConcurrent(ctx *validationCtx, node *Node, path []string) {
	if len(node.Children) == 0 {
		ctx.addError(path, "concurrent requires at least one child")
		return
	}

	// Validate reducer if specified
	if node.Reducer != "" && ctx.checker.reducer != nil && !ctx.checker.reducer(node.Reducer) {
		ctx.addError(path, fmt.Sprintf("reducer '%s' not found", node.Reducer))
	}

	validateChildren(ctx, node.Children, path)
}

func validateRace(ctx *validationCtx, node *Node, path []string) {
	if len(node.Children) == 0 {
		ctx.addError(path, "race requires at least one child")
		return
	}
	validateChildren(ctx, node.Children, path)
}

func validateFallback(ctx *validationCtx, node *Node, path []string) {
	if len(node.Children) != 2 {
		ctx.addError(path, "fallback requires exactly 2 children")
		return
	}

	// Primary and fallback are mutually exclusive branches
	primaryPath := append(append([]string(nil), path...), "children[0](primary)")
	validateNode(ctx.copyForBranch(), &node.Children[0], primaryPath)

	fallbackPath := append(append([]string(nil), path...), "children[1](fallback)")
	validateNode(ctx.copyForBranch(), &node.Children[1], fallbackPath)
}

func validateRetry(ctx *validationCtx, node *Node, path []string) {
	if node.Child == nil {
		ctx.addError(path, "retry requires a child")
		return
	}

	if node.Attempts < 0 {
		ctx.addError(append(append([]string(nil), path...), "attempts"), "attempts must be positive")
	}

	if node.Backoff != "" {
		if _, err := time.ParseDuration(node.Backoff); err != nil {
			ctx.addError(append(append([]string(nil), path...), "backoff"), fmt.Sprintf("invalid backoff duration: %s", err))
		}
	}

	childPath := append(append([]string(nil), path...), "child")
	validateNode(ctx, node.Child, childPath)
}

func validateTimeout(ctx *validationCtx, node *Node, path []string) {
	if node.Child == nil {
		ctx.addError(path, "timeout requires a child")
		return
	}

	if node.Duration != "" {
		if _, err := time.ParseDuration(node.Duration); err != nil {
			ctx.addError(append(append([]string(nil), path...), "duration"), fmt.Sprintf("invalid duration: %s", err))
		}
	}

	childPath := append(append([]string(nil), path...), "child")
	validateNode(ctx, node.Child, childPath)
}

func validateFilter(ctx *validationCtx, node *Node, path []string) {
	if node.Predicate == "" {
		ctx.addError(path, "filter requires a predicate")
	} else if ctx.checker.predicate != nil && !ctx.checker.predicate(node.Predicate) {
		ctx.addError(path, fmt.Sprintf("predicate '%s' not found", node.Predicate))
	}

	// Then and else are mutually exclusive branches
	if node.Then == nil {
		ctx.addError(path, "filter requires a 'then' branch")
	} else {
		thenPath := append(append([]string(nil), path...), "then")
		validateNode(ctx.copyForBranch(), node.Then, thenPath)
	}

	if node.Else != nil {
		elsePath := append(append([]string(nil), path...), "else")
		validateNode(ctx.copyForBranch(), node.Else, elsePath)
	}
}

func validateSwitch(ctx *validationCtx, node *Node, path []string) {
	if node.Condition == "" {
		ctx.addError(path, "switch requires a condition")
	} else if ctx.checker.condition != nil && !ctx.checker.condition(node.Condition) {
		ctx.addError(path, fmt.Sprintf("condition '%s' not found", node.Condition))
	}

	if len(node.Routes) == 0 {
		ctx.addError(path, "switch requires at least one route")
	} else {
		// Routes are mutually exclusive branches
		for key := range node.Routes {
			routePath := append(append([]string(nil), path...), fmt.Sprintf("routes.%s", key))
			route := node.Routes[key]
			validateNode(ctx.copyForBranch(), &route, routePath)
		}
	}

	if node.Default != nil {
		defaultPath := append(append([]string(nil), path...), "default")
		validateNode(ctx.copyForBranch(), node.Default, defaultPath)
	}
}

func validateCircuitBreaker(ctx *validationCtx, node *Node, path []string) {
	if node.Child == nil {
		ctx.addError(path, "circuit-breaker requires a child")
		return
	}

	if node.RecoveryTimeout != "" {
		if _, err := time.ParseDuration(node.RecoveryTimeout); err != nil {
			ctx.addError(path, fmt.Sprintf("invalid recovery timeout: %s", err))
		}
	}

	childPath := append(append([]string(nil), path...), "child")
	validateNode(ctx, node.Child, childPath)
}

func validateRateLimit(ctx *validationCtx, node *Node, path []string) {
	if node.Child == nil {
		ctx.addError(path, "rate-limit requires a child")
		return
	}

	if node.RequestsPerSecond < 0 {
		ctx.addError(path, "requests_per_second must be non-negative")
	}

	if node.BurstSize < 0 {
		ctx.addError(path, "burst_size must be non-negative")
	}

	childPath := append(append([]string(nil), path...), "child")
	validateNode(ctx, node.Child, childPath)
}

func validateStream(ctx *validationCtx, node *Node, path []string) {
	if node.Stream == "" {
		ctx.addError(path, "stream node requires a stream name")
		return
	}

	// Check if channel is registered
	if ctx.checker.channel != nil && !ctx.checker.channel(node.Stream) {
		ctx.addError(path, fmt.Sprintf("channel '%s' not found", node.Stream))
	}

	if node.StreamTimeout != "" {
		if _, err := time.ParseDuration(node.StreamTimeout); err != nil {
			ctx.addError(append(append([]string(nil), path...), "stream_timeout"), fmt.Sprintf("invalid stream_timeout: %s", err))
		}
	}

	if node.Child != nil && len(node.Children) > 0 {
		ctx.addError(path, "stream node has both 'child' and 'children' specified; prefer using only one")
	}

	if node.Child != nil {
		childPath := append(append([]string(nil), path...), "child")
		validateNode(ctx, node.Child, childPath)
	}

	validateChildren(ctx, node.Children, path)

	if node.Type != "" || node.Ref != "" {
		ctx.addError(path, "stream node should not have type or ref fields")
	}
}

func validateContest(ctx *validationCtx, node *Node, path []string) {
	if node.Predicate == "" {
		ctx.addError(path, "contest requires a predicate")
	} else if ctx.checker.predicate != nil && !ctx.checker.predicate(node.Predicate) {
		ctx.addError(path, fmt.Sprintf("predicate '%s' not found", node.Predicate))
	}

	if len(node.Children) == 0 {
		ctx.addError(path, "contest requires at least one child")
		return
	}

	// Contest children are mutually exclusive branches
	validateChildrenBranched(ctx, node.Children, path)
}

func validateHandle(ctx *validationCtx, node *Node, path []string) {
	if node.Child == nil {
		ctx.addError(path, "handle requires a child")
		return
	}

	if node.ErrorHandler == "" {
		ctx.addError(path, "handle requires an error_handler")
	} else if ctx.checker.errorHandler != nil && !ctx.checker.errorHandler(node.ErrorHandler) {
		ctx.addError(path, fmt.Sprintf("error handler '%s' not found", node.ErrorHandler))
	}

	childPath := append(append([]string(nil), path...), "child")
	validateNode(ctx, node.Child, childPath)
}

func validateScaffold(ctx *validationCtx, node *Node, path []string) {
	if len(node.Children) == 0 {
		ctx.addError(path, "scaffold requires at least one child")
		return
	}
	validateChildren(ctx, node.Children, path)
}

func validateWorkerPool(ctx *validationCtx, node *Node, path []string) {
	if len(node.Children) == 0 {
		ctx.addError(path, "worker-pool requires at least one child")
		return
	}

	if node.Workers < 0 {
		ctx.addError(append(append([]string(nil), path...), "workers"), "workers must be non-negative")
	}

	validateChildren(ctx, node.Children, path)
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
	ctx := &validationCtx{
		errors:      &errors,
		visitedRefs: nil,    // No cycle detection for structure-only validation
		checker:     refChecker{}, // All nil - skip reference checks
	}

	validateNode(ctx, &schema.Node, []string{"root"})

	if len(errors) == 0 {
		return nil
	}
	return errors
}
