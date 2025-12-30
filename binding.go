package flume

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/pipz"
)

// Default configuration values for Binding.
const (
	DefaultHistoryCap = 10
)

// Binding errors.
var (
	ErrNoHistory       = errors.New("no previous version in history")
	ErrVersionNotFound = errors.New("version not found in history")
)

// pipelineVersion represents a single version of a built pipeline.
type pipelineVersion[T any] struct {
	builtAt  time.Time
	pipeline *pipz.Pipeline[T]
	version  string
	schema   Schema
}

// VersionInfo provides metadata about a pipeline version without exposing internals.
type VersionInfo struct {
	BuiltAt time.Time
	Version string
}

// Binding represents a live, versioned pipeline that can be updated and rolled back.
// It wraps a pipz.Pipeline with schema versioning and history management.
//
// Bindings are created via Factory.Bind() and provide the primary execution interface.
// Each binding maintains a history of previous versions, allowing rollback to earlier
// schema definitions without rebuilding from scratch.
//
// Example:
//
//	orderPipelineID := factory.Identity("order-pipeline", "Main order processing")
//	binding, err := factory.Bind(orderPipelineID, schema)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Execute the pipeline
//	result, err := binding.Process(ctx, order)
//
//	// Update to a new schema version
//	err = binding.Update(newSchema)
//
//	// Rollback if needed
//	err = binding.Rollback()
type Binding[T pipz.Cloner[T]] struct {
	identity   pipz.Identity
	factory    *Factory[T]
	current    *pipelineVersion[T]
	history    []*pipelineVersion[T]
	historyCap int
	mu         sync.RWMutex
}

// BindingOption configures a Binding.
type BindingOption[T pipz.Cloner[T]] func(*Binding[T])

// WithHistoryCap sets the maximum number of versions to retain in history.
// When exceeded, the oldest versions are discarded.
func WithHistoryCap[T pipz.Cloner[T]](capacity int) BindingOption[T] {
	return func(b *Binding[T]) {
		if capacity > 0 {
			b.historyCap = capacity
		}
	}
}

// Process executes the current pipeline version with the given data.
// The pipeline's identity is used for tracing correlation.
func (b *Binding[T]) Process(ctx context.Context, data T) (T, error) {
	b.mu.RLock()
	pipeline := b.current.pipeline
	b.mu.RUnlock()

	return pipeline.Process(ctx, data)
}

// Update builds a new pipeline version from the schema and makes it current.
// The previous version is pushed to history (subject to history cap).
func (b *Binding[T]) Update(schema Schema) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Build the new pipeline using factory's buildNode
	b.factory.mu.RLock()
	chainable, err := b.factory.buildNode(&schema.Node, "root")
	b.factory.mu.RUnlock()

	if err != nil {
		capitan.Emit(context.Background(), SchemaUpdateFailed,
			KeyName.Field(b.identity.Name()),
			KeyError.Field(err.Error()))
		return fmt.Errorf("failed to build schema: %w", err)
	}

	// Wrap in Pipeline for tracing
	pipeline := pipz.NewPipeline(b.identity, chainable)

	// Determine new version
	version := schema.Version
	if version == "" {
		// Auto-increment version
		if b.current != nil {
			if v, err := strconv.Atoi(b.current.version); err == nil {
				version = strconv.Itoa(v + 1)
			} else {
				version = b.current.version + ".1"
			}
		} else {
			version = "1"
		}
	}

	// Push current to history
	if b.current != nil {
		b.history = append(b.history, b.current)

		// Trim history if needed
		if len(b.history) > b.historyCap {
			b.history = b.history[len(b.history)-b.historyCap:]
		}
	}

	oldVersion := ""
	if b.current != nil {
		oldVersion = b.current.version
	}

	// Set new current
	b.current = &pipelineVersion[T]{
		schema:   schema,
		pipeline: pipeline,
		version:  version,
		builtAt:  time.Now(),
	}

	capitan.Emit(context.Background(), SchemaUpdated,
		KeyName.Field(b.identity.Name()),
		KeyOldVersion.Field(oldVersion),
		KeyNewVersion.Field(version))

	return nil
}

// Rollback reverts to the previous version in history.
// Returns ErrNoHistory if there is no previous version.
func (b *Binding[T]) Rollback() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.history) == 0 {
		return ErrNoHistory
	}

	// Pop from history
	prev := b.history[len(b.history)-1]
	b.history = b.history[:len(b.history)-1]

	oldVersion := b.current.version
	b.current = prev

	capitan.Emit(context.Background(), SchemaUpdated,
		KeyName.Field(b.identity.Name()),
		KeyOldVersion.Field(oldVersion),
		KeyNewVersion.Field(prev.version))

	return nil
}

// RollbackTo reverts to a specific version in history.
// Returns ErrVersionNotFound if the version is not in history.
func (b *Binding[T]) RollbackTo(version string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Find version in history
	idx := -1
	for i, v := range b.history {
		if v.version == version {
			idx = i
			break
		}
	}

	if idx == -1 {
		return ErrVersionNotFound
	}

	// Move current to history temporarily, then restore target
	oldVersion := b.current.version

	// Target becomes current, everything after target is discarded
	target := b.history[idx]
	b.history = b.history[:idx]
	b.current = target

	capitan.Emit(context.Background(), SchemaUpdated,
		KeyName.Field(b.identity.Name()),
		KeyOldVersion.Field(oldVersion),
		KeyNewVersion.Field(version))

	return nil
}

// Identity returns the binding's identity.
func (b *Binding[T]) Identity() pipz.Identity {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.identity
}

// Version returns the current pipeline version string.
func (b *Binding[T]) Version() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.current.version
}

// Schema returns the current schema definition.
func (b *Binding[T]) Schema() Schema {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.current.schema
}

// History returns metadata about all versions in history (oldest first).
func (b *Binding[T]) History() []VersionInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	info := make([]VersionInfo, len(b.history))
	for i, v := range b.history {
		info[i] = VersionInfo{
			Version: v.version,
			BuiltAt: v.builtAt,
		}
	}
	return info
}

// HistoryCap returns the maximum number of versions retained in history.
func (b *Binding[T]) HistoryCap() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.historyCap
}

// SetHistoryCap updates the history capacity.
// If the new capacity is smaller than current history size, oldest entries are trimmed.
func (b *Binding[T]) SetHistoryCap(capacity int) {
	if capacity < 1 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.historyCap = capacity
	if len(b.history) > capacity {
		b.history = b.history[len(b.history)-capacity:]
	}
}

// Pipeline returns the underlying pipz.Pipeline for advanced use cases.
// Most users should use Process() instead.
func (b *Binding[T]) Pipeline() *pipz.Pipeline[T] {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.current.pipeline
}
