package flume

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/pipz"
)

// Binding represents a live pipeline bound to a schema in the factory registry.
// It provides lock-free execution and optional auto-sync when the source schema changes.
//
// Bindings are created via Factory.Bind() and provide the primary execution interface.
// When auto-sync is enabled, the binding automatically rebuilds when its source schema
// is updated via Factory.SetSchema().
//
// Example:
//
//	// Register schema
//	factory.SetSchema("order-pipeline", schema)
//
//	// Create binding with auto-sync
//	orderPipelineID := factory.Identity("order-pipeline", "Main order processing")
//	binding, err := factory.Bind(orderPipelineID, "order-pipeline", flume.WithAutoSync())
//
//	// Execute the pipeline (lock-free)
//	result, err := binding.Process(ctx, order)
//
//	// Update schema - all auto-sync bindings rebuild automatically
//	factory.SetSchema("order-pipeline", newSchema)
type Binding[T pipz.Cloner[T]] struct {
	current  atomic.Pointer[pipz.Pipeline[T]]
	identity pipz.Identity
	factory  *Factory[T]
	schemaID string
	autoSync bool
}

// BindingOption configures a Binding.
type BindingOption[T pipz.Cloner[T]] func(*Binding[T])

// WithAutoSync enables automatic pipeline rebuilding when the source schema changes.
// When enabled, calls to Factory.SetSchema() will automatically rebuild this binding.
func WithAutoSync[T pipz.Cloner[T]]() BindingOption[T] {
	return func(b *Binding[T]) {
		b.autoSync = true
	}
}

// Process executes the current pipeline with the given data.
// This is a lock-free operation using atomic pointer access.
func (b *Binding[T]) Process(ctx context.Context, data T) (T, error) {
	return b.current.Load().Process(ctx, data)
}

// rebuild constructs a new pipeline from the schema and atomically swaps it in.
// Called internally by Factory.SetSchema() for auto-sync bindings.
func (b *Binding[T]) rebuild(schema Schema) error {
	start := time.Now()

	b.factory.mu.RLock()
	chainable, err := b.factory.buildNode(&schema.Node, "root")
	b.factory.mu.RUnlock()

	if err != nil {
		capitan.Emit(context.Background(), SchemaUpdateFailed,
			KeyName.Field(b.identity.Name()),
			KeySchema.Field(b.schemaID),
			KeyError.Field(err.Error()))
		return err
	}

	pipeline := pipz.NewPipeline(b.identity, chainable)
	b.current.Store(pipeline)

	capitan.Emit(context.Background(), SchemaUpdated,
		KeyName.Field(b.identity.Name()),
		KeySchema.Field(b.schemaID),
		KeyDuration.Field(time.Since(start)))

	return nil
}

// Identity returns the binding's identity.
func (b *Binding[T]) Identity() pipz.Identity {
	return b.identity
}

// SchemaID returns the ID of the schema this binding is bound to.
func (b *Binding[T]) SchemaID() string {
	return b.schemaID
}

// AutoSync returns whether this binding automatically rebuilds on schema changes.
func (b *Binding[T]) AutoSync() bool {
	return b.autoSync
}

// Pipeline returns the underlying pipz.Pipeline for advanced use cases.
// Most users should use Process() instead.
func (b *Binding[T]) Pipeline() *pipz.Pipeline[T] {
	return b.current.Load()
}
