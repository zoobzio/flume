package plugz

import (
	"fmt"

	"github.com/zoobzio/pipz"
	"github.com/zoobzio/sctx"
)

type PlugzContract[T any, K comparable] struct {
	inner     *pipz.Contract[T] // For composition
	key       K
	signature string
}

// GetContract creates a secure contract - requires sctx context with proper permissions
func GetContract[T any, K comparable](key K, ctx sctx.Context) (*PlugzContract[T, K], error) {
	// Validate context and check permissions
	data, err := validateContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("invalid context: %w", err)
	}
	
	if !data.HasPermission("plugz:create-contract") {
		return nil, ErrInsufficientCapabilities
	}

	signature := generateSignature[T, K](key)

	return &PlugzContract[T, K]{
		inner:     pipz.NewContract[T](),
		key:       key,
		signature: signature,
	}, nil
}

// Register processors
func (c *PlugzContract[T, K]) Register(ctx sctx.Context, processors ...pipz.Processor[T]) error {
	// Validate context and check permissions
	data, err := validateContext(ctx)
	if err != nil {
		return fmt.Errorf("invalid context: %w", err)
	}
	
	cap := fmt.Sprintf("plugz:extend:%s", c.signature)
	if !data.HasPermission(cap) {
		return fmt.Errorf("%w: %s", ErrInsufficientCapabilities, cap)
	}

	// Register with inner pipz contract for composition
	c.inner.Register(processors...)

	// Convert to byte processors
	for _, proc := range processors {
		byteProc := func(input []byte) ([]byte, error) {
			// Decode
			var value T
			if err := Decode(input, &value); err != nil {
				return nil, fmt.Errorf("decode failed: %w", err)
			}

			// Process (TODO:SHOTEL - field-level encryption handled here)
			result, err := proc(value)
			if err != nil {
				return nil, err
			}

			// Encode
			return Encode(result)
		}

		if err := globalService.RegisterExternal(ctx, c.signature, byteProc); err != nil {
			return fmt.Errorf("failed to register processor: %w", err)
		}
	}

	return nil
}

// Process value through pipeline
func (c *PlugzContract[T, K]) Process(value T) (T, error) {
	// Encode input
	data, err := Encode(value)
	if err != nil {
		return value, fmt.Errorf("encode failed: %w", err)
	}

	// Process through byte pipeline
	result, err := globalService.Process(c.signature, data)
	if err != nil {
		return value, fmt.Errorf("process failed: %w", err)
	}

	// Decode output
	var output T
	if err := Decode(result, &output); err != nil {
		return value, fmt.Errorf("decode failed: %w", err)
	}

	return output, nil
}

// Runtime management methods
func (c *PlugzContract[T, K]) Clear(ctx sctx.Context) error {
	// Validate context and check permissions
	data, err := validateContext(ctx)
	if err != nil {
		return fmt.Errorf("invalid context: %w", err)
	}
	
	if !data.HasPermission(fmt.Sprintf("plugz:clear:%s", c.signature)) {
		return ErrInsufficientCapabilities
	}

	// Reset inner contract
	c.inner = pipz.NewContract[T]()

	// Clear byte processors
	return globalService.ClearProcessors(ctx, c.signature)
}

func (c *PlugzContract[T, K]) RemoveProcessor(ctx sctx.Context, index int) error {
	return globalService.RemoveProcessor(ctx, c.signature, index)
}

func (c *PlugzContract[T, K]) ProcessorCount(ctx sctx.Context) (int, error) {
	return globalService.ProcessorCount(ctx, c.signature)
}

func (c *PlugzContract[T, K]) HasProcessors() bool {
	return globalService.HasProcessors(c.signature)
}

// Composition support
func (c *PlugzContract[T, K]) Link() pipz.Chainable[T] {
	return c.inner.Link()
}