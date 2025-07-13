package plugz

import (
	"fmt"
	"sync"

	"github.com/zoobzio/sctx"
)

type service struct {
	mu         sync.RWMutex
	processors map[string][]ByteProcessor
}

var globalService = &service{
	processors: make(map[string][]ByteProcessor),
}

// Check if processors exist
func (s *service) HasProcessors(signature string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.processors[signature]) > 0
}

// Register external processor
func (s *service) RegisterExternal(ctx sctx.Context, signature string, proc ByteProcessor) error {
	// Validate context and check permissions
	data, err := validateContext(ctx)
	if err != nil {
		return fmt.Errorf("invalid context: %w", err)
	}
	
	if !data.HasPermission(fmt.Sprintf("plugz:extend:%s", signature)) {
		return ErrInsufficientCapabilities
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.processors[signature] = append(s.processors[signature], proc)

	// TODO:CAPITAN - Emit registration event
	return nil
}

// Process through pipeline
func (s *service) Process(signature string, data []byte) ([]byte, error) {
	s.mu.RLock()
	processors := s.processors[signature]
	s.mu.RUnlock()

	if len(processors) == 0 {
		return nil, fmt.Errorf("%w for signature: %s", ErrNoProcessors, signature)
	}

	// TODO:SHOTEL - Each processor should run in isolated room
	result := data
	for i, proc := range processors {
		output, err := proc(result)
		if err != nil {
			return nil, fmt.Errorf("processor %d failed: %w", i, err)
		}
		result = output
	}

	return result, nil
}

// Runtime management - remove specific processor
func (s *service) RemoveProcessor(ctx sctx.Context, signature string, index int) error {
	// Validate context and check permissions
	data, err := validateContext(ctx)
	if err != nil {
		return fmt.Errorf("invalid context: %w", err)
	}
	
	if !data.HasPermission(fmt.Sprintf("plugz:modify:%s", signature)) {
		return ErrInsufficientCapabilities
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	processors := s.processors[signature]
	if index < 0 || index >= len(processors) {
		return fmt.Errorf("%w: %d (total: %d)", ErrInvalidProcessorIndex, index, len(processors))
	}

	// Remove processor at index
	s.processors[signature] = append(processors[:index], processors[index+1:]...)

	// TODO:CAPITAN - Emit removal event
	return nil
}

// Clear all processors
func (s *service) ClearProcessors(ctx sctx.Context, signature string) error {
	// Validate context and check permissions
	data, err := validateContext(ctx)
	if err != nil {
		return fmt.Errorf("invalid context: %w", err)
	}
	
	if !data.HasPermission(fmt.Sprintf("plugz:clear:%s", signature)) {
		return ErrInsufficientCapabilities
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.processors, signature)

	// TODO:CAPITAN - Emit clear event
	return nil
}

// Get processor count
func (s *service) ProcessorCount(ctx sctx.Context, signature string) (int, error) {
	// Validate context and check permissions
	data, err := validateContext(ctx)
	if err != nil {
		return 0, fmt.Errorf("invalid context: %w", err)
	}
	
	if !data.HasPermission(fmt.Sprintf("plugz:inspect:%s", signature)) {
		return 0, ErrInsufficientCapabilities
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.processors[signature]), nil
}