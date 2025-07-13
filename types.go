package plugz

import "errors"

// ByteProcessor is the core processing unit
type ByteProcessor func([]byte) ([]byte, error)

// Common errors
var (
	ErrInsufficientCapabilities = errors.New("insufficient capabilities")
	ErrInvalidProcessorIndex    = errors.New("invalid processor index")
	ErrNoProcessors             = errors.New("no processors registered")
	ErrNoPublicKey              = errors.New("no public key configured for context validation")
)