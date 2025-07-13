package plugz

import (
	"crypto"
	"sync"

	"github.com/zoobzio/sctx"
)

// contextValidator wraps sctx validation functionality
type contextValidator struct {
	publicKey crypto.PublicKey
	mu        sync.RWMutex
	cache     map[sctx.Context]*sctx.ContextData
}

// global validator instance
var (
	validator     *contextValidator
	validatorOnce sync.Once
)

// SetPublicKey configures the public key for context validation.
// This must be called before using any secure contracts.
func SetPublicKey(publicKey crypto.PublicKey) {
	validatorOnce.Do(func() {
		validator = &contextValidator{
			publicKey: publicKey,
			cache:     make(map[sctx.Context]*sctx.ContextData),
		}
	})
	validator.mu.Lock()
	validator.publicKey = publicKey
	validator.cache = make(map[sctx.Context]*sctx.ContextData) // Clear cache on key change
	validator.mu.Unlock()
}

// validateContext verifies and caches context data
func validateContext(ctx sctx.Context) (*sctx.ContextData, error) {
	if validator == nil || validator.publicKey == nil {
		return nil, ErrNoPublicKey
	}

	validator.mu.RLock()
	if data, ok := validator.cache[ctx]; ok {
		validator.mu.RUnlock()
		return data, nil
	}
	validator.mu.RUnlock()

	// Verify the context
	data, err := sctx.VerifyContext(ctx, validator.publicKey)
	if err != nil {
		return nil, err
	}

	// Cache the verified data
	validator.mu.Lock()
	validator.cache[ctx] = data
	validator.mu.Unlock()

	return data, nil
}