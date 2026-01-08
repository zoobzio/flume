# Integration Tests

End-to-end tests that verify flume's behaviour with realistic scenarios.

## Test Files

- `complex_pipelines_test.go` - Tests for nested and multi-branch pipelines
- `hot_reload_test.go` - Tests for runtime schema updates and binding sync
- `schema_validation_test.go` - Tests for schema validation edge cases

## Running Integration Tests

```bash
make test-integration
```

Or directly:

```bash
go test -v -race -timeout=10m ./testing/integration/...
```

## Writing Integration Tests

Integration tests should:

1. Test realistic multi-component scenarios
2. Verify cross-cutting concerns (concurrency, hot-reload, channels)
3. Use longer timeouts for complex operations
4. Clean up resources (channels, goroutines) properly

Example structure:

```go
func TestComplexPipeline(t *testing.T) {
    // Setup factory with multiple components
    factory := flume.New[TestData]()
    // ... register processors, predicates, conditions

    // Build complex schema
    pipeline, err := factory.BuildFromYAML(`...`)
    require.NoError(t, err)

    // Execute with realistic data
    result, err := pipeline.Process(ctx, testData)

    // Verify end-to-end behaviour
    assert.Equal(t, expected, result)
}
```
