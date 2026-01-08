# Testing

This directory contains test utilities and helpers for flume-based applications.

## Structure

```
testing/
├── helpers.go          # Test utilities, mocks, and assertion helpers
├── helpers_test.go     # Tests for the helpers themselves
├── benchmarks/         # Performance benchmarks
└── integration/        # End-to-end integration tests
```

## Test Utilities

### TestFactory

A wrapper around `flume.Factory` with call tracking:

```go
func TestMyPipeline(t *testing.T) {
    factory := testing.NewTestFactory[MyData](t)

    factory.AddProcessor("validate", func(ctx context.Context, d MyData) (MyData, error) {
        return d, nil
    })

    // Build and run pipeline...

    factory.AssertCalled("validate")
    factory.AssertCallCount("validate", 1)
}
```

### SchemaBuilder

Fluent API for constructing test schemas:

```go
schema := testing.NewSchemaBuilder().
    Sequence().
    Child("validate").
    Child("process").
    Build()
```

### MockProcessor

Configurable mock for testing pipeline behaviour:

```go
mock := testing.NewMockProcessor[MyData]("mock").
    WithReturn(expectedData, nil).
    WithDelay(100 * time.Millisecond)
```

## Running Tests

```bash
# Unit tests
make test

# Integration tests
make test-integration

# Benchmarks
make bench
```

## Writing Tests

1. Use `TestFactory` for tracking processor invocations
2. Use `SchemaBuilder` for readable schema construction
3. Use `MockProcessor` for controlling processor behaviour
4. Call `t.Helper()` in custom assertion functions
