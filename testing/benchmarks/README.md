# Benchmarks

Performance tests for flume operations.

## Test Files

- `factory_performance_test.go` - Factory creation and registration benchmarks
- `builder_performance_test.go` - Schema building and pipeline construction
- `schema_performance_test.go` - Schema parsing and validation

## Running Benchmarks

```bash
make bench
```

Or directly:

```bash
go test -bench=. -benchmem -benchtime=100ms ./testing/benchmarks/...
```

## Benchmark Categories

### Factory Operations

- Component registration throughput
- Lookup performance at various registry sizes
- Concurrent access patterns

### Schema Building

- Simple vs complex schema build times
- Nested schema depth impact
- Validation overhead

### Pipeline Execution

- Processor chain throughput
- Concurrent execution overhead
- Memory allocation patterns

## Writing Benchmarks

```go
func BenchmarkFactoryBuild(b *testing.B) {
    factory := setupFactory()
    schema := loadSchema()

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = factory.Build(schema)
    }
}
```

Use `BenchmarkHelper` for common setup patterns:

```go
helper := testing.NewBenchmarkHelper[TestData]()
helper.RegisterNoOpProcessors(100)
schema := helper.GenerateSequenceSchema(10)
```
