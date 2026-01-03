# Flume

[![CI Status](https://github.com/zoobzio/flume/workflows/CI/badge.svg)](https://github.com/zoobzio/flume/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobzio/flume/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobzio/flume)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/flume)](https://goreportcard.com/report/github.com/zoobzio/flume)
[![CodeQL](https://github.com/zoobzio/flume/workflows/CodeQL/badge.svg)](https://github.com/zoobzio/flume/security/code-scanning)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/flume.svg)](https://pkg.go.dev/github.com/zoobzio/flume)
[![License](https://img.shields.io/github/license/zoobzio/flume)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/flume)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/flume)](https://github.com/zoobzio/flume/releases)

A dynamic pipeline factory for [pipz](https://github.com/zoobzio/pipz) that enables schema-driven pipeline construction with hot-reloading capabilities.

## Three Operations

```go
// 1. Register components
factory := flume.New[Order]()
factory.Add(
    pipz.Apply("validate", func(ctx context.Context, o Order) (Order, error) {
        if o.Total <= 0 {
            return o, fmt.Errorf("invalid total")
        }
        return o, nil
    }),
    pipz.Apply("apply-discount", func(ctx context.Context, o Order) (Order, error) {
        o.Total *= 0.9 // 10% discount
        return o, nil
    }),
)
factory.AddPredicate(flume.Predicate[Order]{
    Name:      "is-premium",
    Predicate: func(ctx context.Context, o Order) bool { return o.Tier == "premium" },
})

// 2. Build from schema
pipeline, _ := factory.BuildFromYAML(`
type: sequence
children:
  - ref: validate
  - type: filter
    predicate: is-premium
    then:
      ref: apply-discount
`)

// 3. Process data
result, err := pipeline.Process(ctx, order)
```

## Installation

Requires Go 1.23+

```bash
go get github.com/zoobzio/flume
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "github.com/zoobzio/flume"
    "github.com/zoobzio/pipz"
)

type Order struct {
    ID    string
    Total float64
}

func (o Order) Clone() Order { return Order{ID: o.ID, Total: o.Total} }

func main() {
    factory := flume.New[Order]()

    // Register processors
    factory.Add(
        pipz.Apply("validate", func(ctx context.Context, o Order) (Order, error) {
            if o.Total <= 0 {
                return o, fmt.Errorf("invalid total")
            }
            return o, nil
        }),
        pipz.Transform("enrich", func(ctx context.Context, o Order) Order {
            o.ID = "ORD-" + o.ID
            return o
        }),
    )

    // Build pipeline from YAML
    pipeline, err := factory.BuildFromYAML(`
type: sequence
children:
  - ref: validate
  - ref: enrich
`)
    if err != nil {
        panic(err)
    }

    // Process
    result, err := pipeline.Process(context.Background(), Order{ID: "123", Total: 99.99})
    fmt.Printf("Result: %+v, Error: %v\n", result, err)
}
```

## Hot Reloading

Update pipelines at runtime without restarts:

```go
// Register a named schema
factory.SetSchema("order-pipeline", schema)

// Create a binding with auto-sync enabled
binding, _ := factory.Bind(pipelineID, "order-pipeline", flume.WithAutoSync())

// Process requests (lock-free)
result, _ := binding.Process(ctx, order)

// Update schema - all auto-sync bindings rebuild automatically
factory.SetSchema("order-pipeline", newSchema)
```

## Why Flume?

- **Schema-driven**: Define pipelines in YAML/JSON, not code
- **Hot-reloadable**: Update pipeline behavior without restarts
- **Type-safe**: Full generics support with compile-time safety
- **Composable**: Build complex flows from simple, tested components
- **Observable**: Built-in [capitan](https://github.com/zoobzio/capitan) event emission
- **Validated**: Comprehensive schema validation with detailed error messages

## Documentation

Full documentation is available in the [docs/](docs/) directory:

### Learn
- [Quickstart](docs/2.learn/1.quickstart.md) — Your first pipeline in 5 minutes
- [Core Concepts](docs/2.learn/2.core-concepts.md) — Factories, schemas, and components
- [Architecture](docs/2.learn/3.architecture.md) — How Flume works under the hood
- [Building Pipelines](docs/2.learn/4.building-pipelines.md) — From simple to complex

### Guides
- [Schema Design](docs/3.guides/1.schema-design.md) — Best practices for schema structure
- [Hot Reloading](docs/3.guides/2.hot-reloading.md) — Runtime pipeline updates
- [Error Handling](docs/3.guides/3.error-handling.md) — Retry, fallback, circuit breakers
- [Testing](docs/3.guides/4.testing.md) — Testing strategies and CI/CD linting
- [Observability](docs/3.guides/5.observability.md) — Monitoring with capitan events

### Reference
- [API Reference](docs/4.reference/1.api.md) — Complete API documentation
- [Schema Format](docs/4.reference/2.schema-format.md) — YAML/JSON specification
- [Connector Types](docs/4.reference/3.connector-types.md) — All 14 connectors
- [Events](docs/4.reference/4.events.md) — Observability signals

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.

## License

MIT License - see [LICENSE](LICENSE) file.
