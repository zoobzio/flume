---
title: Flume Documentation
description: Schema-driven pipeline factory for pipz with hot-reloading capabilities
author: Flume Team
published: 2025-12-03
tags: [Documentation, Overview, Getting Started]
---

# Flume Documentation

Welcome to the Flume documentation. Flume is a dynamic pipeline factory for [pipz](https://github.com/zoobzio/pipz) that enables schema-driven pipeline construction with hot-reloading capabilities.

## What is Flume?

Flume allows you to define pipelines using declarative YAML/JSON schemas instead of imperative code. Register reusable processors, predicates, and conditions, then compose them into complex pipelines through configuration rather than compilation.

```go
factory := flume.New[MyData]()
factory.Add(pipz.Apply("validate", validateFunc))

schema := `
type: sequence
children:
  - ref: validate
  - ref: process
`

pipeline, _ := factory.BuildFromYAML(schema)
result, _ := pipeline.Process(ctx, data)
```

## Documentation Sections

### [Learn](1.learn/1.introduction.md)

Understand Flume's core concepts and architecture:

- [Introduction](1.learn/1.introduction.md) - What Flume is and when to use it
- [Core Concepts](1.learn/2.core-concepts.md) - Factories, schemas, and components
- [Architecture](1.learn/3.architecture.md) - How Flume works under the hood

### [Tutorials](2.tutorials/1.quickstart.md)

Step-by-step guides to get you started:

- [Quickstart](2.tutorials/1.quickstart.md) - Your first Flume pipeline in 5 minutes
- [Building Pipelines](2.tutorials/2.building-pipelines.md) - From simple to complex schemas

### [Guides](3.guides/1.schema-design.md)

In-depth guides for specific features:

- [Schema Design](3.guides/1.schema-design.md) - Best practices for schema structure
- [Hot Reloading](3.guides/2.hot-reloading.md) - Update pipelines without restarts
- [Error Handling](3.guides/3.error-handling.md) - Retry, fallback, circuit breakers
- [Testing](3.guides/4.testing.md) - Testing schema-driven pipelines
- [Observability](3.guides/5.observability.md) - Monitoring with Capitan events

### [Cookbook](4.cookbook/1.recipes.md)

Practical examples and patterns:

- [Common Recipes](4.cookbook/1.recipes.md) - Ready-to-use pipeline patterns
- [Channel Integration](4.cookbook/2.channel-integration.md) - Stream processing patterns

### [Reference](5.reference/1.api.md)

Complete API documentation:

- [API Reference](5.reference/1.api.md) - Factory methods and types
- [Schema Format](5.reference/2.schema-format.md) - Complete YAML/JSON schema specification
- [Connector Types](5.reference/3.connector-types.md) - All available connectors
- [Events](5.reference/4.events.md) - Observability signals and keys

## Quick Links

- [GitHub Repository](https://github.com/zoobzio/flume)
- [Go Package Documentation](https://pkg.go.dev/github.com/zoobzio/flume)
- [pipz Documentation](https://github.com/zoobzio/pipz)
- [Capitan Documentation](https://github.com/zoobzio/capitan)

## Installation

```bash
go get github.com/zoobzio/flume
```

## Requirements

- Go 1.21+
- Your data type must implement `pipz.Cloner[T]`

```go
type MyData struct {
    Value string
}

func (d MyData) Clone() MyData {
    return MyData{Value: d.Value}
}
```
