# Flume

A dynamic pipeline factory for [pipz](https://github.com/zoobzio/pipz) that enables schema-driven pipeline construction with hot-reloading capabilities.

## Overview

Flume allows you to:
- Define pipelines using YAML/JSON schemas instead of code
- Register reusable processors, predicates, and conditions
- Dynamically update pipeline behavior without restarts
- Build any conceivable pipeline from registered building blocks

## Installation

```bash
go get github.com/zoobzio/flume
```

## Requirements

Your data type `T` must implement `pipz.Cloner[T]` to support parallel processing:

```go
type MyData struct {
    Value string
}

func (d MyData) Clone() MyData {
    return MyData{Value: d.Value}
}
```

## Basic Usage

```go
// Create a factory for your data type
factory := flume.New[MyData]()

// Add processors
factory.Add(
    pipz.Apply("validate", validateFunc),
    pipz.Transform("normalize", normalizeFunc),
    pipz.Effect("log", logFunc),
)

// Define pipeline structure in YAML
schema := `
version: "1.0.0"  # Optional version tracking
type: sequence
children:
  - ref: validate
  - ref: normalize
  - ref: log
`

// Build the pipeline
pipeline, err := factory.BuildFromYAML(schema)

// Use it
result, err := pipeline.Process(ctx, data)
```

## Schema Format

### Schema Version
All schemas support optional version tracking:
```yaml
version: "1.0.0"  # Semantic versioning recommended
ref: processor-name
```

### Simple Processor Reference
```yaml
ref: processor-name
```

### Sequence (Sequential Processing)
```yaml
type: sequence
children:
  - ref: step1
  - ref: step2
  - ref: step3
```

### Conditional Processing (Filter)
```yaml
type: filter
predicate: is-premium  # Reference to registered predicate
then:
  ref: premium-handler
```

### Multi-way Routing (Switch)
```yaml
type: switch
condition: get-status  # Reference to registered condition
routes:
  pending:
    ref: handle-pending
  approved:
    ref: handle-approved
default:
  ref: handle-unknown
```

### Error Handling
```yaml
# Retry
type: retry
config:
  attempts: 3
child:
  ref: flaky-operation

# Fallback
type: fallback
children:
  - ref: primary-handler
  - ref: backup-handler

# Timeout
type: timeout
config:
  duration: "30s"
child:
  ref: slow-operation
```

## Registration API

### Processors
Processors are the building blocks - any `pipz.Chainable[T]`:

```go
// Add processors - they name themselves
factory.Add(
    pipz.Apply("validate", validateOrder),
    pipz.Transform("normalize", normalizeOrder),
    pipz.Effect("notify", sendNotification),
)
```

### Predicates (for Filter)
Boolean functions for conditional processing:

```go
factory.AddPredicate(flume.Predicate[Order]{
    Name: "is-premium",
    Predicate: func(ctx context.Context, o Order) bool {
        return o.Customer.Tier == "premium"
    },
})
```

### Conditions (for Switch)
String-returning functions for multi-way routing:

```go
factory.AddCondition(flume.Condition[Order]{
    Name: "order-status",
    Condition: func(ctx context.Context, o Order) string {
        return o.Status // "pending", "approved", "rejected", etc.
    },
})
```

## Dynamic Schemas

Register schemas that can be updated at runtime:

```go
// Set a named schema (adds or updates)
factory.SetSchema("order-pipeline", schema)

// Use the pipeline - always gets current version
pipeline, ok := factory.Pipeline("order-pipeline")
if !ok {
    // Handle missing schema
}
result, err := pipeline.Process(ctx, order)

// Update the schema - running pipelines continue unaffected
factory.SetSchema("order-pipeline", newSchema)

// Next call uses the updated pipeline automatically
pipeline, _ = factory.Pipeline("order-pipeline")
```

## Complex Example

```go
factory := flume.New[Order]()

// Add processors
factory.Add(
    pipz.Apply("validate", validateOrder),
    pipz.Apply("check-inventory", checkInventory),
    pipz.Apply("charge-payment", chargePayment),
    pipz.Effect("send-confirmation", sendEmail),
    pipz.Apply("premium-discount", applyDiscount),
)

// Add predicates
factory.AddPredicate(
    flume.Predicate[Order]{Name: "is-premium", Predicate: isPremiumCustomer},
    flume.Predicate[Order]{Name: "high-value", Predicate: isHighValueOrder},
)

// Add conditions
factory.AddCondition(
    flume.Condition[Order]{Name: "payment-type", Condition: getPaymentType},
)

// Complex schema with nested logic
schema := `
type: sequence
name: order-processing
children:
  - ref: validate
  
  - type: filter
    predicate: is-premium
    then:
      ref: premium-discount
      
  - type: parallel  # Requires Cloner[T]
    children:
      - ref: check-inventory
      - type: filter
          predicate: high-value
          then:
            ref: fraud-check
            
  - type: switch
    condition: payment-type
    routes:
      credit:
        type: retry
        config: { attempts: 3 }
        child:
          ref: charge-payment
      paypal:
        ref: paypal-handler
    default:
      ref: manual-review
      
  - ref: send-confirmation
`

pipeline, err := factory.BuildFromYAML(schema)
```

## Schema Validation

Flume validates schemas before building, catching errors early with helpful messages:

```go
schema := `
type: sequence
children:
  - ref: missing-processor
  - type: filter
    predicate: missing-predicate
    then:
      ref: another-missing
`

err := factory.ValidateSchema(schema)
// Returns: 3 validation errors:
//   1. root.children[0]: processor 'missing-processor' not found
//   2. root.children[1]: predicate 'missing-predicate' not found  
//   3. root.children[1].then: processor 'another-missing' not found
```

Validation checks:
- All processor/predicate/condition references exist
- Required fields are present (e.g., filter needs predicate + then)
- Connector constraints (e.g., fallback needs exactly 2 children)
- Configuration values are valid (e.g., positive retry attempts)

## Supported Connectors

- **sequence**: Sequential processing
- **parallel/concurrent**: Parallel execution (requires `Cloner[T]`)
- **race**: First successful result (requires `Cloner[T]`)
- **fallback**: Try primary, fall back on error
- **retry**: Retry with configurable attempts
- **timeout**: Enforce time limits
- **filter**: Conditional execution based on predicates
- **switch**: Multi-way routing based on conditions

## Design Philosophy

- **Minimal API**: Registration (`Add*`, `Remove*`), Schema management (`SetSchema`, `GetSchema`, `RemoveSchema`), and Building (`Build`, `ValidateSchema`)
- **Type Safety**: Full type safety through Go generics
- **Zero Magic**: Processors name themselves, schemas are declarative
- **Dynamic**: Hot-reload schemas without restarts
- **Composable**: Build complex pipelines from simple, tested components

## License

Same as pipz - see LICENSE file.