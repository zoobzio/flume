# Data Pipeline: From Hardcoded Fanout to Channel Streams

**The story**: You built a data pipeline that fans out to multiple destinations. It works great. Then you needed different fanout patterns for different data types.

## The Problem

Your data pipeline sends records to multiple destinations:

- **Storage**: Persist for later queries
- **Analytics**: Real-time dashboards
- **Alerts**: Notify on critical data
- **Archive**: Cold storage for compliance

With pure pipz, the fanout is hardcoded:

```go
pipz.NewConcurrent[Record](
    "fanout",
    nil,
    Store,      // Always runs
    Analytics,  // Always runs
    Alert,      // Always runs
    Archive,    // Always runs
)
```

What if:
- Dashboard data doesn't need storage?
- Batch imports should skip real-time systems?
- Critical alerts need different routing than normal data?

## Before: Hardcoded Fanout

Every record goes to every destination. The only filtering is done inside processors (e.g., `Alert` checks priority internally).

```bash
cd before
go run -tags examples .
```

## After: Schema-Driven Channel Fanout

Different pipelines for different use cases, all using the same processors:

```go
// Standard: storage + analytics + conditional alerts + archive
var StandardPipelineSchema = flume.Schema{
    Node: flume.Node{
        Type: "concurrent",
        Children: []flume.Node{
            {Stream: StreamStorage},
            {Stream: StreamAnalytics},
            {Type: "filter", Predicate: "is-high-priority",
                Then: &flume.Node{Stream: StreamAlerts}},
            {Stream: StreamArchive},
        },
    },
}

// Analytics only: skip storage for real-time dashboards
var AnalyticsOnlySchema = flume.Schema{
    Node: flume.Node{
        Type: "sequence",
        Children: []flume.Node{
            {Ref: ProcessorValidate},
            {Ref: ProcessorEnrich},
            {Stream: StreamAnalytics},
        },
    },
}
```

Consumers subscribe to channels they care about:

```go
// Storage consumer
go func() {
    for r := range StorageChannel {
        StorageService.Store(ctx, r)
    }
}()

// Analytics consumer
go func() {
    for r := range AnalyticsChannel {
        AnalyticsService.Track(ctx, r)
    }
}()
```

```bash
cd after
go run -tags examples .
```

## What Changed

| Aspect | Before | After |
|--------|--------|-------|
| Fanout | Hardcoded concurrent | Schema-defined streams |
| Destinations | Fixed in code | Channels consumers subscribe to |
| Different patterns | Requires code changes | Different schemas |
| Adding destination | Add processor + modify concurrent | Add channel + update schema |
| Conditional routing | Inside processor logic | Schema filter nodes |

## Key Insight

The **processors don't change**. The **services don't change**. What changes is:

1. **Fanout** moves from hardcoded concurrent to schema streams
2. **Consumers** become independent goroutines on channels
3. **Routing** becomes declarative through filter/switch nodes

Same data, multiple destinations, zero code duplication.

## Running the Examples

```bash
# Before: see hardcoded fanout
cd before && go run -tags examples .

# After: see schema-driven channel fanout
cd after && go run -tags examples .
```
