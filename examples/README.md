# flume Examples

These examples tell a migration story: you've built pipelines with `pipz`, and now you need more flexibility. Here's how `flume` extends what you've built.

## The Migration Story

Both examples follow the same pattern:

1. **Before**: A working `pipz` pipeline with hardcoded structure
2. **After**: The same processors, composed through `flume` schemas

The processors don't change. The services don't change. Only the **composition** becomes declarative.

## Examples

### 1. [Content Moderation](./content-moderation/)

**Story**: One pipeline became ten. Same code, different behaviour per customer.

A SaaS platform serving multiple tenants, each with different moderation policies:
- Gaming platform: aggressive spam filtering
- News site: focus on toxicity, escalate borderline content
- Adult platform: permissive, only block obvious spam

**Key concept**: Multiple schemas using the same processors with different conditions and routing.

### 2. [Data Pipeline](./data-pipeline/)

**Story**: Same data, three destinations, zero code duplication.

A data ingestion pipeline that fans out to multiple destinations:
- Storage, analytics, alerts, archive

Different data types need different routing:
- Critical data: storage + immediate alerts
- Dashboard data: analytics only
- Batch imports: archive only

**Key concept**: Channel integration for stream processing, schema-driven fanout.

## Running Examples

Each example uses build tags to avoid affecting code coverage:

```bash
# Content Moderation
cd content-moderation/before && go run -tags examples .
cd content-moderation/after && go run -tags examples .

# Data Pipeline
cd data-pipeline/before && go run -tags examples .
cd data-pipeline/after && go run -tags examples .
```

## What Changes, What Stays

| Component | Before (pipz) | After (flume) |
|-----------|---------------|---------------|
| Processors | `pipz.Apply(...)` | Same |
| Services | External integrations | Same |
| Composition | Hardcoded in Go | Schema-defined |
| Adding variants | Code change + deploy | Add schema |
| Policy updates | Code change + deploy | `SetSchema()` |
| Runtime changes | Requires restart | Hot-reload |

## Philosophy

These examples follow the same principles as the `pipz` examples:

- **Real Problems**: Address actual pain points developers experience
- **Progressive Enhancement**: Show how flume extends pipz, not replaces it
- **Same Primitives**: The processors you wrote still work
- **Stubbed Services**: Demonstrate behaviour without external dependencies
