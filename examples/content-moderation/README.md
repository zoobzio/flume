# Content Moderation: From Pipeline to Factory

**The story**: You built a content moderation pipeline. It works great. Then you got more customers, each wanting different moderation policies.

## The Problem

You're a SaaS platform serving multiple tenants:

- **Gaming Platform**: "Spam score > 0.3 should be rejected. We have zero tolerance."
- **News Site**: "We want to escalate borderline content for human review, not auto-reject."
- **Adult Platform**: "Only block obvious spam. Everything else is fine."

With pure pipz, you'd need separate pipeline definitions and a switch statement to route to each one. Adding a new tenant means code changes. Updating a policy means redeploying.

## Before: One Pipeline, One Policy

```go
// THE PROBLEM: This is hardcoded for ONE moderation policy.
var RiskRouter = pipz.NewSwitch[Content](
    ConditionRiskLevel,
    func(_ context.Context, c Content) string {
        if c.SpamScore >= 0.8 || c.ToxicityScore >= 0.8 {
            return RiskHigh
        }
        // ... one set of thresholds for everyone
    },
)
```

```bash
cd before
go run -tags examples .
```

## After: Same Processors, Different Policies

```go
// Each tenant gets their own condition with different thresholds
ModerationFactory.AddCondition(
    flume.Condition[Content]{
        Name: "risk-level-gaming",
        Condition: func(_ context.Context, c Content) string {
            if c.SpamScore >= 0.3 { // Gaming: aggressive
                return RiskHigh
            }
            // ...
        },
    },
    flume.Condition[Content]{
        Name: "risk-level-news",
        Condition: func(_ context.Context, c Content) string {
            if c.ToxicityScore >= 0.7 { // News: focus on toxicity
                return RiskHigh
            }
            // ...
        },
    },
)

// Schemas define which condition each tenant uses
var GamingPlatformSchema = flume.Schema{
    Node: flume.Node{
        Type:      "switch",
        Condition: "risk-level-gaming",  // Uses aggressive thresholds
        Routes: map[string]flume.Node{
            RiskHigh: {Ref: ProcessorReject},
            // ...
        },
    },
}
```

```bash
cd after
go run -tags examples .
```

## What Changed

| Aspect | Before | After |
|--------|--------|-------|
| Processors | `pipz.Apply(...)` | Same `pipz.Apply(...)` |
| Services | Mock services | Same mock services |
| Composition | Hardcoded in Go | Defined in schemas |
| Adding tenant | Code change + deploy | Add schema |
| Policy update | Code change + deploy | `SetSchema()` (hot-reload) |

## Key Insight

The **processors don't change**. The **services don't change**. Only the **composition** changes - and that's now declarative through schemas.

## Running the Examples

```bash
# Before: see the single-policy limitation
cd before && go run -tags examples .

# After: see multi-tenant policies in action
cd after && go run -tags examples .
```
