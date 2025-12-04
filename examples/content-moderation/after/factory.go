//go:build examples

// Content Moderation Factory - flume Implementation
// ==================================================
// Same processors, but now composed through schemas.
// Each tenant gets their own policy without code changes.

package main

import (
	"context"
	"time"

	"github.com/zoobzio/flume"
)

// ModerationFactory manages all tenant pipelines.
var ModerationFactory *flume.Factory[Content]

// InitFactory creates the factory and registers all components.
func InitFactory() {
	ModerationFactory = flume.New[Content]()

	// Register all processors (same as before)
	ModerationFactory.Add(AllProcessors()...)

	// Register predicates
	ModerationFactory.AddPredicate(
		flume.Predicate[Content]{
			Name:        PredicateHasMedia,
			Description: "Content contains media attachments",
			Predicate: func(_ context.Context, c Content) bool {
				return len(c.MediaURLs) > 0
			},
		},
	)

	// Register conditions for routing
	// Note: Each tenant can define their own thresholds in their schema!
	ModerationFactory.AddCondition(
		// Gaming platform: aggressive filtering
		flume.Condition[Content]{
			Name:        "risk-level-gaming",
			Description: "Risk assessment for gaming platform (aggressive)",
			Values:      []string{RiskHigh, RiskMedium, RiskLow},
			Condition: func(_ context.Context, c Content) string {
				// Gaming: low tolerance for spam
				if c.SpamScore >= 0.3 || c.ToxicityScore >= 0.5 {
					return RiskHigh
				}
				if c.SpamScore >= 0.2 || c.ToxicityScore >= 0.3 {
					return RiskMedium
				}
				return RiskLow
			},
		},
		// News site: moderate filtering
		flume.Condition[Content]{
			Name:        "risk-level-news",
			Description: "Risk assessment for news site (moderate)",
			Values:      []string{RiskHigh, RiskMedium, RiskLow},
			Condition: func(_ context.Context, c Content) string {
				// News: higher tolerance, focus on toxicity
				if c.ToxicityScore >= 0.7 {
					return RiskHigh
				}
				if c.SpamScore >= 0.8 || c.ToxicityScore >= 0.4 {
					return RiskMedium
				}
				return RiskLow
			},
		},
		// Adult platform: permissive filtering
		flume.Condition[Content]{
			Name:        "risk-level-adult",
			Description: "Risk assessment for adult platform (permissive)",
			Values:      []string{RiskHigh, RiskMedium, RiskLow},
			Condition: func(_ context.Context, c Content) string {
				// Adult: very high tolerance, only block obvious spam/illegal
				if c.SpamScore >= 0.95 {
					return RiskHigh
				}
				if c.SpamScore >= 0.9 || c.ToxicityScore >= 0.9 {
					return RiskMedium
				}
				return RiskLow
			},
		},
	)
}

// ============================================================================
// TENANT SCHEMAS
// ============================================================================
// Each tenant's moderation policy as a declarative schema.
// These could be loaded from files, a database, or a config service.

// GamingPlatformSchema - aggressive spam filtering.
var GamingPlatformSchema = flume.Schema{
	Version: "1.0.0",
	Node: flume.Node{
		Type: "sequence",
		Name: "gaming-moderation",
		Children: []flume.Node{
			{Ref: ProcessorValidate},
			{Ref: ProcessorSpamCheck},
			{Ref: ProcessorToxicityCheck},
			// Optional media scan
			{
				Type:      "filter",
				Predicate: PredicateHasMedia,
				Then:      &flume.Node{Ref: ProcessorMediaScan},
			},
			{Ref: ProcessorCategorize},
			// Aggressive routing
			{
				Type:      "switch",
				Condition: "risk-level-gaming",
				Routes: map[string]flume.Node{
					RiskHigh:   {Ref: ProcessorReject},
					RiskMedium: {Ref: ProcessorEscalate},
					RiskLow:    {Ref: ProcessorApprove},
				},
			},
			{Ref: ProcessorNotifyUser},
			{Ref: ProcessorAuditLog},
		},
	},
}

// NewsSiteSchema - moderate filtering, focus on toxicity.
var NewsSiteSchema = flume.Schema{
	Version: "1.0.0",
	Node: flume.Node{
		Type: "sequence",
		Name: "news-moderation",
		Children: []flume.Node{
			{Ref: ProcessorValidate},
			{Ref: ProcessorSpamCheck},
			{Ref: ProcessorToxicityCheck},
			{Ref: ProcessorCategorize},
			// More lenient routing - escalate rather than reject
			{
				Type:      "switch",
				Condition: "risk-level-news",
				Routes: map[string]flume.Node{
					RiskHigh:   {Ref: ProcessorEscalate},
					RiskMedium: {Ref: ProcessorEscalate}, // Same processor in multiple routes now works!
					RiskLow:    {Ref: ProcessorApprove},
				},
			},
			{Ref: ProcessorAuditLog},
		},
	},
}

// AdultPlatformSchema - permissive filtering.
var AdultPlatformSchema = flume.Schema{
	Version: "1.0.0",
	Node: flume.Node{
		Type: "sequence",
		Name: "adult-moderation",
		Children: []flume.Node{
			{Ref: ProcessorValidate},
			// Just spam check, minimal toxicity concern
			{Ref: ProcessorSpamCheck},
			// Very permissive routing
			{
				Type:      "switch",
				Condition: "risk-level-adult",
				Routes: map[string]flume.Node{
					RiskHigh:   {Ref: ProcessorReject},
					RiskMedium: {Ref: ProcessorApprove}, // Same processor in multiple routes now works!
					RiskLow:    {Ref: ProcessorApprove},
				},
			},
			{Ref: ProcessorAuditLog},
		},
	},
}

// LoadTenantSchemas registers all tenant schemas with the factory.
func LoadTenantSchemas() error {
	schemas := map[string]flume.Schema{
		TenantGaming: GamingPlatformSchema,
		TenantNews:   NewsSiteSchema,
		TenantAdult:  AdultPlatformSchema,
	}

	for name, schema := range schemas {
		if err := ModerationFactory.SetSchema(name, schema); err != nil {
			return err
		}
	}

	return nil
}

// ProcessContent moderates content using the tenant's pipeline.
func ProcessContent(ctx context.Context, content Content) (Content, error) {
	// Set defaults
	if content.Status == "" {
		content.Status = StatusPending
	}
	if content.CreatedAt.IsZero() {
		content.CreatedAt = time.Now()
	}
	if content.ProcessLog == nil {
		content.ProcessLog = make([]string, 0)
	}

	// Get the tenant's pipeline
	pipeline, ok := ModerationFactory.Pipeline(content.TenantID)
	if !ok {
		// Fall back to a default (could also return error)
		pipeline, ok = ModerationFactory.Pipeline(TenantNews)
		if !ok {
			panic("no default pipeline configured")
		}
	}

	return pipeline.Process(ctx, content)
}
