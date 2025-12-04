//go:build examples

// Content Moderation Pipeline - pipz Implementation
// ==================================================
// A single hardcoded content moderation pipeline using pipz.
//
// This works well for one platform, but what happens when you need
// different moderation policies for different tenants?

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/zoobzio/pipz"
)

// ============================================================================
// PROCESSORS
// ============================================================================

// Validate ensures content has required fields.
var Validate = pipz.Apply(ProcessorValidate, func(_ context.Context, c Content) (Content, error) {
	if c.ID == "" {
		return c, errors.New("content ID required")
	}
	if c.Body == "" && len(c.MediaURLs) == 0 {
		return c, errors.New("content must have body or media")
	}
	c.ProcessLog = append(c.ProcessLog, "validated")
	return c, nil
})

// SpamCheck runs spam detection.
var SpamCheck = pipz.Apply(ProcessorSpamCheck, func(ctx context.Context, c Content) (Content, error) {
	c.SpamScore = SpamDetector.Analyze(ctx, c)
	c.ProcessLog = append(c.ProcessLog, fmt.Sprintf("spam-score: %.2f", c.SpamScore))
	return c, nil
})

// ToxicityCheck runs toxicity analysis.
var ToxicityCheck = pipz.Apply(ProcessorToxicityCheck, func(ctx context.Context, c Content) (Content, error) {
	c.ToxicityScore = ToxicityAnalyzer.Analyze(ctx, c)
	c.ProcessLog = append(c.ProcessLog, fmt.Sprintf("toxicity-score: %.2f", c.ToxicityScore))
	return c, nil
})

// MediaScan scans media content.
var MediaScan = pipz.Apply(ProcessorMediaScan, func(ctx context.Context, c Content) (Content, error) {
	if len(c.MediaURLs) == 0 {
		return c, nil
	}
	flags, err := MediaScanner.Scan(ctx, c.MediaURLs)
	if err != nil {
		return c, fmt.Errorf("media scan failed: %w", err)
	}
	c.Flags = append(c.Flags, flags...)
	c.ProcessLog = append(c.ProcessLog, fmt.Sprintf("media-scanned: %d flags", len(flags)))
	return c, nil
})

// Categorize determines content categories.
var Categorize = pipz.Apply(ProcessorCategorize, func(ctx context.Context, c Content) (Content, error) {
	c.Categories = Categorizer.Categorize(ctx, c)
	c.ProcessLog = append(c.ProcessLog, fmt.Sprintf("categorized: %v", c.Categories))
	return c, nil
})

// Approve marks content as approved.
var Approve = pipz.Apply(ProcessorApprove, func(_ context.Context, c Content) (Content, error) {
	c.Status = StatusApproved
	c.ModeratedAt = time.Now()
	c.ProcessLog = append(c.ProcessLog, "approved")
	return c, nil
})

// Reject marks content as rejected.
var Reject = pipz.Apply(ProcessorReject, func(_ context.Context, c Content) (Content, error) {
	c.Status = StatusRejected
	c.ModeratedAt = time.Now()
	c.ProcessLog = append(c.ProcessLog, "rejected")
	return c, nil
})

// Escalate marks content for human review.
var Escalate = pipz.Apply(ProcessorEscalate, func(_ context.Context, c Content) (Content, error) {
	c.Status = StatusEscalated
	c.ProcessLog = append(c.ProcessLog, "escalated-to-human-review")
	return c, nil
})

// NotifyUser sends notification to content creator.
var NotifyUser = pipz.Effect(ProcessorNotifyUser, func(ctx context.Context, c Content) error {
	return NotificationService.NotifyUser(ctx, c)
})

// AuditLog records moderation decision.
var AuditLog = pipz.Effect(ProcessorAuditLog, func(ctx context.Context, c Content) error {
	return AuditLogger.Log(ctx, c)
})

// ============================================================================
// PREDICATES
// ============================================================================

// HasMedia checks if content contains media.
func HasMedia(_ context.Context, c Content) bool {
	return len(c.MediaURLs) > 0
}

// IsHighRisk checks if content has high risk scores.
func IsHighRisk(_ context.Context, c Content) bool {
	return c.SpamScore >= 0.8 || c.ToxicityScore >= 0.8 || len(c.Flags) > 0
}

// NeedsReview checks if content needs human review.
func NeedsReview(_ context.Context, c Content) bool {
	return c.SpamScore >= 0.5 || c.ToxicityScore >= 0.5
}

// ============================================================================
// PIPELINE CONSTRUCTION
// ============================================================================
// THE PROBLEM: This is hardcoded for ONE moderation policy.
//
// What if:
// - Gaming platform wants aggressive spam filtering (spam > 0.3 = reject)
// - News site wants lighter moderation (spam > 0.9 = reject)
// - Adult platform has completely different rules
//
// With pure pipz, you'd need to:
// 1. Create separate pipeline variables for each tenant
// 2. Use a switch statement to pick the right pipeline
// 3. Rebuild and redeploy to change any policy

// RiskRouter routes content based on risk level.
var RiskRouter = pipz.NewSwitch[Content](
	ConditionRiskLevel,
	func(_ context.Context, c Content) string {
		if c.SpamScore >= 0.8 || c.ToxicityScore >= 0.8 {
			return RiskHigh
		}
		if c.SpamScore >= 0.5 || c.ToxicityScore >= 0.5 {
			return RiskMedium
		}
		return RiskLow
	},
).
	AddRoute(RiskHigh, Reject).
	AddRoute(RiskMedium, Escalate).
	AddRoute(RiskLow, Approve)

// MediaFilter conditionally runs media scanning.
var MediaFilter = pipz.NewFilter(
	"media-filter",
	HasMedia,
	MediaScan,
)

// ModerationPipeline is THE pipeline - one policy for everyone.
var ModerationPipeline = pipz.NewSequence[Content](
	"content-moderation",
	Validate,
	SpamCheck,
	ToxicityCheck,
	MediaFilter,
	Categorize,
	RiskRouter,
	NotifyUser,
	AuditLog,
)

// ProcessContent moderates a piece of content.
func ProcessContent(ctx context.Context, content Content) (Content, error) {
	if content.Status == "" {
		content.Status = StatusPending
	}
	if content.CreatedAt.IsZero() {
		content.CreatedAt = time.Now()
	}
	if content.ProcessLog == nil {
		content.ProcessLog = make([]string, 0)
	}

	return ModerationPipeline.Process(ctx, content)
}
