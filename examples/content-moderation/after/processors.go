//go:build examples

// Content Moderation Factory - Processors
// ========================================
// The same processors from 'before', now registered with flume.
// These are reusable building blocks that can be composed
// differently for each tenant through schemas.

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
// Same logic as before - these don't change.
// What changes is HOW they're composed into pipelines.

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

// AllProcessors returns all processors for factory registration.
func AllProcessors() []pipz.Chainable[Content] {
	return []pipz.Chainable[Content]{
		Validate,
		SpamCheck,
		ToxicityCheck,
		MediaScan,
		Categorize,
		Approve,
		Reject,
		Escalate,
		NotifyUser,
		AuditLog,
	}
}
