//go:build examples

// Content Moderation Factory - Types
// ===================================
// Shared types for the content moderation example.
// Note: Identical to the 'before' example - the types don't change,
// only how pipelines are constructed and managed.

package main

import (
	"time"
)

// Content represents user-generated content flowing through moderation.
type Content struct {
	ID        string
	UserID    string
	TenantID  string // Which platform/customer this content belongs to
	Type      ContentType
	Body      string
	MediaURLs []string

	// Moderation state (evolves through pipeline)
	Status        ModerationStatus
	SpamScore     float64
	ToxicityScore float64
	Categories    []string // detected content categories
	Flags         []string // reasons for flagging

	// Metadata
	CreatedAt   time.Time
	ModeratedAt time.Time
	ProcessLog  []string
}

// Clone implements pipz.Cloner for concurrent processing.
func (c Content) Clone() Content {
	categories := make([]string, len(c.Categories))
	copy(categories, c.Categories)

	flags := make([]string, len(c.Flags))
	copy(flags, c.Flags)

	mediaURLs := make([]string, len(c.MediaURLs))
	copy(mediaURLs, c.MediaURLs)

	processLog := make([]string, len(c.ProcessLog))
	copy(processLog, c.ProcessLog)

	return Content{
		ID:            c.ID,
		UserID:        c.UserID,
		TenantID:      c.TenantID,
		Type:          c.Type,
		Body:          c.Body,
		MediaURLs:     mediaURLs,
		Status:        c.Status,
		SpamScore:     c.SpamScore,
		ToxicityScore: c.ToxicityScore,
		Categories:    categories,
		Flags:         flags,
		CreatedAt:     c.CreatedAt,
		ModeratedAt:   c.ModeratedAt,
		ProcessLog:    processLog,
	}
}

// ContentType represents the type of content being moderated.
type ContentType string

const (
	ContentTypeText  ContentType = "text"
	ContentTypeImage ContentType = "image"
	ContentTypeVideo ContentType = "video"
)

// ModerationStatus represents the current moderation state.
type ModerationStatus string

const (
	StatusPending   ModerationStatus = "pending"
	StatusApproved  ModerationStatus = "approved"
	StatusRejected  ModerationStatus = "rejected"
	StatusEscalated ModerationStatus = "escalated"
)

// Processor and connector names.
const (
	ProcessorValidate      = "validate"
	ProcessorSpamCheck     = "spam-check"
	ProcessorToxicityCheck = "toxicity-check"
	ProcessorMediaScan     = "media-scan"
	ProcessorCategorize    = "categorize"
	ProcessorApprove       = "approve"
	ProcessorReject        = "reject"
	ProcessorEscalate      = "escalate"
	ProcessorNotifyUser    = "notify-user"
	ProcessorAuditLog      = "audit-log"

	PredicateHasMedia    = "has-media"
	PredicateIsHighRisk  = "is-high-risk"
	PredicateNeedsReview = "needs-review"

	ConditionRiskLevel = "risk-level"
)

// Risk level route names.
const (
	RiskLow    = "low"
	RiskMedium = "medium"
	RiskHigh   = "high"
)

// Tenant IDs for the example.
const (
	TenantGaming = "gaming-platform"
	TenantNews   = "news-site"
	TenantAdult  = "adult-platform"
)
