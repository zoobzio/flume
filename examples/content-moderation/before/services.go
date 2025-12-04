//go:build examples

// Content Moderation Pipeline - Mock Services
// ============================================
// Simulated external services for demonstration.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

func init() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
}

// SpamDetector simulates a spam detection service.
var SpamDetector = &spamDetector{}

type spamDetector struct{}

func (s *spamDetector) Analyze(_ context.Context, content Content) float64 {
	// Simulate spam detection based on content patterns
	score := 0.1

	body := strings.ToLower(content.Body)
	spamPatterns := []string{"buy now", "click here", "free money", "act fast", "limited time"}
	for _, pattern := range spamPatterns {
		if strings.Contains(body, pattern) {
			score += 0.2
		}
	}

	// New users posting links are suspicious
	if strings.Contains(body, "http") {
		score += 0.15
	}

	if score > 1.0 {
		score = 1.0
	}
	return score
}

// ToxicityAnalyzer simulates a toxicity detection service.
var ToxicityAnalyzer = &toxicityAnalyzer{}

type toxicityAnalyzer struct{}

func (t *toxicityAnalyzer) Analyze(_ context.Context, content Content) float64 {
	// Simulate toxicity detection
	score := 0.05

	body := strings.ToLower(content.Body)
	toxicPatterns := []string{"hate", "stupid", "idiot", "terrible"}
	for _, pattern := range toxicPatterns {
		if strings.Contains(body, pattern) {
			score += 0.25
		}
	}

	if score > 1.0 {
		score = 1.0
	}
	return score
}

// MediaScanner simulates media content scanning.
var MediaScanner = &mediaScanner{}

type mediaScanner struct{}

func (m *mediaScanner) Scan(_ context.Context, _ []string) ([]string, error) {
	// Simulate media scanning - randomly flag some content
	var flags []string
	if rand.Float64() < 0.1 {
		flags = append(flags, "explicit-content-detected")
	}
	return flags, nil
}

// Categorizer simulates content categorization.
var Categorizer = &categorizer{}

type categorizer struct{}

func (c *categorizer) Categorize(_ context.Context, content Content) []string {
	categories := []string{}

	body := strings.ToLower(content.Body)
	if strings.Contains(body, "game") || strings.Contains(body, "play") {
		categories = append(categories, "gaming")
	}
	if strings.Contains(body, "news") || strings.Contains(body, "breaking") {
		categories = append(categories, "news")
	}
	if strings.Contains(body, "sale") || strings.Contains(body, "discount") {
		categories = append(categories, "commerce")
	}
	if len(categories) == 0 {
		categories = append(categories, "general")
	}

	return categories
}

// NotificationService simulates user notifications.
var NotificationService = &notificationService{}

type notificationService struct{}

func (n *notificationService) NotifyUser(_ context.Context, content Content) error {
	fmt.Printf("  [Notification] User %s: content %s is %s\n",
		content.UserID, content.ID, content.Status)
	return nil
}

// AuditLogger simulates audit logging.
var AuditLogger = &auditLogger{}

type auditLogger struct{}

func (a *auditLogger) Log(_ context.Context, content Content) error {
	fmt.Printf("  [Audit] Content %s: status=%s, spam=%.2f, toxicity=%.2f, flags=%v\n",
		content.ID, content.Status, content.SpamScore, content.ToxicityScore, content.Flags)
	return nil
}
