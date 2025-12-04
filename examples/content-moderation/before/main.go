//go:build examples

// Content Moderation Pipeline - Before (pipz only)
// =================================================
// A single content moderation pipeline that works great...
// until you need different policies for different tenants.
//
// Run: go run -tags examples .

package main

import (
	"context"
	"fmt"
)

func main() {
	fmt.Println("=== Content Moderation Pipeline (pipz only) ===")
	fmt.Println("One pipeline, one policy, works great... for now.")
	fmt.Println()

	ctx := context.Background()

	// Test cases representing different content types
	testCases := []struct {
		name    string
		content Content
	}{
		{
			name: "Clean content",
			content: Content{
				ID:     "content-001",
				UserID: "user-123",
				Type:   ContentTypeText,
				Body:   "Just sharing my thoughts on the new game release!",
			},
		},
		{
			name: "Spam content",
			content: Content{
				ID:     "content-002",
				UserID: "user-456",
				Type:   ContentTypeText,
				Body:   "BUY NOW! Click here for FREE MONEY! Act fast, limited time offer!",
			},
		},
		{
			name: "Borderline content",
			content: Content{
				ID:     "content-003",
				UserID: "user-789",
				Type:   ContentTypeText,
				Body:   "This product is terrible, what a stupid design decision.",
			},
		},
		{
			name: "Content with media",
			content: Content{
				ID:        "content-004",
				UserID:    "user-101",
				Type:      ContentTypeImage,
				Body:      "Check out this cool photo!",
				MediaURLs: []string{"https://example.com/image.jpg"},
			},
		},
	}

	fmt.Println("Processing content through single pipeline...")
	fmt.Println()

	for _, tc := range testCases {
		fmt.Printf("--- %s ---\n", tc.name)
		fmt.Printf("Input: %q\n", truncate(tc.content.Body, 50))

		result, err := ProcessContent(ctx, tc.content)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("Result: %s (spam=%.2f, toxicity=%.2f)\n",
				result.Status, result.SpamScore, result.ToxicityScore)
		}
		fmt.Println()
	}

	// Show the limitation
	fmt.Println("=== THE PROBLEM ===")
	fmt.Println()
	fmt.Println("This pipeline works great for one platform.")
	fmt.Println("But what if you're a SaaS serving multiple tenants?")
	fmt.Println()
	fmt.Println("  Gaming Platform: 'Spam score > 0.3 should be rejected!'")
	fmt.Println("  News Site:       'We want spam score > 0.9 to reject, otherwise escalate'")
	fmt.Println("  Adult Platform:  'Different rules entirely...'")
	fmt.Println()
	fmt.Println("With pure pipz, you'd need to:")
	fmt.Println("  1. Create separate pipeline variables for each tenant")
	fmt.Println("  2. Hardcode a switch statement to pick the right one")
	fmt.Println("  3. Rebuild and redeploy to change ANY policy")
	fmt.Println()
	fmt.Println("See the 'after' example to see how flume solves this.")
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
