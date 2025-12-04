//go:build examples

// Content Moderation Factory - After (flume)
// ==========================================
// Same processors, same logic - but now each tenant gets their
// own moderation policy without code changes.
//
// Run: go run -tags examples .

package main

import (
	"context"
	"fmt"
)

func main() {
	fmt.Println("=== Content Moderation Factory (flume) ===")
	fmt.Println("Same processors, different policies per tenant.")
	fmt.Println()

	// Initialize factory and load tenant schemas
	InitFactory()
	if err := LoadTenantSchemas(); err != nil {
		fmt.Printf("Failed to load schemas: %v\n", err)
		return
	}

	ctx := context.Background()

	// The same borderline content - processed differently per tenant
	baseContent := Content{
		UserID: "user-123",
		Type:   ContentTypeText,
		Body:   "Check out this sale! Click here for a discount. This product is terrible though.",
	}

	tenants := []struct {
		id          string
		name        string
		description string
	}{
		{TenantGaming, "Gaming Platform", "Aggressive filtering (spam > 0.3 = reject)"},
		{TenantNews, "News Site", "Moderate filtering (escalates borderline content)"},
		{TenantAdult, "Adult Platform", "Permissive filtering (only blocks obvious spam)"},
	}

	fmt.Printf("Test content: %q\n\n", truncate(baseContent.Body, 60))

	for _, tenant := range tenants {
		fmt.Printf("--- %s ---\n", tenant.name)
		fmt.Printf("Policy: %s\n", tenant.description)

		content := baseContent.Clone()
		content.ID = fmt.Sprintf("content-%s", tenant.id)
		content.TenantID = tenant.id

		result, err := ProcessContent(ctx, content)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("Result: %s (spam=%.2f, toxicity=%.2f)\n",
				result.Status, result.SpamScore, result.ToxicityScore)
			fmt.Printf("Process log: %v\n", result.ProcessLog)
		}
		fmt.Println()
	}

	// Show the key benefits
	fmt.Println("=== THE SOLUTION ===")
	fmt.Println()
	fmt.Println("Same content, three different outcomes based on tenant policy.")
	fmt.Println()
	fmt.Println("What changed from 'before':")
	fmt.Println("  - Processors are IDENTICAL (same code)")
	fmt.Println("  - Services are IDENTICAL (same integrations)")
	fmt.Println("  - Only the COMPOSITION changed (via schemas)")
	fmt.Println()
	fmt.Println("Benefits:")
	fmt.Println("  1. Add new tenant = add new schema (no code change)")
	fmt.Println("  2. Update policy = update schema (no redeploy)")
	fmt.Println("  3. Hot-reload = factory.SetSchema() (no restart)")
	fmt.Println("  4. Validation = catch errors before runtime")
	fmt.Println()

	// Demonstrate hot-reload capability
	fmt.Println("=== HOT-RELOAD DEMO ===")
	fmt.Println()
	fmt.Println("Gaming platform decides spam > 0.5 is fine now...")

	// Update the gaming platform's condition (simulating a policy change)
	// In production, this would come from a config file or API
	updatedSchema := GamingPlatformSchema
	updatedSchema.Version = "2.0.0"
	// The schema stays the same, but in real use you'd update the condition
	// or load a new schema from config

	if err := ModerationFactory.SetSchema(TenantGaming, updatedSchema); err != nil {
		fmt.Printf("Failed to update schema: %v\n", err)
		return
	}

	fmt.Printf("Schema updated to version %s - no restart required!\n", updatedSchema.Version)
	fmt.Println()
	fmt.Println("In production, you could:")
	fmt.Println("  - Watch config files for changes")
	fmt.Println("  - Expose an API to update schemas")
	fmt.Println("  - Pull schemas from a config service")
	fmt.Println("  - All without application restarts")
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
