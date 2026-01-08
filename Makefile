.PHONY: test test-unit test-integration test-bench bench lint coverage clean all help lint-fix check ci install-tools install-hooks

# Default target
.DEFAULT_GOAL := help

# Display help - self-documenting via grep
help: ## Display available commands
	@echo "flume Development Commands"
	@echo "=========================="
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# Run all tests with race detector
test: ## Run all tests with race detector
	@echo "Running all tests..."
	@go test -v -race ./...

# Run unit tests only (short mode)
test-unit: ## Run unit tests only (short mode)
	@echo "Running unit tests..."
	@go test -v -race -short ./...

# Run integration tests
test-integration: ## Run integration tests
	@echo "Running integration tests..."
	@go test -v -race -timeout=10m ./testing/integration/...

# Run benchmark tests (as tests, not benchmarks)
test-bench: ## Run benchmark tests
	@echo "Running benchmark tests..."
	@go test -v -race -timeout=10m ./testing/benchmarks/...

# Run benchmarks
bench: ## Run benchmarks
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem -benchtime=100ms -timeout=15m .
	@echo ""
	@echo "=== Testing Benchmarks ==="
	@go test -bench=. -benchmem -benchtime=100ms -timeout=15m ./testing/benchmarks/...

# Run linters
lint: ## Run linters
	@echo "Running linters..."
	@golangci-lint run --config=.golangci.yml --timeout=5m

# Run linters with auto-fix
lint-fix: ## Run linters with auto-fix
	@echo "Running linters with auto-fix..."
	@golangci-lint run --config=.golangci.yml --fix

# Generate coverage report
coverage: ## Generate coverage report (HTML)
	@echo "Generating coverage report..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1
	@echo "Coverage report generated: coverage.html"

# Clean generated files
clean: ## Remove generated files
	@echo "Cleaning..."
	@rm -f coverage.out coverage.html
	@find . -name "*.test" -delete
	@find . -name "*.prof" -delete
	@find . -name "*.out" -delete

# Install development tools
install-tools: ## Install required development tools
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2

# Install git hooks
install-hooks: ## Install git hooks
	@echo "Installing git hooks..."
	@echo '#!/bin/sh' > .git/hooks/pre-commit
	@echo 'make check' >> .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed"

# Quick check - run tests and lint
check: test lint ## Quick validation (test + lint)
	@echo "All checks passed!"

# CI simulation - what CI runs locally
ci: clean lint test coverage bench ## Full CI simulation
	@echo "Full CI simulation complete!"

# Default target (kept for backwards compatibility)
all: test lint
