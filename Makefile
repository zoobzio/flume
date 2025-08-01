.PHONY: test lint coverage clean all help test-examples bench lint-fix check ci install-tools

# Default target
all: test lint

# Display help
help:
	@echo "flume Development Commands"
	@echo "========================="
	@echo ""
	@echo "Testing & Quality:"
	@echo "  make test         - Run all tests with race detector"
	@echo "  make test-examples- Test and build all examples"
	@echo "  make bench        - Run benchmarks"
	@echo "  make lint         - Run linters"
	@echo "  make lint-fix     - Run linters with auto-fix"
	@echo "  make coverage     - Generate coverage report (HTML)"
	@echo "  make check        - Run tests and lint (quick check)"
	@echo ""
	@echo "Other:"
	@echo "  make install-tools- Install required development tools"
	@echo "  make clean        - Clean generated files"
	@echo "  make ci           - Run full CI pipeline locally"
	@echo "  make all          - Run tests and lint (default)"

# Run tests with race detector
test:
	@echo "Running tests..."
	@go test -v -race ./...

# Test and build all examples
test-examples:
	@echo "Testing examples..."
	@for dir in examples/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Building $$dir"; \
			(cd "$$dir" && go build -o demo . && echo "✓ Built successfully"); \
		fi \
	done

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem -benchtime=1s ./...

# Run linters
lint:
	@echo "Running linters..."
	@golangci-lint run --config=.golangci.yml --timeout=5m

# Run linters with auto-fix
lint-fix:
	@echo "Running linters with auto-fix..."
	@golangci-lint run --config=.golangci.yml --fix

# Generate coverage report
coverage:
	@echo "Generating coverage report..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1
	@echo "Coverage report generated: coverage.html"

# Clean generated files
clean:
	@echo "Cleaning..."
	@rm -f coverage.out coverage.html
	@find . -name "*.test" -delete
	@find . -name "*.prof" -delete
	@find . -name "*.out" -delete
	@find examples -name "demo" -type f -delete

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8

# Quick check - run tests and lint
check: test lint
	@echo "All checks passed!"

# CI simulation - what CI runs
ci: clean lint test coverage bench test-examples
	@echo "CI simulation complete!"