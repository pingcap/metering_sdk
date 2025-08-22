# TiDB Cloud Metering Go SDK Makefile

.PHONY: help build test clean fmt vet lint install-deps

# Default target
help:
	@echo "Available commands:"
	@echo "  build        - Build the SDK"
	@echo "  test         - Run tests"
	@echo "  fmt          - Format code"
	@echo "  vet          - Run go vet"
	@echo "  lint         - Run golangci-lint"
	@echo "  clean        - Clean build artifacts"
	@echo "  install-deps - Install development dependencies"

# Build
build:
	@echo "Building SDK..."
	go build ./...

# Run tests
test:
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Code check
vet:
	@echo "Running go vet..."
	go vet ./...

# Code linting
lint:
	@echo "Running golangci-lint..."
	golangci-lint run

# Clean
clean:
	@echo "Cleaning..."
	rm -f coverage.out coverage.html
	go clean -testcache

# Install development dependencies
install-deps:
	@echo "Installing development dependencies..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Run example
example:
	@echo "Running example..."
	go run examples/basic_usage.go

# Initialize module
init:
	@echo "Initializing go module..."
	go mod tidy
	go mod download

# Update dependencies
update:
	@echo "Updating dependencies..."
	go get -u ./...
	go mod tidy
