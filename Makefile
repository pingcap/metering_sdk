# TiDB Cloud Metering Go SDK Makefile

.PHONY: help build test clean fmt vet lint install-deps

PACKAGE_LIST  := go list ./...| grep -vE "test|docs|proto|examples"
PACKAGES  ?= $$($(PACKAGE_LIST))
GOLANGCI_LINT_VERSION ?= v2.4.0
TEST_DIR := /tmp/metering_sdk_test

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
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	golangci-lint run --fix -v $$($(PACKAGES)) --config .golangci.yml
	git diff --exit-code

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

# CI unit test
.PHONY: ci_ut
ci_ut: ## UT for CI, do not run locally, run `make ut` instead.
	go install github.com/axw/gocov/gocov@latest
	go install github.com/jstemmer/go-junit-report@latest
	go install github.com/AlekSi/gocov-xml@latest
	go install gotest.tools/gotestsum@latest
	@-(gotestsum --junitfile ./unit-tests.xml --packages=${PACKAGES})
	-go test -v ${PACKAGES} -coverprofile=cover.out
	gocov convert cover.out | gocov-xml > coverage.xml

# Unit test
.PHONY: ut
ut:
	mkdir -p "$(TEST_DIR)"
	go install gotest.tools/gotestsum@latest
	@-(gotestsum --junitfile "$(TEST_DIR)/unit-tests.xml" --packages=${PACKAGES})
	-go test -v ${PACKAGES} -coverprofile="$(TEST_DIR)/cover.out"
	go tool cover -html "$(TEST_DIR)/cover.out" -o "$(TEST_DIR)/cover.html"
	@echo "check ut results in $(TEST_DIR)/unit-tests.xml"
	@echo "check ut coverage by opening $(TEST_DIR)/cover.html using browser"

# same version as TiDB
.PHONY: mockgen
mockgen:
	GOBIN=$(shell pwd)/tools/bin go install go.uber.org/mock/mockgen@v0.5.2

.PHONY: gen_mock
gen_mock: mockgen
	tools/bin/mockgen -package metering github.com/pingcap/metering_sdk/writer MeteringWriter > writer/mock/writer_mock.go
