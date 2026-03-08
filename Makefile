.PHONY: help \
        build build-gateway build-server \
        install \
        test test-gateway test-server test-coverage \
        lint lint-gateway lint-server \
        fmt fmt-gateway fmt-server \
        vet \
        clean \
        run run-server run-gateway \
        docker-build docker-up docker-up-gateway docker-down \
        tidy download \
        check ci dev-tools install-hooks

# ── Variables ──────────────────────────────────────────────────────────────────

BINARY_NAME     = gateway
GATEWAY_MAIN    = ./gateway/cmd/gateway
BUILD_DIR       = ./build
COVERAGE_FILE   = coverage.txt
CONFIG_PATH     ?= config/config.yaml

# Go parameters
GOCMD     = go
GOBUILD   = $(GOCMD) build
GOTEST    = $(GOCMD) test
GOGET     = $(GOCMD) get
GOMOD     = $(GOCMD) mod
GOFMT     = $(GOCMD) fmt
GOVET     = $(GOCMD) vet
GOINSTALL = $(GOCMD) install

# Build flags
# CGO_ENABLED=0 on build/install: gateway is pure stdlib Go, produces a static binary.
# Tests use CGO_ENABLED=1 so the -race detector works.
LDFLAGS     = -ldflags "-s -w"
BUILD_FLAGS = -v $(LDFLAGS)

# Python parameters
PYTHON  = python3
PIP     = pip3
SERVER  = server/main.py

# Docker
COMPOSE = docker compose

# ── Default ────────────────────────────────────────────────────────────────────

.DEFAULT_GOAL := help

help: ## Display this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

# ── Build ──────────────────────────────────────────────────────────────────────

build: build-gateway ## Build all components
	@echo "Build complete"

build-gateway: ## Build the Go gateway binary
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 $(GOBUILD) $(BUILD_FLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(GATEWAY_MAIN)
	@echo "Binary built: $(BUILD_DIR)/$(BINARY_NAME)"

build-server: ## Validate Python server syntax (no compilation step needed)
	@echo "Checking Python server syntax..."
	$(PYTHON) -m py_compile $(SERVER)
	@echo "Python syntax OK"

install: ## Install the gateway binary to $$GOPATH/bin
	@echo "Installing $(BINARY_NAME)..."
	CGO_ENABLED=0 $(GOINSTALL) $(BUILD_FLAGS) $(GATEWAY_MAIN)
	@echo "$(BINARY_NAME) installed to $$(go env GOPATH)/bin/$(BINARY_NAME)"

# ── Test ───────────────────────────────────────────────────────────────────────

test: test-gateway test-server ## Run all tests (Go + Python)

test-gateway: ## Run Go gateway tests
	@echo "Running Go tests..."
	$(GOTEST) -v -race ./gateway/...

test-server: ## Run Python server tests
	@echo "Running Python tests..."
	$(PYTHON) -m pytest server/ -v; ret=$$?; [ $$ret -eq 5 ] && exit 0 || exit $$ret

test-coverage: test-coverage-gateway test-coverage-server ## Run coverage for all components

test-coverage-gateway: ## Run Go tests with coverage report
	@echo "Running Go tests with coverage..."
	$(GOTEST) -v -race -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./gateway/...
	@echo "Coverage report: $(COVERAGE_FILE)"
	@echo "View with: go tool cover -html=$(COVERAGE_FILE)"

test-coverage-server: ## Run Python tests with coverage report
	@echo "Running Python tests with coverage..."
	$(PYTHON) -m pytest server/ -v --cov=server --cov-report=term-missing --cov-report=xml:coverage-python.xml; \
	ret=$$?; [ $$ret -eq 5 ] && exit 0 || exit $$ret

test-coverage-html: test-coverage-gateway ## Open Go coverage as HTML report
	$(GOCMD) tool cover -html=$(COVERAGE_FILE)

# ── Lint / Format / Vet ────────────────────────────────────────────────────────

lint: lint-gateway lint-server ## Run all linters

lint-gateway: ## Run golangci-lint on the gateway
	@echo "Linting Go code..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not found. Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest" && exit 1)
	golangci-lint run ./gateway/...

lint-server: ## Run ruff on the Python server
	@echo "Linting Python code..."
	@which ruff > /dev/null || (echo "ruff not found. Install with: pip install ruff" && exit 1)
	ruff check server/

fmt: fmt-gateway fmt-server ## Format all code

fmt-gateway: ## Format Go code
	@echo "Formatting Go code..."
	$(GOFMT) ./...
	@which goimports > /dev/null && goimports -w . || echo "goimports not found (optional). Install with: go install golang.org/x/tools/cmd/goimports@latest"

fmt-server: ## Format Python code with ruff
	@echo "Formatting Python code..."
	@which ruff > /dev/null || (echo "ruff not found. Install with: pip install ruff" && exit 1)
	ruff format server/

vet: ## Run go vet
	@echo "Running go vet..."
	$(GOVET) ./gateway/...

# ── Run ────────────────────────────────────────────────────────────────────────

run: run-server ## Run the MCP server in stdio mode (Phase 1 / Claude Desktop)

run-server: ## Run the Python MCP server in stdio mode
	@echo "Starting MCP server (stdio)..."
	CONFIG_PATH=$(CONFIG_PATH) $(PYTHON) $(SERVER)

run-server-http: ## Run the Python MCP server in HTTP mode on port 8000
	@echo "Starting MCP server (HTTP, port 8000)..."
	CONFIG_PATH=$(CONFIG_PATH) $(PYTHON) $(SERVER) --transport http --port 8000

run-gateway: build-gateway ## Build and run the Go gateway (Phase 2)
	@echo "Starting gateway..."
	$(BUILD_DIR)/$(BINARY_NAME) --config $(CONFIG_PATH) --workers 4 --port 8080

# ── Docker ─────────────────────────────────────────────────────────────────────

docker-build: ## Build Docker images for all services
	@echo "Building Docker images..."
	$(COMPOSE) build

docker-up: ## Start Phase 1 stack (Python MCP server only)
	@echo "Starting MCP server stack..."
	$(COMPOSE) up mcp-server

docker-up-gateway: ## Start Phase 2 stack (MCP server + Go gateway)
	@echo "Starting full stack with gateway..."
	$(COMPOSE) --profile gateway up

docker-down: ## Stop and remove all containers
	@echo "Stopping containers..."
	$(COMPOSE) down

docker-logs: ## Tail logs for all running containers
	$(COMPOSE) logs -f

# ── Dependencies ───────────────────────────────────────────────────────────────

tidy: ## Tidy and verify go.mod
	@echo "Tidying go.mod..."
	$(GOMOD) tidy
	$(GOMOD) verify

download: ## Download Go dependencies
	@echo "Downloading Go dependencies..."
	$(GOMOD) download

pip-install: ## Install Python dependencies
	@echo "Installing Python dependencies..."
	$(PIP) install -r server/requirements.txt

pip-install-dev: ## Install Python dev dependencies (pytest, pytest-cov, ruff, etc.)
	@echo "Installing Python dev dependencies..."
	$(PIP) install -r server/requirements.txt pytest pytest-cov ruff

# ── Quality gates ──────────────────────────────────────────────────────────────

check: fmt vet lint test ## Run all checks (format, vet, lint, test)
	@echo "All checks passed!"

ci: tidy check test-coverage ## Run CI pipeline (tidy, check, coverage)
	@echo "CI checks complete!"

install-hooks: ## Install git hooks (runs make check before every commit)
	@echo "Installing git hooks..."
	git config core.hooksPath .githooks
	@echo "Git hooks installed. Pre-commit hook will run 'make check'."

dev-tools: install-hooks ## Install Go development tools and git hooks
	@echo "Installing Go development tools..."
	$(GOINSTALL) github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	$(GOINSTALL) golang.org/x/tools/cmd/goimports@latest
	@echo "Development tools installed"

# ── Cleanup ────────────────────────────────────────────────────────────────────

clean: ## Remove build artifacts and coverage files
	@echo "Cleaning..."
	rm -rf $(BUILD_DIR)
	rm -f $(COVERAGE_FILE) coverage.html
	find . -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name '*.pyc' -delete 2>/dev/null || true
	@echo "Clean complete"

# ── Composite ──────────────────────────────────────────────────────────────────

all: clean tidy check build ## Clean, check, and build everything
	@echo "All done!"
