.DEFAULT_GOAL := help

GO ?= go
PNPM ?= pnpm
GOLANGCI_LINT ?= golangci-lint
HELM ?= helm
TERRAFORM ?= terraform
TFLINT ?= tflint
GO_TEST_FLAGS ?=
GO_BUILD_FLAGS ?=

.PHONY: help bootstrap fmt fmt-check lint lint-go lint-ts typecheck vet test test-go test-ts build build-go build-ts ci dev e2e chaos bench demo helm-lint helm-install terraform-validate terraform-lint

help: ## Show the available targets.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

bootstrap: ## Download reproducibly locked Go and pnpm dependencies.
	$(GO) mod download
	$(PNPM) install --frozen-lockfile

fmt: ## Format Go and TypeScript workspace sources.
	$(GO) fmt ./...
	$(PNPM) --recursive --if-present run format

fmt-check: ## Check formatting without modifying files.
	$(GOLANGCI_LINT) fmt --diff
	$(PNPM) --recursive --if-present run format:check

lint: fmt-check lint-go vet lint-ts typecheck ## Run all static checks.

lint-go: ## Lint Go packages.
	$(GOLANGCI_LINT) run ./...

vet: ## Run Go's built-in analyzers.
	$(GO) vet ./...

lint-ts: ## Run the strict TypeScript static-analysis gate.
	$(PNPM) run typecheck

typecheck: ## Type-check every TypeScript workspace package.
	$(PNPM) --recursive --if-present run typecheck

test: test-go test-ts ## Run the Phase 0 unit-test suites.

test-go: ## Run Go tests; set GO_TEST_FLAGS=-race in CI.
	$(GO) test $(GO_TEST_FLAGS) ./...

test-ts: ## Run TypeScript tests across the pnpm workspace.
	$(PNPM) --recursive --if-present run test

build: build-go build-ts ## Build all Go and TypeScript packages.

build-go: ## Build all Go packages.
	$(GO) build $(GO_BUILD_FLAGS) ./...

build-ts: ## Build every TypeScript workspace package.
	$(PNPM) --recursive --if-present run build

ci: lint test build ## Run the complete local CI gate.

dev: ## Run the development proxy.
	$(GO) run ./cmd/streamweld-proxy

e2e: ## Run the kind-based end-to-end suite.
	$(GO) test -race ./test/e2e/...

chaos: ## Run the deterministic chaos suite.
	$(GO) test -race ./test/chaos/...

bench: ## Generate and verify committed benchmark reports using the CLI harness.
	$(GO) run ./cmd/streamweldctl bench
	test -s benchmarks/results.md
	test -s benchmarks/results.json

demo: ## Start the failure-injection demo.
	$(PNPM) --filter @streamweld/demo run dev

helm-lint: ## Lint the Streamweld Helm chart.
	$(HELM) lint deploy/helm/streamweld

helm-install: ## Install the chart into the active Kubernetes context.
	$(HELM) upgrade --install streamweld deploy/helm/streamweld --namespace streamweld-system --create-namespace --wait --timeout 5m

terraform-validate: ## Initialize without remote state and validate Terraform.
	$(TERRAFORM) -chdir=infra/terraform init -backend=false
	$(TERRAFORM) -chdir=infra/terraform validate

terraform-lint: ## Initialize and run TFLint recursively.
	$(TFLINT) --chdir=infra/terraform --init
	$(TFLINT) --chdir=infra/terraform --recursive
