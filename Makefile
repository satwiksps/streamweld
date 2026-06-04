.DEFAULT_GOAL := help

GO ?= go
PNPM ?= pnpm
NODE ?= node
DOCKER ?= docker
GORELEASER ?= goreleaser
GOLANGCI_LINT ?= golangci-lint
HELM ?= helm
HELM_INSTALL_ARGS ?=
TERRAFORM ?= terraform
TFLINT ?= tflint
GO_TEST_FLAGS ?=
GO_BUILD_FLAGS ?=
IMAGE_VERSION ?= dev
IMAGE_COMMIT ?= unknown
IMAGE_DATE ?= unknown
RELEASE_VERSION ?=

.PHONY: help bootstrap fmt fmt-check lint lint-go lint-ts typecheck vet test test-go test-ts build build-go build-ts ci dev e2e chaos chaos-kind bench bench-check demo docs-build images image-proxy image-operator helm-lint helm-install helm-package terraform-validate terraform-lint release-version goreleaser-check release-snapshot npm-pack release-check

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
	bash test/e2e/run-kind.sh

chaos: ## Run the deterministic chaos suite.
	$(GO) test $(GO_TEST_FLAGS) ./test/chaos/... -count=1

chaos-kind: ## Provision kind and execute every physical failure injection.
	bash test/chaos/run-kind.sh

bench: ## Generate and verify committed benchmark reports using the CLI harness.
	$(GO) run ./cmd/streamweldctl bench
	$(GO) run ./cmd/streamweldctl bench --verify

bench-check: ## Verify committed benchmark provenance and correctness without re-running timings.
	$(GO) run ./cmd/streamweldctl bench --verify

demo: ## Start the failure-injection demo.
	$(PNPM) --filter @streamweld/demo run dev

docs-build: ## Build the Astro Starlight documentation site.
	$(PNPM) --filter @streamweld/docs-site run build

images: image-proxy image-operator ## Build both production container images.

image-proxy: ## Build the production proxy container image.
	$(DOCKER) build --target proxy --tag streamweld-proxy:$(IMAGE_VERSION) --build-arg VERSION=$(IMAGE_VERSION) --build-arg COMMIT=$(IMAGE_COMMIT) --build-arg DATE=$(IMAGE_DATE) .

image-operator: ## Build the production operator container image.
	$(DOCKER) build --target operator --tag streamweld-operator:$(IMAGE_VERSION) --build-arg VERSION=$(IMAGE_VERSION) --build-arg COMMIT=$(IMAGE_COMMIT) --build-arg DATE=$(IMAGE_DATE) .

helm-lint: ## Lint the Streamweld Helm chart.
	$(HELM) lint deploy/helm/streamweld --strict --kube-version 1.32.0
	$(HELM) template streamweld deploy/helm/streamweld --namespace streamweld-system --kube-version 1.32.0 >/dev/null
	@if $(HELM) template unsafe-memory deploy/helm/streamweld --namespace streamweld-system --kube-version 1.32.0 --set proxy.replicaCount=2 >/dev/null 2>&1; then \
		echo "expected replicas>1 with journal.backend=memory to fail rendering" >&2; exit 1; \
	fi

helm-install: ## Install the chart into the active Kubernetes context.
	$(HELM) upgrade --install streamweld deploy/helm/streamweld --namespace streamweld-system --create-namespace --wait --timeout 5m $(HELM_INSTALL_ARGS)

helm-package: release-version ## Package the Helm chart using the synchronized release version.
	@mkdir -p dist/helm
	@version="$$( $(NODE) scripts/verify-release-version.mjs )"; \
		$(HELM) package deploy/helm/streamweld --destination dist/helm --version "$$version" --app-version "$$version"

terraform-validate: ## Format, initialize, validate, and test Terraform roots without remote state.
	$(TERRAFORM) -chdir=infra/terraform fmt -check -recursive
	$(TERRAFORM) -chdir=infra/terraform init -backend=false
	$(TERRAFORM) -chdir=infra/terraform validate
	$(TERRAFORM) -chdir=infra/terraform test
	$(TERRAFORM) -chdir=infra/terraform/examples/basic init -backend=false
	$(TERRAFORM) -chdir=infra/terraform/examples/basic validate

terraform-lint: ## Initialize and run TFLint recursively.
	$(TFLINT) --chdir=infra/terraform --init
	$(TFLINT) --chdir=infra/terraform --recursive

release-version: ## Verify that npm, Helm, and an optional RELEASE_VERSION tag agree.
	$(NODE) scripts/verify-release-version.mjs $(RELEASE_VERSION)

goreleaser-check: ## Validate the GoReleaser v2 configuration.
	$(GORELEASER) check

release-snapshot: release-version ## Build local release archives without publishing.
	$(GORELEASER) release --snapshot --clean

npm-pack: release-version build-ts ## Pack the public npm artifacts without publishing them.
	@mkdir -p dist/npm
	$(PNPM) --dir packages/client pack --pack-destination ../../dist/npm
	$(PNPM) --dir packages/ai-sdk pack --pack-destination ../../dist/npm

release-check: ci bench-check helm-lint terraform-validate terraform-lint docs-build goreleaser-check release-snapshot npm-pack ## Run every local release gate that does not require a cluster or registry.
