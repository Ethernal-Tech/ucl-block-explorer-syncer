.PHONY: check-git
check-git:
	@which git > /dev/null || (echo "git is not installed. Please install and try again."; exit 1)

.PHONY: check-go
check-go:
	@which go > /dev/null || (echo "Go is not installed.. Please install and try again."; exit 1)

.PHONY: check-lint
check-lint:
	@which golangci-lint > /dev/null || (echo "golangci-lint is not installed. Please install and try again."; exit 1)

.PHONY: build
build: check-go check-git
	$(eval COMMIT_HASH = $(shell git rev-parse HEAD))
	$(eval VERSION = $(shell git tag --points-at ${COMMIT_HASH}))
	$(eval BRANCH = $(shell git rev-parse --abbrev-ref HEAD | tr -d '\040\011\012\015\n'))
	$(eval TIME = $(shell date))
	go build -o ucl-block-explorer-syncer -ldflags="\
    	-X 'https://github.com/Ethernal-Tech/ucl-block-explorer-syncer/versioning.Version=$(VERSION)' \
		-X 'https://github.com/Ethernal-Tech/ucl-block-explorer-syncer/versioning.Commit=$(COMMIT_HASH)'\
		-X 'https://github.com/Ethernal-Tech/ucl-block-explorer-syncer/versioning.Branch=$(BRANCH)'\
		-X 'https://github.com/Ethernal-Tech/ucl-block-explorer-syncer/versioning.BuildTime=$(TIME)'" \
	main.go

.PHONY: lint
lint: check-lint
	golangci-lint run --config .golangci.yml

.PHONY: test
test: check-go
	go test -race -shuffle=on -coverprofile coverage.out -timeout 30m `go list ./... | grep -v e2e`

.PHONY: test-integration
test-integration: check-go
	go test ./e2e/ -v -run TestIntegration -timeout 30m

.PHONY: test-e2e
test-e2e: check-go
	go test ./e2e/ -v -run TestE2E -timeout 30m

.PHONY: benchmark-test
benchmark-test: check-go
	go test -bench=. -run=^$ `go list ./... | grep -v /e2e`

.PHONY: help
help:
	@echo "Available targets:"
	@printf "  %-35s - %s\n" "build" "Build the project"
	@printf "  %-35s - %s\n" "lint" "Run linters on the codebase"
	@printf "  %-35s - %s\n" "test" "Run unit tests"
	@printf "  %-35s - %s\n" "test-integration" "Run integration tests"
	@printf "  %-35s - %s\n" "test-e2e" "Run E2E tests"
	@printf "  %-35s - %s\n" "benchmark-test" "Run benchmark tests"
	@printf "  %-35s - %s\n" "check-git" "Check if git is installed"
	@printf "  %-35s - %s\n" "check-go" "Check if Go is installed"
	@printf "  %-35s - %s\n" "check-lint" "Check if golangci-lint is installed"
	@printf "  %-35s - %s\n" "help" "Show this help message"
	
	
