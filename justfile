set shell := ["bash", "-euo", "pipefail", "-c"]

gotestsum := "go run gotest.tools/gotestsum@v1.13.0"
govulncheck := "go run golang.org/x/vuln/cmd/govulncheck@v1.9.0"
coverage_threshold := "80"
excluded := "/examples/"

default:
    @just --list

# Vet and lint with the govengers baseline (.golangci.yml, copied from govengers/standards)
lint:
    go vet ./...
    go vet -tags e2e ./test/e2e/...
    golangci-lint run ./...

# Unit tests with the race detector; fails below the coverage threshold (examples excluded)
test:
    mkdir -p sandbox
    {{gotestsum}} --format testname --jsonfile sandbox/unit.json -- -race -coverprofile=sandbox/unit.raw.cover ./...
    grep -v -E '{{excluded}}' sandbox/unit.raw.cover > sandbox/unit.cover
    go tool cover -func=sandbox/unit.cover > sandbox/unit.func
    awk -v threshold={{coverage_threshold}} '/^total:/ { found = 1; sub("%", "", $3); print "coverage " $3 "%"; if ($3 + 0 < threshold) { print "coverage is below " threshold "%"; exit 1 } } END { if (!found) { print "no coverage total"; exit 1 } }' sandbox/unit.func

# End-to-end tests against Redis and a Sentinel group in Docker; the stack is always removed
test-e2e:
    #!/usr/bin/env bash
    set -euo pipefail
    compose="docker compose -f test/e2e/compose.yml"
    trap 'status=$?; [ "$status" -eq 0 ] || $compose logs --no-color --tail=50 sentinel; $compose down --remove-orphans > /dev/null 2>&1' EXIT
    E2E_OWNER="$(id -u):$(id -g)" $compose run --rm tests

# Scan for known vulnerabilities in reachable code
vulncheck:
    {{govulncheck}} ./...

# Everything CI runs
check: lint test test-e2e vulncheck
