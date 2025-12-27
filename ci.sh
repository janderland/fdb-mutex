#!/usr/bin/env bash
set -eo pipefail

# Change directory to the script's location (repo root).
cd "${0%/*}"

# Lint, build, & test.
./fenv/fenv.sh --docker ./Dockerfile --build --exec sh -c '
  set -x
  shellcheck ci.sh
  hadolint Dockerfile
  go build ./...
  golangci-lint run ./...
  go test ./... -timeout 5s
'
