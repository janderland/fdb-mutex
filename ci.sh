#!/usr/bin/env bash
set -eo pipefail

# Change directory to the script's location (repo root).
cd "${0%/*}"

# Lint, build, & test.
./fenv/fenv.sh --bake ./docker/bake.hcl --build --exec sh -c '
  set -ex
  shellcheck ci.sh
  hadolint docker/Dockerfile
  go build ./...
  golangci-lint run ./...
  go test ./... -timeout 5s
'
