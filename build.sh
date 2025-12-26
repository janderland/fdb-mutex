#!/usr/bin/env bash
set -eo pipefail

# Change directory to the script's location (repo root).
cd "${0%/*}"

# Build images.
./fenv/fenv.sh --bake ./bake.hcl --image

# Lint, build, & test.
./fenv/fenv.sh --compose ./compose.yaml --exec sh -c '
  set -ex
  shellcheck build.sh
  hadolint Dockerfile
  go build ./...
  golangci-lint run ./...
  go test ./... -timeout 5s
'
