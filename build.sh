#!/usr/bin/env bash
set -eo pipefail

# Change directory to the script's location (repo root).
cd "${0%/*}"

# Set image tag based on this repo's commit hash.
FDB_MUTEX_DOCKER_TAG="$(./fenv/docker_tag.sh)"
export FDB_MUTEX_DOCKER_TAG

# Build images.
./fenv/build.sh --bake ./bake.hcl --image

# Lint, build, & test.
./fenv/build.sh --compose ./compose.yaml --exec sh -c '
  set -ex
  shellcheck build.sh
  hadolint Dockerfile
  go build ./...
  golangci-lint run ./...
  go test ./... -timeout 5s
'
