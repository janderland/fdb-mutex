#!/usr/bin/env bash
set -eo pipefail

# Change directory to the script's location (repo root).
cd "${0%/*}"

# Set environment variables for docker compose.
# Run docker_tag.sh from fenv directory so it uses fenv's git hash.
FENV_DOCKER_TAG="$(cd fenv && ./docker_tag.sh)"
export FENV_DOCKER_TAG

FENV_FDB_VER="${FENV_FDB_VER:-7.1.61}"
export FENV_FDB_VER

# Build images.
./fenv/build.sh --image
docker compose -f ./fenv/compose.yaml -f compose.yaml build

# Lint, build, & test.
docker compose -f ./fenv/compose.yaml -f compose.yaml run --rm build sh -c '
  set -ex
  shellcheck build.sh
  hadolint Dockerfile
  go build ./...
  golangci-lint run ./...
  go test ./... -timeout 5s
'
