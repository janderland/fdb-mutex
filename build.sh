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
docker compose build

# Lint shell scripts (exclude fenv submodule).
docker compose run --rm build \
  sh -c "find . -path ./fenv -prune -o -type f -iname '*.sh' -print0 | xargs -0 shellcheck"

# Build, lint, & test Go code.
docker compose run --rm build go build ./...
docker compose run --rm build golangci-lint run ./...
docker compose run --rm build go test ./... -timeout 5s
