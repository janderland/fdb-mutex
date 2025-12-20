#!/usr/bin/env bash
set -eo pipefail
cd "${0%/*}"
docker compose run --build --rm build
