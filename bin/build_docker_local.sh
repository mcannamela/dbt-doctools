#!/usr/bin/env bash

set -Eeuo pipefail

cd "$(git rev-parse --show-toplevel)"

docker build -f docker/Dockerfile -t dbt-doctools:local .