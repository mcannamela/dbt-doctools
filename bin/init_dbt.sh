#!/usr/bin/env bash

set -Eeuo pipefail

cd "$(git rev-parse --show-toplevel)"
# an easy way to get dbt initialized from the local terminal
docker compose run -w /dbt-doctools/dummy_dbt_project/dummy dbt-doctools ../init_dbt.sh