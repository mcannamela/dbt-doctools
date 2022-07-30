#!/usr/bin/env bash

set -Eeuo pipefail

dbt deps
dbt seed
dbt run
dbt test
dbt docs generate