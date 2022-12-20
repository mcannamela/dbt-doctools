#!/usr/bin/env bash

set -Eeuo pipefail

dbt clean
dbt deps
dbt seed
dbt run
dbt test
dbt docs generate