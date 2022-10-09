#!/usr/bin/env bash

set -Eeuo pipefail

cd "$(git rev-parse --show-toplevel)"

apt-get update
apt-get install -y postgresql
curl --retry 5 -sSL https://install.python-poetry.org | python3 - --preview

poetry config virtualenvs.create false
poetry config installer.parallel false