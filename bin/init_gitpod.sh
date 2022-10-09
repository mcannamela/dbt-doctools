#!/usr/bin/env bash

set -Eeuo pipefail

cd "$(git rev-parse --show-toplevel)"

apt-get update
apt-get install -y postgresql
curl --retry 5 -sSL https://install.python-poetry.org | python3 - --preview

pyenv install 3.9.14
pyenv local 3.9.14

poetry install