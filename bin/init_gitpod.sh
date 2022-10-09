#!/usr/bin/env bash

set -Eeuo pipefail

cd "$(git rev-parse --show-toplevel)"

sudo apt-get update
sudo apt-get install -y postgresql
curl --retry 5 -sSL https://install.python-poetry.org | python3 - --preview
poetry config installer.parallel false

pyenv install 3.9.14
pyenv local 3.9.14

poetry install