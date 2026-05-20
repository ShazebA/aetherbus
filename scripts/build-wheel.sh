#!/usr/bin/env bash
# Build the SPA + the Python wheel in one shot.
#
# Run before tagging a release. The wheel that drops into ./dist/ will contain
# the built Replay UI under aetherbus/replay_ui/frontend/dist/, so end users
# `pip install aetherbus[replay]` + `aetherbus-ui serve` with no npm step.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Building Replay UI SPA"
(
  cd src/aetherbus/replay_ui/frontend
  if [ ! -d node_modules ]; then
    npm install
  fi
  npm run build
)

echo "==> Building Python wheel"
hatch build

echo ""
echo "Built artifacts:"
ls -1 dist/
