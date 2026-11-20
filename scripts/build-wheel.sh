#!/usr/bin/env bash
# Build the Replay UI SPA + the Python wheel in one shot — the release path.
#
# Run before tagging a release. The wheel that drops into ./dist/ bundles the
# built Replay UI under aetherbus/replay_ui/frontend/dist/, so end users
# `pip install aetherbus[replay]` + `aetherbus-ui serve` with no npm step.
#
# dist/ is gitignored and bundled via `artifacts` in pyproject.toml (not
# `force-include`, which hard-errors when dist/ is missing). A plain git/sdist
# install therefore builds fine *without* the SPA and falls back to the Jinja
# viewer — that path is covered by CI. This script is the opposite case: it
# must never produce a release wheel that silently lacks the SPA, so it builds
# the SPA first and verifies the finished wheel actually contains it.
set -euo pipefail

cd "$(dirname "$0")/.."

SPA_DIST="src/aetherbus/replay_ui/frontend/dist"

echo "==> Building Replay UI SPA"
(
  cd src/aetherbus/replay_ui/frontend
  if [ ! -d node_modules ]; then
    npm install
  fi
  npm run build
)

if [ ! -f "$SPA_DIST/index.html" ]; then
  echo "ERROR: SPA build did not produce $SPA_DIST/index.html." >&2
  echo "       Refusing to build a release wheel without the Replay UI." >&2
  exit 1
fi

echo "==> Building Python wheel (SPA bundled via [tool.hatch...wheel].artifacts)"
# --wheel builds the wheel directly from the source tree. A plain `build`
# (sdist -> wheel) would drop the gitignored dist/ in the sdist and ship a
# SPA-less wheel, so pin the wheel target explicitly.
rm -f dist/*.whl
python -m build --wheel

WHEEL="$(ls -t dist/*.whl | head -n1)"
MARKER="aetherbus/replay_ui/frontend/dist/index.html"
echo "==> Verifying $WHEEL bundles the SPA ($MARKER)"
if ! python -m zipfile -l "$WHEEL" | grep -q "$MARKER"; then
  echo "ERROR: $WHEEL does not contain $MARKER." >&2
  echo "       The release wheel is missing the Replay UI SPA." >&2
  exit 1
fi
echo "OK: wheel bundles the Replay UI SPA."

echo ""
echo "Built artifacts:"
ls -1 dist/
