#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_BIN="${OVERLORD_VENV_BIN:-/Users/matthewschwartz/.venvs/overlord/bin}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8080}"
DATA_DIR="${OVERLORD_DATA_DIR:-/Users/matthewschwartz/.local/share/overlord-dev}"
ALLOWED_ROOTS="${OVERLORD_ALLOWED_REPO_ROOTS:-/Users/matthewschwartz/projects}"
RELOAD="${OVERLORD_RELOAD:-0}"

ARGS=(
  overlord.app:create_app
  --factory
  --host "$HOST"
  --port "$PORT"
)

if [[ "$RELOAD" == "1" ]]; then
  ARGS+=(
    --reload
    --reload-dir "$ROOT/overlord"
    --reload-dir "$ROOT/tests"
  )
fi

cd "$ROOT"
export OVERLORD_DATA_DIR="$DATA_DIR"
export OVERLORD_ALLOWED_REPO_ROOTS="$ALLOWED_ROOTS"

exec "$VENV_BIN/uvicorn" "${ARGS[@]}"
