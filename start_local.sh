#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

args=("$@")

if [[ " ${args[*]} " != *" --date "* ]]; then
  args=(--date "${NTFY_DATE:-$(date -d '+2 days' +%F)}" "${args[@]}")
fi

if [[ " ${args[*]} " != *" --port "* ]]; then
  args=(--port "${NTFY_PORT:-5058}" "${args[@]}")
fi

if [[ -x "$SCRIPT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

exec "$PYTHON_BIN" meal_chooser_web.py "${args[@]}"
