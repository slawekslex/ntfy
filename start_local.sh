#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

args=("$@")

if [[ " ${args[*]} " != *" --date "* ]]; then
  args=(--date "${NTFY_DATE:-$(date +%F)}" "${args[@]}")
fi

exec python3 meal_chooser_web.py "${args[@]}"
