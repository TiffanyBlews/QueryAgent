#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 1) Load API credentials (LLM + optional crawlers)
if [[ -f "$ROOT_DIR/API_Key.md" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/API_Key.md"
else
  echo "[ERROR] API_Key.md not found at $ROOT_DIR" >&2
  exit 1
fi

# 2) Default runtime configuration (override via env)
PACKAGE_ROOT="${PACKAGE_ROOT:-$ROOT_DIR/packages/cn_ai_class}"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT_DIR/output/feasibility}"
MAX_WORKERS="${MAX_WORKERS:-8}"
LIMIT="${LIMIT:-}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

mkdir -p "$OUTPUT_DIR"

cmd=(
  python3 -m query_agent.feasibility_agent
  --package-root "$PACKAGE_ROOT"
  --output-dir "$OUTPUT_DIR"
  --max-workers "$MAX_WORKERS"
  --log-level "$LOG_LEVEL"
)

if [[ -n "$LIMIT" ]]; then
  cmd+=(--limit "$LIMIT")
fi

if [[ $# -gt 0 ]]; then
  cmd+=("$@")
fi

echo "[info] Step3 Feasibility Agent command: ${cmd[*]}"
"${cmd[@]}"
